import quandl
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta


pd.options.mode.chained_assignment = None


def main():
    # set API
    quandl.ApiConfig.api_key = "ZNk1zPJuz7sGTV2xrnqe"

    lists = (pd.read_csv('E:/Python/data_preprocessing/SP500.csv', usecols=['free_code'], ).values.flatten().tolist())

    # define function tp create empty aggregate monthly data frame with 33 columns\
    def create_empty_df():
        df = pd.DataFrame(columns=range(1, 33, 1))
        df['Next_Is_Jannuary'] = []
        return (df)


    # create empty df
    aggregate_monthly_df = create_empty_df()
    aggregate_annually_df = create_empty_df()
    full_list_df = create_empty_df()

    # for one stock during a specified amount of years
    for list in lists:
        full_month = quandl.get(list,
                                  trim_start="1970-01-01",
                                  trim_end="2016-01-01",
                                  collapse='monthly',
                                  returns='dataframe')

        full_day   = quandl.get(list,
                                trim_start="1970-01-01",
                                trim_end="2016-01-01",
                                collapse='daily',
                                returns='dataframe')
        for year in range(1980, 2016, 1):
            for month in range(1, 13, 1):
                day = 1

                stock_name = list.split(sep="/")[1]

                first_day = (datetime.date(year, month, day).isoformat())

                last_day = (datetime.date(year, month, day) + relativedelta(months=1) - relativedelta(days=1)).isoformat()

                month_minus_2 = (datetime.date(year, month, day) - relativedelta(months=1)).isoformat()

                month_minus_13 = (datetime.date(year, month, day) - relativedelta(months=13)).isoformat()

                next_month = (datetime.date(year, month, day) + relativedelta(months=1)).month

                # monthly return month - 2 to month - 13
                data_monthly = full_month.loc[month_minus_13:month_minus_2]

                # cummulative monthly returns
                cum_return_monthly = ((data_monthly.ix[:, 3] - data_monthly.ix[:, 0]) / data_monthly.ix[:, 0]).cumsum()

                # calculate Z score of cummulative monthly returns
                monthly_z_score = pd.DataFrame((cum_return_monthly - cum_return_monthly.mean()) / cum_return_monthly.std(ddof=0),
                                               columns=['Z_Score'])

                # 20 daily returns for the same month
                data_daily = full_day.loc[first_day:last_day][:20]

                # cummulative daily returns
                cum_return_daily = ((data_daily.ix[:, 3] - data_daily.ix[:, 0]) / data_daily.ix[:, 0]).cumsum()

                # calculate Z score of cummulative daily returns
                daily_z_score = pd.DataFrame((cum_return_daily - cum_return_daily.mean()) / cum_return_daily.std(ddof=0),
                                             columns=['Z_Score'])

                #  transpose and join daily and monthly
                month_data_t = pd.concat([monthly_z_score.transpose(), daily_z_score.transpose()], axis=1)

                # change indext name from Z_Score to the chosen day, column name to range 1:32
                month_data_t = month_data_t.rename(index={'Z_Score': first_day})

                month_data_t.columns = range(1, len(month_data_t.columns) + 1, 1)

                # aggregate all monthly data into one data frame

                aggregate_monthly_df = aggregate_monthly_df.append(month_data_t)

                print("data for month", month, "in year", year, "of", stock_name, "is collected", sep=" ")

            # create another column to see if the next month is jannuay or not, based on the date index
            for indexes in range(0, len(aggregate_monthly_df), 1):
                if datetime.datetime.strptime(aggregate_monthly_df.index[indexes], "%Y-%m-%d").month == 12:
                    aggregate_monthly_df['Next_Is_Jannuary'][indexes] = 1
                else:
                    aggregate_monthly_df['Next_Is_Jannuary'][indexes] = 0

            aggregate_annually_df = aggregate_annually_df.append(aggregate_monthly_df)

            print("data for year", year, "of", stock_name, "is collected", sep=" ")

            # create empty df
            aggregate_monthly_df = create_empty_df()

        # print(aggregate_annually_df)

        # full_list_df = full_list_df.append(aggregate_annually_df)
        aggregate_annually_df.to_csv("E:/Python/data_preprocessing/processed_data/" + stock_name + ".csv", na_rep="NA")

        # create empty df
        aggregate_annually_df = create_empty_df()

        print("file written to csv for " + stock_name + " data", sep=" ")




try:
    main()
except Exception:
    pass







