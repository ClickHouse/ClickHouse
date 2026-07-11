#include <Parsers/tests/gtest_common.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Aggregate, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | summarize t = stdev(Age) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSamp(Age)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = stdevif(Age, Age < 10) by FirstName",
            "SELECT\n    FirstName,\n    sqrt(varSampIf(Age, Age < 10)) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_and(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitAnd(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = binary_all_or(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitOr(Age) AS t\nFROM Customers\nGROUP BY FirstName"

        },
        {
            "Customers | summarize t = binary_all_xor(Age) by FirstName",
            "SELECT\n    FirstName,\n    groupBitXor(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize percentiles(Age, 30, 40, 50, 60, 70) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(30 / 100, 40 / 100, 50 / 100, 60 / 100, 70 / 100)(Age) AS percentiles_Age\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, 10, 20, 30, 50) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = percentiles_array(Age, dynamic([10, 20, 30, 50])) by FirstName",
            "SELECT\n    FirstName,\n    quantiles(10 / 100, 20 / 100, 30 / 100, 50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "DataTable | summarize t = percentilesw(Bucket, Frequency, 50, 75, 99.9)",
            "SELECT quantilesExactWeighted(50 / 100, 75 / 100, 99.9 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
            "DataTable| summarize t = percentilesw_array(Bucket, Frequency, dynamic([10, 50, 30]))",
            "SELECT quantilesExactWeighted(10 / 100, 50 / 100, 30 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
            "Customers | summarize t = percentile(Age, 50) by FirstName",
            "SELECT\n    FirstName,\n    quantile(50 / 100)(Age) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "DataTable | summarize t = percentilew(Bucket, Frequency, 50)",
            "SELECT quantileExactWeighted(50 / 100)(Bucket, Frequency) AS t\nFROM DataTable"
        },
        {
             "Customers | summarize t = make_list_with_nulls(Age) by FirstName",
             "SELECT\n    FirstName,\n    arrayConcat(groupArray(Age), arrayMap(x -> NULL, range(0, toUInt32(count(*) - length(groupArray(Age))), 1))) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize count() by bin(Age, 10)",
            "SELECT\n    toInt64(toFloat64(Age) / 10) * 10 AS Age,\n    count() AS count_\nFROM Customers\nGROUP BY Age"
        },
        {
            "Customers | summarize count(Age+1) by bin(Age+1, 10)",
            "SELECT\n    toInt64(toFloat64(Age + 1) / 10) * 10 AS Columns1,\n    count(Age + 1) AS count_\nFROM Customers\nGROUP BY Columns1"
        },
        {
            "Customers | summarize count(Age) by bin(Age, 10)",
            "SELECT\n    toInt64(toFloat64(Age) / 10) * 10 AS Age,\n    count(Age) AS count_Age\nFROM Customers\nGROUP BY Age"
        }
})));
