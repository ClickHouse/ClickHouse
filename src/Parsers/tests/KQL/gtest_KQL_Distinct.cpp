#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_Distinct, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | distinct *",
            "SELECT DISTINCT *\nFROM Customers"
        },
        {
            "Customers | distinct Occupation",
            "SELECT DISTINCT Occupation\nFROM Customers"
        },
        {
            "Customers | distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM Customers"
        },
        {
            "Customers |where Age <30| distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM Customers\nWHERE Age < 30"
        },
        {
            "Customers |where Age <30 | order by Age| distinct Occupation, Education",
            "SELECT DISTINCT\n    Occupation,\n    Education\nFROM Customers\nWHERE Age < 30\nORDER BY Age DESC"
        }
})));
