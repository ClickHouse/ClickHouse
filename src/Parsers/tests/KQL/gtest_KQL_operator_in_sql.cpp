#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/ParserSelectQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_operator_in_sql, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserSelectQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "select * from kql($$Customers | where FirstName !in ('Peter', 'Latoya')$$)",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE FirstName NOT IN ('Peter', 'Latoya')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !contains 'Pet'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%Pet%')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !contains_cs 'Pet'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName LIKE '%Pet%')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !endswith 'ter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%ter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !endswith_cs 'ter'$$);"
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT endsWith(FirstName, 'ter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName != 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE FirstName != 'Peter'\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !has 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT hasTokenCaseInsensitive(FirstName, 'Peter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !has_cs 'peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT hasToken(FirstName, 'peter')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hasprefix 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE 'Peter%')) AND (NOT (FirstName ILIKE '% Peter%'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hasprefix_cs 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT startsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '% Peter%'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hassuffix 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE '%Peter')) AND (NOT (FirstName ILIKE '%Peter %'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !hassuffix_cs 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE (NOT endsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '%Peter %'))\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !startswith 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT (FirstName ILIKE 'Peter%')\n)"
        },
        {
            "select * from kql($$Customers | where FirstName !startswith_cs 'Peter'$$);",
            "SELECT *\nFROM view(\n    SELECT *\n    FROM Customers\n    WHERE NOT startsWith(FirstName, 'Peter')\n)"
        }
})));
