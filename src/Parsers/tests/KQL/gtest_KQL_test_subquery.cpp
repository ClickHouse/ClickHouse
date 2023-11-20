#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_TestSubquery, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !in ('Peter', 'Latoya')));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE FirstName NOT IN ('Peter', 'Latoya')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName, Age | where Age !in (28, 29)));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE Age NOT IN (28, 29)\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains 'ste'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%ste%')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains_cs 'Ste'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT (FirstName LIKE '%Ste%')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !contains_cs 'ste'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT (FirstName LIKE '%ste%')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !endswith 'ore'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT (FirstName ILIKE '%ore')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !endswith_cs 'Ore'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT endsWith(FirstName, 'Ore')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName != 'Theodore'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE FirstName != 'Theodore'\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !~ 'theodore'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE lower(FirstName) != lower('theodore')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !has 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT hasTokenCaseInsensitive(FirstName, 'Peter')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !has_cs 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT hasToken(FirstName, 'Peter')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hasprefix 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE 'Peter%')) AND (NOT (FirstName ILIKE '% Peter%'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hasprefix_cs 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE (NOT startsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '% Peter%'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hassuffix 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE (NOT (FirstName ILIKE '%Peter')) AND (NOT (FirstName ILIKE '%Peter %'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !hassuffix_cs 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE (NOT endsWith(FirstName, 'Peter')) AND (NOT (FirstName LIKE '%Peter %'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !startswith 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT (FirstName ILIKE 'Peter%')\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName  | where FirstName !startswith_cs 'Peter'));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE NOT startsWith(FirstName, 'Peter')\n)"
        },
        {
            "Customers | where FirstName !in~ ((Customers | project FirstName  | where FirstName !in~ ('peter', 'apple')));",
            "SELECT *\nFROM Customers\nWHERE lower(FirstName) NOT IN (\n    SELECT lower(FirstName)\n    FROM Customers\n    WHERE lower(FirstName) NOT IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName | where FirstName in~ ('peter', 'apple')));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE lower(FirstName) IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "Customers | where substring(FirstName,0,3) in~ ((Customers | project substring(FirstName,0,3) | where FirstName in~ ('peter', 'apple')));",
            "SELECT *\nFROM Customers\nWHERE lower(if(toInt64(length(FirstName)) <= 0, '', substr(FirstName, (((0 % toInt64(length(FirstName))) + toInt64(length(FirstName))) % toInt64(length(FirstName))) + 1, 3))) IN (\n    SELECT lower(if(toInt64(length(FirstName)) <= 0, '', substr(FirstName, (((0 % toInt64(length(FirstName))) + toInt64(length(FirstName))) % toInt64(length(FirstName))) + 1, 3)))\n    FROM Customers\n    WHERE lower(FirstName) IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "Customers | where FirstName in~ ((Customers |  where FirstName !in~ ('peter', 'apple')| project FirstName));",
            "SELECT *\nFROM Customers\nWHERE lower(FirstName) IN (\n    SELECT lower(FirstName)\n    FROM Customers\n    WHERE lower(FirstName) NOT IN (lower('peter'), lower('apple'))\n)"
        },
        {
            "Customers | where FirstName in ((Customers | project FirstName, LastName, Age));",
            "SELECT *\nFROM Customers\nWHERE FirstName IN (\n    SELECT FirstName\n    FROM Customers\n)"
        },
        {
            "Customers | where FirstName in~ ((Customers | project FirstName, LastName, Age|where Age <30));",
            "SELECT *\nFROM Customers\nWHERE lower(FirstName) IN (\n    SELECT lower(FirstName)\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers | where FirstName !in ((Customers | project FirstName, LastName, Age |where Age <30 ));",
            "SELECT *\nFROM Customers\nWHERE FirstName NOT IN (\n    SELECT FirstName\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers | where FirstName !in~ ((Customers | project FirstName, LastName, Age |where Age <30));",
            "SELECT *\nFROM Customers\nWHERE lower(FirstName) NOT IN (\n    SELECT lower(FirstName)\n    FROM Customers\n    WHERE Age < 30\n)"
        }
})));
