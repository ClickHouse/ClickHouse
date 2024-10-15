#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAttachAccessEntity.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Common/re2.h>
#include <string_view>
#include <gtest/gtest.h>
#include "gtest_common.h"
#include <boost/algorithm/string/replace.hpp>


namespace
{
using namespace DB;
using namespace std::literals;
}

std::ostream & operator<<(std::ostream & ostr, const std::shared_ptr<IParser> parser)
{
    return ostr << "Parser: " << parser->getName();
}

std::ostream & operator<<(std::ostream & ostr, const ParserTestCase & test_case)
{
    // New line characters are removed because at the time of writing this the unit test results are parsed from the
    // command line output, and multi-line string representations are breaking the parsing logic.
    std::string input_text{test_case.input_text};
    boost::replace_all(input_text, "\n", "\\n");
    return ostr << "ParserTestCase input: " << input_text;
}

TEST_P(ParserTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_NE(nullptr, parser);

    if (expected_ast)
    {
        if (std::string(expected_ast).starts_with("throws"))
        {
            EXPECT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0), DB::Exception);
        }
        else
        {
            ASTPtr ast;
            ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0));
            if (std::string("CREATE USER or ALTER USER query") != parser->getName()
                    && std::string("ATTACH access entity query") != parser->getName())
            {
                ASTPtr ast_clone = ast->clone();
                {
                    WriteBufferFromOwnString buf;
                    formatAST(*ast_clone, buf, false, false);
                    String formatted_ast = buf.str();
                    EXPECT_EQ(expected_ast, formatted_ast);
                }


                ASTPtr ast_clone2 = ast_clone->clone();
                /// Break `ast_clone2`, it should not affect `ast_clone` if `clone()` implemented properly
                for (auto & child : ast_clone2->children)
                {
                    if (auto * identifier = dynamic_cast<ASTIdentifier *>(child.get()))
                        identifier->setShortName("new_name");
                }

                {
                    WriteBufferFromOwnString buf;
                    formatAST(*ast_clone, buf, false, false);
                    String formatted_ast = buf.str();
                    EXPECT_EQ(expected_ast, formatted_ast);
                }
            }
            else
            {
                if (input_text.starts_with("ATTACH"))
                {
                    auto salt = (dynamic_cast<const ASTCreateUserQuery *>(ast.get())->authentication_methods.back())->getSalt().value_or("");
                    EXPECT_TRUE(re2::RE2::FullMatch(salt, expected_ast));
                }
                else
                {
                    WriteBufferFromOwnString buf;
                    formatAST(*ast->clone(), buf, false, false);
                    String formatted_ast = buf.str();
                    EXPECT_TRUE(re2::RE2::FullMatch(formatted_ast, expected_ast));
                }
            }
        }
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0, 0), DB::Exception);
    }
}

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserOptimizeQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY *"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery_FAIL, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>(false)),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * APPLY(x)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * REPLACE(y)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY db.a, db.b, db.c",
            }
        }
)));


INSTANTIATE_TEST_SUITE_P(ParserAlterCommand_MODIFY_COMMENT, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>(false)),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                // Empty comment value
                "MODIFY COMMENT ''",
                "MODIFY COMMENT ''",
            },
            {
                // Non-empty comment value
                "MODIFY COMMENT 'some comment value'",
                "MODIFY COMMENT 'some comment value'",
            }
        }
)));


INSTANTIATE_TEST_SUITE_P(ParserCreateQuery_DICTIONARY_WITH_COMMENT, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            R"sql(CREATE DICTIONARY 2024_dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'Test dictionary with comment';
)sql",
        R"sql(CREATE DICTIONARY `2024_dictionary_with_comment`
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT())
COMMENT 'Test dictionary with comment')sql"
    }}
)));

INSTANTIATE_TEST_SUITE_P(ParserCreateDatabaseQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')"
        },
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE `tbl`\n(PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE tbl\n(\n    PARTITION BY toYYYYMM(created)\n)"
        },
        {
            "CREATE DATABASE db ENGINE=Foo TABLE OVERRIDE `tbl` (), TABLE OVERRIDE a (COLUMNS (_created DateTime MATERIALIZED now())), TABLE OVERRIDE b (PARTITION BY rand())",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE tbl\n(\n\n),\nTABLE OVERRIDE a\n(\n    COLUMNS\n    (\n        `_created` DateTime MATERIALIZED now()\n    )\n),\nTABLE OVERRIDE b\n(\n    PARTITION BY rand()\n)"
        },
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE tbl (COLUMNS (id UUID) PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE tbl\n(\n    COLUMNS\n    (\n        `id` UUID\n    )\n    PARTITION BY toYYYYMM(created)\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE tbl (COLUMNS (INDEX foo foo TYPE minmax GRANULARITY 1) PARTITION BY if(_staged = 1, 'staging', toYYYYMM(created)))",
            "CREATE DATABASE db\nTABLE OVERRIDE tbl\n(\n    COLUMNS\n    (\n        INDEX foo foo TYPE minmax GRANULARITY 1\n    )\n    PARTITION BY if(_staged = 1, 'staging', toYYYYMM(created))\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE t1 (TTL inserted + INTERVAL 1 MONTH DELETE), TABLE OVERRIDE t2 (TTL `inserted` + INTERVAL 2 MONTH DELETE)",
            "CREATE DATABASE db\nTABLE OVERRIDE t1\n(\n    TTL inserted + toIntervalMonth(1)\n),\nTABLE OVERRIDE t2\n(\n    TTL inserted + toIntervalMonth(2)\n)"
        },
        {
            "CREATE DATABASE db ENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw') SETTINGS allows_query_when_mysql_lost = 1 TABLE OVERRIDE tab3 (COLUMNS (_staged UInt8 MATERIALIZED 1) PARTITION BY (c3) TTL c3 + INTERVAL 10 minute), TABLE OVERRIDE tab5 (PARTITION BY (c3) TTL c3 + INTERVAL 10 minute)",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw')\nSETTINGS allows_query_when_mysql_lost = 1\nTABLE OVERRIDE tab3\n(\n    COLUMNS\n    (\n        `_staged` UInt8 MATERIALIZED 1\n    )\n    PARTITION BY c3\n    TTL c3 + toIntervalMinute(10)\n),\nTABLE OVERRIDE tab5\n(\n    PARTITION BY c3\n    TTL c3 + toIntervalMinute(10)\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE tbl (PARTITION BY toYYYYMM(created) COLUMNS (created DateTime CODEC(Delta)))",
            "CREATE DATABASE db\nTABLE OVERRIDE tbl\n(\n    COLUMNS\n    (\n        `created` DateTime CODEC(Delta)\n    )\n    PARTITION BY toYYYYMM(created)\n)"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1", 
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2", 
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE a\n(\n    ORDER BY (id, version)\n)"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() SETTINGS a = 1, b = 2 COMMENT 'db comment' TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nSETTINGS a = 1, b = 2\nTABLE OVERRIDE a\n(\n    ORDER BY (id, version)\n)\nCOMMENT 'db comment'"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserCreateUserQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateUserQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'"
        },
        {
            "CREATE USER user1 IDENTIFIED WITH no_password",
            "CREATE USER user1 IDENTIFIED WITH no_password"
        },
        {
            "CREATE USER user1",
            "CREATE USER user1 IDENTIFIED WITH no_password"
        },
        {
            "CREATE USER user1 IDENTIFIED WITH plaintext_password BY 'abc123', plaintext_password BY 'def123', sha256_password BY 'ghi123'",
            "CREATE USER user1 IDENTIFIED WITH plaintext_password BY 'abc123', plaintext_password BY 'def123', sha256_password BY 'ghi123'"
        },
        {
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "CREATE USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'"
        },
        {
            "ALTER USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'",
            "ALTER USER user1 IDENTIFIED WITH sha256_password BY 'qwe123'"
        },
        {
            "ALTER USER user1 IDENTIFIED WITH plaintext_password BY 'abc123', plaintext_password BY 'def123', sha256_password BY 'ghi123'",
            "ALTER USER user1 IDENTIFIED WITH plaintext_password BY 'abc123', plaintext_password BY 'def123', sha256_password BY 'ghi123'"
        },
        {
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'",
            "ALTER USER user1 IDENTIFIED WITH sha256_hash BY '7A37B85C8918EAC19A9089C0FA5A2AB4DCE3F90528DCDEEC108B23DDF3607B99' SALT 'salt'"
        },
        {
            "CREATE USER user1 IDENTIFIED WITH sha256_password BY 'qwe123' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264'",
            "throws Syntax error"
        },
        {
            "ALTER USER user1 IDENTIFIED WITH plaintext_password BY 'abc123' IDENTIFIED WITH plaintext_password BY 'def123'",
            "throws Only one identified with is permitted"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserAttachUserQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAttachAccessEntity>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF' SALT 'EFFD7F6B03B3EA68B8F86C1E91614DD50E42EB31EF7160524916444D58B5E264';",
            "^[A-Za-z0-9]{64}$"
        },
        {
            "ATTACH USER user1 IDENTIFIED WITH sha256_hash BY '2CC4880302693485717D34E06046594CFDFE425E3F04AA5A094C4AABAB3CB0BF'",  //for users created in older releases that sha256_password has no salt
            "^$"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserRenameQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserRenameQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "RENAME TABLE eligible_test TO eligible_test2",
            "RENAME TABLE eligible_test TO eligible_test2"
        }
})));

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "Customers",
            "SELECT *\nFROM Customers"
        },
        {
            "Customers | project FirstName,LastName,Occupation",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | limit 3",
            "SELECT\n    FirstName,\n    LastName,\n    Occupation\nFROM Customers\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 1 | take 3",
            "SELECT *\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 1\n)\nLIMIT 3"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3 | take 1",
            "SELECT *\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 3\n)\nLIMIT 1"
        },
        {
            "Customers | project FirstName,LastName,Occupation | take 3 | project FirstName,LastName",
            "SELECT\n    FirstName,\n    LastName\nFROM\n(\n    SELECT\n        FirstName,\n        LastName,\n        Occupation\n    FROM Customers\n    LIMIT 3\n)"
        },
        {
            "Customers | sort by FirstName desc",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC"
        },
        {
            "Customers | take 3 | order by FirstName desc",
            "SELECT *\nFROM\n(\n    SELECT *\n    FROM Customers\n    LIMIT 3\n)\nORDER BY FirstName DESC"
        },
        {
            "Customers | sort by FirstName asc",
            "SELECT *\nFROM Customers\nORDER BY FirstName ASC"
        },
        {
            "Customers | sort by FirstName",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC"
        },
        {
            "Customers | order by LastName",
            "SELECT *\nFROM Customers\nORDER BY LastName DESC"
        },
        {
            "Customers | order by Age desc, FirstName asc  ",
            "SELECT *\nFROM Customers\nORDER BY\n    Age DESC,\n    FirstName ASC"
        },
        {
            "Customers | order by Age asc, FirstName desc",
            "SELECT *\nFROM Customers\nORDER BY\n    Age ASC,\n    FirstName DESC"
        },
        {
            "Customers | sort by FirstName | order by Age ",
            "SELECT *\nFROM Customers\nORDER BY\n    Age DESC,\n    FirstName DESC"
        },
        {
            "Customers | sort by FirstName nulls first",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS FIRST"
        },
        {
            "Customers | sort by FirstName nulls last",
            "SELECT *\nFROM Customers\nORDER BY FirstName DESC NULLS LAST"
        },
        {
            "Customers | where Occupation == 'Skilled Manual'",
            "SELECT *\nFROM Customers\nWHERE Occupation = 'Skilled Manual'"
        },
        {
            "Customers | where Occupation != 'Skilled Manual'",
            "SELECT *\nFROM Customers\nWHERE Occupation != 'Skilled Manual'"
        },
        {
            "Customers |where Education in  ('Bachelors','High School')",
            "SELECT *\nFROM Customers\nWHERE Education IN ('Bachelors', 'High School')"
        },
        {
            "Customers |  where Education !in  ('Bachelors','High School')",
            "SELECT *\nFROM Customers\nWHERE Education NOT IN ('Bachelors', 'High School')"
        },
        {
            "Customers |where Education contains_cs  'Degree'",
            "SELECT *\nFROM Customers\nWHERE Education LIKE '%Degree%'"
        },
        {
            "Customers | where Occupation startswith_cs  'Skil'",
            "SELECT *\nFROM Customers\nWHERE startsWith(Occupation, 'Skil')"
        },
        {
            "Customers | where FirstName endswith_cs  'le'",
            "SELECT *\nFROM Customers\nWHERE endsWith(FirstName, 'le')"
        },
        {
            "Customers | where Age == 26",
            "SELECT *\nFROM Customers\nWHERE Age = 26"
        },
        {
            "Customers | where Age > 20 and Age < 30",
            "SELECT *\nFROM Customers\nWHERE (Age > 20) AND (Age < 30)"
        },
        {
            "Customers | where Age > 30 | where Education == 'Bachelors'",
            "SELECT *\nFROM Customers\nWHERE (Education = 'Bachelors') AND (Age > 30)"
        },
        {
            "Customers |summarize count() by Occupation",
            "SELECT\n    Occupation,\n    count() AS count_\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize sum(Age) by Occupation",
            "SELECT\n    Occupation,\n    sum(Age) AS sum_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize  avg(Age) by Occupation",
            "SELECT\n    Occupation,\n    avg(Age) AS avg_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers|summarize  min(Age) by Occupation",
            "SELECT\n    Occupation,\n    min(Age) AS min_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers |summarize  max(Age) by Occupation",
            "SELECT\n    Occupation,\n    max(Age) AS max_Age\nFROM Customers\nGROUP BY Occupation"
        },
        {
            "Customers | where FirstName contains 'pet'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE '%pet%'"
        },
        {
            "Customers | where FirstName !contains 'pet'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE '%pet%')"
        },
        {
            "Customers | where FirstName endswith 'er'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE '%er'"
        },
        {
            "Customers | where FirstName !endswith 'er'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE '%er')"
        },
        {
            "Customers | where Education has 'School'",
            "SELECT *\nFROM Customers\nWHERE hasTokenCaseInsensitive(Education, 'School')"
        },
        {
            "Customers | where Education !has 'School'",
            "SELECT *\nFROM Customers\nWHERE NOT hasTokenCaseInsensitive(Education, 'School')"
        },
        {
            "Customers | where Education has_cs 'School'",
            "SELECT *\nFROM Customers\nWHERE hasToken(Education, 'School')"
        },
        {
            "Customers | where Education !has_cs 'School'",
            "SELECT *\nFROM Customers\nWHERE NOT hasToken(Education, 'School')"
        },
        {
            "Customers | where FirstName matches regex 'P.*r'",
            "SELECT *\nFROM Customers\nWHERE match(FirstName, 'P.*r')"
        },
        {
            "Customers | where FirstName startswith 'pet'",
            "SELECT *\nFROM Customers\nWHERE FirstName ILIKE 'pet%'"
        },
        {
            "Customers | where FirstName !startswith 'pet'",
            "SELECT *\nFROM Customers\nWHERE NOT (FirstName ILIKE 'pet%')"
        },
        {
            "Customers | where Age in ((Customers|project Age|where Age < 30))",
            "SELECT *\nFROM Customers\nWHERE Age IN (\n    SELECT Age\n    FROM Customers\n    WHERE Age < 30\n)"
        },
        {
            "Customers|where Occupation has_any ('Skilled','abcd')",
            "SELECT *\nFROM Customers\nWHERE hasTokenCaseInsensitive(Occupation, 'Skilled') OR hasTokenCaseInsensitive(Occupation, 'abcd')"
        },
        {
            "Customers|where Occupation has_all ('Skilled','abcd')",
            "SELECT *\nFROM Customers\nWHERE hasTokenCaseInsensitive(Occupation, 'Skilled') AND hasTokenCaseInsensitive(Occupation, 'abcd')"
        },
        {
            "Customers|where Occupation has_all (strcat('Skill','ed'),'Manual')",
            "SELECT *\nFROM Customers\nWHERE hasTokenCaseInsensitive(Occupation, concat('Skill', 'ed')) AND hasTokenCaseInsensitive(Occupation, 'Manual')"
        },
        {
            "Customers | where Occupation == strcat('Pro','fessional') | take 1",
            "SELECT *\nFROM Customers\nWHERE Occupation = concat('Pro', 'fessional')\nLIMIT 1"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at')",
            "SELECT countSubstrings('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at', 'normal')",
            "SELECT countSubstrings('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project countof('The cat sat on the mat', 'at', 'regex')",
            "SELECT countMatches('The cat sat on the mat', 'at')\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 0, 'The price of PINEAPPLE ice cream is 10')",
            "SELECT extract('The price of PINEAPPLE ice cream is 10', '\\b[A-Z]+\\b.+\\b\\\\d+')\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 1, 'The price of PINEAPPLE ice cream is 20')",
            "SELECT extract('The price of PINEAPPLE ice cream is 20', '\\b[A-Z]+\\b')\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 30')",
            "SELECT extract('The price of PINEAPPLE ice cream is 30', '\\b\\\\d+')\nFROM Customers"
        },
        {
            "Customers | project extract('(\\b[A-Z]+\\b).+(\\b\\d+)', 2, 'The price of PINEAPPLE ice cream is 40', typeof(int))",
            "SELECT accurateCastOrNull(extract('The price of PINEAPPLE ice cream is 40', '\\b\\\\d+'), 'Int32')\nFROM Customers"
        },
        {
            "Customers | project extract_all('(\\w)(\\w+)(\\w)','The price of PINEAPPLE ice cream is 50')",
            "SELECT extractAllGroups('The price of PINEAPPLE ice cream is 50', '(\\\\w)(\\\\w+)(\\\\w)')\nFROM Customers"
        },
        {
            " Customers | project split('aa_bb', '_')",
            "SELECT if(empty('_'), splitByString(' ', 'aa_bb'), splitByString('_', 'aa_bb'))\nFROM Customers"
        },
        {
            "Customers | project split('aaa_bbb_ccc', '_', 1)",
            "SELECT multiIf((length(if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc'))) >= 2) AND (2 > 0), arrayPushBack([], if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc'))[2]), 2 = 0, if(empty('_'), splitByString(' ', 'aaa_bbb_ccc'), splitByString('_', 'aaa_bbb_ccc')), arrayPushBack([], NULL[1]))\nFROM Customers"
        },
        {
            "Customers | project strcat_delim('-', '1', '2', 'A')",
            "SELECT concat('1', '-', '2', '-', 'A')\nFROM Customers"
        },
        {
            "Customers | project indexof('abcdefg','cde')",
            "SELECT position('abcdefg', 'cde', 1) - 1\nFROM Customers"
        },
        {
            "Customers | project indexof('abcdefg','cde', 2) ",
            "SELECT position('abcdefg', 'cde', 3) - 1\nFROM Customers"
        },
        {
            "print x=1, s=strcat('Hello', ', ', 'World!')",
            "SELECT\n    1 AS x,\n    concat('Hello', ', ', 'World!') AS s"
        },
        {
            "print parse_urlquery('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')",
            "SELECT concat('{', concat('\"Query Parameters\":', concat('{\"', replace(replace(if(position('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment', '?') > 0, queryString('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), 'https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '=', '\":\"'), '&', '\",\"'), '\"}')), '}')"
        },
        {
            "print strcmp('a','b')",
            "SELECT multiIf('a' = 'b', 0, 'a' < 'b', -1, 1)"
        },
        {
            "print parse_url('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')",
            "SELECT concat('{', concat('\"Scheme\":\"', protocol('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '\"'), ',', concat('\"Host\":\"', domain('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '\"'), ',', concat('\"Port\":\"', toString(port('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment')), '\"'), ',', concat('\"Path\":\"', path('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '\"'), ',', concat('\"Username\":\"', splitByChar(':', splitByChar('@', netloc('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'))[1])[1], '\"'), ',', concat('\"Password\":\"', splitByChar(':', splitByChar('@', netloc('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'))[1])[2], '\"'), ',', concat('\"Query Parameters\":', concat('{\"', replace(replace(queryString('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '=', '\":\"'), '&', '\",\"'), '\"}')), ',', concat('\"Fragment\":\"', fragment('https://john:123@google.com:1234/this/is/a/path?k1=v1&k2=v2#fragment'), '\"'), '}')"
        },
        {
            "Customers | summarize t = make_list(FirstName) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(FirstName, FirstName IS NOT NULL) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list(FirstName, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(10)(FirstName, FirstName IS NOT NULL) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list_if(FirstName, Age > 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_list_if(FirstName, Age > 10, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupArrayIf(10)(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set(FirstName) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArray(FirstName) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set(FirstName, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArray(10)(FirstName) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set_if(FirstName, Age > 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArrayIf(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "Customers | summarize t = make_set_if(FirstName, Age > 10, 10) by FirstName",
            "SELECT\n    FirstName,\n    groupUniqArrayIf(10)(FirstName, Age > 10) AS t\nFROM Customers\nGROUP BY FirstName"
        },
        {
            "print output = dynamic([1, 2, 3])",
            "SELECT [1, 2, 3] AS output"
        },
        {
            "print output = dynamic(['a', 'b', 'c'])",
            "SELECT ['a', 'b', 'c'] AS output"
        },
        {
            "T | extend duration = endTime - startTime",
            "SELECT\n    * EXCEPT duration,\n    endTime - startTime AS duration\nFROM T"
        },
        {
            "T |project endTime, startTime | extend duration = endTime - startTime",
            "SELECT\n    * EXCEPT duration,\n    endTime - startTime AS duration\nFROM\n(\n    SELECT\n        endTime,\n        startTime\n    FROM T\n)"
        },
        {
            "T | extend c =c*2, b-a, d = a + b, a*b",
            "SELECT\n    * EXCEPT c EXCEPT d,\n    c * 2 AS c,\n    b - a AS Column1,\n    a + b AS d,\n    a * b AS Column2\nFROM T"
        }
})));

static constexpr size_t kDummyMaxQuerySize = 256 * 1024;
static constexpr size_t kDummyMaxParserDepth = 256;
static constexpr size_t kDummyMaxParserBacktracks = 1000000;

INSTANTIATE_TEST_SUITE_P(
    ParserPRQL,
    ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserPRQLQuery>(kDummyMaxQuerySize, kDummyMaxParserDepth, kDummyMaxParserBacktracks)),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {
                "from albums\ngroup {author_id} (\n  aggregate {first_published = min published}\n)\njoin a=author side:left (==author_id)\njoin p=purchases side:right (==author_id)\ngroup {a.id, p.purchase_id} (\n  aggregate {avg_sell = min first_published}\n)",
                "WITH table_0 AS\n    (\n        SELECT\n            MIN(published) AS _expr_0,\n            author_id\n        FROM albums\n        GROUP BY author_id\n    )\nSELECT\n    a.id,\n    p.purchase_id,\n    MIN(table_0._expr_0) AS avg_sell\nFROM table_0\nLEFT JOIN author AS a ON table_0.author_id = a.author_id\nRIGHT JOIN purchases AS p ON table_0.author_id = p.author_id\nGROUP BY\n    a.id,\n    p.purchase_id",
            },
            {
                "from matches\nfilter start_date > @2023-05-30                 # Some comment here\nderive {\n  some_derived_value_1 = a + (b ?? 0),          # And there\n  some_derived_value_2 = c + some_derived_value\n}\nfilter some_derived_value_2 > 0\ngroup {country, city} (\n  aggregate {\n    average some_derived_value_2,\n    aggr = max some_derived_value_2\n  }\n)\nderive place = f\"{city} in {country}\"\nderive country_code = s\"LEFT(country, 2)\"\nsort {aggr, -country}\ntake 1..20",
                "WITH\n    table_1 AS\n    (\n        SELECT\n            country,\n            city,\n            c + some_derived_value AS _expr_1\n        FROM matches\n        WHERE start_date > toDate('2023-05-30')\n    ),\n    table_0 AS\n    (\n        SELECT\n            country,\n            city,\n            AVG(_expr_1) AS _expr_0,\n            MAX(_expr_1) AS aggr\n        FROM table_1\n        WHERE _expr_1 > 0\n        GROUP BY\n            country,\n            city\n    )\nSELECT\n    country,\n    city,\n    _expr_0,\n    aggr,\n    CONCAT(city, ' in ', country) AS place,\n    LEFT(country, 2) AS country_code\nFROM table_0\nORDER BY\n    aggr ASC,\n    country DESC\nLIMIT 20",
            },
        })));
