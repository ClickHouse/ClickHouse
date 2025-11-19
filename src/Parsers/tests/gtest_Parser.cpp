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
#include <Parsers/parseQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Common/re2.h>
#include <string_view>
#include <gtest/gtest.h>
#include <Parsers/tests/gtest_common.h>
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
            EXPECT_THROW(parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0), DB::Exception);  /// NOLINT(bugprone-suspicious-stringview-data-usage)
        }
        else
        {
            ASTPtr ast;
            ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0));  /// NOLINT(bugprone-suspicious-stringview-data-usage)
            if (std::string("CREATE USER or ALTER USER query") != parser->getName()
                    && std::string("ATTACH access entity query") != parser->getName())
            {
                ASTPtr ast_clone = ast->clone();
                {
                    String formatted_ast = ast_clone->formatWithSecretsMultiLine();
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
                    String formatted_ast = ast_clone->formatWithSecretsMultiLine();
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
                    String formatted_ast = ast->clone()->formatWithSecretsMultiLine();
                    EXPECT_TRUE(re2::RE2::FullMatch(formatted_ast, expected_ast));
                }
            }
        }
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0), DB::Exception);  /// NOLINT(bugprone-suspicious-stringview-data-usage)
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
                "(MODIFY COMMENT '')",
            },
            {
                // Non-empty comment value
                "MODIFY COMMENT 'some comment value'",
                "(MODIFY COMMENT 'some comment value')",
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserAlterCommand_MODIFY_COMMENT_WITH_PARENS, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>(true)),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                // Empty comment value
                "(MODIFY COMMENT '')",
                "(MODIFY COMMENT '')",
            },
            {
                // Non-empty comment value
                "(MODIFY COMMENT 'some comment value')",
                "(MODIFY COMMENT 'some comment value')",
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
            "CREATE DATABASE db ENGINE=Foo TABLE OVERRIDE `tbl` (), TABLE OVERRIDE a (COLUMNS (_created DateTime MATERIALIZED now())), TABLE OVERRIDE b (PARTITION BY rand())",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE tbl\n(\n\n),\nTABLE OVERRIDE a\n(\n    COLUMNS\n    (\n        `_created` DateTime MATERIALIZED now()\n    )\n),\nTABLE OVERRIDE b\n(\n    PARTITION BY rand()\n)"
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
            "CREATE USER user1 IDENTIFIED WITH scram_sha256_password BY 'qwe123'",
            "CREATE USER user1 IDENTIFIED WITH scram_sha256_password BY 'qwe123'"
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
            "CREATE USER user1 IDENTIFIED WITH scram_sha256_hash BY '04e7a70338d7af7bb6142fe7e19fef46d9b605f3e78b932a60e8200ef9154976' SALT ''",
            "CREATE USER user1 IDENTIFIED WITH scram_sha256_hash BY '04e7a70338d7af7bb6142fe7e19fef46d9b605f3e78b932a60e8200ef9154976' SALT ''"
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
