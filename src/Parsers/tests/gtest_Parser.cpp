#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserAttachAccessEntity.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/stripQuerySettings.h>
#include <Parsers/Lexer.h>
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

[[maybe_unused]] static std::ostream & operator<<(std::ostream & ostr, const std::shared_ptr<IParser> parser)
{
    return ostr << "Parser: " << parser->getName();
}

static std::ostream & operator<<(std::ostream & ostr, const ParserTestCase & test_case)
{
    // New line characters are removed because at the time of writing this the unit test results are parsed from the
    // command line output, and multi-line string representations are breaking the parsing logic.
    std::string input_text{test_case.input_text};
    boost::replace_all(input_text, "\n", "\\n");
    return ostr << "ParserTestCase input: " << input_text;
}

TEST(Lexer, NullInputWithMaxQuerySize)
{
    Lexer lexer(nullptr, nullptr, 262144);
    Token token = lexer.nextToken();
    EXPECT_EQ(TokenType::EndOfStream, token.type);
}

/// The output-option children (INTO OUTFILE, COMPRESSION, FORMAT, SETTINGS) must end up
/// in the same canonical order whether the AST is freshly parsed, cloned, or obtained by
/// a format+reparse roundtrip. Otherwise the tree hash differs across these paths, which
/// trips the `Inconsistent AST formatting` consistency check. See
/// `ASTQueryWithOutput::output_option_members`.
TEST(ParserQueryWithOutput, OutputOptionChildOrderIsCanonical)
{
    const std::vector<String> queries = {
        "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' FORMAT JSONEachRow",
        "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' FORMAT JSONEachRow SETTINGS max_threads = 1",
        "SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' SETTINGS max_threads = 1 FORMAT JSONEachRow",
        "SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 1",
        "SELECT 1 SETTINGS max_threads = 1 FORMAT JSONEachRow",
    };

    for (const auto & query : queries)
    {
        ParserQueryWithOutput parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        /// A clone must have the same tree hash as the original.
        ASTPtr cloned = ast->clone();
        EXPECT_EQ(ast->getTreeHash(false), cloned->getTreeHash(false)) << "clone of: " << query;

        /// A format+reparse roundtrip must reproduce the same tree hash.
        String formatted = ast->formatWithSecretsOneLine();
        ASTPtr reparsed = parseQuery(parser, formatted, "", 0, 0, 0);
        ASSERT_NE(nullptr, reparsed) << "reparse of: " << formatted;
        EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false)) << "roundtrip of: " << query;
    }
}

/// `ASTExplainQuery` also carries its own EXPLAIN-level settings, which `ParserExplainQuery`
/// parses *before* the explained query (so `children = [ast_settings, query]`). The clone must
/// re-add the children in the same order, otherwise it gets a different `getTreeHash` than a
/// freshly parsed AST. See `ASTExplainQuery::clone`.
TEST(ParserExplainQuery, ExplainSettingsChildOrderIsCanonical)
{
    const std::vector<String> queries = {
        "EXPLAIN header = 1 SELECT 1",
        "EXPLAIN PLAN actions = 1, indexes = 1 SELECT 1",
        "EXPLAIN AST optimize = 1 SELECT 1",
        "EXPLAIN header = 1 SELECT 1 FORMAT JSONEachRow",
    };

    for (const auto & query : queries)
    {
        ParserQueryWithOutput parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        ASTPtr cloned = ast->clone();
        EXPECT_EQ(ast->getTreeHash(false), cloned->getTreeHash(false)) << "clone of: " << query;

        String formatted = ast->formatWithSecretsOneLine();
        ASTPtr reparsed = parseQuery(parser, formatted, "", 0, 0, 0);
        ASSERT_NE(nullptr, reparsed) << "reparse of: " << formatted;
        EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false)) << "roundtrip of: " << query;
    }
}

/// `ASTExecuteAsQuery` is another `ASTQueryWithOutput` carrier, but it is parsed outside
/// `ParserQueryWithOutput`: `ParserExecuteAsQuery` hoists the subquery output options to the
/// outer query. The hoisting appends them to `children` after the subquery and in the canonical
/// `output_option_members` order, so that a freshly parsed `EXECUTE AS ... <output options>` and
/// its clone share the same child order (and therefore the same tree hash).
TEST(ParserExecuteAsQuery, OutputOptionChildOrderIsCanonical)
{
    const std::vector<String> queries = {
        "EXECUTE AS u SELECT 1 INTO OUTFILE 'x' COMPRESSION 'gz' FORMAT JSONEachRow",
        "EXECUTE AS u SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 1",
        "EXECUTE AS u SELECT 1 INTO OUTFILE 'x' FORMAT JSONEachRow",
    };

    for (const auto & query : queries)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        ASTPtr cloned = ast->clone();
        EXPECT_EQ(ast->getTreeHash(false), cloned->getTreeHash(false)) << "clone of: " << query;

        String formatted = ast->formatWithSecretsOneLine();
        ASTPtr reparsed = parseQuery(parser, formatted, "", 0, 0, 0);
        ASSERT_NE(nullptr, reparsed) << "reparse of: " << formatted;
        EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false)) << "roundtrip of: " << query;
    }
}

TEST(ParserCreateDatabaseQuery, MaskDataLakeCatalogStorageCredentials)
{
    /// Both the `aws_*` and the backward-compatible `storage_aws_*` static credentials must be hidden
    /// in `SHOW CREATE DATABASE` for the `DataLakeCatalog` engine, otherwise secrets leak.
    const String query =
        "CREATE DATABASE test_unity ENGINE = DataLakeCatalog('http://localhost:8181') "
        "SETTINGS aws_access_key_id = 'AKIA_PLAIN', aws_secret_access_key = 'plain_secret', "
        "storage_aws_access_key_id = 'AKIA_STORAGE', storage_aws_secret_access_key = 'storage_secret'";

    DB::ParserCreateQuery parser;
    DB::ASTPtr ast = DB::parseQuery(parser, query, 0, 0, 0);

    /// formatForLogging always hides secrets.
    const String masked = ast->formatForLogging();

    EXPECT_EQ(masked.find("plain_secret"), String::npos);
    EXPECT_EQ(masked.find("storage_secret"), String::npos);
    EXPECT_EQ(masked.find("AKIA_PLAIN"), String::npos);
    EXPECT_EQ(masked.find("AKIA_STORAGE"), String::npos);
    EXPECT_NE(masked.find("[HIDDEN]"), String::npos);
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
            EXPECT_THROW(parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0), DB::Exception); /// NOLINT(bugprone-suspicious-stringview-data-usage)
        }
        else
        {
            ASTPtr ast;
            ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0)); /// NOLINT(bugprone-suspicious-stringview-data-usage)
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
        ASSERT_THROW(parseQuery(*parser, input_text.data(), input_text.data() + input_text.size(), 0, 0, 0), DB::Exception); /// NOLINT(bugprone-suspicious-stringview-data-usage)
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

INSTANTIATE_TEST_SUITE_P(ParserCreateTableQuery_SQL_SECURITY, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "CREATE TABLE t (x UInt8) ENGINE = Memory SQL SECURITY INVOKER",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nSQL SECURITY INVOKER"
        },
        {
            "CREATE TABLE t (x UInt8) ENGINE = Memory SQL SECURITY NONE",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nSQL SECURITY NONE"
        },
        {
            "CREATE TABLE t (x UInt8) ENGINE = Memory DEFINER = alice SQL SECURITY DEFINER",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nDEFINER = alice SQL SECURITY DEFINER"
        },
        {
            "CREATE TABLE t (x UInt8) ENGINE = Memory DEFINER = CURRENT_USER",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nDEFINER = CURRENT_USER SQL SECURITY DEFINER"
        },
        {
            "CREATE TABLE t (x UInt8) ENGINE = Memory SQL SECURITY DEFINER",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nDEFINER = CURRENT_USER SQL SECURITY DEFINER"
        },
        {
            "CREATE TABLE db.t ON CLUSTER c (x UInt8) ENGINE = MergeTree ORDER BY x SQL SECURITY INVOKER",
            "CREATE TABLE db.t ON CLUSTER c\n(\n    `x` UInt8\n)\nENGINE = MergeTree\nORDER BY x\nSQL SECURITY INVOKER"
        },
        {
            "ATTACH TABLE t UUID '123e4567-e89b-12d3-a456-426614174000' (x UInt8) ENGINE = Memory SQL SECURITY INVOKER",
            "ATTACH TABLE t UUID '123e4567-e89b-12d3-a456-426614174000'\n(\n    `x` UInt8\n)\nENGINE = Memory\nSQL SECURITY INVOKER"
        },
        {
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nDEFINER = alice SQL SECURITY DEFINER",
            "CREATE TABLE t\n(\n    `x` UInt8\n)\nENGINE = Memory\nDEFINER = alice SQL SECURITY DEFINER"
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
            "CREATE USER user1"
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

// ATTACH MASKING POLICY (used when RESTORE-ing a backup) must parse without an UPDATE clause,
// unlike CREATE / ALTER. See ParserCreateMaskingPolicy::parseImpl.
INSTANTIATE_TEST_SUITE_P(ParserAttachMaskingPolicyQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateMaskingPolicy>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "ATTACH MASKING POLICY p ON db.t TO ALL",
            "ATTACH MASKING POLICY p ON db.t TO ALL"
        }
})));

// In CREATE / ALTER mode the UPDATE clause is still mandatory, so omitting it must fail to parse.
INSTANTIATE_TEST_SUITE_P(ParserCreateMaskingPolicyQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateMaskingPolicy>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "CREATE MASKING POLICY p ON db.t UPDATE email = '***' TO ALL",
            "CREATE MASKING POLICY p ON db.t UPDATE email = '***' TO ALL"
        },
        {
            "CREATE MASKING POLICY p ON db.t TO ALL",
            nullptr  // missing UPDATE clause
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
                "WITH\n    table_0 AS\n    (\n        SELECT\n            MIN(published) AS _expr_0,\n            author_id\n        FROM albums\n        GROUP BY author_id\n    )\nSELECT\n    a.id,\n    p.purchase_id,\n    MIN(table_0._expr_0) AS avg_sell\nFROM table_0\nLEFT JOIN author AS a ON table_0.author_id = a.author_id\nRIGHT JOIN purchases AS p ON table_0.author_id = p.author_id\nGROUP BY\n    a.id,\n    p.purchase_id",
            },
            {
                "from matches\nfilter start_date > @2023-05-30                 # Some comment here\nderive {\n  some_derived_value_1 = a + (b ?? 0),          # And there\n  some_derived_value_2 = c + some_derived_value\n}\nfilter some_derived_value_2 > 0\ngroup {country, city} (\n  aggregate {\n    average some_derived_value_2,\n    aggr = max some_derived_value_2\n  }\n)\nderive place = f\"{city} in {country}\"\nderive country_code = s\"LEFT(country, 2)\"\nsort {aggr, -country}\ntake 1..20",
                "WITH\n    table_0 AS\n    (\n        SELECT\n            country,\n            city,\n            AVG(c + some_derived_value) AS _expr_0,\n            MAX(c + some_derived_value) AS aggr\n        FROM matches\n        WHERE (start_date > toDate('2023-05-30')) AND ((c + some_derived_value) > 0)\n        GROUP BY\n            country,\n            city\n    )\nSELECT\n    country,\n    city,\n    _expr_0,\n    aggr,\n    CONCAT(city, ' in ', country) AS place,\n    LEFT(country, 2) AS country_code\nFROM table_0\nORDER BY\n    aggr ASC,\n    country DESC\nLIMIT 20",
            },
        })));

namespace
{

/// Walk the AST and fail if any node still owns an empty SETTINGS clause. Such a node serializes to a
/// bare `SETTINGS` keyword (ASTSelectQuery / ASTInsertQuery / ASTQueryWithOutput formatters all print
/// `SETTINGS ` whenever the node pointer is set), which throws on re-parse.
bool hasEmptySettingsNode(const ASTPtr & ast)
{
    std::vector<const IAST *> nodes{ast.get()};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();
        if (const auto * set_query = node->as<ASTSetQuery>())
            if (set_query->changes.empty() && set_query->default_settings.empty() && set_query->query_parameters.empty())
                return true;
        for (const auto & child : node->children)
            if (child)
                nodes.push_back(child.get());
    }
    return false;
}

/// True if any SETTINGS node still references `name`, either as `name = value` (in `changes`) or as
/// `name = DEFAULT` (in `default_settings`). A surviving `default_settings` entry is what re-parsing
/// the query would feed to InterpreterSetQuery::resetSettingsToDefaultValue, undoing a fuzz-context cap.
bool settingNamePresent(const ASTPtr & ast, std::string_view name)
{
    std::vector<const IAST *> nodes{ast.get()};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();
        if (const auto * set_query = node->as<ASTSetQuery>())
        {
            if (set_query->changes.tryGet(name))
                return true;
            for (const auto & default_name : set_query->default_settings)
                if (default_name == name)
                    return true;
        }
        for (const auto & child : node->children)
            if (child)
                nodes.push_back(child.get());
    }
    return false;
}

/// Count how many times `name` appears across `changes` and `default_settings` in every SETTINGS node.
/// ParserSetQuery appends one entry per occurrence (no de-duplication), so a repeated override yields a
/// count > 1; the strip transform must bring it to 0, erasing every copy rather than just the first.
size_t countSettingOccurrences(const ASTPtr & ast, std::string_view name)
{
    size_t count = 0;
    std::vector<const IAST *> nodes{ast.get()};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();
        if (const auto * set_query = node->as<ASTSetQuery>())
        {
            for (const auto & change : set_query->changes)
                if (change.name == name)
                    ++count;
            for (const auto & default_name : set_query->default_settings)
                if (default_name == name)
                    ++count;
        }
        for (const auto & child : node->children)
            if (child)
                nodes.push_back(child.get());
    }
    return count;
}

}

/// The server-side AST fuzzer strips its safety-limit settings from a fuzzed query so they cannot be
/// overridden. Before this was fixed, stripping the only settings in a clause left an empty SETTINGS
/// node attached to its owner, which re-serialized to a bare `SETTINGS` keyword; re-parsing that
/// threw, so the fuzzer silently skipped the query instead of running it under the caps. The transform
/// must instead prune the empty node, leaving a query that still parses and executes.
/// See `removeSettingsFromQuery` and `executeASTFuzzerQueries`.
TEST(RemoveSettingsFromQuery, PrunesEmptySettingsAndKeepsQueryParseable)
{
    static constexpr std::string_view safety_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
    };

    /// Each query carries ONLY the safety settings (in every SETTINGS clause), so stripping empties
    /// the clause and the owner must be pruned. Covers the owners that re-apply query settings in
    /// InterpreterSetQuery::applySettingsFromQuery: ASTSelectQuery, ASTSelectWithUnionQuery /
    /// ASTQueryWithOutput, ASTInsertQuery, ASTExplainQuery, and a subquery clause.
    const std::vector<String> override_only_queries = {
        "SELECT sum(number) FROM numbers(100) SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw'",
        "SELECT 1 UNION ALL SELECT 2 SETTINGS max_rows_to_read = 0, max_execution_time = 0",
        "INSERT INTO t SELECT number FROM numbers(100) SETTINGS max_rows_to_read = 0, read_overflow_mode = 'throw'",
        "EXPLAIN SELECT number FROM numbers(100) SETTINGS max_rows_to_read = 0",
        "SELECT * FROM (SELECT number FROM numbers(100) SETTINGS max_rows_to_read = 0) SETTINGS max_execution_time = 0",
    };

    for (const auto & query : override_only_queries)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        /// No owner may keep an empty SETTINGS node.
        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty SETTINGS left for: " << query;

        /// The serialized query must re-parse (this is exactly what failed before the fix: a bare
        /// `SETTINGS` threw, so the fuzzer skipped the query instead of running it under the caps).
        String formatted = ast->formatWithSecretsOneLine();
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// A clause that also holds a non-safety setting must keep that setting (and the SETTINGS clause).
    {
        const String query = "SELECT 1 SETTINGS max_rows_to_read = 0, max_threads = 4";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "query: " << query;
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_NE(String::npos, formatted.find("max_threads")) << "dropped a non-safety setting: " << formatted;
        EXPECT_EQ(String::npos, formatted.find("max_rows_to_read")) << "kept a safety setting: " << formatted;
    }
}

/// `SETTINGS max_rows_to_read = DEFAULT` parks the name in ASTSetQuery::default_settings, not in
/// `changes`. On re-parse InterpreterSetQuery::executeForCurrentContext calls
/// resetSettingsToDefaultValue(default_settings), which would reset the fuzz-context cap back to its
/// unbounded default. The transform must strip the names from `default_settings` too, so a fuzzed
/// `= DEFAULT` cannot re-open the read cap.
TEST(RemoveSettingsFromQuery, StripsResetToDefaultOverrides)
{
    static constexpr std::string_view safety_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
    };

    /// Every safety setting appears as `= DEFAULT`; some clauses also carry a `= value` form and a
    /// non-safety setting. Covers SELECT, SELECT-UNION (ASTQueryWithOutput), INSERT and a subquery.
    const std::vector<String> queries = {
        "SELECT sum(number) FROM numbers(100) SETTINGS max_rows_to_read = DEFAULT, read_overflow_mode = DEFAULT",
        "SELECT 1 UNION ALL SELECT 2 SETTINGS max_rows_to_read = DEFAULT, max_execution_time = DEFAULT",
        "INSERT INTO t SELECT number FROM numbers(100) SETTINGS max_rows_to_read = DEFAULT, read_overflow_mode = 'throw'",
        "SELECT number FROM numbers(100) SETTINGS max_rows_to_read = DEFAULT, max_threads = 4",
    };

    for (const auto & query : queries)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        /// Sanity: a safety setting really is parked in default_settings before the strip.
        ASSERT_TRUE(settingNamePresent(ast, "max_rows_to_read")) << "setup: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        /// No safety setting may survive, in either list, in any clause.
        for (const auto & name : safety_settings)
            EXPECT_FALSE(settingNamePresent(ast, name))
                << "safety setting '" << name << "' survived for: " << query;

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty SETTINGS left for: " << query;

        /// The serialized query still re-parses (no bare `SETTINGS`, no stray `= DEFAULT`).
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("DEFAULT")) << "kept a `= DEFAULT` override: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// A non-safety `= DEFAULT` must be preserved (we only strip the named caps).
    {
        const String query = "SELECT 1 SETTINGS max_rows_to_read = DEFAULT, max_threads = DEFAULT";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_TRUE(settingNamePresent(ast, "max_threads")) << "dropped a non-safety `= DEFAULT`";
        EXPECT_FALSE(settingNamePresent(ast, "max_rows_to_read")) << "kept a safety `= DEFAULT`";
        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "query: " << query;
    }
}

/// ParserSetQuery appends a SettingChange for every occurrence, so a SETTINGS clause may repeat a
/// safety setting. The strip transform must erase *all* copies: if it removed only the first, the
/// surviving duplicate would re-apply the override (`max_rows_to_read = 0`) on top of the pinned
/// fuzz context in executeQueryImpl after re-parse, re-opening the unbounded read.
TEST(RemoveSettingsFromQuery, StripsRepeatedOverrides)
{
    static constexpr std::string_view safety_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
    };

    /// Each clause repeats a safety setting (direct `= value`, `= DEFAULT`, or a mix). Covers SELECT,
    /// SELECT-UNION (ASTQueryWithOutput), INSERT and a subquery.
    const std::vector<String> queries = {
        "SELECT sum(number) FROM numbers(100) "
        "SETTINGS max_rows_to_read = 0, max_rows_to_read = 0, read_overflow_mode = 'throw'",
        "SELECT 1 UNION ALL SELECT 2 "
        "SETTINGS max_rows_to_read = 0, max_execution_time = 0, max_rows_to_read = DEFAULT",
        "INSERT INTO t SELECT number FROM numbers(100) "
        "SETTINGS read_overflow_mode = 'throw', max_rows_to_read = 0, read_overflow_mode = DEFAULT",
        "SELECT * FROM (SELECT number FROM numbers(100) "
        "SETTINGS max_rows_to_read = 0, max_rows_to_read = DEFAULT) SETTINGS max_execution_time = 0",
    };

    for (const auto & query : queries)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        /// Sanity: the duplicate really is parsed as two separate entries before the strip.
        ASSERT_GT(countSettingOccurrences(ast, "max_rows_to_read"), 1u) << "setup: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        /// Every copy of every safety setting must be gone, in either list, in any clause.
        for (const auto & name : safety_settings)
            EXPECT_EQ(0u, countSettingOccurrences(ast, name))
                << "safety setting '" << name << "' survived (count > 0) for: " << query;

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty SETTINGS left for: " << query;

        /// The serialized query still re-parses and carries no residual override.
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("max_rows_to_read")) << "kept a safety setting: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }
}

/// Not every SETTINGS carrier the fuzzer can use lives where a plain `children` walk reaches it, and
/// some carriers were stripped but never pruned. Two carriers that InterpreterSetQuery::
/// applySettingsFromQuery reads back onto the query context were missed:
///   - `ASTBackupQuery::settings` is held outside `children` (and outside `settings_ast`), so a
///     `BACKUP ... SETTINGS max_execution_time = 0` survived the strip and re-applied the override
///     (a strip miss, only reachable with `ast_fuzzer_any_query = 1`).
///   - `ASTStorage::settings` is in `children` and was stripped, but had no prune branch, so a
///     `CREATE ... SETTINGS <only safety setting>` left an empty node that ASTStorage::formatImpl
///     serialized to a bare `SETTINGS`, making the re-parse throw and the query be skipped (a prune
///     miss, same class as the original empty-node bug but for a different owner).
/// The serialized-query checks here use the actual format+reparse roundtrip, which exercises every
/// carrier regardless of whether it is in `children`.
TEST(RemoveSettingsFromQuery, StripsSettingsCarriersOutsideChildrenWalk)
{
    static constexpr std::string_view safety_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
    };

    /// BACKUP carrier (strip miss): the cap lives in ASTBackupQuery::settings, outside `children`.
    {
        const String query = "BACKUP TABLE t TO Null SETTINGS max_execution_time = 0";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        auto * backup_query = ast->as<ASTBackupQuery>();
        ASSERT_NE(nullptr, backup_query) << "expected a BACKUP query";
        /// Sanity: the cap really is parked in the out-of-children `settings` slot before the strip.
        ASSERT_NE(nullptr, backup_query->settings);
        ASSERT_NE(nullptr, backup_query->settings->as<ASTSetQuery>()->changes.tryGet("max_execution_time"));

        removeSettingsFromQuery(ast, safety_settings);

        /// The whole clause held only the cap, so it must be pruned (no bare `SETTINGS`).
        EXPECT_EQ(nullptr, backup_query->settings) << "empty BACKUP SETTINGS node not pruned";
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("max_execution_time")) << "kept a safety setting: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// BACKUP carrier with a surviving non-safety setting: the cap is stripped, the clause stays.
    {
        const String query = "BACKUP TABLE t TO Null SETTINGS max_execution_time = 0, async = true";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        auto * backup_query = ast->as<ASTBackupQuery>();
        ASSERT_NE(nullptr, backup_query);
        ASSERT_NE(nullptr, backup_query->settings) << "dropped a non-safety BACKUP setting";
        EXPECT_EQ(nullptr, backup_query->settings->as<ASTSetQuery>()->changes.tryGet("max_execution_time"))
            << "kept a safety setting in the BACKUP clause";
        EXPECT_NE(nullptr, backup_query->settings->as<ASTSetQuery>()->changes.tryGet("async"))
            << "dropped the non-safety `async` setting";
        const String formatted = ast->formatWithSecretsOneLine();
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// CREATE storage carrier (prune miss): the only setting is a safety cap, so after stripping the
    /// ASTStorage SETTINGS node is empty and must be pruned, or formatImpl emits a bare `SETTINGS`.
    {
        const String query = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS max_rows_to_read = 0";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty CREATE storage SETTINGS not pruned";
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("max_rows_to_read")) << "kept a safety setting: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse (bare SETTINGS): " << formatted;
    }

    /// CREATE storage carrier with a real engine setting: the cap is stripped, the engine setting and
    /// the SETTINGS clause stay.
    {
        const String query
            = "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS max_rows_to_read = 0, index_granularity = 1024";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "query: " << query;
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_NE(String::npos, formatted.find("index_granularity")) << "dropped the engine setting: " << formatted;
        EXPECT_EQ(String::npos, formatted.find("max_rows_to_read")) << "kept a safety setting: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// `CREATE ... AS SELECT ... SETTINGS <only safety>` parks the cap in the inherited
    /// ASTQueryWithOutput::settings_ast (the query-level SETTINGS), which must also be pruned.
    {
        const String query = "CREATE TABLE t ENGINE = MergeTree ORDER BY a AS SELECT number AS a FROM numbers(100) "
                             "SETTINGS max_rows_to_read = 0";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty CREATE AS SELECT SETTINGS not pruned";
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("max_rows_to_read")) << "kept a safety setting: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }
}

/// max_rows_to_read + read_overflow_mode = break bound the number of chunks a fuzzed query reads, but
/// not the size of the first chunk. The block-forming settings decide that size: for a trivial
/// INSERT ... SELECT into a table that prefers large blocks, applyTrivialInsertSelectOptimization
/// copies min_insert_block_size_rows (default ~1M) into the SELECT's max_block_size, so one ~1M-row
/// block can still reach the part writer and spend minutes in a single pipeline task that the cancel
/// callback (which only runs between tasks) cannot interrupt. The fuzzer therefore pins both block-
/// forming settings small AND strips the query's own overrides of them, so a fuzzed
/// `SETTINGS min_insert_block_size_rows = <large>` (or `= DEFAULT`, which would reset the pinned small
/// value back to the ~1M default) cannot re-inflate the first chunk. This checks the strip half for the
/// two block-forming names across the carriers the fuzzer uses.
TEST(RemoveSettingsFromQuery, StripsBlockFormingOverrides)
{
    /// The full set the server-side AST fuzzer strips, including the two block-forming settings.
    static constexpr std::string_view safety_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
        "max_block_size",
        "min_insert_block_size_rows",
    };

    /// Both block-forming settings appear as `= value`, `= DEFAULT`, repeated, and on the INSERT carrier
    /// (the path the bot reported). After the strip none of them may survive in any clause.
    const std::vector<String> queries = {
        "SELECT sum(number) FROM numbers(100) SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000",
        "INSERT INTO t SELECT number FROM numbers(100) SETTINGS min_insert_block_size_rows = 1000000",
        "SELECT 1 SETTINGS max_block_size = DEFAULT, min_insert_block_size_rows = DEFAULT",
        "SELECT 1 SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_rows = 1000000",
    };

    for (const auto & query : queries)
    {
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        /// No block-forming override may survive, in either list, in any clause (a `= DEFAULT` survivor
        /// would reset the pinned small value back to the large default on re-parse).
        EXPECT_EQ(0u, countSettingOccurrences(ast, "max_block_size"))
            << "max_block_size survived for: " << query;
        EXPECT_EQ(0u, countSettingOccurrences(ast, "min_insert_block_size_rows"))
            << "min_insert_block_size_rows survived for: " << query;

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "empty SETTINGS left for: " << query;

        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_EQ(String::npos, formatted.find("max_block_size")) << "kept max_block_size: " << formatted;
        EXPECT_EQ(String::npos, formatted.find("min_insert_block_size_rows")) << "kept min_insert_block_size_rows: " << formatted;
        ParserQuery reparser(formatted.data() + formatted.size());
        ASTPtr reparsed = parseQuery(reparser, formatted, "", 0, 0, 0);
        EXPECT_NE(nullptr, reparsed) << "did not re-parse: " << formatted;
    }

    /// A non-safety setting alongside a block-forming override survives; the SETTINGS clause stays.
    {
        const String query = "SELECT 1 SETTINGS min_insert_block_size_rows = 1000000, max_threads = 4";
        ParserQuery parser(query.data() + query.size());
        ASTPtr ast = parseQuery(parser, query, "", 0, 0, 0);
        ASSERT_NE(nullptr, ast) << "query: " << query;

        removeSettingsFromQuery(ast, safety_settings);

        EXPECT_FALSE(hasEmptySettingsNode(ast)) << "query: " << query;
        const String formatted = ast->formatWithSecretsOneLine();
        EXPECT_NE(String::npos, formatted.find("max_threads")) << "dropped a non-safety setting: " << formatted;
        EXPECT_EQ(String::npos, formatted.find("min_insert_block_size_rows")) << "kept a block-forming setting: " << formatted;
    }
}
