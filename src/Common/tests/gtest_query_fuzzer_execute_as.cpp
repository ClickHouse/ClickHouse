#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/QueryFuzzer.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{
ASTPtr parseOne(const String & sql)
{
    ParserQuery parser(sql.data() + sql.size());
    return parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);
}
}

/// `ASTExecuteAsQuery` keeps `target_user` and `subquery` both as members and as entries of
/// `children`. The AST fuzzer walks `children` and can replace a child slot in place. If a member
/// did not own the node it points to, replacing that slot would free the node and leave the member
/// dangling; `formatQueryImpl` then dereferences it -- the client-side SIGSEGV reported by the
/// fuzzer.
///
/// Deterministic oracle: after replacing the `children` slot the member aliases, the member must
/// still point at the ORIGINAL node (kept alive by its own ownership) and format to the original
/// text. On the buggy (non-owning) version the original node is freed here, so this either crashes
/// or formats garbage; on the fixed version it is stable.
TEST(QueryFuzzer, ExecuteAsMemberOutlivesChildReplacement)
{
    ASTPtr ast = parseOne("EXECUTE AS u SELECT 1");
    ASSERT_NE(nullptr, ast);
    auto * execute_as = ast->as<ASTExecuteAsQuery>();
    ASSERT_NE(nullptr, execute_as);
    ASSERT_NE(nullptr, execute_as->target_user);
    ASSERT_NE(nullptr, execute_as->subquery);

    const String target_user_before = execute_as->target_user->formatWithSecretsOneLine();
    const String subquery_before = execute_as->subquery->formatWithSecretsOneLine();

    /// Replace every child slot in place with a fresh node, dropping the only reference `children`
    /// held to the old nodes -- exactly what `QueryFuzzer::fuzz(ast->children)` can do.
    for (auto & child : execute_as->children)
        child = parseOne("SELECT 2");

    /// The members must have kept their nodes alive and unchanged.
    ASSERT_NE(nullptr, execute_as->target_user);
    ASSERT_NE(nullptr, execute_as->subquery);
    EXPECT_EQ(execute_as->target_user->formatWithSecretsOneLine(), target_user_before);
    EXPECT_EQ(execute_as->subquery->formatWithSecretsOneLine(), subquery_before);

    /// And the whole query still formats without crashing.
    const String formatted = ast->formatWithSecretsOneLine();
    EXPECT_NE(formatted.find("EXECUTE AS"), String::npos);
}

/// Same bug, exercised through the real fuzzer over a fixed range of seeds, formatting the result
/// each round exactly as the client does. Complements the deterministic oracle above by covering
/// the actual mutation paths (including the ALTER->lightweight rewrite). Fails (crashes) on the
/// non-owning version.
TEST(QueryFuzzer, ExecuteAsFormatDoesNotCrash)
{
    const String sql = "EXECUTE AS u ALTER TABLE t (UPDATE c = 1 WHERE k = 1)";

    for (UInt64 seed = 0; seed < 300; ++seed)
    {
        ASTPtr base = parseOne(sql);
        ASSERT_NE(nullptr, base);
        QueryFuzzer fuzzer{pcg64(seed)};

        /// Feed the same query repeatedly so the persistent fuzzer accumulates fragments and
        /// exercises the child-replacement paths across several mutation rounds.
        for (int step = 0; step < 8; ++step)
        {
            ASTPtr fuzzed = base->clone();
            try
            {
                fuzzer.fuzzMain(fuzzed);
            }
            catch (...)
            {
                /// The fuzzer can build queries whose mutation throws; that is fine (unrelated to the
                /// child/member ownership invariant under test), we only require it does not crash.
                (void)getCurrentExceptionMessage(false);
                continue;
            }

            /// The client formats every fuzzed tree; this is the crash site. Formatting may
            /// legitimately throw, but it must never crash.
            try
            {
                (void)fuzzed->formatWithSecretsOneLine();
            }
            catch (...)
            {
                /// A thrown formatting exception is fine, we only require it does not crash.
                (void)getCurrentExceptionMessage(false);
            }
        }
    }

    SUCCEED();
}
