#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/QueryFuzzer.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_TABLE;
}

using namespace DB;

/// A table name QueryFuzzer rewrote into `{fuzz_param_N:Identifier}` has empty name parts, so
/// reading it through ASTTableIdentifier::getTableId() throws UNKNOWN_TABLE, out of fuzzMain. Only
/// the fuzzer's own rewrite builds such an identifier: a client's `{p:Identifier}` is substituted
/// before the server-side fuzzer sees the AST, which is why this coverage is not a SQL test.
TEST(QueryFuzzer, DoesNotThrowOnParameterizedTableName)
{
    const String sql = "SELECT count() FROM {p:Identifier}";

    for (UInt64 seed = 0; seed < 100; ++seed)
    {
        ParserQuery parser(sql.data() + sql.size());
        ASTPtr base = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0, 0);
        QueryFuzzer fuzzer{pcg64(seed)};

        for (int step = 0; step < 4; ++step)
        {
            ASTPtr fuzzed = base->clone();
            try
            {
                fuzzer.fuzzMain(fuzzed);
            }
            catch (const Exception & e)
            {
                ASSERT_NE(e.code(), ErrorCodes::UNKNOWN_TABLE)
                    << "seed=" << seed << " step=" << step << ": " << e.message();
            }
            catch (...) // Ok: a non-DB failure says nothing about the name-resolution path
            {
            }
        }
    }
}
