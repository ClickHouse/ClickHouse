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

/// QueryFuzzer sometimes rewrites a table name into `{fuzz_param_N:Identifier}` and then keeps
/// fuzzing that AST. Such an identifier carries empty name parts, so reading its name through
/// ASTTableIdentifier::getTableId() throws UNKNOWN_TABLE ("Both table name and UUID are empty")
/// instead of yielding an empty StorageID, and the throw leaves fuzzMain. A caller that runs the
/// fuzzer repeatedly (the server-side fuzzer of `ast_fuzzer_runs`) then loses its remaining runs.
/// A client's own `{p:Identifier}` cannot cover this: query parameters are substituted before the
/// server-side fuzzer sees the AST, so only the fuzzer's own rewrite produces the identifier.
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
