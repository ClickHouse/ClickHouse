#include <gtest/gtest.h>

#include <Storages/HybridSegmentPruner.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

ASTPtr parseExpression(const std::string & text)
{
    ParserExpression parser;
    return parseQuery(parser, text, 4096, 1000, 1000000);
}

NamesAndTypesList hybridColumnsForTests()
{
    return {
        {"ts", std::make_shared<DataTypeDateTime>()},
        {"date", std::make_shared<DataTypeDate>()},
        {"customerid", std::make_shared<DataTypeUInt64>()},
        {"x", std::make_shared<DataTypeInt64>()},
        {"y", std::make_shared<DataTypeInt64>()},
    };
}

/// Build a HybridSegmentPruner over `where_text` and ask whether `segment_text` can be pruned.
/// The user-side ActionsDAG is built via the same `TreeRewriter + ExpressionAnalyzer` idiom
/// the planner uses to populate `query_info.filter_actions_dag`.
bool canPrune(const std::string & where_text, const std::string & segment_text)
{
    auto context = getContext().context;
    auto cols = hybridColumnsForTests();

    auto where_ast = parseExpression(where_text);
    auto syntax_result = TreeRewriter(context).analyze(where_ast, cols);
    /// `add_aliases=true` projects the DAG outputs to the predicate only, mirroring the shape of
    /// the analyzer-built `query_info.filter_actions_dag` (one output = the filter expression).
    /// With `add_aliases=false` the source columns are also kept as outputs, so `getOutputs().at(0)`
    /// can point to an input column instead of the predicate.
    auto dag = ExpressionAnalyzer(where_ast, syntax_result, context).getActionsDAG(true);

    ActionsDAGWithInversionPushDown inverted(dag.getOutputs().at(0), context, /* boolean_context */ true);
    HybridSegmentPruner pruner(inverted, cols, context);

    return pruner.canBePruned(parseExpression(segment_text));
}

class HybridSegmentPrunerTest : public ::testing::Test
{
public:
    static void SetUpTestSuite()
    {
        tryRegisterFunctions();
    }
};

}

TEST_F(HybridSegmentPrunerTest, RangeContradictionPrunes)
{
    /// `ts > '2025-10-01'` (user) ∧ `ts <= '2025-09-01'` (segment) is unsat → prune.
    EXPECT_TRUE(canPrune("ts > '2025-10-01'", "ts <= '2025-09-01'"));
}

TEST_F(HybridSegmentPrunerTest, OverlappingRangeKeeps)
{
    /// `ts > '2025-10-01'` (user) ∧ `ts > '2025-08-01'` (segment) is satisfiable → keep.
    EXPECT_FALSE(canPrune("ts > '2025-10-01'", "ts > '2025-08-01'"));
}

TEST_F(HybridSegmentPrunerTest, BoundedDnfWithConstantFolding)
{
    /// `(date = yesterday() AND customerid IN (2, 3)) OR (date = today() AND customerid IN (2, 3))`
    /// (user) ∧ `date < '2015-01-01'` (segment): KeyCondition handles the OR by itself; the segment
    /// hyperrectangle on `date` is (-∞, '2015-01-01'), which excludes both yesterday() and today().
    EXPECT_TRUE(canPrune(
        "(date = yesterday() AND customerid IN (2, 3)) OR (date = today() AND customerid IN (2, 3))",
        "date < '2015-01-01'"));
}

TEST_F(HybridSegmentPrunerTest, OrAlternativeNotMandatoryConstraint)
{
    /// `(x < 0 OR y = 1) AND x > 5` (user) ∧ `x > 0` (segment): the OR's `y = 1` branch is
    /// satisfiable inside the segment hyperrectangle (e.g. x = 10, y = 1) → keep.
    EXPECT_FALSE(canPrune("(x < 0 OR y = 1) AND x > 5", "x > 0"));
}

TEST_F(HybridSegmentPrunerTest, UnsupportedAtomInOrKeeps)
{
    /// The OR contains an atom KeyCondition can't analyze (`length(toString(x)) > 10`),
    /// so it conservatively keeps the segment.
    EXPECT_FALSE(canPrune("(length(toString(x)) > 10 OR x = 1) AND x = 2", "x > 0"));
}
