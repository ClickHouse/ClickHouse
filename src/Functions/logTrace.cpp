#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    class FunctionLogTrace final : public IFunction
    {
    public:
        static constexpr auto name = "logTrace";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLogTrace>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        /// Do not emit the log message during query analysis, and actually run the function for each block during execution.
        bool isSuitableForConstantFolding() const override { return false; }

        /// The function has an observable execution-time side effect (a trace message per block), so it must run for
        /// every block and must not be dropped by deterministic-only optimizations. `ActionsDAG` merges structurally
        /// equivalent deterministic nodes, and `QueryResultCache` only refuses to cache queries that contain a
        /// function with `isDeterministic() == false`; either of these would otherwise skip the per-block logging.
        bool isDeterministic() const override { return false; }
        bool isDeterministicInScopeOfQuery() const override { return false; }

        /// Marking the function stateful preserves its per-block side effect against optimizations that would
        /// otherwise reduce how many blocks reach it. Optimizations that move or split expressions guard on
        /// `hasStatefulFunctions()` and skip stateful functions. In particular, `FilterStep` splits a
        /// `WHERE a AND logTrace(...)` chain into separate filter transforms, which would run the other conditions
        /// first and evaluate `logTrace` only on the surviving blocks - logging fewer times than there are input
        /// blocks. The same guard keeps `splitFilter`, `filterPushDown`, and redundant-sort removal from reordering
        /// the call, keeps `tryExecuteFunctionsAfterSorting` from lifting it above an `ORDER BY [... LIMIT]`, keeps
        /// lazy materialization from deferring it past the `LIMIT`, and keeps the top-K `ORDER BY ... LIMIT`
        /// optimizations (`optimizeTopK`, `topKThroughJoin`) from dropping source rows before it via a prewhere
        /// filter, skip-index pruning, or a sort pushed below a `JOIN`. The plain `LIMIT` paths check it too:
        /// the trivial-`LIMIT` source fast paths (`maxBlockSizeByLimit`, `mainQueryNodeBlockSizeByLimit`,
        /// `numbersLikeUtils::shouldPushdownLimit`), the generic limit pushdown (`tryPushDownLimit`,
        /// `optimizePrimaryKeyConditionAndLimit`), and the read-in-order `ORDER BY ... LIMIT` early termination
        /// (`buildSortingDAG`) all keep the limit from truncating the function's input. It also keeps
        /// `MergeTreeWhereOptimizer` from moving a deterministic sibling conjunct of a stateful `WHERE` into
        /// reader-side `PREWHERE`, which would prune granules before the stateful predicate runs, and keeps
        /// projection selection (`QueryDAG::build`, used by `optimizeUseNormalProjections` and
        /// `optimizeUseAggregateProjections`) from substituting a projection read or index whose different sort
        /// key and granularity would change the observed row and block stream, and keeps join reordering
        /// (`optimizeJoin`) from flattening an expression that wraps a child join into the global join graph,
        /// which could reattach the call at a different (reordered) join and change the rows and blocks it sees,
        /// and keeps `tryMergeFilters` from collapsing a stateful outer filter together with an inner filter
        /// (e.g. from a subquery or view boundary) into one `and(...)` filter, which would evaluate the stateful
        /// predicate on the inner filter's input instead of its output, and keeps `tryPushDownLimit` from setting
        /// the `DistinctStep` limit hint when a stateful expression sits below the distinct, because the distinct
        /// transforms stop reading input once the hint is reached. The AST-side detectors behind the trivial-`LIMIT`
        /// fast paths and `MergeTreeWhereOptimizer` also descend into SQL UDF bodies, so wrapping a stateful
        /// function (or `arrayJoin`) in `CREATE FUNCTION` does not bypass these fences.
        /// This mirrors other functions with block-level semantics such as `neighbor`.
        bool isStateful() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isString(arguments[0]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arguments[0]->getName(), getName());
            return std::make_shared<DataTypeUInt8>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            return execute(arguments, result_type, input_rows_count, /* dry_run= */ false);
        }

        /// Do not emit the log message during query analysis, e.g. while calculating the result header on an empty block.
        ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            return execute(arguments, result_type, input_rows_count, /* dry_run= */ true);
        }

        ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count, bool dry_run) const
        {
            String message;
            if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
                message = col->getDataAt(0);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be Constant string",
                    getName());

            if (!dry_run)
            {
                static auto log = getLogger("FunctionLogTrace");
                LOG_TRACE(log, fmt::runtime(message));
            }

            return DataTypeUInt8().createColumnConst(input_rows_count, 0);
        }
    };

}

REGISTER_FUNCTION(LogTrace)
{
    FunctionDocumentation::Description description = R"(
Emits a trace log message to the server log for each [Block](/development/architecture/#block).
    )";
    FunctionDocumentation::Syntax syntax = "logTrace(message)";
    FunctionDocumentation::Arguments arguments = {
        {"message", "Message that is emitted to the server log.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0` always.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic example",
        R"(
SELECT logTrace('logTrace message');
        )",
        R"(
┌─logTrace('logTrace message')─┐
│                            0 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLogTrace>(documentation);
}

}
