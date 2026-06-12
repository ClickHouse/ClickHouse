#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/BloomFilter.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <IO/WriteHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int LOGICAL_ERROR;
}

/// Special function for JOIN runtime filtering
/// Syntax: __applyFilter(filter_id, key)
/// - filter_id: a String const whose VALUE is the per-plan-build rendezvous id under which the
///   runtime filter is registered in the query-context `IRuntimeFilterLookup`; the build side
///   registers under the same id. Its DAG result NAME is the STABLE structural id
///   (`_runtime_filter_<hash>`), which is what travels into EXPLAIN and the plan-step hash. The const
///   is marked `is_deterministic_constant=false`, so the cache-key paths skip its (volatile) VALUE
///   while still keying on the stable NAME — the id can therefore be serialized for distributed
///   propagation without destabilising plan hashes.
/// - key: Value of any type that is checked to be present in the filter.
/// An empty id (e.g. a placeholder/inert plan) makes the function pass all rows.
/// Returns false if the key should be filtered.
class FunctionApplyFilter final : public IFunction
{
public:
    static constexpr auto name = "__applyFilter";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionApplyFilter>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    /// A runtime filter's result is not a pure function of its arguments — it depends on the
    /// dynamically built filter, which differs between executions of the same plan (e.g. recursive
    /// CTE iterations or materialized-view blocks). `isDeterministic() == false` keeps it out of
    /// the query condition cache (which keys on the now-deterministic filter expression and would
    /// otherwise serve a stale per-granule result to a later execution with different keys). We
    /// keep `isDeterministicInScopeOfQuery() == true` so the filter can still be pushed into
    /// PREWHERE: within a single read the built filter is fixed, so the predicate is stable there.
    bool isDeterministic() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be 2",
                            getName(), arguments.size());

        if (!WhichDataType(arguments[0]).isString())
            throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of function '{}' must be a String filter id",
                    getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// The rendezvous id is the VALUE of the const first argument (set at plan build, the same id
        /// the build side registers under). An empty id means an inert/placeholder filter: all rows pass.
        String filter_id;
        if (const auto * id_const = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            filter_id = id_const->getValue<String>();
        else if (const auto * id_column = checkAndGetColumn<ColumnString>(arguments[0].column.get()); id_column && !id_column->empty())
            filter_id = String(id_column->getDataAt(0));

        if (filter_id.empty())
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        auto query_context = CurrentThread::tryGetQueryContext();
        if (!query_context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for {}", getName());
        auto filter_lookup = query_context->getRuntimeFilterLookup();
        if (!filter_lookup)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter lookup was not initialized");

        /// Look up the filter by the rendezvous id; if it has not been registered/built yet, all rows pass.
        auto filter = filter_lookup->find(filter_id);
        if (!filter)
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        const auto & data_column = arguments[1];

        return filter->find(data_column);
    }
};

REGISTER_FUNCTION(FilterContains)
{
    factory.registerFunction<FunctionApplyFilter>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS, FunctionFactory::Case::Sensitive);
}

}
