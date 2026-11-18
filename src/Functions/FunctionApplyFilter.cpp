#include <memory>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/Context.h>
#include <Interpreters/BloomFilter.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <IO/WriteHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/FunctionDocumentation.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event RuntimeFilterLookupsBeforeBuildFinished;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// `mask` is a `UInt8` mask, or a constant. The result type is `LowCardinality(UInt8)` for a `LowCardinality` key, see
/// `FunctionApplyFilter::getReturnTypeImpl`.
ColumnPtr toResultType(const ColumnPtr & mask, const DataTypePtr & result_type)
{
    const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(result_type.get());
    if (!low_cardinality_type)
        return mask;

    if (const auto * mask_const = typeid_cast<const ColumnConst *>(mask.get()))
        return result_type->createColumnConst(mask->size(), mask_const->getField());

    const auto * mask_column = typeid_cast<const ColumnUInt8 *>(mask.get());
    if (!mask_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Runtime filter returned a mask of unexpected column {}", mask->getName());

    /// The dictionary holds only false and true, so each row is indexed without a lookup.
    auto dictionary = DataTypeLowCardinality::createColumnUnique(*low_cardinality_type->getDictionaryType());
    const auto index_of_false = static_cast<UInt8>(dictionary->uniqueInsert(Field(UInt64(0))));
    const auto index_of_true = static_cast<UInt8>(dictionary->uniqueInsert(Field(UInt64(1))));

    const auto & mask_data = mask_column->getData();
    auto indexes = ColumnUInt8::create(mask_data.size());
    auto & indexes_data = indexes->getData();
    for (size_t row = 0; row < mask_data.size(); ++row)
        indexes_data[row] = mask_data[row] ? index_of_true : index_of_false;

    return ColumnLowCardinality::create(MutableColumnPtr(std::move(dictionary)), MutableColumnPtr(std::move(indexes)), /*is_shared=*/false);
}

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
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionApplyFilter>(/*passes_key_as_is_=*/true); }

    explicit FunctionApplyFilter(bool passes_key_as_is_)
        : passes_key_as_is(passes_key_as_is_)
    {
    }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isVariadic() const override { return false; }
    [[nodiscard]] bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    /// A runtime filter's result is not a pure function of its arguments — it depends on the
    /// dynamically built filter, which differs between executions of the same plan (e.g. recursive
    /// CTE iterations or materialized-view blocks). `isDeterministic() == false` keeps it out of
    /// the query condition cache (which keys on the now-deterministic filter expression and would
    /// otherwise serve a stale per-granule result to a later execution with different keys). We
    /// keep `isDeterministicInScopeOfQuery() == true` so the filter can still be pushed into
    /// PREWHERE: within a single read the built filter is fixed, so the predicate is stable there.
    [[nodiscard]] bool isDeterministic() const override { return false; }

    [[nodiscard]] bool isSuitableForConstantFolding() const override { return false; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(String, Any) -> UInt8";
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    /// The default implementations would reduce a constant, `LowCardinality`, sparse or replicated key to the values
    /// that it stores, and the statistics of the filter would count those values instead of the rows. They decide
    /// whether the filter is worth evaluating, so `RuntimeFilter::find` gets the key as it is.
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return !passes_key_as_is; }
    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return !passes_key_as_is; }
    [[nodiscard]] bool useDefaultImplementationForSparseColumns() const override { return !passes_key_as_is; }
    [[nodiscard]] bool useDefaultImplementationForReplicatedColumns() const override { return !passes_key_as_is; }

    /// Without the default implementation for `LowCardinality`, the result type for a `LowCardinality` key would be
    /// `UInt8` instead of `LowCardinality(UInt8)`. The type is in serialized plans, which a server of another version
    /// may read, so it is deduced with that implementation.
    using IFunction::getReturnTypeImpl;
    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (passes_key_as_is && hasLowCardinalityTypes(arguments))
            return FunctionToOverloadResolverAdaptor(std::make_shared<FunctionApplyFilter>(/*passes_key_as_is_=*/false))
                .getReturnType(arguments);

        return IFunction::getReturnTypeImpl(arguments);
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return toResultType(applyFilter(arguments, input_rows_count), result_type);
    }

private:
    /// Off only in the function that deduces the result type, see `getReturnTypeImpl`.
    const bool passes_key_as_is;

    /// Returns a `UInt8` mask, or a constant.
    [[nodiscard]] ColumnPtr applyFilter(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
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

        /// Look up the filter by the rendezvous id; if it has not been registered yet, all rows pass.
        auto filter = filter_lookup->find(filter_id);
        if (!filter)
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        /// A registered filter may still be building: it becomes findable after the first build
        /// stream registers it, but is ready only after the last stream merges. The join itself
        /// keeps the result correct, so the block passes through unfiltered; count it so the
        /// window is observable.
        if (!filter->isReady())
        {
            ProfileEvents::increment(ProfileEvents::RuntimeFilterLookupsBeforeBuildFinished);
            return DataTypeUInt8().createColumnConst(input_rows_count, true);
        }

        const auto & data_column = arguments[1];

        return filter->find(data_column);
    }
};

REGISTER_FUNCTION(FilterContains)
{
    factory.registerFunction<FunctionApplyFilter>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS, FunctionFactory::Case::Sensitive);
}

}
