#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <absl/container/inlined_vector.h>


namespace DB
{

/** A wrapper that preserves the standard NULL-skipping contract of an aggregate function over Variant arguments.
  *
  * A value of type Variant(T1, ..., TN) is, in each row, a value of one of the types Ti or NULL. A Variant is not
  * Nullable, so the "Null" combinator is never applied to it, and an aggregate function that accepts the Variant
  * natively would otherwise process the NULL rows as ordinary values (e.g. `any` would return NULL from a group
  * that has non-NULL values, contradicting its documented "ignores NULLs by default" contract, and `groupArray`
  * would store the NULLs its documentation promises to remove). This wrapper restores the contract: rows where a
  * Variant argument is NULL are skipped, exactly as the "Null" combinator skips rows with NULL values of Nullable
  * arguments,
  *
  *     f(variant) behaves like f(nullable) with respect to NULL values.
  *
  * Unlike the "Null" combinator, the argument columns are forwarded to the nested function unchanged (it was
  * resolved over the Variant argument types and expects the genuine Variant columns); only the rows are filtered.
  *
  * Also unlike the "Null" combinator, the result type, the state layout and the serialization are exactly the
  * nested function's: an all-NULL group produces the empty nested state and returns the nested function's result
  * for an empty set, without a Nullable promotion of the result and without a "was there a value" flag in front
  * of the nested state. A function that accepts a Variant natively already accepted it before this wrapper
  * existed, with exactly the nested result type and state representation, and both are persisted: promoting the
  * result to Nullable would change the type of `AggregateFunction(f, Variant(...))` columns read by materialized
  * views, and a flag byte would make aggregate states written by an older server unreadable after an upgrade
  * (and vice versa on a downgrade). For a function whose result type is the Variant itself (`any`, `argMin`, ...)
  * nothing is lost: a Variant represents NULL on its own, so an all-NULL group reports NULL anyway.
  *
  * The wrapper is applied by AggregateFunctionFactory at the point where a function is resolved over argument
  * types that contain a Variant (unless the function declares AggregateFunctionProperties::skips_variant_nulls,
  * i.e. implements the contract itself, like `count`, or is a window function, which must handle its argument
  * types itself -- see AggregateFunctionFactory::getImpl).
  */
class AggregateFunctionVariantNull final : public IAggregateFunctionHelper<AggregateFunctionVariantNull>
{
private:
    AggregateFunctionPtr nested_function;
    /// Which argument positions are Variant and therefore checked for NULL values.
    absl::InlinedVector<char, 8> is_variant_argument;

    bool isAnyVariantArgumentNullAt(const IColumn ** columns, size_t row_num) const
    {
        for (size_t i = 0; i < is_variant_argument.size(); ++i)
            if (is_variant_argument[i]
                && assert_cast<const ColumnVariant &>(*columns[i]).localDiscriminatorAt(row_num) == ColumnVariant::NULL_DISCRIMINATOR)
                return true;
        return false;
    }

public:
    AggregateFunctionVariantNull(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionVariantNull>(arguments, params, nested_function_->getResultType())
        , nested_function{nested_function_}
    {
        is_variant_argument.resize(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            is_variant_argument[i] = isVariant(arguments[i]);
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Variant arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }

    using IAggregateFunction::argument_types;
    using IAggregateFunction::parameters;

    AggregateFunctionPtr getAggregateFunctionForMergingFinal() const override
    {
        auto nested_function_for_merging_final = nested_function->getAggregateFunctionForMergingFinal();
        /// Create a new wrapper for merging final states if the nested function has a different implementation.
        if (nested_function_for_merging_final.get() != nested_function.get())
            return std::make_shared<AggregateFunctionVariantNull>(nested_function_for_merging_final, argument_types, parameters);
        return IAggregateFunction::getAggregateFunctionForMergingFinal();
    }

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (!this->haveSameDefinition(rhs))
            return false;

        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        return nested_function->canMergeStateFromDifferentVariant(*rhs_nested);
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override
    {
        auto rhs_nested = rhs.getNestedFunction();
        chassert(rhs_nested != nullptr);

        nested_function->mergeStateFromDifferentVariant(place, *rhs_nested, rhs_place, arena);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroyUpToState(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (isAnyVariantArgumentNullAt(columns, row_num))
            return;

        nested_function->add(place, columns, row_num, arena);
    }

    void addManyDefaults(
        AggregateDataPtr __restrict /*place*/,
        const IColumn ** /*columns*/,
        size_t /*length*/,
        Arena * /*arena*/) const override
    {
        /// The default value of a Variant is NULL, and NULL rows are skipped.
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (row_begin >= row_end)
            return;

        /// Combine the NULL rows of every Variant argument into a single null map for the nested batch method.
        /// The local discriminators are indexed by the row number, and NULL_DISCRIMINATOR is the same in the
        /// local and the global order, so the Variant's null map does not have to be materialized per argument.
        PaddedPODArray<UInt8> null_map(row_end, 0);
        for (size_t i = 0; i < is_variant_argument.size(); ++i)
        {
            if (!is_variant_argument[i])
                continue;
            const auto & discriminators = assert_cast<const ColumnVariant &>(*columns[i]).getLocalDiscriminators();
            for (size_t row = row_begin; row < row_end; ++row)
                null_map[row] |= (discriminators[row] == ColumnVariant::NULL_DISCRIMINATOR);
        }

        nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map.data(), arena, if_argument_pos);
    }

    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }
    bool isParallelizeMergePrepareNeeded() const override { return nested_function->isParallelizeMergePrepareNeeded(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_function->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_function->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
    }

    void mergeImpl(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override
    {
        nested_function->merge(place, rhs, thread_pool, is_cancelled, arena);
    }

    void parallelizeMergeMulti(
        AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_function->parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        /// The state representation is exactly the nested function's.
        nested_function->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    UnorderedSetWithMemoryTracking<size_t> getArgumentsThatCanBeOnlyNull() const override
    {
        return nested_function->getArgumentsThatCanBeOnlyNull();
    }

    /// Forward the ML prediction hooks (used by evalMLMethod via ColumnAggregateFunction::predictValues), so that
    /// a model trained over Variant arguments -- this wrapper is the function stored in the resulting state's
    /// AggregateFunction(...) type -- can still be used to predict.
    DataTypePtr getReturnTypeToPredict() const override { return nested_function->getReturnTypeToPredict(); }

    void predictValues(
        ConstAggregateDataPtr __restrict place,
        IColumn & to,
        const ColumnsWithTypeAndName & arguments,
        size_t offset,
        size_t limit,
        ContextPtr context) const override
    {
        nested_function->predictValues(place, to, arguments, offset, limit, context);
    }
};

}
