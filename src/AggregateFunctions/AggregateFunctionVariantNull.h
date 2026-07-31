#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
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
  * The result type mirrors the "Null" combinator rules: when the nested function does not return its default
  * value for an empty set (`returns_default_when_only_null` is false) and its result type can be wrapped in
  * Nullable, the result becomes Nullable and an all-NULL group yields NULL (`result_is_nullable`), which requires
  * a "was there a value" flag in front of the nested state, serialized before it. Otherwise the wrapper adds no
  * flag: its state layout and serialization are exactly the nested function's. In particular the result of a
  * function whose result type is the Variant itself (`any`, `argMin`, ...) needs no Nullable wrapping, because a
  * Variant represents NULL on its own: an all-NULL group produces the empty nested state, whose result is the
  * default value of the Variant, i.e. NULL.
  *
  * The wrapper is applied by AggregateFunctionFactory at the point where a function is resolved over argument
  * types that contain a Variant (unless the function declares AggregateFunctionProperties::skips_variant_nulls,
  * i.e. implements the contract itself, like `count`, or is a window function, which must handle its argument
  * types itself -- see AggregateFunctionFactory::getImpl).
  */
template <bool result_is_nullable>
class AggregateFunctionVariantNull final
    : public IAggregateFunctionHelper<AggregateFunctionVariantNull<result_is_nullable>>
{
private:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;
    /// Which argument positions are Variant and therefore checked for NULL values.
    absl::InlinedVector<char, 8> is_variant_argument;

    /** In addition to the data of the nested aggregate function, we keep a flag indicating
      * whether there was at least one row with a non-NULL value accumulated.
      * If there were none, the function returns NULL.
      *
      * We use prefix_size bytes for the flag to satisfy the alignment requirement of the nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        if constexpr (result_is_nullable)
            return place + prefix_size;
        else
            return place;
    }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        if constexpr (result_is_nullable)
            return place + prefix_size;
        else
            return place;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            place[0] = 1;
    }

    static bool getFlag(ConstAggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            return place[0];
        else
            return true;
    }

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
        : IAggregateFunctionHelper<AggregateFunctionVariantNull<result_is_nullable>>(
            arguments, params, createResultType(nested_function_))
        , nested_function{nested_function_}
        , prefix_size(result_is_nullable ? nested_function->alignOfData() : 0)
    {
        is_variant_argument.resize(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            is_variant_argument[i] = isVariant(arguments[i]);
    }

    static DataTypePtr createResultType(const AggregateFunctionPtr & nested_function_)
    {
        if constexpr (result_is_nullable)
            return makeNullable(nested_function_->getResultType());
        else
            return nested_function_->getResultType();
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
            return std::make_shared<AggregateFunctionVariantNull<result_is_nullable>>(
                nested_function_for_merging_final, argument_types, parameters);
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

        if constexpr (result_is_nullable)
            if (getFlag(rhs_place))
                setFlag(place);

        const size_t rhs_prefix_size = result_is_nullable ? rhs_nested->alignOfData() : 0;
        nested_function->mergeStateFromDifferentVariant(nestedPlace(place), *rhs_nested, rhs_place + rhs_prefix_size, arena);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroyUpToState(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (isAnyVariantArgumentNullAt(columns, row_num))
            return;

        setFlag(place);
        nested_function->add(nestedPlace(place), columns, row_num, arena);
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
        bool has_non_null = false;
        for (size_t i = 0; i < is_variant_argument.size(); ++i)
        {
            if (!is_variant_argument[i])
                continue;
            const auto & discriminators = assert_cast<const ColumnVariant &>(*columns[i]).getLocalDiscriminators();
            for (size_t row = row_begin; row < row_end; ++row)
                null_map[row] |= (discriminators[row] == ColumnVariant::NULL_DISCRIMINATOR);
        }
        for (size_t row = row_begin; row < row_end; ++row)
        {
            if (!null_map[row])
            {
                has_non_null = true;
                break;
            }
        }

        if (!has_non_null)
            return;

        setFlag(place);
        nested_function->addBatchSinglePlaceNotNull(
            row_begin, row_end, nestedPlace(place), columns, null_map.data(), arena, if_argument_pos);
    }

    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }
    bool isParallelizeMergePrepareNeeded() const override { return nested_function->isParallelizeMergePrepareNeeded(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_function->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        AggregateDataPtrs nested_places(places.begin(), places.end());
        for (auto & nested_place : nested_places)
            nested_place = nestedPlace(nested_place);

        nested_function->parallelizeMergePrepare(nested_places, thread_pool, is_cancelled);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
            if (getFlag(rhs))
                setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
    }

    void mergeImpl(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override
    {
        if constexpr (result_is_nullable)
            if (getFlag(rhs))
                setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), thread_pool, is_cancelled, arena);
    }

    void parallelizeMergeMulti(
        AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
            for (size_t i = 1; i < places.size(); ++i)
                if (getFlag(places[i]))
                {
                    setFlag(places[0]);
                    break;
                }

        AggregateDataPtrs nested_places(places.size());
        for (size_t i = 0; i < places.size(); ++i)
            nested_places[i] = nestedPlace(places[i]);

        nested_function->parallelizeMergeMulti(nested_places, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if constexpr (result_is_nullable)
        {
            bool flag = getFlag(place);
            writeBinary(flag, buf);
            if (flag)
                nested_function->serialize(nestedPlace(place), buf, version);
        }
        else
        {
            /// Without the flag, the state representation is exactly the nested function's.
            nested_function->serialize(nestedPlace(place), buf, version);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            bool flag = false;
            readBinary(flag, buf);
            if (flag)
            {
                setFlag(place);
                nested_function->deserialize(nestedPlace(place), buf, version, arena);
            }
        }
        else
        {
            nested_function->deserialize(nestedPlace(place), buf, version, arena);
        }
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        if constexpr (result_is_nullable)
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            if (getFlag(place))
            {
                if constexpr (merge)
                    nested_function->insertMergeResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                else
                    nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(false);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            if constexpr (merge)
                nested_function->insertMergeResultInto(nestedPlace(place), to, arena);
            else
                nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
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
        nested_function->predictValues(nestedPlace(place), to, arguments, offset, limit, context);
    }
};

}
