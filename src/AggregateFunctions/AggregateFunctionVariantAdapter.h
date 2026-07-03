#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeVariant.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>

#include <vector>


namespace DB
{

/** An adapter (despite the "Adapter" name it is used exactly like the "Null" combinator adapter) that allows
  * aggregate functions that do not natively support the Variant data type to be applied to Variant arguments.
  *
  * A value of type Variant(T1, ..., TN) is, in each row, a value of one of the types Ti or NULL. To aggregate
  * such a column with a function like sum/avg/min/max we cast the whole Variant argument to the least common
  * supertype of its nested types, wrapped in Nullable (so that the implicit NULLs of the Variant are preserved
  * and skipped by the aggregation, exactly as ordinary NULLs are). In other words,
  *
  *     f(variant) == f(CAST(variant AS Nullable(supertype(T1, ..., TN))))
  *
  * The adapter wraps the nested aggregate function that was resolved for the supertype arguments: it converts
  * the Variant argument columns on the fly and forwards everything else (state management, merge, serialization,
  * result insertion) directly to the nested function, whose state layout it shares as-is.
  *
  * The adapter is instantiated by AggregateFunctionFactory only as a fallback: it is applied when the requested
  * function rejects the Variant argument, so aggregate functions that handle Variant themselves are unaffected.
  */
class AggregateFunctionVariantAdapter final : public IAggregateFunctionHelper<AggregateFunctionVariantAdapter>
{
private:
    AggregateFunctionPtr nested_function;
    /// Argument types expected by the nested function: each Variant argument is replaced by Nullable(supertype).
    DataTypes nested_argument_types;
    /// Which argument positions are Variant and thus need conversion before being passed to the nested function.
    std::vector<UInt8> is_variant_argument;
    size_t num_arguments;

    /// Cache of the internal cast functions (Variant -> Nullable(supertype)) reused across blocks.
    mutable InternalCastFunctionCache cast_cache;

    /// The number of columns passed to an add* method: the arguments plus, optionally, a trailing "if" flag column.
    size_t numColumns(ssize_t if_argument_pos) const
    {
        if (if_argument_pos >= 0)
            return std::max(num_arguments, static_cast<size_t>(if_argument_pos) + 1);
        return num_arguments;
    }

    /// Owns the converted columns and exposes a pointer array that mirrors the incoming `columns`,
    /// with the Variant arguments replaced by their Nullable(supertype) counterparts.
    struct ConvertedColumns
    {
        Columns holder;
        std::vector<const IColumn *> columns;

        const IColumn ** data() { return columns.data(); }
    };

    ConvertedColumns castArguments(const IColumn ** columns, size_t num_columns) const
    {
        ConvertedColumns result;
        result.columns.resize(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
        {
            if (i < num_arguments && is_variant_argument[i])
            {
                ColumnPtr converted = castColumn({columns[i]->getPtr(), argument_types[i], {}}, nested_argument_types[i], &cast_cache);
                result.columns[i] = converted.get();
                result.holder.push_back(std::move(converted));
            }
            else
                result.columns[i] = columns[i];
        }
        return result;
    }

public:
    AggregateFunctionVariantAdapter(
        AggregateFunctionPtr nested_function_,
        const DataTypes & arguments,
        const DataTypes & nested_arguments,
        const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionVariantAdapter>(arguments, params, nested_function_->getResultType())
        , nested_function(std::move(nested_function_))
        , nested_argument_types(nested_arguments)
        , num_arguments(arguments.size())
    {
        is_variant_argument.reserve(num_arguments);
        for (const auto & type : arguments)
            is_variant_argument.push_back(isVariant(type));
    }

    String getName() const override
    {
        /// This is just an adapter, the function is named the same as the nested function itself.
        return nested_function->getName();
    }

    bool isVersioned() const override { return nested_function->isVersioned(); }
    size_t getVersionFromRevision(size_t revision) const override { return nested_function->getVersionFromRevision(revision); }
    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    /// The state layout is exactly the nested function's state, so all state operations are forwarded directly.
    void create(AggregateDataPtr __restrict place) const override { nested_function->create(place); }
    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroy(place); }
    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroyUpToState(place); }
    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }
    size_t sizeOfData() const override { return nested_function->sizeOfData(); }
    size_t alignOfData() const override { return nested_function->alignOfData(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// Rare, non-batched path: convert the single row to the nested argument types and delegate.
        Columns holder;
        std::vector<const IColumn *> nested_columns(num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
        {
            ColumnPtr one_row = columns[i]->cut(row_num, 1);
            if (is_variant_argument[i])
                one_row = castColumn({one_row, argument_types[i], {}}, nested_argument_types[i], &cast_cache);
            nested_columns[i] = one_row.get();
            holder.push_back(std::move(one_row));
        }
        nested_function->add(place, nested_columns.data(), 0, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        auto converted = castArguments(columns, num_arguments);
        nested_function->addManyDefaults(place, converted.data(), length, arena);
    }

    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        auto converted = castArguments(columns, numColumns(if_argument_pos));
        nested_function->addBatch(row_begin, row_end, places, place_offset, converted.data(), arena, if_argument_pos);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        auto converted = castArguments(columns, numColumns(if_argument_pos));
        nested_function->addBatchSinglePlace(row_begin, row_end, place, converted.data(), arena, if_argument_pos);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        auto converted = castArguments(columns, numColumns(if_argument_pos));
        nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, place, converted.data(), null_map, arena, if_argument_pos);
    }

    void addBatchArray(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        const UInt64 * offsets,
        Arena * arena) const override
    {
        auto converted = castArguments(columns, num_arguments);
        nested_function->addBatchArray(row_begin, row_end, places, place_offset, converted.data(), offsets, arena);
    }

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override
    {
        auto converted = castArguments(columns, num_arguments);
        nested_function->addBatchLookupTable8(row_begin, row_end, map, place_offset, std::move(init), key, converted.data(), arena);
    }

    bool isParallelizeMergePrepareNeeded() const override { return nested_function->isParallelizeMergePrepareNeeded(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_function->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_function->canOptimizeEqualKeysRanges(); }

    void mergeImpl(
        AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_function->merge(place, rhs, thread_pool, is_cancelled, arena);
    }

    void parallelizeMergeMulti(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_function->parallelizeMergeMulti(places, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);
    }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }
    bool isState() const override { return nested_function->isState(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
