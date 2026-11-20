#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Interpreters/castColumn.h>


namespace DB
{

/** Allows to apply an aggregate function that does not support the `Variant` type to `Variant` arguments.
  * Every `Variant` argument is cast to `Nullable` of the least common supertype of its alternatives:
  *
  *     f(variant) == f(CAST(variant AS Nullable(supertype(T1, ..., TN))))
  *
  * The NULL values of the `Variant` become NULL values of `Nullable` and are skipped as usual.
  * For combinators like -ForEach and -Map, the `Variant` inside the argument is cast: `Array(Variant(...))` to `Array(Nullable(...))`.
  * The adapter shares the state of the nested function, which is created for the converted argument types.
  * It is applied by `AggregateFunctionFactory` according to `AggregateFunctionProperties::variant_arguments_conversion`.
  */
class AggregateFunctionVariantAdapter final : public IAggregateFunctionHelper<AggregateFunctionVariantAdapter>
{
private:
    AggregateFunctionPtr nested_function;
    DataTypes nested_argument_types;
    size_t num_arguments;
    mutable InternalCastFunctionCache cast_cache;

    /// Holds the converted columns and the array of pointers to pass to the nested function.
    struct ConvertedColumns
    {
        Columns holder;
        std::vector<const IColumn *> pointers; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        /// If the rows are filtered by the condition, the original numbers of the remaining rows.
        PaddedPODArray<UInt64> row_numbers;
    };

    ConvertedColumns convert(const IColumn ** columns) const
    {
        ConvertedColumns res;
        res.pointers.assign(columns, columns + num_arguments);
        for (size_t i = 0; i < num_arguments; ++i)
        {
            if (argument_types[i]->equals(*nested_argument_types[i]))
                continue;
            res.holder.push_back(castColumn({columns[i]->getPtr(), argument_types[i], ""}, nested_argument_types[i], &cast_cache));
            res.pointers[i] = res.holder.back().get();
        }
        return res;
    }

    /// Converts only the rows in [row_begin, row_end). If it is not the whole block, the columns are cut to this range
    /// and the rows are renumbered from zero. Otherwise, e.g. a window function that adds one row at a time
    /// would convert the whole block for every row.
    /// If there is an `if` argument (from the -If combinator), only the rows where it is true are kept, and the condition
    /// is not passed to the nested function: the `Null` combinator inside it does not support the `if` argument.
    ConvertedColumns convertRange(size_t row_begin, size_t row_end, const IColumn ** columns, ssize_t if_argument_pos = -1) const
    {
        if (if_argument_pos < 0 && row_begin == 0 && row_end == columns[0]->size())
            return convert(columns);

        IColumn::Filter filter;
        PaddedPODArray<UInt64> row_numbers;
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            filter.assign(flags.begin() + row_begin, flags.begin() + row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                if (flags[i])
                    row_numbers.push_back(i);
        }

        Columns cut_columns(num_arguments);
        std::vector<const IColumn *> cut_pointers(num_arguments); // STYLE_CHECK_ALLOW_STD_CONTAINERS
        for (size_t i = 0; i < num_arguments; ++i)
        {
            cut_columns[i] = columns[i]->cut(row_begin, row_end - row_begin);
            if (if_argument_pos >= 0)
                cut_columns[i] = cut_columns[i]->filter(filter, row_numbers.size());
            cut_pointers[i] = cut_columns[i].get();
        }

        auto res = convert(cut_pointers.data());
        res.holder.insert(res.holder.end(), cut_columns.begin(), cut_columns.end());
        res.row_numbers = std::move(row_numbers);
        return res;
    }

    /// The number of rows to pass to the nested function after `convertRange`.
    static size_t numRows(size_t row_begin, size_t row_end, const ConvertedColumns & converted, ssize_t if_argument_pos)
    {
        return if_argument_pos >= 0 ? converted.row_numbers.size() : row_end - row_begin;
    }

    /// The places of the rows passed to the nested function after `convertRange`.
    static AggregateDataPtr * getPlaces(
        AggregateDataPtr * places, size_t row_begin, const ConvertedColumns & converted, ssize_t if_argument_pos,
        PaddedPODArray<AggregateDataPtr> & filtered_places)
    {
        if (if_argument_pos < 0)
            return places + row_begin;
        filtered_places.resize(converted.row_numbers.size());
        for (size_t i = 0; i < converted.row_numbers.size(); ++i)
            filtered_places[i] = places[converted.row_numbers[i]];
        return filtered_places.data();
    }

public:
    AggregateFunctionVariantAdapter(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const DataTypes & nested_arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionVariantAdapter>(arguments, params, nested_function_->getResultType())
        , nested_function(std::move(nested_function_))
        , nested_argument_types(nested_arguments)
        , num_arguments(arguments.size())
    {
    }

    String getName() const override { return nested_function->getName(); }

    const IAggregateFunction & getBaseAggregateFunctionWithSameStateRepresentation() const override
    {
        return nested_function->getBaseAggregateFunctionWithSameStateRepresentation();
    }

    DataTypePtr getNormalizedStateType() const override { return nested_function->getNormalizedStateType(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }
    size_t getVersionFromRevision(size_t revision) const override { return nested_function->getVersionFromRevision(revision); }
    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }
    DataTypePtr getStateType() const override { return this->getStateTypeWithVersionOf(*nested_function); }

    void create(AggregateDataPtr __restrict place) const override { nested_function->create(place); }
    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroy(place); }
    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroyUpToState(place); }
    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }
    size_t sizeOfData() const override { return nested_function->sizeOfData(); }
    size_t alignOfData() const override { return nested_function->alignOfData(); }
    bool isState() const override { return nested_function->isState(); }
    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// Slow path, the batch methods below are used in most cases.
        auto converted = convertRange(row_num, row_num + 1, columns);
        nested_function->add(place, converted.pointers.data(), 0, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        /// Only the first row is used.
        nested_function->addManyDefaults(place, convertRange(0, 1, columns).pointers.data(), length, arena);
    }

    void addBatch(
        size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset,
        const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        auto converted = convertRange(row_begin, row_end, columns, if_argument_pos);
        PaddedPODArray<AggregateDataPtr> filtered_places;
        nested_function->addBatch(
            0, numRows(row_begin, row_end, converted, if_argument_pos),
            getPlaces(places, row_begin, converted, if_argument_pos, filtered_places), place_offset, converted.pointers.data(), arena);
    }

    void addBatchWithNonNullPlaces(
        size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset,
        const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        auto converted = convertRange(row_begin, row_end, columns, if_argument_pos);
        PaddedPODArray<AggregateDataPtr> filtered_places;
        nested_function->addBatchWithNonNullPlaces(
            0, numRows(row_begin, row_end, converted, if_argument_pos),
            getPlaces(places, row_begin, converted, if_argument_pos, filtered_places), place_offset, converted.pointers.data(), arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place,
        const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        auto converted = convertRange(row_begin, row_end, columns, if_argument_pos);
        nested_function->addBatchSinglePlace(0, numRows(row_begin, row_end, converted, if_argument_pos), place, converted.pointers.data(), arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place,
        const IColumn ** columns, const UInt8 * null_map, Arena * arena, ssize_t if_argument_pos) const override
    {
        auto converted = convertRange(row_begin, row_end, columns, if_argument_pos);
        if (if_argument_pos < 0)
        {
            nested_function->addBatchSinglePlaceNotNull(0, row_end - row_begin, place, converted.pointers.data(), null_map + row_begin, arena);
            return;
        }

        PaddedPODArray<UInt8> filtered_null_map(converted.row_numbers.size());
        for (size_t i = 0; i < converted.row_numbers.size(); ++i)
            filtered_null_map[i] = null_map[converted.row_numbers[i]];
        nested_function->addBatchSinglePlaceNotNull(
            0, converted.row_numbers.size(), place, converted.pointers.data(), filtered_null_map.data(), arena);
    }

    void addBatchArray(
        size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset,
        const IColumn ** columns, const UInt64 * offsets, Arena * arena) const override
    {
        /// The rows of the nested arrays are not contiguous with `row_begin`, so the whole columns are converted. It is called once per block.
        nested_function->addBatchArray(row_begin, row_end, places, place_offset, convert(columns).pointers.data(), offsets, arena);
    }

    void addBatchConsecutivePlaces(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, size_t place_stride,
        const IColumn ** columns, Arena * arena) const override
    {
        auto converted = convertRange(row_begin, row_end, columns);
        nested_function->addBatchConsecutivePlaces(0, row_end - row_begin, place, place_stride, converted.pointers.data(), arena);
    }

    void addBatchLookupTable8(
        size_t row_begin, size_t row_end, AggregateDataPtr * places, size_t place_offset,
        std::function<void(AggregateDataPtr &)> init, const UInt8 * key, const IColumn ** columns, Arena * arena) const override
    {
        /// `places` is the lookup table indexed by `key`, not by row.
        auto converted = convertRange(row_begin, row_end, columns);
        nested_function->addBatchLookupTable8(
            0, row_end - row_begin, places, place_offset, std::move(init), key + row_begin, converted.pointers.data(), arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_function->merge(place, rhs, arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }
    bool isParallelizeMergePrepareNeeded() const override { return nested_function->isParallelizeMergePrepareNeeded(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_function->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_function->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

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

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
    }

    void rollbackInsertResult(ConstAggregateDataPtr __restrict place, IColumn & to) const noexcept override
    {
        nested_function->rollbackInsertResult(place, to);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
