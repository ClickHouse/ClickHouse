#pragma once

#include <algorithm>
#include <cmath>
#include <utility>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <base/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

struct Settings;

/// The kind of selection performed by the `timeSeries{TopK,BottomK,LimitK}Masks` aggregate functions.
enum class TimeSeriesTopKMasksKind : UInt8
{
    TopK,    /// At each time step keep the series with the k greatest values.
    BottomK, /// At each time step keep the series with the k smallest values.
    LimitK,  /// At each time step keep the k series with the smallest sampling keys among those having a value.
};

/// State of the `timeSeries{TopK,BottomK,LimitK}Masks` aggregate functions: one bounded heap per time step, O(number_of_time_steps * k) in total.
template <typename RankType>
struct AggregateFunctionTimeSeriesTopKMasksData
{
    /// A candidate series at one time step: `rank` is what candidates are compared by, `key` identifies the series.
    struct Entry
    {
        RankType rank;
        UInt64 key;
    };

    /// A binary heap of at most k entries with the worst kept entry at the front (see `better()` in the aggregate function).
    using Heap = VectorWithMemoryTracking<Entry>;

    /// `heaps[t]` keeps the best (at most `k_per_step[t]`) candidates seen so far at time step `t`.
    VectorWithMemoryTracking<Heap> heaps;

    /// Per-step capacity: either the same value at every step or per-step values if `k` was given as an array.
    VectorWithMemoryTracking<UInt64> k_per_step;

    /// Becomes true when the first row is added, after the number of time steps and `k` are known.
    bool initialized = false;
};

/// Implements timeSeries{TopK,BottomK,LimitK}Masks(k, key[, sampling_key], values): bounded per-step selection of series for PromQL topk/bottomk/limitk (the semantics are documented with the factory registration in the .cpp file).
template <TimeSeriesTopKMasksKind kind, typename ValueType>
class AggregateFunctionTimeSeriesTopKMasks final :
    public IAggregateFunctionDataHelper<
        AggregateFunctionTimeSeriesTopKMasksData<std::conditional_t<kind == TimeSeriesTopKMasksKind::LimitK, UInt64, Float64>>,
        AggregateFunctionTimeSeriesTopKMasks<kind, ValueType>>
{
public:
    /// For `topk` and `bottomk` candidates are ranked by their Float64 value, for `limitk` by their UInt64 sampling key.
    using RankType = std::conditional_t<kind == TimeSeriesTopKMasksKind::LimitK, UInt64, Float64>;

    using Data = AggregateFunctionTimeSeriesTopKMasksData<RankType>;
    using Entry = typename Data::Entry;
    using Heap = typename Data::Heap;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionTimeSeriesTopKMasks<kind, ValueType>>;

    static constexpr UInt8 FORMAT_VERSION = 1;

    /// The arguments are (k, key, values) for `topk` and `bottomk`, and (k, key, sampling_key, values) for `limitk`.
    static constexpr size_t values_argument_index = (kind == TimeSeriesTopKMasksKind::LimitK) ? 3 : 2;

    static constexpr const char * getNameImpl()
    {
        if constexpr (kind == TimeSeriesTopKMasksKind::TopK)
            return "timeSeriesTopKMasks";
        else if constexpr (kind == TimeSeriesTopKMasksKind::BottomK)
            return "timeSeriesBottomKMasks";
        else
            return "timeSeriesLimitKMasks";
    }

    String getName() const override { return getNameImpl(); }

    static DataTypePtr createResultType()
    {
        DataTypes element_types{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>())};
        Names element_names{"key", "steps_mask"};
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names)));
    }

    explicit AggregateFunctionTimeSeriesTopKMasks(const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType())
        , k_is_per_step(argument_types_[0]->getTypeId() == TypeIndex::Array)
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const UInt64 key = columns[1]->getUInt(row_num);

        const auto & values_column = assert_cast<const ColumnArray &>(*columns[values_argument_index]);
        const auto & values_offsets = values_column.getOffsets();
        const size_t values_begin = values_offsets[row_num - 1];
        const size_t num_steps = values_offsets[row_num] - values_begin;

        const IColumn * values_nested = &values_column.getData();
        const NullMap * null_map = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(values_nested))
        {
            null_map = &nullable->getNullMapData();
            values_nested = &nullable->getNestedColumn();
        }
        [[maybe_unused]] const auto & values_data = assert_cast<const ColumnVector<ValueType> &>(*values_nested).getData();

        auto & state = this->data(place);
        if (!state.initialized)
            initializeState(state, num_steps, columns, row_num);
        else
            checkStateMatchesRow(state, num_steps, columns, row_num);

        [[maybe_unused]] UInt64 sampling_key = 0;
        if constexpr (kind == TimeSeriesTopKMasksKind::LimitK)
            sampling_key = columns[2]->getUInt(row_num);

        for (size_t t = 0; t != num_steps; ++t)
        {
            /// A series with no value at this time step is not a candidate (this matches how `arrayTopK` skips NULLs).
            if (null_map && (*null_map)[values_begin + t])
                continue;

            Entry entry;
            entry.key = key;
            if constexpr (kind == TimeSeriesTopKMasksKind::LimitK)
                entry.rank = sampling_key;
            else
                entry.rank = static_cast<Float64>(values_data[values_begin + t]);

            insertToHeap(state.heaps[t], state.k_per_step[t], entry);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        const auto & rhs_state = this->data(rhs);
        if (!rhs_state.initialized)
            return;

        auto & state = this->data(place);
        if (!state.initialized)
        {
            state = rhs_state;
            return;
        }

        if (state.heaps.size() != rhs_state.heaps.size() || state.k_per_step != rhs_state.k_per_step)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot merge states of aggregate function {} created with different arguments: "
                            "the sizes of the `values` arrays and the values of `k` must be the same for all rows",
                            getName());

        /// The kept set is the k best entries of the union, so inserting one by one is order-independent.
        for (size_t t = 0; t != state.heaps.size(); ++t)
            for (const Entry & entry : rhs_state.heaps[t])
                insertToHeap(state.heaps[t], state.k_per_step[t], entry);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(FORMAT_VERSION, buf);

        const auto & state = this->data(place);
        writeBinaryLittleEndian(UInt8{state.initialized}, buf);
        if (!state.initialized)
            return;

        writeVarUInt(state.heaps.size(), buf);
        for (UInt64 k : state.k_per_step)
            writeBinaryLittleEndian(k, buf);

        for (const Heap & heap : state.heaps)
        {
            writeVarUInt(heap.size(), buf);
            for (const Entry & entry : heap)
            {
                writeBinaryLittleEndian(entry.rank, buf);
                writeBinaryLittleEndian(entry.key, buf);
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt8 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Cannot deserialize state of aggregate function {}: expected format version {}, got {}",
                            getName(), UInt32{FORMAT_VERSION}, UInt32{format_version});

        auto & state = this->data(place);

        UInt8 initialized = 0;
        readBinaryLittleEndian(initialized, buf);
        if (!initialized)
            return;

        size_t num_steps = 0;
        readVarUInt(num_steps, buf);

        /// The number of time steps and the heap sizes have no upper bound known at this point (there is no
        /// fixed limit that `add` would enforce), so the state is grown incrementally while reading: the
        /// allocated memory stays proportional to the actual payload, and corrupted data claiming a huge size
        /// fails with an end-of-buffer error instead of attempting a huge upfront allocation.
        for (size_t t = 0; t != num_steps; ++t)
        {
            UInt64 k = 0;
            readBinaryLittleEndian(k, buf);
            state.k_per_step.push_back(k);
        }

        for (size_t t = 0; t != num_steps; ++t)
        {
            size_t heap_size = 0;
            readVarUInt(heap_size, buf);
            if (heap_size > state.k_per_step[t])
                throw Exception(ErrorCodes::INCORRECT_DATA,
                                "Cannot deserialize state of aggregate function {}: {} entries at time step {} exceed k = {}",
                                getName(), heap_size, t, state.k_per_step[t]);

            state.heaps.emplace_back();
            Heap & heap = state.heaps.back();

            /// Entries are written in the order of the underlying array, so reading them back preserves the heap layout.
            for (size_t i = 0; i != heap_size; ++i)
            {
                Entry entry;
                readBinaryLittleEndian(entry.rank, buf);
                readBinaryLittleEndian(entry.key, buf);
                heap.push_back(entry);
            }
        }

        state.initialized = true;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);

        auto & result_column = assert_cast<ColumnArray &>(to);
        auto & result_tuple = assert_cast<ColumnTuple &>(result_column.getData());
        auto & key_data = assert_cast<ColumnUInt64 &>(result_tuple.getColumn(0)).getData();
        auto & mask_column = assert_cast<ColumnArray &>(result_tuple.getColumn(1));
        auto & mask_offsets = mask_column.getOffsets();
        auto & mask_data = assert_cast<ColumnUInt8 &>(mask_column.getData()).getData();

        const size_t num_steps = state.heaps.size();

        /// Invert the per-step heaps into (key, step) pairs sorted by key, i.e. one run of pairs per selected series.
        VectorWithMemoryTracking<std::pair<UInt64, UInt64>> selected;
        size_t num_selected = 0;
        for (const Heap & heap : state.heaps)
            num_selected += heap.size();
        selected.reserve(num_selected);
        for (size_t t = 0; t != num_steps; ++t)
            for (const Entry & entry : state.heaps[t])
                selected.emplace_back(entry.key, t);
        ::sort(selected.begin(), selected.end());

        for (size_t i = 0; i != selected.size();)
        {
            const UInt64 key = selected[i].first;
            key_data.push_back(key);

            const size_t mask_begin = mask_data.size();
            mask_data.resize_fill(mask_begin + num_steps, 0);
            for (; i != selected.size() && selected[i].first == key; ++i)
                mask_data[mask_begin + selected[i].second] = 1;
            mask_offsets.push_back(mask_data.size());
        }

        result_column.getOffsets().push_back(key_data.size());
    }

private:
    /// Returns whether candidate `a` should be kept rather than candidate `b`, a strict total order (the ranking rules are documented in the .cpp file).
    static bool better(const Entry & a, const Entry & b)
    {
        if constexpr (kind == TimeSeriesTopKMasksKind::LimitK)
        {
            if (a.rank != b.rank)
                return a.rank < b.rank;
        }
        else
        {
            const bool a_nan = std::isnan(a.rank);
            const bool b_nan = std::isnan(b.rank);
            if (a_nan || b_nan)
            {
                /// Any non-NaN value beats NaN, both for `topk` and for `bottomk` (this matches how `arrayTopK` and `arrayBottomK` order NaNs).
                if (a_nan != b_nan)
                    return b_nan;
            }
            else if (a.rank != b.rank)
            {
                return (kind == TimeSeriesTopKMasksKind::TopK) ? (a.rank > b.rank) : (a.rank < b.rank);
            }
        }
        /// Deterministic tie-breaking: prefer the series with the smaller key.
        return a.key < b.key;
    }

    /// Keeps `heap` holding the (at most k) best entries seen so far, with the worst kept entry at the front (`better` is the "less" comparator of the heap).
    static void insertToHeap(Heap & heap, UInt64 k, const Entry & entry)
    {
        if (heap.size() < k)
        {
            heap.push_back(entry);
            std::push_heap(heap.begin(), heap.end(), better);
        }
        else if (k && better(entry, heap.front()))
        {
            std::pop_heap(heap.begin(), heap.end(), better);
            heap.back() = entry;
            std::push_heap(heap.begin(), heap.end(), better);
        }
    }

    /// Reads the value of `k` for time step `step` from the first argument at row `row_num`.
    UInt64 readK(const IColumn ** columns, size_t row_num, size_t step) const
    {
        if (!k_is_per_step)
            return columns[0]->getUInt(row_num);

        const auto & k_column = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & k_offsets = k_column.getOffsets();
        return k_column.getData().getUInt(k_offsets[row_num - 1] + step);
    }

    /// Returns the size of the `k` array at row `row_num` if `k` is given as an array, otherwise the fallback.
    size_t getKArraySize(const IColumn ** columns, size_t row_num, size_t fallback) const
    {
        if (!k_is_per_step)
            return fallback;
        const auto & k_offsets = assert_cast<const ColumnArray &>(*columns[0]).getOffsets();
        return k_offsets[row_num] - k_offsets[row_num - 1];
    }

    /// Initializes the state from the first added row: the number of time steps and the per-step capacities.
    void initializeState(Data & state, size_t num_steps, const IColumn ** columns, size_t row_num) const
    {
        if (getKArraySize(columns, row_num, num_steps) != num_steps)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Aggregate function {} requires the `k` array to have the same size as the `values` array, got {} and {}",
                            getName(), getKArraySize(columns, row_num, num_steps), num_steps);

        state.k_per_step.resize(num_steps);
        for (size_t t = 0; t != num_steps; ++t)
            state.k_per_step[t] = readK(columns, row_num, t);

        state.heaps.resize(num_steps);
        state.initialized = true;
    }

    /// Checks that the row being added agrees with the first added row on the number of time steps and on `k`.
    void checkStateMatchesRow(const Data & state, size_t num_steps, const IColumn ** columns, size_t row_num) const
    {
        if (state.heaps.size() != num_steps)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Aggregate function {} requires all `values` arrays to have the same size, got {} and {}",
                            getName(), num_steps, state.heaps.size());

        if (getKArraySize(columns, row_num, num_steps) != num_steps)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Aggregate function {} requires the `k` array to have the same size as the `values` array, got {} and {}",
                            getName(), getKArraySize(columns, row_num, num_steps), num_steps);

        const size_t num_k_to_check = k_is_per_step ? num_steps : (num_steps ? 1 : 0);
        for (size_t t = 0; t != num_k_to_check; ++t)
            if (state.k_per_step[t] != readK(columns, row_num, t))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Aggregate function {} requires the same `k` for all rows, got {} and {}",
                                getName(), readK(columns, row_num, t), state.k_per_step[t]);
    }

    /// Whether the first argument (`k`) is an array with one value per time step rather than a single value.
    const bool k_is_per_step;
};

}
