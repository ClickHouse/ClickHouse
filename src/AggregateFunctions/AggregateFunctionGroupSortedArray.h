#pragma once

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/AggregateFunctionGroupSortedArrayData.h>
#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{
template <typename T, bool is_plain_column>
inline T readItem(const IColumn * column, Arena * arena, size_t row)
{
    if constexpr (std::is_same_v<T, StringRef>)
    {
        if constexpr (is_plain_column)
        {
            StringRef str = column->getDataAt(row);
            auto ptr = arena->alloc(str.size);
            std::copy(str.data, str.data + str.size, ptr);
            return StringRef(ptr, str.size);
        }
        else
        {
            const char * begin = nullptr;
            return column->serializeValueIntoArena(row, *arena, begin);
        }
    }
    else
    {
        return column->getUInt(row);
    }
}

template <typename T>
void getFirstNElements(const T * data, int num_elements, int threshold, size_t * results)
{
    for (int i = 0; i < threshold; i++)
    {
        results[i] = 0;
    }

    threshold = std::min(num_elements, threshold);
    int current_max = 0;
    int cur;
    int z;
    for (int i = 0; i < num_elements; i++)
    {
        //Starting from the highest values and we look for the immediately lower than the given one
        for (cur = current_max; cur > 0 && (data[i] < data[results[cur - 1]]); cur--)
            ;

        if (cur < threshold)
        {
            //Move all the higher values 1 position to the right
            for (z = current_max - 1; z >= cur; z--)
                results[z] = results[z - 1];

            if (current_max < threshold)
                ++current_max;

            //insert element into the given position
            results[cur] = i;
        }
    }
}

template <bool is_plain_column, typename T, bool expr_sorted, typename TIndex>
class AggregateFunctionGroupSortedArray : public IAggregateFunctionDataHelper<
                                              AggregateFunctionGroupSortedArrayData<T, expr_sorted, TIndex>,
                                              AggregateFunctionGroupSortedArray<is_plain_column, T, expr_sorted, TIndex>>
{
protected:
    using State = AggregateFunctionGroupSortedArrayData<T, expr_sorted, TIndex>;
    using Base
        = IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, expr_sorted, TIndex>, AggregateFunctionGroupSortedArray>;

    UInt64 threshold;
    DataTypePtr & input_data_type;
    mutable std::mutex mutex;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupSortedArray(UInt64 threshold_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupSortedArrayData<T, expr_sorted, TIndex>, AggregateFunctionGroupSortedArray>(
            argument_types_, params)
        , threshold(threshold_)
        , input_data_type(this->argument_types[0])
    {
    }

    void create(AggregateDataPtr place) const override
    {
        Base::create(place);
        this->data(place).threshold = threshold;
    }

    String getName() const override { return "groupSortedArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override
    {
        if constexpr (std::is_same_v<T, StringRef>)
            return true;
        else
            return false;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        State & data = this->data(place);
        if constexpr (expr_sorted)
        {
            data.add(readItem<T, is_plain_column>(columns[0], arena, row_num), readItem<TIndex, false>(columns[1], arena, row_num));
        }
        else
        {
            data.add(readItem<T, is_plain_column>(columns[0], arena, row_num));
        }
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        State & data = this->data(place);
        if constexpr (expr_sorted)
        {
            StringRef ref = columns[1]->getRawData();
            TIndex values[batch_size];
            memcpy(values, ref.data, batch_size * sizeof(TIndex));
            size_t num_results = std::min(this->threshold, batch_size);
            size_t * bestRows = new size_t[batch_size];

            //First store the first n elements with the column number
            if (if_argument_pos >= 0)
            {
                TIndex * value_w = values;

                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                {
                    if (flags[i])
                        *(value_w++) = values[i];
                }

                batch_size = value_w - values;
            }

            num_results = std::min(this->threshold, batch_size);
            getFirstNElements(values, batch_size, num_results, bestRows);
            for (size_t i = 0; i < num_results; i++)
            {
                auto row = bestRows[i];
                data.add(readItem<T, is_plain_column>(columns[0], arena, row), values[row]);
            }
            delete[] bestRows;
        }
        else
        {
            if (if_argument_pos >= 0)
            {
                const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
                for (size_t i = 0; i < batch_size; ++i)
                {
                    if (flags[i])
                    {
                        data.add(readItem<T, is_plain_column>(columns[0], arena, i));
                    }
                }
            }
            else
            {
                for (size_t i = 0; i < batch_size; ++i)
                {
                    data.add(readItem<T, is_plain_column>(columns[0], arena, i));
                }
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void
    deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /*arena*/) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        auto & values = this->data(place).values;
        offsets_to.push_back(offsets_to.back() + values.size());

        IColumn & data_to = arr_to.getData();
        for (auto value : values)
        {
            if constexpr (std::is_same_v<T, StringRef>)
            {
                auto str = State::itemValue(value);
                if constexpr (is_plain_column)
                {
                    data_to.insertData(str.data, str.size);
                }
                else
                {
                    data_to.deserializeAndInsertFromArena(str.data);
                }
            }
            else
            {
                data_to.insert(State::itemValue(value));
            }
        }
    }
};
}
