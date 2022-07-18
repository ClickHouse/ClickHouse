#pragma once

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/AggregateFunctionGroupArraySortedData.h>
#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{
template <typename TColumn, bool is_plain>
inline TColumn readItem(const IColumn * column, Arena * arena, size_t row)
{
    if constexpr (std::is_same_v<TColumn, StringRef>)
    {
        if constexpr (is_plain)
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
        if constexpr (std::is_same_v<TColumn, UInt64>)
            return column->getUInt(row);
        else
            return column->getInt(row);
    }
}

template <typename TColumn, typename TFilter = void>
size_t
getFirstNElements_low_threshold(const TColumn * data, int num_elements, int threshold, size_t * results, const TFilter * filter = nullptr)
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
        if constexpr (!std::is_same_v<TFilter, void>)
        {
            if (filter[i] == 0)
                continue;
        }

        //Starting from the highest values and we look for the immediately lower than the given one
        for (cur = current_max; cur > 0; cur--)
        {
            if (data[i] > data[results[cur - 1]])
                break;
        }

        if (cur < threshold)
        {
            //Move all the higher values 1 position to the right
            for (z = std::min(threshold - 1, current_max); z > cur; z--)
                results[z] = results[z - 1];

            if (current_max < threshold)
                ++current_max;

            //insert element into the given position
            results[cur] = i;
        }
    }

    return current_max;
}

template <typename T>
struct SortableItem
{
    T a;
    size_t b;
    bool operator<(const SortableItem & other) const { return (this->a < other.a); }
};

template <typename TColumn, typename TFilter = void>
size_t getFirstNElements_high_threshold(
    const TColumn * data, size_t num_elements, size_t threshold, size_t * results, const TFilter * filter = nullptr)
{
    std::vector<SortableItem<TColumn>> dataIndexed(num_elements);
    size_t num_elements_filtered = 0;

    for (size_t i = 0; i < num_elements; i++)
    {
        if constexpr (!std::is_same_v<TFilter, void>)
        {
            if (filter[i] == 0)
                continue;
        }

        dataIndexed.data()[num_elements_filtered].a = data[i];
        dataIndexed.data()[num_elements_filtered].b = i;
        num_elements_filtered++;
    }

    threshold = std::min(num_elements_filtered, threshold);

    std::nth_element(dataIndexed.data(), dataIndexed.data() + threshold, dataIndexed.data() + num_elements_filtered);
    std::sort(dataIndexed.data(), dataIndexed.data() + threshold);

    for (size_t i = 0; i < threshold; i++)
    {
        results[i] = dataIndexed[i].b;
    }

    return threshold;
}

static const size_t THRESHOLD_MAX_CUSTOM_FUNCTION = 1000;

template <typename TColumn>
size_t getFirstNElements(const TColumn * data, size_t num_elements, size_t threshold, size_t * results, const UInt8 * filter = nullptr)
{
    if (threshold < THRESHOLD_MAX_CUSTOM_FUNCTION)
    {
        if (filter != nullptr)
            return getFirstNElements_low_threshold(data, num_elements, threshold, results, filter);
        else
            return getFirstNElements_low_threshold(data, num_elements, threshold, results);
    }
    else
    {
        if (filter != nullptr)
            return getFirstNElements_high_threshold(data, num_elements, threshold, results, filter);
        else
            return getFirstNElements_high_threshold(data, num_elements, threshold, results);
    }
}

template <typename TColumnA, bool is_plain_a, bool use_column_b, typename TColumnB, bool is_plain_b>
class AggregateFunctionGroupArraySorted : public IAggregateFunctionDataHelper<
                                              AggregateFunctionGroupArraySortedData<TColumnA, use_column_b, TColumnB>,
                                              AggregateFunctionGroupArraySorted<TColumnA, is_plain_a, use_column_b, TColumnB, is_plain_b>>
{
protected:
    using State = AggregateFunctionGroupArraySortedData<TColumnA, use_column_b, TColumnB>;
    using Base = IAggregateFunctionDataHelper<
        AggregateFunctionGroupArraySortedData<TColumnA, use_column_b, TColumnB>,
        AggregateFunctionGroupArraySorted>;

    UInt64 threshold;
    DataTypePtr & input_data_type;
    mutable std::mutex mutex;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionGroupArraySorted(UInt64 threshold_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregateFunctionGroupArraySortedData<TColumnA, use_column_b, TColumnB>,
            AggregateFunctionGroupArraySorted>(argument_types_, params)
        , threshold(threshold_)
        , input_data_type(this->argument_types[0])
    {
    }

    void create(AggregateDataPtr place) const override
    {
        Base::create(place);
        this->data(place).threshold = threshold;
    }

    String getName() const override { return "groupArraySorted"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(input_data_type); }

    bool allocatesMemoryInArena() const override
    {
        if constexpr (std::is_same_v<TColumnA, StringRef>)
            return true;
        else
            return false;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        State & data = this->data(place);
        if constexpr (use_column_b)
        {
            data.add(
                readItem<TColumnA, is_plain_a>(columns[0], arena, row_num), readItem<TColumnB, is_plain_b>(columns[1], arena, row_num));
        }
        else
        {
            data.add(readItem<TColumnA, is_plain_a>(columns[0], arena, row_num));
        }
    }

    template <typename TColumn, bool is_plain, typename TFunc>
    void
    forFirstRows(size_t batch_size, const IColumn ** columns, size_t data_column, Arena * arena, ssize_t if_argument_pos, TFunc func) const
    {
        const TColumn * values = nullptr;
        std::unique_ptr<std::vector<TColumn>> values_vector;
        std::vector<size_t> best_rows(threshold);

        if constexpr (std::is_same_v<TColumn, StringRef>)
        {
            values_vector.reset(new std::vector<TColumn>(batch_size));
            for (size_t i = 0; i < batch_size; i++)
                (*values_vector)[i] = readItem<TColumn, is_plain>(columns[data_column], arena, i);
            values = (*values_vector).data();
        }
        else
        {
            const auto & column = assert_cast<const ColumnVector<TColumn> &>(*columns[data_column]);
            values = column.getData().data();
        }

        const UInt8 * filter = nullptr;
        StringRef refFilter;

        if (if_argument_pos >= 0)
        {
            refFilter = columns[if_argument_pos]->getRawData();
            filter = reinterpret_cast<const UInt8 *>(refFilter.data);
        }

        size_t num_elements = getFirstNElements(values, batch_size, threshold, best_rows.data(), filter);
        for (size_t i = 0; i < num_elements; i++)
        {
            func(best_rows[i], values);
        }
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos) const override
    {
        State & data = this->data(place);

        if constexpr (use_column_b)
        {
            forFirstRows<TColumnB, is_plain_b>(
                batch_size, columns, 1, arena, if_argument_pos, [columns, &arena, &data](size_t row, const TColumnB * values)
                {
                    data.add(readItem<TColumnA, is_plain_a>(columns[0], arena, row), values[row]);
                });
        }
        else
        {
            forFirstRows<TColumnA, is_plain_a>(
                batch_size, columns, 0, arena, if_argument_pos, [&data](size_t row, const TColumnA * values)
                {
                    data.add(values[row]);
                });
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
            if constexpr (std::is_same_v<TColumnA, StringRef>)
            {
                auto str = State::itemValue(value);
                if constexpr (is_plain_a)
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
