#pragma once

#include <Common/HashTable/HashMap.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/IBinaryAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>

#include <Columns/ColumnArray.h>


namespace DB
{


/** The state is a hash table of the form: value -> how many times it happened.
  */
template <typename T>
struct AggregateFunctionQuantileExactWeightedData
{
    using Key = T;
    using Weight = UInt64;

    /// When creating, the hash table must be small.
    using Map = HashMap<
        Key, Weight,
        HashCRC32<Key>,
        HashTableGrower<4>,
        HashTableAllocatorWithStackMemory<sizeof(std::pair<Key, Weight>) * (1 << 3)>
    >;

    Map map;
};


/** Exactly calculates a quantile over a set of values, for each of which a weight is given - how many times the value was encountered.
  * You can consider a set of pairs `values, weight` - as a set of histograms,
  * where value is the value rounded to the middle of the column, and weight is the height of the column.
  * The argument type can only be a numeric type (including date and date-time).
  * The result type is the same as the argument type.
  */
template <typename ValueType, typename WeightType>
class AggregateFunctionQuantileExactWeighted final
    : public IBinaryAggregateFunction<
        AggregateFunctionQuantileExactWeightedData<ValueType>,
        AggregateFunctionQuantileExactWeighted<ValueType, WeightType>>
{
private:
    double level;
    DataTypePtr type;

public:
    AggregateFunctionQuantileExactWeighted(double level_ = 0.5) : level(level_) {}

    String getName() const override { return "quantileExactWeighted"; }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        type = arguments[0];
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num, Arena *) const
    {
        this->data(place)
            .map[static_cast<const ColumnVector<ValueType> &>(column_value).getData()[row_num]]
            += static_cast<const ColumnVector<WeightType> &>(column_weight).getData()[row_num];
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & map = this->data(place).map;
        const auto & rhs_map = this->data(rhs).map;

        for (const auto & pair : rhs_map)
            map[pair.first] += pair.second;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).map.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::Reader reader(buf);

        auto & map = this->data(place).map;
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & map = this->data(place).map;
        size_t size = map.size();

        if (0 == size)
        {
            static_cast<ColumnVector<ValueType> &>(to).getData().push_back(ValueType());
            return;
        }

        /// Copy the data to a temporary array to get the element you need in order.
        using Pair = typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::value_type;
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        UInt64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.second;
            array[i] = pair;
            ++i;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        UInt64 threshold = std::ceil(sum_weight * level);
        UInt64 accumulated = 0;

        const Pair * it = array;
        const Pair * end = array + size;
        while (it < end)
        {
            accumulated += it->second;

            if (accumulated >= threshold)
                break;

            ++it;
        }

        if (it == end)
            --it;

        static_cast<ColumnVector<ValueType> &>(to).getData().push_back(it->first);
    }
};


/** Same, but allows you to calculate several quantiles at once.
  * For this, takes as parameters several levels. Example: quantilesExactWeighted(0.5, 0.8, 0.9, 0.95)(ConnectTiming, Weight).
  * Returns an array of results.
  */
template <typename ValueType, typename WeightType>
class AggregateFunctionQuantilesExactWeighted final
    : public IBinaryAggregateFunction<
        AggregateFunctionQuantileExactWeightedData<ValueType>,
        AggregateFunctionQuantilesExactWeighted<ValueType, WeightType>>
{
private:
    QuantileLevels<double> levels;
    DataTypePtr type;

public:
    String getName() const override { return "quantilesExactWeighted"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        type = arguments[0];
    }

    void setParameters(const Array & params) override
    {
        levels.set(params);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num, Arena *) const
    {
        this->data(place)
            .map[static_cast<const ColumnVector<ValueType> &>(column_value).getData()[row_num]]
            += static_cast<const ColumnVector<WeightType> &>(column_weight).getData()[row_num];
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & map = this->data(place).map;
        const auto & rhs_map = this->data(rhs).map;

        for (const auto & pair : rhs_map)
            map[pair.first] += pair.second;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).map.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::Reader reader(buf);

        auto & map = this->data(place).map;
        while (reader.next())
        {
            const auto & pair = reader.get();
            map[pair.first] = pair.second;
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & map = this->data(place).map;
        size_t size = map.size();

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

        size_t num_levels = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + num_levels);

        if (!num_levels)
            return;

        typename ColumnVector<ValueType>::Container_t & data_to = static_cast<ColumnVector<ValueType> &>(arr_to.getData()).getData();

        size_t old_size = data_to.size();
        data_to.resize(old_size + num_levels);

        if (0 == size)
        {
            for (size_t i = 0; i < num_levels; ++i)
                data_to[old_size + i] = ValueType();
            return;
        }

        /// Copy the data to a temporary array to get the element you need in order.
        using Pair = typename AggregateFunctionQuantileExactWeightedData<ValueType>::Map::value_type;
        std::unique_ptr<Pair[]> array_holder(new Pair[size]);
        Pair * array = array_holder.get();

        size_t i = 0;
        UInt64 sum_weight = 0;
        for (const auto & pair : map)
        {
            sum_weight += pair.second;
            array[i] = pair;
            ++i;
        }

        std::sort(array, array + size, [](const Pair & a, const Pair & b) { return a.first < b.first; });

        UInt64 accumulated = 0;

        const Pair * it = array;
        const Pair * end = array + size;

        size_t level_index = 0;
        UInt64 threshold = std::ceil(sum_weight * levels.levels[levels.permutation[level_index]]);

        while (it < end)
        {
            accumulated += it->second;

            while (accumulated >= threshold)
            {
                data_to[old_size + levels.permutation[level_index]] = it->first;
                ++level_index;

                if (level_index == num_levels)
                    return;

                threshold = std::ceil(sum_weight * levels.levels[levels.permutation[level_index]]);
            }

            ++it;
        }

        while (level_index < num_levels)
        {
            data_to[old_size + levels.permutation[level_index]] = array[size - 1].first;
            ++level_index;
        }
    }
};

}
