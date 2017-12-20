#pragma once

#include <limits>

#include <Common/MemoryTracker.h>
#include <Common/HashTable/Hash.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include <ext/range.h>






template <typename ArgumentFieldType>
class AggregateFunctionQuantileTiming final : public IAggregateFunctionDataHelper<QuantileTiming, AggregateFunctionQuantileTiming<ArgumentFieldType>>
{
private:
    double level;

public:
    AggregateFunctionQuantileTiming(double level_) : level(level_) {}

    String getName() const override { return "quantileTiming"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat32>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnFloat32 &>(to).getData().push_back(this->data(place).getFloat(level));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/** Same, but with two arguments. The second argument is "weight" (integer) - how many times to consider the value.
  */
template <typename ArgumentFieldType, typename WeightFieldType>
class AggregateFunctionQuantileTimingWeighted final
    : public IAggregateFunctionDataHelper<QuantileTiming, AggregateFunctionQuantileTimingWeighted<ArgumentFieldType, WeightFieldType>>
{
private:
    double level;

public:
    AggregateFunctionQuantileTimingWeighted(double level_) : level(level_) {}

    String getName() const override { return "quantileTimingWeighted"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat32>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insertWeighted(
            static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num],
            static_cast<const ColumnVector<WeightFieldType> &>(*columns[1]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnFloat32 &>(to).getData().push_back(this->data(place).getFloat(level));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/** Same, but allows you to calculate several quantiles at once.
  * To do this, takes several levels as parameters. Example: quantilesTiming(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Returns an array of results.
  */
template <typename ArgumentFieldType>
class AggregateFunctionQuantilesTiming final : public IAggregateFunctionDataHelper<QuantileTiming, AggregateFunctionQuantilesTiming<ArgumentFieldType>>
{
private:
    QuantileLevels<double> levels;

public:
    String getName() const override { return "quantilesTiming"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        size_t size = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        if (!size)
            return;

        typename ColumnFloat32::Container & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(data_to.size() + size);

        this->data(place).getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


template <typename ArgumentFieldType, typename WeightFieldType>
class AggregateFunctionQuantilesTimingWeighted final
    : public IAggregateFunctionDataHelper<QuantileTiming, AggregateFunctionQuantilesTimingWeighted<ArgumentFieldType, WeightFieldType>>
{
private:
    QuantileLevels<double> levels;

public:
    String getName() const override { return "quantilesTimingWeighted"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).insertWeighted(
            static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num],
            static_cast<const ColumnVector<WeightFieldType> &>(*columns[1]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        size_t size = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        if (!size)
            return;

        typename ColumnFloat32::Container & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(data_to.size() + size);

        this->data(place).getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


}
