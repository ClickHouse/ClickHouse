#pragma once

#include <AggregateFunctions/ReservoirSamplerDeterministic.h>

#include <Common/FieldVisitors.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

template <typename ArgumentFieldType>
struct AggregateFunctionQuantileDeterministicData
{
    using Sample = ReservoirSamplerDeterministic<ArgumentFieldType, ReservoirSamplerDeterministicOnEmpty::RETURN_NAN_OR_ZERO>;
    Sample sample;  /// TODO Add MemoryTracker
};


/** Approximately calculates the quantile.
  * The argument type can only be a numeric type (including date and date-time).
  * If returns_float = true, the result type is Float64, otherwise - the result type is the same as the argument type.
  * For dates and date-time, returns_float should be set to false.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantileDeterministic final
    : public IAggregateFunctionDataHelper<
        AggregateFunctionQuantileDeterministicData<ArgumentFieldType>,
        AggregateFunctionQuantileDeterministic<ArgumentFieldType, returns_float>>
{
private:
    using Sample = typename AggregateFunctionQuantileDeterministicData<ArgumentFieldType>::Sample;

    double level;
    DataTypePtr type;

public:
    AggregateFunctionQuantile(const DataTypePtr & argument, double level_) : level(level_)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat64>();
        else
            type = argument;
    }

    String getName() const override { return "quantileDeterministic"; }

    DataTypePtr getReturnType() const override
    {
        return type;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).sample.insert(
            static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num],
            columns[1]->get64(row_num));
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sample.merge(this->data(rhs).sample);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).sample.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).sample.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        /// `Sample` can be sorted when a quantile is received, but in this context, you can not think of this as a violation of constancy.
        Sample & sample = const_cast<Sample &>(this->data(place).sample);

        if (returns_float)
            static_cast<ColumnFloat64 &>(to).getData().push_back(sample.quantileInterpolated(level));
        else
            static_cast<ColumnVector<ArgumentFieldType> &>(to).getData().push_back(sample.quantileInterpolated(level));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/** The same, but allows you to calculate several quantiles at once.
  * To do this, takes several levels as parameters. Example: quantiles(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Returns an array of results.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantilesDeterministic final
    : public IAggregateFunctionDataHelper<
        AggregateFunctionQuantileDeterministicData<ArgumentFieldType>,
        AggregateFunctionQuantilesDeterministic<ArgumentFieldType, returns_float>>
{
private:
    using Sample = typename AggregateFunctionQuantileDeterministicData<ArgumentFieldType>::Sample;

    using Levels = std::vector<double>;
    Levels levels;
    DataTypePtr type;

public:
    AggregateFunctionQuantiles(const DataTypePtr & argument, const Levels & levels) : levels(levels)
    {
        if (returns_float)
            type = std::make_shared<DataTypeFloat64>();
        else
            type = argument;
    }

    String getName() const override { return "quantilesDeterministic"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).sample.insert(
            static_cast<const ColumnVector<ArgumentFieldType> &>(*columns[0]).getData()[row_num],
            columns[1]->get64(row_num));
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sample.merge(this->data(rhs).sample);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).sample.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).sample.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        /// `Sample` can be sorted when a quantile is received, but in this context, you can not think of this as a violation of constancy.
        Sample & sample = const_cast<Sample &>(this->data(place).sample);

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        size_t size = levels.size();
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        if (returns_float)
        {
            ColumnFloat64::Container & data_to = static_cast<ColumnFloat64 &>(arr_to.getData()).getData();

            for (size_t i = 0; i < size; ++i)
                data_to.push_back(sample.quantileInterpolated(levels[i]));
        }
        else
        {
            typename ColumnVector<ArgumentFieldType>::Container & data_to = static_cast<ColumnVector<ArgumentFieldType> &>(arr_to.getData()).getData();

            for (size_t i = 0; i < size; ++i)
                data_to.push_back(sample.quantileInterpolated(levels[i]));
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
