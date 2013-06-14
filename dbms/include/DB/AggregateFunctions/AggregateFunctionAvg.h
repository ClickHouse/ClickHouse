#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionAvgData
{
	Float64 sum;
	UInt64 count;

	AggregateFunctionAvgData() : sum(0), count(0) {}
};


/// Считает арифметическое среднее значение чисел. Параметром шаблона может быть UInt64, Int64 или Float64.
template <typename T>
class AggregateFunctionAvg : public IUnaryAggregateFunction<AggregateFunctionAvgData>
{
public:
	String getName() const { return "avg"; }
	String getTypeID() const { return "avg_" + TypeName<T>::get(); }

	DataTypePtr getReturnType() const
	{
		return new DataTypeFloat64;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (!argument->isNumeric())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}


	void addOne(AggregateDataPtr place, const Field & value) const
	{
		data(place).sum += get<const T &>(value);
		++data(place).count;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).sum += data(rhs).sum;
		data(place).count += data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeFloatBinary(data(place).sum, buf);
		writeVarUInt(data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Float64 tmp_sum = 0;
		UInt64 tmp_count = 0;
		readFloatBinary(tmp_sum, buf);
		readVarUInt(tmp_count, buf);
		data(place).sum += tmp_sum;
		data(place).count += tmp_count;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return data(place).sum / data(place).count;
	}
};


/** Принимает два аргумента - значение и условие.
  * Вычисляет среднее значений при выполнении условия.
  * avgIf(x, cond) эквивалентно sum(cond ? x : 0) / sum(cond).
  */
template <typename T>
class AggregateFunctionAvgIf : public IAggregateFunctionHelper<AggregateFunctionAvgData>
{
public:
	String getName() const { return "avgIf"; }
	String getTypeID() const { return "avgIf_" + TypeName<T>::get(); }

	DataTypePtr getReturnType() const
	{
		return new DataTypeFloat64;
	}

	void setArguments(const DataTypes & arguments)
	{
		if (!arguments[0]->isNumeric())
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument for aggregate function " + getName() + ". Must be UInt8.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void add(AggregateDataPtr place, const Row & row) const
	{
		if (get<UInt64>(row[1]))
		{
			data(place).sum += get<const T &>(row[0]);
			++data(place).count;
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		data(place).sum += data(rhs).sum;
		data(place).count += data(rhs).count;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		writeFloatBinary(data(place).sum, buf);
		writeVarUInt(data(place).count, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Float64 tmp_sum = 0;
		UInt64 tmp_count = 0;
		readFloatBinary(tmp_sum, buf);
		readVarUInt(tmp_count, buf);
		data(place).sum += tmp_sum;
		data(place).count += tmp_count;
	}

	Field getResult(ConstAggregateDataPtr place) const
	{
		return data(place).sum / data(place).count;
	}
};

}
