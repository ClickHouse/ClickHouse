#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/Columns/ColumnVector.h>

#include <cmath>

namespace DB
{

template<typename T, typename Op>
class AggregateFunctionVarianceData
{
public:
	AggregateFunctionVarianceData() = default;

	void update(const IColumn & column, size_t row_num)
	{
		T received = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
		Float64 val = static_cast<Float64>(received);
		Float64 delta = val - mean;

		++count;
		mean += delta / count;
		m2 += delta * (val - mean);
	}

	void mergeWith(const AggregateFunctionVarianceData & source)
	{
		UInt64 total_count = count + source.count;
		Float64 factor = static_cast<Float64>(count * source.count) / total_count;
		Float64 delta = mean - source.mean;

		count = total_count;
		mean += delta * (source.count / count);
		m2 += source.m2 + delta * delta * factor;
	}

	void serialize(WriteBuffer & buf) const
	{
		writeVarUInt(count, buf);
		writeBinary(mean, buf);
		writeBinary(m2, buf);
	}

	void deserialize(ReadBuffer & buf)
	{
		readVarUInt(count, buf);
		readBinary(mean, buf);
		readBinary(m2, buf);
	}

	void publish(IColumn & to) const
	{
		static_cast<ColumnFloat64 &>(to).getData().push_back(Op::apply(m2, count));
	}

private:
	UInt64 count = 0;
	Float64 mean = 0.0;
	Float64 m2 = 0.0;
};

template<typename T, typename Op>
class AggregateFunctionVariance final : public IUnaryAggregateFunction<AggregateFunctionVarianceData<T, Op>, AggregateFunctionVariance<T, Op> >
{
public:
	String getName() const override { return Op::name; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeFloat64;
	}

	void setArgument(const DataTypePtr & argument) override
	{
		if (!argument->behavesAsNumber())
			throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).update(column, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).mergeWith(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		AggregateFunctionVarianceData<T, Op> source;
		source.deserialize(buf);

		this->data(place).mergeWith(source);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		this->data(place).publish(to);
	}
};

struct VarSampImpl
{
	static constexpr auto name = "varSamp";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		if (count == 1)
			return 0.0;
		else
			return m2 / (count - 1);
	}
};

struct StdDevSampImpl
{
	static constexpr auto name = "stddevSamp";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		return sqrt(VarSampImpl::apply(m2, count));
	}
};

struct VarPopImpl
{
	static constexpr auto name = "varPop";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		if (count == 1)
			return 0.0;
		else
			return m2 / count;
	}
};

struct StdDevPopImpl
{
	static constexpr auto name = "stddevPop";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		return sqrt(VarPopImpl::apply(m2, count));
	}
};

template<typename T>
using AggregateFunctionVarSamp = AggregateFunctionVariance<T, VarSampImpl>;

template<typename T>
using AggregateFunctionStdDevSamp = AggregateFunctionVariance<T, StdDevSampImpl>;

template<typename T>
using AggregateFunctionVarPop = AggregateFunctionVariance<T, VarPopImpl>;

template<typename T>
using AggregateFunctionStdDevPop = AggregateFunctionVariance<T, StdDevPopImpl>;

}
