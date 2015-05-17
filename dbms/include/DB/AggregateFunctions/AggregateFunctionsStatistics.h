#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>
#include <DB/Columns/ColumnVector.h>

#include <cmath>

struct C;
struct C;
namespace DB
{

/// XXX Реализовать корреляцию (corr).

/** Статистические аггрегатные функции:
  * varSamp - выборочная дисперсия
  * stddevSamp - среднее выборочное квадратичное отклонение
  * varPop - дисперсия
  * stddevPop - среднее квадратичное отклонение
  * covarSamp - выборочная ковариация
  * covarPop - ковариация
  */

/** Параллельный и инкрементальный алгоритм для вычисления дисперсии.
  * Источник: "Updating formulae and a pairwise algorithm for computing sample variances"
  * (Chan et al., Stanford University, 12.1979)
  */
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
		if (total_count == 0)
			return;

		Float64 factor = static_cast<Float64>(count * source.count) / total_count;
		Float64 delta = mean - source.mean;

		auto res = std::minmax(count, source.count);
		if (((1 - static_cast<Float64>(res.first) / res.second) < 0.001) && (res.first > 10000))
		{
			/// Эта формула более стабильная, когда размеры обоих источников велики и сравнимы.
			mean = (source.count * source.mean + count * mean) / total_count;
		}
		else
			mean = source.mean + delta * (count / total_count);

		m2 += source.m2 + delta * delta * factor;
		count = total_count;
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

/** Основной код для реализации функций varSamp, stddevSamp, varPop, stddevPop.
  */
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

namespace
{

/** Реализации функции varSamp.
  */
struct VarSampImpl
{
	static constexpr auto name = "varSamp";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		if (count < 2)
			return 0.0;
		else
			return m2 / (count - 1);
	}
};

/** Реализация функции stddevSamp.
  */
struct StdDevSampImpl
{
	static constexpr auto name = "stddevSamp";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		return sqrt(VarSampImpl::apply(m2, count));
	}
};

/** Реализация функции varPop.
  */
struct VarPopImpl
{
	static constexpr auto name = "varPop";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		if (count < 2)
			return 0.0;
		else
			return m2 / count;
	}
};

/** Реализация функции stddevPop.
  */
struct StdDevPopImpl
{
	static constexpr auto name = "stddevPop";

	static inline Float64 apply(Float64 m2, UInt64 count)
	{
		return sqrt(VarPopImpl::apply(m2, count));
	}
};

}

/** Параллельный и инкрементальный алгоритм для вычисления ковариации.
  * Источник: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
  * (J. Bennett et al., Sandia National Laboratories,
  *  2009 IEEE International Conference on Cluster Computing)
  */
template<typename T, typename U, typename Op>
class CovarianceData
{
public:
	CovarianceData() = default;

	void update(const IColumn & column_left, const IColumn & column_right, size_t row_num)
	{
		T left_received = static_cast<const ColumnVector<T> &>(column_left).getData()[row_num];
		Float64 val_left = static_cast<Float64>(left_received);

		U right_received = static_cast<const ColumnVector<U> &>(column_right).getData()[row_num];
		Float64 val_right = static_cast<Float64>(right_received);

		Float64 old_right_mean = right_mean;

		++count;
		left_mean += (val_left - left_mean) / count;
		right_mean += (val_right - right_mean) / count;
		co_moment += (val_left - left_mean) * (val_right - old_right_mean);
	}

	void mergeWith(const CovarianceData & source)
	{
		UInt64 total_count = count + source.count;
		if (total_count == 0)
			return;

		Float64 factor = static_cast<Float64>(count * source.count) / total_count;
		Float64 left_delta = left_mean - source.left_mean;
		Float64 right_delta = right_mean - source.right_mean;

		left_mean += left_delta * (source.count / total_count);
		right_mean += right_delta * (source.count / total_count);
		co_moment += source.co_moment + left_delta * right_delta * factor;
		count = total_count;
	}

	void serialize(WriteBuffer & buf) const
	{
		writeVarUInt(count, buf);
		writeBinary(left_mean, buf);
		writeBinary(right_mean, buf);
		writeBinary(co_moment, buf);
	}

	void deserialize(ReadBuffer & buf)
	{
		readVarUInt(count, buf);
		readBinary(left_mean, buf);
		readBinary(right_mean, buf);
		readBinary(co_moment, buf);
	}

	void publish(IColumn & to) const
	{
		static_cast<ColumnFloat64 &>(to).getData().push_back(Op::apply(co_moment, count));
	}

private:
	UInt64 count = 0;
	Float64 left_mean = 0.0;
	Float64 right_mean = 0.0;
	Float64 co_moment = 0.0;
};

template<typename T, typename U, typename Op>
class AggregateFunctionCovariance final : public IBinaryAggregateFunction<CovarianceData<T, U, Op>, AggregateFunctionCovariance<T, U, Op> >
{
public:
	String getName() const override { return Op::name; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeFloat64;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		if (!arguments[0]->behavesAsNumber())
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument to function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!arguments[1]->behavesAsNumber())
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument to function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void addOne(AggregateDataPtr place, const IColumn & column_left, const IColumn & column_right, size_t row_num) const
	{
		this->data(place).update(column_left, column_right, row_num);
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
		CovarianceData<T, U, Op> source;
		source.deserialize(buf);

		this->data(place).mergeWith(source);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		this->data(place).publish(to);
	}
};

namespace
{

/** Реализация функции covarSamp.
  */
struct CovarSampImpl
{
	static constexpr auto name = "covarSamp";

	static inline Float64 apply(Float64 co_moment, UInt64 count)
	{
		if (count < 2)
			return 0.0;
		else
			return co_moment / (count - 1);
	}
};

/** Реализация функции covarPop.
  */
struct CovarPopImpl
{
	static constexpr auto name = "covarPop";

	static inline Float64 apply(Float64 co_moment, UInt64 count)
	{
		if (count < 2)
			return 0.0;
		else
			return co_moment / count;
	}
};

}

template<typename T>
using AggregateFunctionVarSamp = AggregateFunctionVariance<T, VarSampImpl>;

template<typename T>
using AggregateFunctionStdDevSamp = AggregateFunctionVariance<T, StdDevSampImpl>;

template<typename T>
using AggregateFunctionVarPop = AggregateFunctionVariance<T, VarPopImpl>;

template<typename T>
using AggregateFunctionStdDevPop = AggregateFunctionVariance<T, StdDevPopImpl>;

template<typename T, typename U>
using AggregateFunctionCovarSamp = AggregateFunctionCovariance<T, U, CovarSampImpl>;

template<typename T, typename U>
using AggregateFunctionCovarPop = AggregateFunctionCovariance<T, U, CovarPopImpl>;

}
