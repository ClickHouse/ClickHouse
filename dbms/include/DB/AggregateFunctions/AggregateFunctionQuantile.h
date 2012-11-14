#pragma once

#include <stats/ReservoirSampler.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


/** Приближённо вычисляет квантиль.
  * В качестве типа аргумента может быть только числовой тип (в том числе, дата и дата-с-временем).
  * Если returns_float = true, то типом результата будет Float64, иначе - тип результата совпадает с типом аргумента.
  * Для дат и дат-с-временем returns_float следует задавать равным false.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantile : public IUnaryAggregateFunction
{
private:
	typedef AggregateFunctionQuantile<ArgumentFieldType, returns_float> Self;
	typedef ReservoirSampler<ArgumentFieldType> Sample;

	Sample sample;
	double level;
	DataTypePtr type;

public:
	AggregateFunctionQuantile(double level_ = 0.5) : level(level_) {}

	String getName() const { return "quantile"; }
	String getTypeID() const { return (returns_float ? "quantile_float_" : "quantile_rounded_") + TypeName<ArgumentFieldType>::get(); }

	AggregateFunctionPlainPtr cloneEmpty() const
	{
		Self * res = new Self(level);
		res->type = type;
		return res;
	}

	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (returns_float)
			type = new DataTypeFloat64;
		else
			type = argument;
	}

	void setParameters(const Row & params)
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = boost::apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}

	void addOne(const Field & value)
	{
		sample.insert(boost::get<typename NearestFieldType<ArgumentFieldType>::Type>(value));
	}

	void merge(const IAggregateFunction & rhs)
	{
		sample.merge(static_cast<const Self &>(rhs).sample);
	}

	void serialize(WriteBuffer & buf) const
	{
		sample.write(buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		Sample tmp_sample;
		tmp_sample.read(buf);
		sample.merge(tmp_sample);
	}

	Field getResult() const
	{
		/// Sample может отсортироваться при получении квантиля, но в этом контексте можно не считать это нарушением константности.

		if (returns_float)
			return Float64(const_cast<Sample &>(sample).quantileInterpolated(level));
		else
			return typename NearestFieldType<ArgumentFieldType>::Type(const_cast<Sample &>(sample).quantileInterpolated(level));
	}
};

}
