#pragma once

#include <DB/AggregateFunctions/ReservoirSampler.h>

#include <DB/Core/FieldVisitors.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{

template <typename ArgumentFieldType>
struct AggregateFunctionQuantileData
{
	using Sample = ReservoirSampler<ArgumentFieldType, ReservoirSamplerOnEmpty::RETURN_NAN_OR_ZERO>;
	Sample sample;	/// TODO Добавить MemoryTracker
};


/** Приближённо вычисляет квантиль.
  * В качестве типа аргумента может быть только числовой тип (в том числе, дата и дата-с-временем).
  * Если returns_float = true, то типом результата будет Float64, иначе - тип результата совпадает с типом аргумента.
  * Для дат и дат-с-временем returns_float следует задавать равным false.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantile final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileData<ArgumentFieldType>, AggregateFunctionQuantile<ArgumentFieldType, returns_float> >
{
private:
	using Sample = typename AggregateFunctionQuantileData<ArgumentFieldType>::Sample;

	double level;
	DataTypePtr type;

public:
	AggregateFunctionQuantile(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantile"; }

	DataTypePtr getReturnType() const override
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (returns_float)
			type = std::make_shared<DataTypeFloat64>();
		else
			type = argument;
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
	{
		this->data(place).sample.insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
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
		/// Sample может отсортироваться при получении квантиля, но в этом контексте можно не считать это нарушением константности.
		Sample & sample = const_cast<Sample &>(this->data(place).sample);

		if (returns_float)
			static_cast<ColumnFloat64 &>(to).getData().push_back(sample.quantileInterpolated(level));
		else
			static_cast<ColumnVector<ArgumentFieldType> &>(to).getData().push_back(sample.quantileInterpolated(level));
	}
};


/** То же самое, но позволяет вычислить сразу несколько квантилей.
  * Для этого, принимает в качестве параметров несколько уровней. Пример: quantiles(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Возвращает массив результатов.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantiles final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileData<ArgumentFieldType>, AggregateFunctionQuantiles<ArgumentFieldType, returns_float> >
{
private:
	using Sample = typename AggregateFunctionQuantileData<ArgumentFieldType>::Sample;

	using Levels = std::vector<double>;
	Levels levels;
	DataTypePtr type;

public:
	String getName() const override { return "quantiles"; }

	DataTypePtr getReturnType() const override
	{
		return std::make_shared<DataTypeArray>(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (returns_float)
			type = std::make_shared<DataTypeFloat64>();
		else
			type = argument;
	}

	void setParameters(const Array & params) override
	{
		if (params.empty())
			throw Exception("Aggregate function " + getName() + " requires at least one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		size_t size = params.size();
		levels.resize(size);

		for (size_t i = 0; i < size; ++i)
			levels[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[i]);
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
	{
		this->data(place).sample.insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
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
		/// Sample может отсортироваться при получении квантиля, но в этом контексте можно не считать это нарушением константности.
		Sample & sample = const_cast<Sample &>(this->data(place).sample);

		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		if (returns_float)
		{
			ColumnFloat64::Container_t & data_to = static_cast<ColumnFloat64 &>(arr_to.getData()).getData();

			for (size_t i = 0; i < size; ++i)
				data_to.push_back(sample.quantileInterpolated(levels[i]));
		}
		else
		{
			typename ColumnVector<ArgumentFieldType>::Container_t & data_to = static_cast<ColumnVector<ArgumentFieldType> &>(arr_to.getData()).getData();

			for (size_t i = 0; i < size; ++i)
				data_to.push_back(sample.quantileInterpolated(levels[i]));
		}
	}
};

}
