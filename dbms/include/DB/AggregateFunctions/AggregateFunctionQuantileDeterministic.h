#pragma once

#include <DB/AggregateFunctions/ReservoirSamplerDeterministic.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{

template <typename ArgumentFieldType>
struct AggregateFunctionQuantileDeterministicData
{
	using Sample = ReservoirSamplerDeterministic<ArgumentFieldType, ReservoirSamplerDeterministicOnEmpty::RETURN_NAN_OR_ZERO>;
	Sample sample;	/// TODO Добавить MemoryTracker
};


/** Приближённо вычисляет квантиль.
  * В качестве типа аргумента может быть только числовой тип (в том числе, дата и дата-с-временем).
  * Если returns_float = true, то типом результата будет Float64, иначе - тип результата совпадает с типом аргумента.
  * Для дат и дат-с-временем returns_float следует задавать равным false.
  */
template <typename ArgumentFieldType, bool returns_float = true>
class AggregateFunctionQuantileDeterministic final
	: public IBinaryAggregateFunction<
		AggregateFunctionQuantileDeterministicData<ArgumentFieldType>,
		AggregateFunctionQuantileDeterministic<ArgumentFieldType, returns_float>>
{
private:
	using Sample = typename AggregateFunctionQuantileDeterministicData<ArgumentFieldType>::Sample;

	double level;
	DataTypePtr type;

public:
	AggregateFunctionQuantileDeterministic(double level_ = 0.5) : level(level_) {}

	String getName() const { return "quantileDeterministic"; }

	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		type = returns_float ? new DataTypeFloat64 : arguments[0];

		if (!arguments[1]->isNumeric())
			throw Exception{
				"Invalid type of second argument to function " + getName() +
					", got " + arguments[1]->getName() + ", expected numeric",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};
	}

	void setParameters(const Array & params)
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}


	void addOne(AggregateDataPtr place, const IColumn & column, const IColumn & determinator, size_t row_num) const
	{
		this->data(place).sample.insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num],
			determinator.get64(row_num));
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sample.merge(this->data(rhs).sample);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).sample.write(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Sample tmp_sample;
		tmp_sample.read(buf);
		this->data(place).sample.merge(tmp_sample);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
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
class AggregateFunctionQuantilesDeterministic final
	: public IBinaryAggregateFunction<
		AggregateFunctionQuantileDeterministicData<ArgumentFieldType>,
		AggregateFunctionQuantilesDeterministic<ArgumentFieldType, returns_float>>
{
private:
	using Sample = typename AggregateFunctionQuantileDeterministicData<ArgumentFieldType>::Sample;

	typedef std::vector<double> Levels;
	Levels levels;
	DataTypePtr type;

public:
	String getName() const { return "quantilesDeterministic"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeArray(type);
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		type = returns_float ? new DataTypeFloat64 : arguments[0];

		if (!arguments[1]->isNumeric())
			throw Exception{
				"Invalid type of second argument to function " + getName() +
					", got " + arguments[1]->getName() + ", expected numeric",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};
	}

	void setParameters(const Array & params)
	{
		if (params.empty())
			throw Exception("Aggregate function " + getName() + " requires at least one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		size_t size = params.size();
		levels.resize(size);

		for (size_t i = 0; i < size; ++i)
			levels[i] = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[i]);
	}


	void addOne(AggregateDataPtr place, const IColumn & column, const IColumn & determinator, size_t row_num) const
	{
		this->data(place).sample.insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num],
			determinator.get64(row_num));
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).sample.merge(this->data(rhs).sample);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).sample.write(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Sample tmp_sample;
		tmp_sample.read(buf);
		this->data(place).sample.merge(tmp_sample);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
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
