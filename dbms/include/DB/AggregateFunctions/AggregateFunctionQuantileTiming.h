#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>

#include <DB/Columns/ColumnArray.h>


namespace DB
{

/** Вычисляет квантиль для времени в миллисекундах, меньшего 30 сек.
  * Если время меньше 1024 мс., то вычисление точное.
  * Если время меньше 30 сек., то вычисление идёт с округлением до числа, кратного 16 мс.
  * Иначе, время приравнивается к 30 сек.
  */

#define SMALL_THRESHOLD 1024
#define BIG_THRESHOLD 30000
#define BIG_SIZE ((BIG_THRESHOLD - SMALL_THRESHOLD) / BIG_PRECISION)
#define BIG_PRECISION 16

class QuantileTiming
{
private:
	/// Общее число значений.
	UInt64 count;

	/// Число значений для каждого значения меньше small_threshold.
	UInt64 count_small[SMALL_THRESHOLD];

	/// Число значений для каждого значения от small_threshold до big_threshold, округлённого до big_precision.
	UInt64 count_big[BIG_SIZE];

public:
	QuantileTiming()
	{
		memset(this, 0, sizeof(*this));
	}

	QuantileTiming(ReadBuffer & buf)
	{
		deserialize(buf);
	}

	void insert(UInt64 x)
	{
		++count;

		if (x < SMALL_THRESHOLD)
			++count_small[x];
		else if (x < BIG_THRESHOLD)
			++count_big[(x - SMALL_THRESHOLD) / BIG_PRECISION];
	}

	void merge(const QuantileTiming & rhs)
	{
		count += rhs.count;

		for (size_t i = 0; i < SMALL_THRESHOLD; ++i)
			count_small[i] += rhs.count_small[i];

		for (size_t i = 0; i < BIG_SIZE; ++i)
			count_big[i] += rhs.count_big[i];
	}

	void serialize(WriteBuffer & buf) const
	{
		buf.write(reinterpret_cast<const char *>(this), sizeof(*this));
	}

	void deserialize(ReadBuffer & buf)
	{
		buf.readStrict(reinterpret_cast<char *>(this), sizeof(*this));
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		merge(QuantileTiming(buf));
	}


	/// Получить значение квантиля уровня level. Уровень должен быть от 0 до 1.
	UInt16 get(double level) const
	{
		UInt64 pos = count * level;

		UInt64 accumulated = 0;

		size_t i = 0;
		while (i < SMALL_THRESHOLD && accumulated < pos)
		{
			accumulated += count_small[i];
			++i;
		}

		if (i < SMALL_THRESHOLD)
			return i;

		i = 0;
		while (i < BIG_SIZE && accumulated < pos)
		{
			accumulated += count_big[i];
			++i;
		}

		if (i < BIG_SIZE)
			return (i * BIG_PRECISION) + SMALL_THRESHOLD;

		return BIG_THRESHOLD;
	}


	/// Получить значения size квантилей уровней levels. Записать size результатов начиная с адреса result.
	void getMany(const double * levels, size_t size, UInt16 * result) const
	{
		const double * levels_end = levels + size;
		const double * level = levels;
		UInt64 pos = count * *level;

		UInt64 accumulated = 0;

		size_t i = 0;
		while (i < SMALL_THRESHOLD)
		{
			while (i < SMALL_THRESHOLD && accumulated < pos)
			{
				accumulated += count_small[i];
				++i;
			}

			if (i < SMALL_THRESHOLD)
			{
				*result = i;

				++level;
				++result;

				if (level == levels_end)
					return;
				
				pos = count * *level;
			}
		}

		i = 0;
		while (i < BIG_SIZE)
		{
			while (i < BIG_SIZE && accumulated < pos)
			{
				accumulated += count_big[i];
				++i;
			}

			if (i < BIG_SIZE)
			{
				*result = (i * BIG_PRECISION) + SMALL_THRESHOLD;

				++level;
				++result;

				if (level == levels_end)
					return;

				pos = count * *level;
			}
		}

		while (level < levels_end)
		{
			*result = BIG_THRESHOLD;
			
			++level;
			++result;
		}
	}
};

#undef SMALL_THRESHOLD
#undef BIG_THRESHOLD
#undef BIG_SIZE
#undef BIG_PRECISION


template <typename ArgumentFieldType>
class AggregateFunctionQuantileTiming : public IUnaryAggregateFunction<QuantileTiming>
{
private:
	double level;

public:
	AggregateFunctionQuantileTiming(double level_ = 0.5) : level(level_) {}

	String getName() const { return "quantileTiming"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt16;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Row & params)
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		static_cast<ColumnUInt16 &>(to).getData().push_back(this->data(place).get(level));
	}
};


/** То же самое, но позволяет вычислить сразу несколько квантилей.
  * Для этого, принимает в качестве параметров несколько уровней. Пример: quantilesTiming(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Возвращает массив результатов.
  */
template <typename ArgumentFieldType>
class AggregateFunctionQuantilesTiming : public IUnaryAggregateFunction<QuantileTiming>
{
private:
	typedef std::vector<double> Levels;
	Levels levels;

public:
	String getName() const { return "quantilesTiming"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeArray(new DataTypeUInt16);
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Row & params)
	{
		if (params.empty())
			throw Exception("Aggregate function " + getName() + " requires at least one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		size_t size = params.size();
		levels.resize(size);

		for (size_t i = 0; i < size; ++i)
			levels[i] = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[i]);
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);
		
		typename ColumnUInt16::Container_t & data_to = static_cast<ColumnUInt16 &>(arr_to.getData()).getData();
		size_t old_size = data_to.size();
		data_to.resize(data_to.size() + size);
			
		this->data(place).getMany(&levels[0], size, &data_to[old_size]);
	}
};

}
