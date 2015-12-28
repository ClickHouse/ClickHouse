#pragma once

#include <limits>

#include <DB/Common/MemoryTracker.h>
#include <DB/Common/HashTable/Hash.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>
#include <DB/AggregateFunctions/QuantilesCommon.h>

#include <DB/Columns/ColumnArray.h>

#include <ext/range.hpp>


namespace DB
{

/** Вычисляет квантиль для времени в миллисекундах, меньшего 30 сек.
  * Если значение больше 30 сек, то значение приравнивается к 30 сек.
  *
  * Если всего значений не больше 32, то вычисление точное.
  *
  * Иначе:
  *  Если время меньше 1024 мс., то вычисление точное.
  *  Иначе вычисление идёт с округлением до числа, кратного 16 мс.
  */

#define TINY_MAX_ELEMS 31
#define BIG_THRESHOLD 30000

namespace detail
{
	/** Вспомогательная структура для оптимизации в случае маленького количества значений.
	  * Размер - 64 байта. Должна быть POD-типом (используется в union).
	  */
	struct QuantileTimingTiny
	{
		mutable UInt16 elems[TINY_MAX_ELEMS];	/// mutable потому что сортировка массива не считается изменением состояния.
		UInt16 count;	/// Важно, чтобы count был не в первых 8 байтах структуры. Вы должны сами инициализировать его нулём.

		/// Можно использовать только пока count < TINY_MAX_ELEMS.
		void insert(UInt64 x)
		{
			if (unlikely(x > BIG_THRESHOLD))
				x = BIG_THRESHOLD;

			elems[count] = x;
			++count;
		}

		/// Можно использовать только пока count + rhs.count <= TINY_MAX_ELEMS.
		void merge(const QuantileTimingTiny & rhs)
		{
			for (size_t i = 0; i < rhs.count; ++i)
			{
				elems[count] = rhs.elems[i];
				++count;
			}
		}

		void serialize(WriteBuffer & buf) const
		{
			writeBinary(count, buf);
			buf.write(reinterpret_cast<const char *>(elems), count * sizeof(elems[0]));
		}

		void deserialize(ReadBuffer & buf)
		{
			readBinary(count, buf);
			buf.readStrict(reinterpret_cast<char *>(elems), count * sizeof(elems[0]));
		}

		/** Эту функцию обязательно нужно позвать перед get-функциями. */
		void prepare() const
		{
			std::sort(elems, elems + count);
		}

		UInt16 get(double level) const
		{
			return level != 1
				? elems[static_cast<size_t>(count * level)]
				: elems[count - 1];
		}

		template <typename ResultType>
		void getMany(const double * levels, size_t size, ResultType * result) const
		{
			const double * levels_end = levels + size;

			while (levels != levels_end)
			{
				*result = get(*levels);
				++levels;
				++result;
			}
		}

		/// То же самое, но в случае пустого состояния возвращается NaN.
		float getFloat(double level) const
		{
			return count
				? get(level)
				: std::numeric_limits<float>::quiet_NaN();
		}

		void getManyFloat(const double * levels, size_t size, float * result) const
		{
			if (count)
				getMany(levels, size, result);
			else
				for (size_t i = 0; i < size; ++i)
					result[i] = std::numeric_limits<float>::quiet_NaN();
		}
	};


	#define SMALL_THRESHOLD 1024
	#define BIG_SIZE ((BIG_THRESHOLD - SMALL_THRESHOLD) / BIG_PRECISION)
	#define BIG_PRECISION 16


	/** Для большого количества значений. Размер около 20 КБ.
	  * TODO: Есть off-by-one ошибки - может возвращаться значение на 1 больше нужного.
	  */
	class QuantileTimingLarge
	{
	private:
		/// Общее число значений.
		UInt64 count;

		/// Число значений для каждого значения меньше small_threshold.
		UInt64 count_small[SMALL_THRESHOLD];

		/// Число значений для каждого значения от small_threshold до big_threshold, округлённого до big_precision.
		UInt64 count_big[BIG_SIZE];

		/// Получить значение квантиля по индексу в массиве count_big.
		static inline UInt16 indexInBigToValue(size_t i)
		{
			return (i * BIG_PRECISION) + SMALL_THRESHOLD
				+ (intHash32<0>(i) % BIG_PRECISION - (BIG_PRECISION / 2));	/// Небольшая рандомизация, чтобы не было заметно, что все значения чётные.
		}

	public:
		QuantileTimingLarge()
		{
			memset(this, 0, sizeof(*this));
		}

		QuantileTimingLarge(ReadBuffer & buf)
		{
			deserialize(buf);
		}

		void insert(UInt64 x)
		{
			insertWeighted(x, 1);
		}

		void insertWeighted(UInt64 x, size_t weight)
		{
			count += weight;

			if (x < SMALL_THRESHOLD)
				count_small[x] += weight;
			else if (x < BIG_THRESHOLD)
				count_big[(x - SMALL_THRESHOLD) / BIG_PRECISION] += weight;
		}

		void merge(const QuantileTimingLarge & rhs)
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
			merge(QuantileTimingLarge(buf));
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
				return indexInBigToValue(i);

			return BIG_THRESHOLD;
		}

		/// Получить значения size квантилей уровней levels. Записать size результатов начиная с адреса result.
		/// indices - массив индексов levels такой, что соответствующие элементы будут идти в порядке по возрастанию.
		template <typename ResultType>
		void getMany(const double * levels, const size_t * indices, size_t size, ResultType * result) const
		{
			const auto indices_end = indices + size;
			auto index = indices;

			UInt64 pos = count * levels[*index];

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
					result[*index] = i;

					++index;

					if (index == indices_end)
						return;

					pos = count * levels[*index];
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
					result[*index] = indexInBigToValue(i);

					++index;

					if (index == indices_end)
						return;

					pos = count * levels[*index];
				}
			}

			while (index < indices_end)
			{
				result[*index] = BIG_THRESHOLD;

				++index;
			}
		}

		/// То же самое, но в случае пустого состояния возвращается NaN.
		float getFloat(double level) const
		{
			return count
				? get(level)
				: std::numeric_limits<float>::quiet_NaN();
		}

		void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result) const
		{
			if (count)
				getMany(levels, levels_permutation, size, result);
			else
				for (size_t i = 0; i < size; ++i)
					result[i] = std::numeric_limits<float>::quiet_NaN();
		}
	};
}


/** sizeof - 64 байта.
  * Если их не хватает - выделяет дополнительно около 20 КБ памяти.
  */
class QuantileTiming : private boost::noncopyable
{
private:
	union
	{
		detail::QuantileTimingTiny tiny;
		detail::QuantileTimingLarge * large;
	};

	bool isLarge() const { return tiny.count == TINY_MAX_ELEMS + 1; }

	void toLarge()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(detail::QuantileTimingLarge));

		/// На время копирования данных из tiny, устанавливать значение large ещё нельзя (иначе оно перезатрёт часть данных).
		detail::QuantileTimingLarge * tmp_large = new detail::QuantileTimingLarge;

		for (size_t i = 0; i < tiny.count; ++i)
			tmp_large->insert(tiny.elems[i]);

		large = tmp_large;
		tiny.count = TINY_MAX_ELEMS + 1;
	}

public:
	QuantileTiming()
	{
		tiny.count = 0;
	}

	~QuantileTiming()
	{
		if (isLarge())
		{
			delete large;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(detail::QuantileTimingLarge));
		}
	}

	void insert(UInt64 x)
	{
		if (tiny.count < TINY_MAX_ELEMS)
		{
			tiny.insert(x);
		}
		else
		{
			if (unlikely(tiny.count == TINY_MAX_ELEMS))
				toLarge();

			large->insert(x);
		}
	}

	void insertWeighted(UInt64 x, size_t weight)
	{
		/// NOTE: Первое условие - для того, чтобы избежать переполнения.
		if (weight < TINY_MAX_ELEMS && tiny.count + weight <= TINY_MAX_ELEMS)
		{
			for (size_t i = 0; i < weight; ++i)
				tiny.insert(x);
		}
		else
		{
			if (unlikely(tiny.count <= TINY_MAX_ELEMS))
				toLarge();

			large->insertWeighted(x, weight);
		}
	}

	void merge(const QuantileTiming & rhs)
	{
		if (tiny.count + rhs.tiny.count <= TINY_MAX_ELEMS)
		{
			tiny.merge(rhs.tiny);
		}
		else
		{
			if (!isLarge())
				toLarge();

			if (rhs.isLarge())
			{
				large->merge(*rhs.large);
			}
			else
			{
				for (size_t i = 0; i < rhs.tiny.count; ++i)
					large->insert(rhs.tiny.elems[i]);
			}
		}
	}

	void serialize(WriteBuffer & buf) const
	{
		bool is_large = isLarge();
		DB::writeBinary(is_large, buf);

		if (is_large)
			large->serialize(buf);
		else
			tiny.serialize(buf);
	}

	void deserialize(ReadBuffer & buf)
	{
		bool is_rhs_large;
		DB::readBinary(is_rhs_large, buf);

		if (is_rhs_large)
		{
			if (!isLarge())
			{
				tiny.count = TINY_MAX_ELEMS + 1;

				if (current_memory_tracker)
					current_memory_tracker->alloc(sizeof(detail::QuantileTimingLarge));

				large = new detail::QuantileTimingLarge;
			}

			large->deserialize(buf);
		}
		else
			tiny.deserialize(buf);
	}

	void deserializeMerge(ReadBuffer & buf)
	{
		bool is_rhs_large;
		DB::readBinary(is_rhs_large, buf);

		if (is_rhs_large)
		{
			if (!isLarge())
			{
				tiny.count = TINY_MAX_ELEMS + 1;

				if (current_memory_tracker)
					current_memory_tracker->alloc(sizeof(detail::QuantileTimingLarge));

				large = new detail::QuantileTimingLarge;
			}

			large->merge(detail::QuantileTimingLarge(buf));
		}
		else
		{
			QuantileTiming rhs;
			rhs.tiny.deserialize(buf);

			merge(rhs);
		}
	}


	/// Получить значение квантиля уровня level. Уровень должен быть от 0 до 1.
	UInt16 get(double level) const
	{
		if (isLarge())
		{
			return large->get(level);
		}
		else
		{
			tiny.prepare();
			return tiny.get(level);
		}
	}

	/// Получить значения size квантилей уровней levels. Записать size результатов начиная с адреса result.
	template <typename ResultType>
	void getMany(const double * levels, const size_t * levels_permutation, size_t size, ResultType * result) const
	{
		if (isLarge())
		{
			return large->getMany(levels, levels_permutation, size, result);
		}
		else
		{
			tiny.prepare();
			return tiny.getMany(levels, size, result);
		}
	}

	/// То же самое, но в случае пустого состояния возвращается NaN.
	float getFloat(double level) const
	{
		return tiny.count
			? get(level)
			: std::numeric_limits<float>::quiet_NaN();
	}

	void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result) const
	{
		if (tiny.count)
			getMany(levels, levels_permutation, size, result);
		else
			for (size_t i = 0; i < size; ++i)
				result[i] = std::numeric_limits<float>::quiet_NaN();
	}
};

#undef SMALL_THRESHOLD
#undef BIG_THRESHOLD
#undef BIG_SIZE
#undef BIG_PRECISION
#undef TINY_MAX_ELEMS


template <typename ArgumentFieldType>
class AggregateFunctionQuantileTiming final : public IUnaryAggregateFunction<QuantileTiming, AggregateFunctionQuantileTiming<ArgumentFieldType> >
{
private:
	double level;

public:
	AggregateFunctionQuantileTiming(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileTiming"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeFloat32;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnFloat32 &>(to).getData().push_back(this->data(place).getFloat(level));
	}
};


/** То же самое, но с двумя аргументами. Второй аргумент - "вес" (целое число) - сколько раз учитывать значение.
  */
template <typename ArgumentFieldType, typename WeightFieldType>
class AggregateFunctionQuantileTimingWeighted final
	: public IBinaryAggregateFunction<QuantileTiming, AggregateFunctionQuantileTimingWeighted<ArgumentFieldType, WeightFieldType>>
{
private:
	double level;

public:
	AggregateFunctionQuantileTimingWeighted(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileTimingWeighted"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeFloat32;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place).insertWeighted(
			static_cast<const ColumnVector<ArgumentFieldType> &>(column_value).getData()[row_num],
			static_cast<const ColumnVector<WeightFieldType> &>(column_weight).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnFloat32 &>(to).getData().push_back(this->data(place).getFloat(level));
	}
};


/** То же самое, но позволяет вычислить сразу несколько квантилей.
  * Для этого, принимает в качестве параметров несколько уровней. Пример: quantilesTiming(0.5, 0.8, 0.9, 0.95)(ConnectTiming).
  * Возвращает массив результатов.
  */
template <typename ArgumentFieldType>
class AggregateFunctionQuantilesTiming final : public IUnaryAggregateFunction<QuantileTiming, AggregateFunctionQuantilesTiming<ArgumentFieldType> >
{
private:
	QuantileLevels<double> levels;

public:
	String getName() const override { return "quantilesTiming"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(new DataTypeFloat32);
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}


	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).insert(static_cast<const ColumnVector<ArgumentFieldType> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
		size_t old_size = data_to.size();
		data_to.resize(data_to.size() + size);

		this->data(place).getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
	}
};


template <typename ArgumentFieldType, typename WeightFieldType>
class AggregateFunctionQuantilesTimingWeighted final
	: public IBinaryAggregateFunction<QuantileTiming, AggregateFunctionQuantilesTimingWeighted<ArgumentFieldType, WeightFieldType>>
{
private:
	QuantileLevels<double> levels;

public:
	String getName() const override { return "quantilesTimingWeighted"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(new DataTypeFloat32);
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place).insertWeighted(
			static_cast<const ColumnVector<ArgumentFieldType> &>(column_value).getData()[row_num],
			static_cast<const ColumnVector<WeightFieldType> &>(column_weight).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).merge(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).serialize(buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserializeMerge(buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
		size_t old_size = data_to.size();
		data_to.resize(data_to.size() + size);

		this->data(place).getManyFloat(&levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
	}
};


}
