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
  * Если всего значений не больше примерно 5670, то вычисление точное.
  *
  * Иначе:
  *  Если время меньше 1024 мс., то вычисление точное.
  *  Иначе вычисление идёт с округлением до числа, кратного 16 мс.
  *
  * Используется три разные структуры данных:
  * - плоский массив (всех встреченных значений) фиксированной длины, выделяемый inplace, размер 64 байта; хранит 0..31 значений;
  * - плоский массив (всех встреченных значений), выделяемый отдельно, увеличивающейся длины;
  * - гистограмма (то есть, отображение значение -> количество), состоящая из двух частей:
  * -- для значений от 0 до 1023 - с шагом 1;
  * -- для значений от 1024 до 30000 - с шагом 16;
  */

#define TINY_MAX_ELEMS 31
#define BIG_THRESHOLD 30000

namespace detail
{
	/** Вспомогательная структура для оптимизации в случае маленького количества значений
	  * - плоский массив фиксированного размера "на стеке", в который кладутся все встреченные значения подряд.
	  * Размер - 64 байта. Должна быть POD-типом (используется в union).
	  */
	struct QuantileTimingTiny
	{
		mutable UInt16 elems[TINY_MAX_ELEMS];	/// mutable потому что сортировка массива не считается изменением состояния.
		/// Важно, чтобы count был в конце структуры, так как начало структуры будет впоследствии перезатёрто другими объектами.
		/// Вы должны сами инициализировать его нулём.
		/// Почему? Поле count переиспользуется и в тех случаях, когда в union-е лежат другие структуры
		///  (размер которых не дотягивает до этого поля.)
		UInt16 count;

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


	/** Вспомогательная структура для оптимизации в случае среднего количества значений
	  *  - плоский массив, выделенный отдельно, в который кладутся все встреченные значения подряд.
	  */
	struct QuantileTimingMedium
	{
		/// sizeof - 24 байта.
		using Array = PODArray<UInt16, 128>;
		mutable Array elems;	/// mutable потому что сортировка массива не считается изменением состояния.

		QuantileTimingMedium() {}
		QuantileTimingMedium(const UInt16 * begin, const UInt16 * end) : elems(begin, end) {}

		void insert(UInt64 x)
		{
			if (unlikely(x > BIG_THRESHOLD))
				x = BIG_THRESHOLD;

			elems.emplace_back(x);
		}

		void merge(const QuantileTimingMedium & rhs)
		{
			elems.insert(rhs.elems.begin(), rhs.elems.end());
		}

		void serialize(WriteBuffer & buf) const
		{
			writeBinary(elems.size(), buf);
			buf.write(reinterpret_cast<const char *>(&elems[0]), elems.size() * sizeof(elems[0]));
		}

		void deserialize(ReadBuffer & buf)
		{
			size_t size = 0;
			readBinary(size, buf);
			elems.resize(size);
			buf.readStrict(reinterpret_cast<char *>(&elems[0]), size * sizeof(elems[0]));
		}

		UInt16 get(double level) const
		{
			UInt16 quantile = 0;

			if (!elems.empty())
			{
				size_t n = level < 1
					? level * elems.size()
					: (elems.size() - 1);

				/// Сортировка массива не будет считаться нарушением константности.
				auto & array = const_cast<Array &>(elems);
				std::nth_element(array.begin(), array.begin() + n, array.end());
				quantile = array[n];
			}

			return quantile;
		}

		template <typename ResultType>
		void getMany(const double * levels, const size_t * levels_permutation, size_t size, ResultType * result) const
		{
			size_t prev_n = 0;
			auto & array = const_cast<Array &>(elems);
			for (size_t i = 0; i < size; ++i)
			{
				auto level_index = levels_permutation[i];
				auto level = levels[level_index];

				size_t n = level < 1
					? level * elems.size()
					: (elems.size() - 1);

				std::nth_element(array.begin() + prev_n, array.begin() + n, array.end());

				result[level_index] = array[n];
				prev_n = n;
			}
		}

		/// То же самое, но в случае пустого состояния возвращается NaN.
		float getFloat(double level) const
		{
			return !elems.empty()
				? get(level)
				: std::numeric_limits<float>::quiet_NaN();
		}

		void getManyFloat(const double * levels, const size_t * levels_permutation, size_t size, float * result) const
		{
			if (!elems.empty())
				getMany(levels, levels_permutation, size, result);
			else
				for (size_t i = 0; i < size; ++i)
					result[i] = std::numeric_limits<float>::quiet_NaN();
		}
	};


	#define SMALL_THRESHOLD 1024
	#define BIG_SIZE ((BIG_THRESHOLD - SMALL_THRESHOLD) / BIG_PRECISION)
	#define BIG_PRECISION 16

	#define SIZE_OF_LARGE_WITHOUT_COUNT ((SMALL_THRESHOLD + BIG_SIZE) * sizeof(UInt64))


	/** Для большого количества значений. Размер около 22 680 байт.
	  */
	class QuantileTimingLarge
	{
	private:
		/// Общее число значений.
		UInt64 count;
		/// Использование UInt64 весьма расточительно.
		/// Но UInt32 точно не хватает, а изобретать 6-байтные значения слишком сложно.

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

		/// Позволяет перебрать значения гистограммы, пропуская нули.
		class Iterator
		{
		private:
			const UInt64 * begin;
			const UInt64 * pos;
			const UInt64 * end;

			void adjust()
			{
				while (isValid() && 0 == *pos)
					++pos;
			}

		public:
			Iterator(const QuantileTimingLarge & parent)
				: begin(parent.count_small), pos(begin), end(&parent.count_big[BIG_SIZE])
			{
				adjust();
			}

			bool isValid() const { return pos < end; }

			void next()
			{
				++pos;
				adjust();
			}

			UInt64 count() const { return *pos; }

			UInt16 key() const
			{
				return pos - begin < SMALL_THRESHOLD
					? pos - begin
					: indexInBigToValue(pos - begin - SMALL_THRESHOLD);
			}
		};

	public:
		QuantileTimingLarge()
		{
			memset(this, 0, sizeof(*this));
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
			writeBinary(count, buf);

			if (count * 2 > SMALL_THRESHOLD + BIG_SIZE)
			{
				/// Простая сериализация для сильно заполненного случая.
				buf.write(reinterpret_cast<const char *>(this) + sizeof(count), SIZE_OF_LARGE_WITHOUT_COUNT);
			}
			else
			{
				/// Более компактная сериализация для разреженного случая.

				for (size_t i = 0; i < SMALL_THRESHOLD; ++i)
				{
					if (count_small[i])
					{
						writeBinary(UInt16(i), buf);
						writeBinary(count_small[i], buf);
					}
				}

				for (size_t i = 0; i < BIG_SIZE; ++i)
				{
					if (count_big[i])
					{
						writeBinary(UInt16(i + SMALL_THRESHOLD), buf);
						writeBinary(count_big[i], buf);
					}
				}

				/// Символизирует конец данных.
				writeBinary(UInt16(BIG_THRESHOLD), buf);
			}
		}

		void deserialize(ReadBuffer & buf)
		{
			readBinary(count, buf);

			if (count * 2 > SMALL_THRESHOLD + BIG_SIZE)
			{
				buf.readStrict(reinterpret_cast<char *>(this) + sizeof(count), SIZE_OF_LARGE_WITHOUT_COUNT);
			}
			else
			{
				while (true)
				{
					UInt16 index = 0;
					readBinary(index, buf);
					if (index == BIG_THRESHOLD)
						break;

					UInt64 count = 0;
					readBinary(count, buf);

					if (index < SMALL_THRESHOLD)
						count_small[index] = count;
					else
						count_big[index - SMALL_THRESHOLD] = count;
				}
			}
		}


		/// Получить значение квантиля уровня level. Уровень должен быть от 0 до 1.
		UInt16 get(double level) const
		{
			UInt64 pos = std::ceil(count * level);

			UInt64 accumulated = 0;
			Iterator it(*this);

			while (it.isValid())
			{
				accumulated += it.count();

				if (accumulated >= pos)
					break;

				it.next();
			}

			return it.isValid() ? it.key() : BIG_THRESHOLD;
		}

		/// Получить значения size квантилей уровней levels. Записать size результатов начиная с адреса result.
		/// indices - массив индексов levels такой, что соответствующие элементы будут идти в порядке по возрастанию.
		template <typename ResultType>
		void getMany(const double * levels, const size_t * indices, size_t size, ResultType * result) const
		{
			const auto indices_end = indices + size;
			auto index = indices;

			UInt64 pos = std::ceil(count * levels[*index]);

			UInt64 accumulated = 0;
			Iterator it(*this);

			while (it.isValid())
			{
				accumulated += it.count();

				while (accumulated >= pos)
				{
					result[*index] = it.key();
					++index;

					if (index == indices_end)
						return;

					pos = std::ceil(count * levels[*index]);
				}

				it.next();
			}

			while (index != indices_end)
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
  * Если их не хватает - выделяет дополнительно до 20 КБ памяти.
  */
class QuantileTiming : private boost::noncopyable
{
private:
	union
	{
		detail::QuantileTimingTiny tiny;
		detail::QuantileTimingMedium medium;
		detail::QuantileTimingLarge * large;
	};

	enum class Kind : UInt8
	{
		Tiny 	= 1,
		Medium 	= 2,
		Large 	= 3
	};

	Kind which() const
	{
		if (tiny.count <= TINY_MAX_ELEMS)
			return Kind::Tiny;
		if (tiny.count == TINY_MAX_ELEMS + 1)
			return Kind::Medium;
		return Kind::Large;
	}

	void tinyToMedium()
	{
		detail::QuantileTimingTiny tiny_copy = tiny;
		new (&medium) detail::QuantileTimingMedium(tiny_copy.elems, tiny_copy.elems + tiny_copy.count);
		tiny.count = TINY_MAX_ELEMS + 1;
	}

	void mediumToLarge()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(detail::QuantileTimingLarge));

		/// На время копирования данных из medium, устанавливать значение large ещё нельзя (иначе оно перезатрёт часть данных).
		detail::QuantileTimingLarge * tmp_large = new detail::QuantileTimingLarge;

		for (const auto & elem : medium.elems)
			tmp_large->insert(elem);

		medium.~QuantileTimingMedium();
		large = tmp_large;
		tiny.count = TINY_MAX_ELEMS + 2;
	}

	void tinyToLarge()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(detail::QuantileTimingLarge));

		/// На время копирования данных из medium, устанавливать значение large ещё нельзя (иначе оно перезатрёт часть данных).
		detail::QuantileTimingLarge * tmp_large = new detail::QuantileTimingLarge;

		for (size_t i = 0; i < tiny.count; ++i)
			tmp_large->insert(tiny.elems[i]);

		large = tmp_large;
		tiny.count = TINY_MAX_ELEMS + 2;
	}

	bool mediumIsWorthToConvertToLarge() const
	{
		return medium.elems.size() >= sizeof(detail::QuantileTimingLarge) / sizeof(medium.elems[0]) / 2;
	}

public:
	QuantileTiming()
	{
		tiny.count = 0;
	}

	~QuantileTiming()
	{
		Kind kind = which();

		if (kind == Kind::Medium)
		{
			medium.~QuantileTimingMedium();
		}
		else if (kind == Kind::Large)
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
				tinyToMedium();

			if (which() == Kind::Medium)
			{
				if (unlikely(mediumIsWorthToConvertToLarge()))
				{
					mediumToLarge();
					large->insert(x);
				}
				else
					medium.insert(x);
			}
			else
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
				tinyToLarge();	/// Для weighted варианта medium не используем - предположительно, нецелесообразно.

			large->insertWeighted(x, weight);
		}
	}

	/// NOTE Слишком сложный код.
	void merge(const QuantileTiming & rhs)
	{
		if (tiny.count + rhs.tiny.count <= TINY_MAX_ELEMS)
		{
			tiny.merge(rhs.tiny);
		}
		else
		{
			auto kind = which();
			auto rhs_kind = rhs.which();

			/// Если то, с чем сливаем, имеет бОльшую структуру данных, то приводим текущую структуру к такой же.
			if (kind == Kind::Tiny && rhs_kind == Kind::Medium)
			{
				tinyToMedium();
				kind = Kind::Medium;
			}
			else if (kind == Kind::Tiny && rhs_kind == Kind::Large)
			{
				tinyToLarge();
				kind = Kind::Large;
			}
			else if (kind == Kind::Medium && rhs_kind == Kind::Large)
			{
				mediumToLarge();
				kind = Kind::Large;
			}
			/// Случай, когда два состояния маленькие, но при их слиянии, они превратятся в средние.
			else if (kind == Kind::Tiny && rhs_kind == Kind::Tiny)
			{
				tinyToMedium();
				kind = Kind::Medium;
			}

			if (kind == Kind::Medium && rhs_kind == Kind::Medium)
			{
				medium.merge(rhs.medium);
			}
			else if (kind == Kind::Large && rhs_kind == Kind::Large)
			{
				large->merge(*rhs.large);
			}
			else if (kind == Kind::Medium && rhs_kind == Kind::Tiny)
			{
				medium.elems.insert(rhs.tiny.elems, rhs.tiny.elems + rhs.tiny.count);
			}
			else if (kind == Kind::Large && rhs_kind == Kind::Tiny)
			{
				for (size_t i = 0; i < rhs.tiny.count; ++i)
					large->insert(rhs.tiny.elems[i]);
			}
			else if (kind == Kind::Large && rhs_kind == Kind::Medium)
			{
				for (const auto & elem : rhs.medium.elems)
					large->insert(elem);
			}
			else
				throw Exception("Logical error in QuantileTiming::merge function: not all cases are covered", ErrorCodes::LOGICAL_ERROR);

			/// Для детерминированности, мы должны всегда переводить в large при достижении условия на размер
			///  - независимо от порядка мерджей.
			if (kind == Kind::Medium && unlikely(mediumIsWorthToConvertToLarge()))
			{
				mediumToLarge();
			}
		}
	}

	void serialize(WriteBuffer & buf) const
	{
		auto kind = which();
		DB::writePODBinary(kind, buf);

		if (kind == Kind::Tiny)
			tiny.serialize(buf);
		else if (kind == Kind::Medium)
			medium.serialize(buf);
		else
			large->serialize(buf);
	}

	/// Вызывается для пустого объекта.
	void deserialize(ReadBuffer & buf)
	{
		Kind kind;
		DB::readPODBinary(kind, buf);

		if (kind == Kind::Tiny)
		{
			tiny.deserialize(buf);
		}
		else if (kind == Kind::Medium)
		{
			tinyToMedium();
			medium.deserialize(buf);
		}
		else if (kind == Kind::Large)
		{
			tinyToLarge();
			large->deserialize(buf);
		}
	}

	/// Получить значение квантиля уровня level. Уровень должен быть от 0 до 1.
	UInt16 get(double level) const
	{
		Kind kind = which();

		if (kind == Kind::Tiny)
		{
			tiny.prepare();
			return tiny.get(level);
		}
		else if (kind == Kind::Medium)
		{
			return medium.get(level);
		}
		else
		{
			return large->get(level);
		}
	}

	/// Получить значения size квантилей уровней levels. Записать size результатов начиная с адреса result.
	template <typename ResultType>
	void getMany(const double * levels, const size_t * levels_permutation, size_t size, ResultType * result) const
	{
		Kind kind = which();

		if (kind == Kind::Tiny)
		{
			tiny.prepare();
			tiny.getMany(levels, size, result);
		}
		else if (kind == Kind::Medium)
		{
			medium.getMany(levels, levels_permutation, size, result);
		}
		else if (kind == Kind::Large)
		{
			large->getMany(levels, levels_permutation, size, result);
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
		return std::make_shared<DataTypeFloat32>();
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

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserialize(buf);
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
		return std::make_shared<DataTypeFloat32>();
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

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserialize(buf);
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
		return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
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

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserialize(buf);
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
		return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
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

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).deserialize(buf);
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
