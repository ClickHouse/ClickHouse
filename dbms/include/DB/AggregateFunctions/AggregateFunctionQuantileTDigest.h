#pragma once

#include <cmath>
#include <cstdint>
#include <cassert>

#include <vector>
#include <algorithm>

#include <DB/Common/RadixSort.h>
#include <DB/Common/PODArray.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>
#include <DB/AggregateFunctions/QuantilesCommon.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>


namespace DB
{
namespace ErrorCodes
{
	extern const int TOO_LARGE_ARRAY_SIZE;
}
}

/** Алгоритм реализовал Алексей Борзенков https://███████████.yandex-team.ru/snaury
  * Ему принадлежит авторство кода и половины комментариев в данном namespace,
  *  за исключением слияния, сериализации и сортировки, а также выбора типов и других изменений.
  * Мы благодарим Алексея Борзенкова за написание изначального кода.
  */
namespace tdigest
{

/**
  * Центроид хранит вес точек вокруг их среднего значения
  */
template <typename Value, typename Count>
struct Centroid
{
	Value mean;
	Count count;

	Centroid() = default;

	explicit Centroid(Value mean, Count count = 1)
		: mean(mean)
		, count(count)
	{}

	Centroid & operator+=(const Centroid & other)
	{
		count += other.count;
		mean += other.count * (other.mean - mean) / count;
		return *this;
	}

	bool operator<(const Centroid & other) const
	{
		return mean < other.mean;
	}
};


/** :param epsilon: значение \delta из статьи - погрешность в районе
  *				    квантиля 0.5 (по-умолчанию 0.01, т.е. 1%)
  * :param max_unmerged: при накоплении кол-ва новых точек сверх этого
  *					     значения запускается компрессия центроидов
  *					     (по-умолчанию 2048, чем выше значение - тем
  *					     больше требуется памяти, но повышается
  *					     амортизация времени выполнения)
  */
template <typename Value>
struct Params
{
	Value epsilon = 0.01;
	size_t max_unmerged = 2048;
};


/** Реализация алгоритма t-digest (https://github.com/tdunning/t-digest).
  * Этот вариант очень похож на MergingDigest на java, однако решение об
  * объединении принимается на основе оригинального условия из статьи
  * (через ограничение на размер, используя апроксимацию квантиля каждого
  * центроида, а не расстояние на кривой положения их границ). MergingDigest
  * на java даёт значительно меньше центроидов, чем данный вариант, что
  * негативно влияет на точность при том же факторе компрессии, но даёт
  * гарантии размера. Сам автор на предложение об этом варианте сказал, что
  * размер дайжеста растёт как O(log(n)), в то время как вариант на java
  * не зависит от предполагаемого кол-ва точек. Кроме того вариант на java
  * использует asin, чем немного замедляет алгоритм.
  */
template <typename Value, typename CentroidCount, typename TotalCount>
class MergingDigest
{
	using Params = tdigest::Params<Value>;
	using Centroid = tdigest::Centroid<Value, CentroidCount>;

	/// Сразу будет выделена память на несколько элементов так, чтобы состояние занимало 64 байта.
	static constexpr size_t bytes_in_arena = 64 - sizeof(DB::PODArray<Centroid>) - sizeof(TotalCount) - sizeof(uint32_t);

	using Summary = DB::PODArray<Centroid, bytes_in_arena / sizeof(Centroid), AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;

	Summary summary;
	TotalCount count = 0;
	uint32_t unmerged = 0;

	/** Линейная интерполяция в точке x на прямой (x1, y1)..(x2, y2)
	  */
	static Value interpolate(Value x, Value x1, Value y1, Value x2, Value y2)
	{
		double k = (x - x1) / (x2 - x1);
		return y1 + k * (y2 - y1);
	}

	struct RadixSortTraits
	{
		using Element = Centroid;
		using Key = Value;
		using CountType = uint32_t;
		using KeyBits = uint32_t;

		static constexpr size_t PART_SIZE_BITS = 8;

		using Transform = RadixSortFloatTransform<KeyBits>;
		using Allocator = RadixSortMallocAllocator;

		/// Функция получения ключа из элемента массива.
		static Key & extractKey(Element & elem) { return elem.mean; }
	};

public:
	/** Добавляет к дайджесту изменение x с весом cnt (по-умолчанию 1)
	  */
	void add(const Params & params, Value x, CentroidCount cnt = 1)
	{
		add(params, Centroid(x, cnt));
	}

	/** Добавляет к дайджесту центроид c
	  */
	void add(const Params & params, const Centroid & c)
	{
		summary.push_back(c);
		count += c.count;
		++unmerged;
		if (unmerged >= params.max_unmerged)
			compress(params);
	}

	/** Выполняет компрессию накопленных центроидов
	  * При объединении сохраняется инвариант на максимальный размер каждого
	  * центроида, не превышающий 4 q (1 - q) \delta N.
	  */
	void compress(const Params & params)
	{
		if (unmerged > 0)
		{
			RadixSort<RadixSortTraits>::execute(&summary[0], summary.size());

			if (summary.size() > 3)
			{
				/// Пара подряд идущих столбиков гистограммы.
				auto l = summary.begin();
				auto r = std::next(l);

				TotalCount sum = 0;
				while (r != summary.end())
				{
					// we use quantile which gives us the smallest error

					/// Отношение части гистограммы до l, включая половинку l ко всей гистограмме. То есть, какого уровня квантиль в позиции l.
					Value ql = (sum + l->count * 0.5) / count;
					Value err = ql * (1 - ql);

					/// Отношение части гистограммы до l, включая l и половинку r ко всей гистограмме. То есть, какого уровня квантиль в позиции r.
					Value qr = (sum + l->count + r->count * 0.5) / count;
					Value err2 = qr * (1 - qr);

					if (err > err2)
						err = err2;

					Value k = 4 * count * err * params.epsilon;

					/** Отношение веса склеенной пары столбиков ко всем значениям не больше,
					  *  чем epsilon умножить на некий квадратичный коэффициент, который в медиане равен 1 (4 * 1/2 * 1/2),
					  *  а по краям убывает и примерно равен расстоянию до края * 4.
					  */

					if (l->count + r->count <= k)
					{
						// it is possible to merge left and right
						/// Левый столбик "съедает" правый.
						*l += *r;
					}
					else
					{
						// not enough capacity, check the next pair
						sum += l->count;
						++l;

						/// Пропускаем все "съеденные" ранее значения.
						if (l != r)
							*l = *r;
					}
					++r;
				}

				/// По окончании цикла, все значения правее l были "съедены".
				summary.resize(l - summary.begin() + 1);
			}

			unmerged = 0;
		}
	}

	/** Вычисляет квантиль q [0, 1] на основе дайджеста
	  * Для пустого дайджеста возвращает NaN.
	  */
	Value getQuantile(const Params & params, Value q)
	{
		if (summary.empty())
			return NAN;

		compress(params);

		if (summary.size() == 1)
			return summary.front().mean;

		Value x = q * count;
		TotalCount sum = 0;
		Value prev_mean = summary.front().mean;
		Value prev_x = 0;

		for (const auto & c : summary)
		{
			Value current_x = sum + c.count * 0.5;

			if (current_x >= x)
				return interpolate(x, prev_x, prev_mean, current_x, c.mean);

			sum += c.count;
			prev_mean = c.mean;
			prev_x = current_x;
		}

		return summary.back().mean;
	}

	/** Получить несколько квантилей (size штук).
	  * levels - массив уровней нужных квантилей. Они идут в произвольном порядке.
	  * levels_permutation - массив-перестановка уровней. На i-ой позиции будет лежать индекс i-го по возрастанию уровня в массиве levels.
	  * result - массив, куда сложить результаты, в порядке levels,
	  */
	template <typename ResultType>
	void getManyQuantiles(const Params & params, const Value * levels, const size_t * levels_permutation, size_t size, ResultType * result)
	{
		if (summary.empty())
		{
			for (size_t result_num = 0; result_num < size; ++result_num)
				result[result_num] = std::is_floating_point<ResultType>::value ? NAN : 0;
			return;
		}

		compress(params);

		if (summary.size() == 1)
		{
			for (size_t result_num = 0; result_num < size; ++result_num)
				result[result_num] = summary.front().mean;
			return;
		}

		Value x = levels[levels_permutation[0]] * count;
		TotalCount sum = 0;
		Value prev_mean = summary.front().mean;
		Value prev_x = 0;

		size_t result_num = 0;
		for (const auto & c : summary)
		{
			Value current_x = sum + c.count * 0.5;

			while (current_x >= x)
			{
				result[levels_permutation[result_num]] = interpolate(x, prev_x, prev_mean, current_x, c.mean);

				++result_num;
				if (result_num >= size)
					return;

				x = levels[levels_permutation[result_num]] * count;
			}

			sum += c.count;
			prev_mean = c.mean;
			prev_x = current_x;
		}

		auto rest_of_results = summary.back().mean;
		for (; result_num < size; ++result_num)
			result[levels_permutation[result_num]] = rest_of_results;
	}

	/** Объединить с другим состоянием.
	  */
	void merge(const Params & params, const MergingDigest & other)
	{
		for (const auto & c : other.summary)
			add(params, c);
	}

	/** Записать в поток.
	  */
	void write(const Params & params, DB::WriteBuffer & buf)
	{
		compress(params);
		DB::writeVarUInt(summary.size(), buf);
		buf.write(reinterpret_cast<const char *>(&summary[0]), summary.size() * sizeof(summary[0]));
	}

	/** Прочитать из потока.
	  */
	void read(const Params & params, DB::ReadBuffer & buf)
	{
		size_t size = 0;
		DB::readVarUInt(size, buf);

		if (size > params.max_unmerged)
			throw DB::Exception("Too large t-digest summary size", DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		summary.resize(size);
		buf.read(reinterpret_cast<char *>(&summary[0]), size * sizeof(summary[0]));
	}
};

}


namespace DB
{

struct AggregateFunctionQuantileTDigestData
{
	tdigest::MergingDigest<Float32, Float32, Float32> digest;
};


template <typename T, bool returns_float = true>
class AggregateFunctionQuantileTDigest final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantileTDigest<T>>
{
private:
	Float32 level;
	tdigest::Params<Float32> params;
	DataTypePtr type;

public:
	AggregateFunctionQuantileTDigest(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileTDigest"; }

	DataTypePtr getReturnType() const override
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (returns_float)
			type = new DataTypeFloat32;
		else
			type = argument;
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float32>(), params[0]);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).digest.add(params, static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).digest.merge(params, this->data(rhs).digest);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.read(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.getQuantile(params, level);

		if (returns_float)
			static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
		else
			static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
	}
};


template <typename T, typename Weight, bool returns_float = true>
class AggregateFunctionQuantileTDigestWeighted final
	: public IBinaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantileTDigestWeighted<T, Weight, returns_float>>
{
private:
	Float32 level;
	tdigest::Params<Float32> params;
	DataTypePtr type;

public:
	AggregateFunctionQuantileTDigestWeighted(double level_ = 0.5) : level(level_) {}

	String getName() const override { return "quantileTDigestWeighted"; }

	DataTypePtr getReturnType() const override
	{
		return type;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		if (returns_float)
			type = new DataTypeFloat32;
		else
			type = arguments.at(0);
	}

	void setParameters(const Array & params) override
	{
		if (params.size() != 1)
			throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		level = apply_visitor(FieldVisitorConvertToNumber<Float32>(), params[0]);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place).digest.add(params,
			static_cast<const ColumnVector<T> &>(column_value).getData()[row_num],
			static_cast<const ColumnVector<Weight> &>(column_weight).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).digest.merge(params, this->data(rhs).digest);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.read(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.getQuantile(params, level);

		if (returns_float)
			static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
		else
			static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
	}
};


template <typename T, bool returns_float = true>
class AggregateFunctionQuantilesTDigest final
	: public IUnaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantilesTDigest<T>>
{
private:
	QuantileLevels<Float32> levels;
	tdigest::Params<Float32> params;
	DataTypePtr type;

public:
	String getName() const override { return "quantilesTDigest"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(type);
	}

	void setArgument(const DataTypePtr & argument)
	{
		if (returns_float)
			type = new DataTypeFloat32;
		else
			type = argument;
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).digest.add(params, static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).digest.merge(params, this->data(rhs).digest);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.read(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		if (returns_float)
		{
			typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
			size_t old_size = data_to.size();
			data_to.resize(data_to.size() + size);

			this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
				params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
		}
		else
		{
			typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
			size_t old_size = data_to.size();
			data_to.resize(data_to.size() + size);

			this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
				params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
		}
	}
};


template <typename T, typename Weight, bool returns_float = true>
class AggregateFunctionQuantilesTDigestWeighted final
	: public IBinaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantilesTDigestWeighted<T, Weight, returns_float>>
{
private:
	QuantileLevels<Float32> levels;
	tdigest::Params<Float32> params;
	DataTypePtr type;

public:
	String getName() const override { return "quantilesTDigest"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeArray(type);
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		if (returns_float)
			type = new DataTypeFloat32;
		else
			type = arguments.at(0);
	}

	void setParameters(const Array & params) override
	{
		levels.set(params);
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_weight, size_t row_num) const
	{
		this->data(place).digest.add(params,
			static_cast<const ColumnVector<T> &>(column_value).getData()[row_num],
			static_cast<const ColumnVector<Weight> &>(column_weight).getData()[row_num]);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
		this->data(place).digest.merge(params, this->data(rhs).digest);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(const_cast<AggregateDataPtr>(place)).digest.write(params, buf);
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.read(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		ColumnArray & arr_to = static_cast<ColumnArray &>(to);
		ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

		size_t size = levels.size();
		offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

		if (returns_float)
		{
			typename ColumnFloat32::Container_t & data_to = static_cast<ColumnFloat32 &>(arr_to.getData()).getData();
			size_t old_size = data_to.size();
			data_to.resize(data_to.size() + size);

			this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
				params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
		}
		else
		{
			typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
			size_t old_size = data_to.size();
			data_to.resize(data_to.size() + size);

			this->data(const_cast<AggregateDataPtr>(place)).digest.getManyQuantiles(
				params, &levels.levels[0], &levels.permutation[0], size, &data_to[old_size]);
		}
	}
};

}
