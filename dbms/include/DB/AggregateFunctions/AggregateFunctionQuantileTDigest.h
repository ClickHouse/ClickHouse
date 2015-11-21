#pragma once

#include <cmath>
#include <cstdint>
#include <cassert>

#include <vector>
#include <algorithm>

#include <DB/Core/FieldVisitors.h>
#include <DB/Common/RadixSort.h>
#include <DB/Common/PODArray.h>
#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>
#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


/** Алгоритм реализовал Алексей Борзенков https://███████████.yandex-team.ru/snaury
  * Ему принадлежит авторство кода и комментариев в данном namespace,
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
	static constexpr size_t bytes_in_arena = 64 - sizeof(DB::PODArray<Centroid>) - sizeof(TotalCount) * 2;

	using Summary = DB::PODArray<Centroid, bytes_in_arena / sizeof(Centroid), AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;

	Summary summary;
	TotalCount count = 0;
	uint32_t unmerged = 0;

	/** Линейная интерполяция в точке x на прямой (x1, y1)..(x2, y2)
	  */
	static Value interpolate(Value x, Value x1, Value y1, Value x2, Value y2)
	{
		Value delta = x2 - x1;
		Value w1 = (x2 - x) / delta;
		Value w2 = (x - x1) / delta;
		return w1 * y1 + w2 * y2;
	}

	struct RadixSortTraits
	{
		using Element = Centroid;
		using Key = Value;
		using CountType = uint32_t;
		using KeyBits = uint32_t;

		static constexpr size_t PART_SIZE_BITS = 11;

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
			if (summary.size() > 3)
			{
				RadixSort<RadixSortTraits>::execute(&summary[0], summary.size());

				/// Пара подряд идущих столбиков гистограммы.
				auto l = summary.begin();
				auto r = std::next(l);

				TotalCount sum = 1;
				while (r != summary.end())
				{
					// we use quantile which gives us the smallest error

					/// Отношение части гистограммы до l, включая половинку l ко всей гистограмме. То есть, какого уровня квантиль в позиции l.
					Value ql = (sum + (l->count - 1) * 0.5) / count;
					Value err = ql * (1 - ql);

					/// Отношение части гистограммы до l, включая l и половинку r ко всей гистограмме. То есть, какого уровня квантиль в позиции r.
					Value qr = (sum + l->count + (r->count - 1) * 0.5) / count;
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
	Value quantile(const Params & params, Value q)
	{
		if (summary.empty())
			return NAN;

		compress(params);

		if (summary.size() == 1)
			return summary[0].mean;

		Value index = q * count;
		TotalCount sum = 1;
		Value a_mean = summary[0].mean;
		Value a_index = 0.0;
		Value b_mean = summary[0].mean;
		Value b_index = sum + (summary[0].count - 1) * 0.5;

		for (size_t i = 1; i < summary.size(); ++i)
		{
			if (index <= b_index)
				break;

			sum += summary[i-1].count;
			a_mean = b_mean;
			a_index = b_index;
			b_mean = summary[i].mean;
			b_index = sum + (summary[i].count - 1) * 0.5;
		}

		return interpolate(index, a_index, a_mean, b_index, b_mean);
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

	/** Прочитать из потока и объединить с текущим состоянием.
	  */
	void readAndMerge(const Params & params, DB::ReadBuffer & buf)
	{
		size_t size = 0;
		DB::readVarUInt(size, buf);

		if (size > params.max_unmerged)
			throw DB::Exception("Too large t-digest summary size", DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE);

		for (size_t i = 0; i < size; ++i)
		{
			Centroid c;
			DB::readPODBinary(c, buf);
			add(params, c);
		}
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
	double level;
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

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
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

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.readAndMerge(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.quantile(params, level);

		if (returns_float)
			static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
		else
			static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
	}
};


template <typename T, typename Weight, bool returns_float = true>
class AggregateFunctionQuantileTDigestWeighted final
	: public IBinaryAggregateFunction<AggregateFunctionQuantileTDigestData, AggregateFunctionQuantileTDigestWeighted<T, Weight>>
{
private:
	double level;
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

		level = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
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

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		this->data(place).digest.readAndMerge(params, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		auto quantile = this->data(const_cast<AggregateDataPtr>(place)).digest.quantile(params, level);

		if (returns_float)
			static_cast<ColumnFloat32 &>(to).getData().push_back(quantile);
		else
			static_cast<ColumnVector<T> &>(to).getData().push_back(quantile);
	}
};

}
