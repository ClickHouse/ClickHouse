#pragma once

#include <common/Common.h>
#include <DB/Common/HyperLogLogBiasEstimator.h>
#include <DB/Common/CompactArray.h>
#include <DB/Common/HashTable/Hash.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/Defines.h>

#include <cmath>
#include <cstring>


namespace DB
{
namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}
}


namespace details
{

/// Look-up table логарифмов от целых чисел для использования в HyperLogLogCounter.
template<UInt8 K>
struct LogLUT
{
	LogLUT()
	{
		log_table[0] = 0.0;
		for (size_t i = 1; i <= M; ++i)
			log_table[i] = log(static_cast<double>(i));
	}

	double getLog(size_t x) const
	{
		if (x <= M)
			return log_table[x];
		else
			return log(static_cast<double>(x));
	}

private:
	static constexpr size_t M = 1 << ((static_cast<unsigned int>(K) <= 12) ? K : 12);

	double log_table[M + 1];
};

template<UInt8 K> struct MinCounterTypeHelper;
template<> struct MinCounterTypeHelper<0>	{ using Type = UInt8; };
template<> struct MinCounterTypeHelper<1>	{ using Type = UInt16; };
template<> struct MinCounterTypeHelper<2>	{ using Type = UInt32; };
template<> struct MinCounterTypeHelper<3>	{ using Type = UInt64; };

/// Вспомогательная структура для автоматического определения
/// минимального размера типа счетчика в зависимости от максимального значения.
/// Используется там, где нужна максимальная экономия памяти,
/// например, в HyperLogLogCounter
template<UInt64 MaxValue> struct MinCounterType
{
	typedef typename MinCounterTypeHelper<
		(MaxValue >= 1 << 8) +
		(MaxValue >= 1 << 16) +
		(MaxValue >= 1ULL << 32)
		>::Type Type;
};

/** Знаменатель формулы алгоритма HyperLogLog
  */
template<UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
	bool stable_denominator_if_big, typename Enable = void>
class __attribute__ ((packed)) Denominator;

namespace
{

/// Возвращает true, если хранилище для рангов большое.
constexpr bool isBigRankStore(UInt8 precision)
{
	return precision >= 12;
}

}

/** Тип употребляемый для вычисления знаменателя.
  */
template <typename HashValueType>
struct IntermediateDenominator;

template <>
struct IntermediateDenominator<UInt32>
{
	using Type = double;
};

template <>
struct IntermediateDenominator<UInt64>
{
	using Type = long double;
};

/** "Лёгкая" реализация знаменателя формулы HyperLogLog.
  * Занимает минимальный объём памяти, зато вычисления могут быть неустойчивы.
  * Подходит, когда хранилище для рангов небольшое.
  */
template<UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
	bool stable_denominator_if_big>
class __attribute__ ((packed)) Denominator<precision, max_rank, HashValueType, DenominatorType,
	stable_denominator_if_big,
	typename std::enable_if<!details::isBigRankStore(precision) || !stable_denominator_if_big>::type>
{
private:
	using T = typename IntermediateDenominator<HashValueType>::Type;

public:
	Denominator(DenominatorType initial_value)
		: denominator(initial_value)
	{
	}

public:
	inline void update(UInt8 cur_rank, UInt8 new_rank)
	{
		denominator -= static_cast<T>(1.0) / (1ULL << cur_rank);
		denominator += static_cast<T>(1.0) / (1ULL << new_rank);
	}

	inline void update(UInt8 rank)
	{
		denominator += static_cast<T>(1.0) / (1ULL << rank);
	}

	void clear()
	{
		denominator = 0;
	}

	DenominatorType get() const
	{
		return denominator;
	}

private:
	T denominator;
};

/** "Тяжёлая" версия знаменателя формулы HyperLogLog.
  * Занимает больший объём памяти, чем лёгкая версия, зато вычисления всегда устойчивы.
  * Подходит, когда хранилище для рангов довольно большое.
  */
template<UInt8 precision, int max_rank, typename HashValueType, typename DenominatorType,
	bool stable_denominator_if_big>
class __attribute__ ((packed)) Denominator<precision, max_rank, HashValueType, DenominatorType,
	stable_denominator_if_big,
	typename std::enable_if<details::isBigRankStore(precision) && stable_denominator_if_big>::type>
{
public:
	Denominator(DenominatorType initial_value)
	{
		rank_count[0] = initial_value;
	}

	inline void update(UInt8 cur_rank, UInt8 new_rank)
	{
		--rank_count[cur_rank];
		++rank_count[new_rank];
	}

	inline void update(UInt8 rank)
	{
		++rank_count[rank];
	}

	void clear()
	{
		memset(rank_count, 0, size * sizeof(UInt32));
	}

	DenominatorType get() const
	{
		long double val = rank_count[size - 1];
		for (int i = size - 2; i >= 0; --i)
		{
			val /= 2.0;
			val += rank_count[i];
		}
		return val;
	}

private:
	static constexpr size_t size = max_rank + 1;
	UInt32 rank_count[size] = { 0 };
};

/** Число хвостовых (младших) нулей.
  */
template <typename T>
struct TrailingZerosCounter;

template <>
struct TrailingZerosCounter<UInt32>
{
	static int apply(UInt32 val)
	{
		return __builtin_ctz(val);
	}
};

template <>
struct TrailingZerosCounter<UInt64>
{
	static int apply(UInt64 val)
	{
		return __builtin_ctzll(val);
	}
};

/** Размер счётчика ранга в битах.
  */
template <typename T>
struct RankWidth;

template <>
struct RankWidth<UInt32>
{
	static constexpr UInt8 get()
	{
		return 5;
	}
};

template <>
struct RankWidth<UInt64>
{
	static constexpr UInt8 get()
	{
		return 6;
	}
};

}

/** Поведение класса HyperLogLogCounter.
  */
enum class HyperLogLogMode
{
	Raw,            /// Применить алгоритм HyperLogLog без исправления погрешности
	LinearCounting, /// Исправить погрешность по алгоритму LinearCounting
	BiasCorrected,  /// Исправить погрешность по алгоритму HyperLogLog++
	FullFeatured    /// Исправить погрешность по алгоритму LinearCounting или HyperLogLog++
};

/** Подсчёт уникальных значений алгоритмом HyperLogLog.
  *
  * Теоретическая относительная погрешность ~1.04 / sqrt(2^precision)
  * precision - длина префикса хэш-функции для индекса (число ячеек M = 2^precision)
  * Рекомендуемые значения precision: 3..20
  *
  * Источник: "HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm"
  * (P. Flajolet et al., AOFA '07: Proceedings of the 2007 International Conference on Analysis
  * of Algorithms)
  */
template <
	UInt8 precision,
	typename Hash = IntHash32<UInt64>,
	typename HashValueType = UInt32,
	typename DenominatorType = double,
	typename BiasEstimator = TrivialBiasEstimator,
	HyperLogLogMode mode = HyperLogLogMode::FullFeatured,
	bool stable_denominator_if_big = true>
class __attribute__ ((packed)) HyperLogLogCounter : private Hash
{
private:
	/// Число ячеек.
	static constexpr size_t bucket_count = 1ULL << precision;
	/// Размер счётчика ранга в битах.
	static constexpr UInt8 rank_width = details::RankWidth<HashValueType>::get();

private:
	using Value_t = UInt64;
	using RankStore = DB::CompactArray<HashValueType, rank_width, bucket_count>;

public:
	void insert(Value_t value)
	{
		HashValueType hash = getHash(value);

		/// Разбиваем хэш-значение на два подзначения. Первое из них является номером ячейки
		/// в хранилище для рангов (rank_storage), а со второго вычисляем ранг.
		HashValueType bucket = extractBitSequence(hash, 0, precision);
		HashValueType tail = extractBitSequence(hash, precision, sizeof(HashValueType) * 8);
		UInt8 rank = calculateRank(tail);

		/// Обновляем максимальный ранг для текущей ячейки.
		update(bucket, rank);
	}

	UInt32 size() const
	{
		/// Нормализующий коэффициент, входящий в среднее гармоническое.
		static constexpr double alpha_m =
			bucket_count == 2 	? 0.351 :
			bucket_count == 4  ? 0.532 :
			bucket_count == 8  ? 0.626 :
			bucket_count == 16 ? 0.673 :
			bucket_count == 32 ? 0.697 :
			bucket_count == 64 ? 0.709 : 0.7213 / (1 + 1.079 / bucket_count);

		/** Среднее гармоническое по всем корзинам из величин 2^rank равно:
		  *  bucket_count / ∑ 2^-rank_i.
		  * Величина ∑ 2^-rank_i - это denominator.
		  */

		double raw_estimate = alpha_m * bucket_count * bucket_count / denominator.get();

		double final_estimate = fixRawEstimate(raw_estimate);

		return static_cast<UInt32>(final_estimate + 0.5);
	}

	void merge(const HyperLogLogCounter & rhs)
	{
		const auto & rhs_rank_store = rhs.rank_store;
		for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
			update(bucket, rhs_rank_store[bucket]);
	}

	void read(DB::ReadBuffer & in)
	{
		in.readStrict(reinterpret_cast<char *>(this), sizeof(*this));
	}

	void readAndMerge(DB::ReadBuffer & in)
	{
		typename RankStore::Reader reader(in);
		while (reader.next())
		{
			const auto & data = reader.get();
			update(data.first, data.second);
		}

		in.ignore(sizeof(DenominatorCalculatorType) + sizeof(ZerosCounterType));
	}

	static void skip(DB::ReadBuffer & in)
	{
		in.ignore(sizeof(RankStore) + sizeof(DenominatorCalculatorType) + sizeof(ZerosCounterType));
	}

	void write(DB::WriteBuffer & out) const
	{
		out.write(reinterpret_cast<const char *>(this), sizeof(*this));
	}

	/// Запись и чтение в текстовом виде неэффективно (зато совместимо с OLAPServer-ом и Metrage).
	void readText(DB::ReadBuffer & in)
	{
		rank_store.readText(in);

		zeros = 0;
		denominator.clear();
		for (HashValueType bucket = 0; bucket < bucket_count; ++bucket)
		{
			UInt8 rank = rank_store[bucket];
			if (rank == 0)
				++zeros;
			denominator.update(rank);
		}
	}

	static void skipText(DB::ReadBuffer & in)
	{
		UInt8 dummy;
		for (size_t i = 0; i < RankStore::size(); ++i)
		{
			if (i != 0)
				DB::assertChar(',', in);
			DB::readIntText(dummy, in);
		}
	}

	void writeText(DB::WriteBuffer & out) const
	{
		rank_store.writeText(out);
	}

private:
	/// Извлечь подмножество битов [begin, end[.
	inline HashValueType extractBitSequence(HashValueType val, UInt8 begin, UInt8 end) const
	{
		return (val >> begin) & ((1ULL << (end - begin)) - 1);
	}

	/// Ранг = число хвостовых (младших) нулей + 1
	inline UInt8 calculateRank(HashValueType val) const
	{
		if (unlikely(val == 0))
			return max_rank;

		auto zeros_plus_one = details::TrailingZerosCounter<HashValueType>::apply(val) + 1;

		if (unlikely(zeros_plus_one) > max_rank)
			return max_rank;

		return zeros_plus_one;
	}

	inline HashValueType getHash(Value_t key) const
	{
		return Hash::operator()(key);
	}

	/// Обновить максимальный ранг для заданной ячейки.
	void update(HashValueType bucket, UInt8 rank)
	{
		typename RankStore::Locus content = rank_store[bucket];
		UInt8 cur_rank = static_cast<UInt8>(content);

		if (rank > cur_rank)
		{
			if (cur_rank == 0)
				--zeros;
			denominator.update(cur_rank, rank);
			content = rank;
		}
	}

	double fixRawEstimate(double raw_estimate) const
	{
		if ((mode == HyperLogLogMode::Raw) || ((mode == HyperLogLogMode::BiasCorrected) && BiasEstimator::isTrivial()))
			return raw_estimate;
		else if (mode == HyperLogLogMode::LinearCounting)
			return applyLinearCorrection(raw_estimate);
		else if ((mode == HyperLogLogMode::BiasCorrected) && !BiasEstimator::isTrivial())
			return applyBiasCorrection(raw_estimate);
		else if (mode == HyperLogLogMode::FullFeatured)
		{
			static constexpr bool fix_big_cardinalities = std::is_same<HashValueType, UInt32>::value;
			static constexpr double pow2_32 = 4294967296.0;

			double fixed_estimate;

			if (fix_big_cardinalities && (raw_estimate > (pow2_32 / 30.0)))
				fixed_estimate = -pow2_32 * log(1.0 - raw_estimate / pow2_32);
			else
				fixed_estimate = applyCorrection(raw_estimate);

			return fixed_estimate;
		}
		else
			throw Poco::Exception("Internal error", DB::ErrorCodes::LOGICAL_ERROR);
	}

	inline double applyCorrection(double raw_estimate) const
	{
		double fixed_estimate;

		if (BiasEstimator::isTrivial())
		{
			if (raw_estimate <= (2.5 * bucket_count))
			{
				/// Поправка в случае маленкой оценки.
				fixed_estimate = applyLinearCorrection(raw_estimate);
			}
			else
				fixed_estimate = raw_estimate;
		}
		else
		{
			fixed_estimate = applyBiasCorrection(raw_estimate);
			double linear_estimate = applyLinearCorrection(fixed_estimate);

			if (linear_estimate < BiasEstimator::getThreshold())
				fixed_estimate = linear_estimate;
		}

		return fixed_estimate;
	}

	/// Поправка из алгоритма HyperLogLog++.
	/// Источник: "HyperLogLog in Practice: Algorithmic Engineering of a State of The Art
	/// Cardinality Estimation Algorithm".
	/// (S. Heule et al., Proceedings of the EDBT 2013 Conference).
	inline double applyBiasCorrection(double raw_estimate) const
	{
		double fixed_estimate;

		if (raw_estimate <= (5 * bucket_count))
			fixed_estimate = raw_estimate - BiasEstimator::getBias(raw_estimate);
		else
			fixed_estimate = raw_estimate;

		return fixed_estimate;
	}

	/// Подсчет уникальных значений по алгоритму LinearCounting.
	/// Источник: "A Linear-time Probabilistic Counting Algorithm for Database Applications"
	/// (Whang et al., ACM Trans. Database Syst., pp. 208-229, 1990)
	inline double applyLinearCorrection(double raw_estimate) const
	{
		double fixed_estimate;

		if (zeros != 0)
			fixed_estimate = bucket_count * (log_lut.getLog(bucket_count) - log_lut.getLog(zeros));
		else
			fixed_estimate = raw_estimate;

		return fixed_estimate;
	}

private:
	/// Максимальный ранг.
	static constexpr int max_rank = sizeof(HashValueType) * 8 - precision + 1;

	/// Хранилище для рангов.
	RankStore rank_store;

	/// Знаменатель формулы алгоритма HyperLogLog.
	using DenominatorCalculatorType = details::Denominator<precision, max_rank, HashValueType, DenominatorType, stable_denominator_if_big>;
	DenominatorCalculatorType denominator{bucket_count};

	/// Число нулей в хранилище для рангов.
	using ZerosCounterType = typename details::MinCounterType<bucket_count>::Type;
	ZerosCounterType zeros = bucket_count;

	static details::LogLUT<precision> log_lut;

	/// Проверки.
	static_assert(precision < (sizeof(HashValueType) * 8), "Invalid parameter value");
};


/// Определения статических переменных, нужные во время линковки.
template
<
	UInt8 precision,
	typename Hash,
	typename HashValueType,
	typename DenominatorType,
	typename BiasEstimator,
	HyperLogLogMode mode,
	bool stable_denominator_if_big
>
details::LogLUT<precision> HyperLogLogCounter
<
	precision,
	Hash,
	HashValueType,
	DenominatorType,
	BiasEstimator,
	mode,
	stable_denominator_if_big
>::log_lut;


/// Для Metrage, используется лёгкая реализация знаменателя формулы HyperLogLog,
/// чтобы формат сериализации не изменился.
typedef HyperLogLogCounter<
	12,
	IntHash32<UInt64>,
	UInt32,
	double,
	TrivialBiasEstimator,
	HyperLogLogMode::FullFeatured,
	false
> HLL12;
