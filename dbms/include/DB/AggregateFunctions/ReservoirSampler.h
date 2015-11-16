#pragma once

#include <limits>
#include <algorithm>
#include <climits>
#include <sstream>
#include <common/Common.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Common/PODArray.h>
#include <Poco/Exception.h>
#include <boost/random.hpp>


/// Реализация алгоритма Reservoir Sampling. Инкрементально выбирает из добавленных объектов случайное подмножество размера sample_count.
/// Умеет приближенно получать квантили.
/// Вызов quantile занимает O(sample_count log sample_count), если после предыдущего вызова quantile был хотя бы один вызов insert. Иначе, O(1).
/// То есть, имеет смысл сначала добавлять, потом получать квантили, не добавляя.

const size_t DEFAULT_SAMPLE_COUNT = 8192;

/// Что делать, если нет ни одного значения - кинуть исключение, или вернуть 0 или NaN в случае double?
namespace ReservoirSamplerOnEmpty
{
	enum Enum
	{
		THROW,
		RETURN_NAN_OR_ZERO,
	};
}

template<typename ResultType, bool IsFloatingPoint>
struct NanLikeValueConstructor
{
	static ResultType getValue()
	{
		return std::numeric_limits<ResultType>::quiet_NaN();
	}
};
template<typename ResultType>
struct NanLikeValueConstructor<ResultType, false>
{
	static ResultType getValue()
	{
		return ResultType();
	}
};

template<typename T, ReservoirSamplerOnEmpty::Enum OnEmpty = ReservoirSamplerOnEmpty::THROW, typename Comparer = std::less<T> >
class ReservoirSampler
{
public:
	ReservoirSampler(size_t sample_count_ = DEFAULT_SAMPLE_COUNT)
		: sample_count(sample_count_)
	{
		rng.seed(123456);
	}

	void clear()
	{
		samples.clear();
		sorted = false;
		total_values = 0;
		rng.seed(123456);
	}

	void insert(const T & v)
	{
		sorted = false;
		++total_values;
		if (samples.size() < sample_count)
		{
			samples.push_back(v);
		}
		else
		{
			UInt64 rnd = genRandom(total_values);
			if (rnd < sample_count)
				samples[rnd] = v;
		}
	}

	size_t size() const
	{
		return total_values;
	}

	T quantileNearest(double level)
	{
		if (samples.empty())
			return onEmpty<T>();

		sortIfNeeded();

		double index = level * (samples.size() - 1);
		size_t int_index = static_cast<size_t>(index + 0.5);
		int_index = std::max(0LU, std::min(samples.size() - 1, int_index));
		return samples[int_index];
	}

	/** Если T не числовой тип, использование этого метода вызывает ошибку компиляции,
	  *  но использование класса ошибки не вызывает. SFINAE.
	  */
	double quantileInterpolated(double level)
	{
		if (samples.empty())
			return onEmpty<double>();

		sortIfNeeded();

		double index = std::max(0., std::min(samples.size() - 1., level * (samples.size() - 1)));

		/// Чтобы получить значение по дробному индексу линейно интерполируем между соседними значениями.
		size_t left_index = static_cast<size_t>(index);
		size_t right_index = left_index + 1;
		if (right_index == samples.size())
			return samples[left_index];

		double left_coef = right_index - index;
		double right_coef = index - left_index;

		return samples[left_index] * left_coef + samples[right_index] * right_coef;
	}

	void merge(const ReservoirSampler<T, OnEmpty> & b)
	{
		if (sample_count != b.sample_count)
			throw Poco::Exception("Cannot merge ReservoirSampler's with different sample_count");
		sorted = false;

		if (b.total_values <= sample_count)
		{
			for (size_t i = 0; i < b.samples.size(); ++i)
				insert(b.samples[i]);
		}
		else if (total_values <= sample_count)
		{
			Array from = std::move(samples);
			samples.assign(b.samples.begin(), b.samples.end());
			total_values = b.total_values;
			for (size_t i = 0; i < from.size(); ++i)
				insert(from[i]);
		}
		else
		{
			randomShuffle(samples);
			total_values += b.total_values;
			for (size_t i = 0; i < sample_count; ++i)
			{
				UInt64 rnd = genRandom(total_values);
				if (rnd < b.total_values)
					samples[i] = b.samples[i];
			}
		}
	}

	void read(DB::ReadBuffer & buf)
	{
		DB::readIntBinary<size_t>(sample_count, buf);
		DB::readIntBinary<size_t>(total_values, buf);
		samples.resize(std::min(total_values, sample_count));

		std::string rng_string;
		DB::readStringBinary(rng_string, buf);
		std::istringstream rng_stream(rng_string);
		rng_stream >> rng;

		for (size_t i = 0; i < samples.size(); ++i)
			DB::readBinary(samples[i], buf);

		sorted = false;
	}

	void write(DB::WriteBuffer & buf) const
	{
		DB::writeIntBinary<size_t>(sample_count, buf);
		DB::writeIntBinary<size_t>(total_values, buf);

		std::ostringstream rng_stream;
		rng_stream << rng;
		DB::writeStringBinary(rng_stream.str(), buf);

		for (size_t i = 0; i < std::min(sample_count, total_values); ++i)
			DB::writeBinary(samples[i], buf);
	}

private:
	friend void qdigest_test(int normal_size, UInt64 value_limit, const std::vector<UInt64> & values, int queries_count, bool verbose);
	friend void rs_perf_test();

	/// Будем выделять немного памяти на стеке - чтобы избежать аллокаций, когда есть много объектов с маленьким количеством элементов.
	static constexpr size_t bytes_on_stack = 64;
	using Array = DB::PODArray<T, bytes_on_stack / sizeof(T), AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;

	size_t sample_count;
	size_t total_values = 0;
	Array samples;
	boost::taus88 rng;
	bool sorted = false;


	UInt64 genRandom(size_t lim)
	{
		/// При большом количестве значений будем генерировать случайные числа в несколько раз медленнее.
		if (lim <= static_cast<UInt64>(rng.max()))
			return static_cast<UInt32>(rng()) % static_cast<UInt32>(lim);
		else
			return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(rng.max()) + 1ULL) + static_cast<UInt64>(rng())) % lim;
	}

	void randomShuffle(Array & v)
	{
		for (size_t i = 1; i < v.size(); ++i)
		{
			size_t j = genRandom(i + 1);
			std::swap(v[i], v[j]);
		}
	}

	void sortIfNeeded()
	{
		if (sorted)
			return;
		sorted = true;
		std::sort(samples.begin(), samples.end(), Comparer());
	}

	template <typename ResultType>
	ResultType onEmpty() const
	{
		if (OnEmpty == ReservoirSamplerOnEmpty::THROW)
			throw Poco::Exception("Quantile of empty ReservoirSampler");
		else
			return NanLikeValueConstructor<ResultType, std::is_floating_point<ResultType>::value>::getValue();
	}
};
