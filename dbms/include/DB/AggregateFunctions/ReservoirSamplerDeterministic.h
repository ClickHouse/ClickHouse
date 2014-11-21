#pragma once

#include <limits>
#include <vector>
#include <algorithm>
#include <climits>
#include <sstream>
#include <stats/ReservoirSampler.h>
#include <Yandex/Common.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <Poco/Exception.h>
#include <boost/random.hpp>


/// Реализация алгоритма Reservoir Sampling. Инкрементально выбирает из добавленных объектов случайное подмножество размера sample_count.
/// Умеет приближенно получать квантили.
/// Вызов quantile занимает O(sample_count log sample_count), если после предыдущего вызова quantile был хотя бы один вызов insert. Иначе, O(1).
/// То есть, имеет смысл сначала добавлять, потом получать квантили, не добавляя.

namespace detail
{
const size_t DEFAULT_SAMPLE_COUNT = 8192;
const auto MAX_SKIP_DEGREE = sizeof(UInt32) * 8;
}

/// Что делать, если нет ни одного значения - кинуть исключение, или вернуть 0 или NaN в случае double?
enum class ReservoirSamplerDeterministicOnEmpty
{
	THROW,
	RETURN_NAN_OR_ZERO,
};

template<typename T,
		 ReservoirSamplerDeterministicOnEmpty OnEmpty = ReservoirSamplerDeterministicOnEmpty::THROW>
class ReservoirSamplerDeterministic
{
	bool good(const UInt32 hash)
	{
		return hash == ((hash >> skip_degree) << skip_degree);
	}

public:
	ReservoirSamplerDeterministic(const size_t sample_count = DEFAULT_SAMPLE_COUNT)
		: sample_count{sample_count}
	{
	}

	void clear()
	{
		samples.clear();
		sorted = false;
		total_values = 0;
	}

	void insert(const T & v, const UInt64 determinator)
	{
		const UInt32 hash = intHash64(determinator);
		if (!good(hash))
			return;

		insertImpl(v, hash);
		sorted = false;
		++total_values;
	}

	void insertImpl(const T & v, const UInt32 hash)
	{
 		while (samples.size() + 1 >= sample_count)
		{
			if (++skip_degree > detail::MAX_SKIP_DEGREE)
				throw DB::Exception{"skip_degree exceeds maximum value", DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED};
			thinOut();
		}

		samples.emplace_back(v, hash);
	}

	void thinOut()
	{
		auto size = samples.size();
		for (size_t i = 0; i < size;)
		{
			if (!good(samples[i].second))
			{
				/// swap current element with the last one
				std::swap(samples[size - 1], samples[i]);
				--size;
			}
			else
				++i;
		}

		if (size != samples.size())
		{
			samples.resize(size);
			sorted = false;
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
		return samples[int_index].first;
	}

	/** Если T не числовой тип, использование этого метода вызывает ошибку компиляции,
	  *  но использование класса ошибки не вызывает. SFINAE.
	  */
	double quantileInterpolated(double level)
	{
		if (samples.empty())
			return onEmpty<double>();

		sortIfNeeded();

		const double index = std::max(0., std::min(samples.size() - 1., level * (samples.size() - 1)));

		/// Чтобы получить значение по дробному индексу линейно интерполируем между соседними значениями.
		size_t left_index = static_cast<size_t>(index);
		size_t right_index = left_index + 1;
		if (right_index == samples.size())
			return samples[left_index].first;

		const double left_coef = right_index - index;
		const double right_coef = index - left_index;

		return samples[left_index].first * left_coef + samples[right_index].first * right_coef;
	}

	void merge(const ReservoirSamplerDeterministic & b)
	{
		if (sample_count != b.sample_count)
			throw Poco::Exception("Cannot merge ReservoirSamplerDeterministic's with different sample_count");
		sorted = false;

		if (b.skip_degree > skip_degree)
		{
			skip_degree = b.skip_degree;
			thinOut();
		}

		for (size_t i = 0; i < b.samples.size(); ++i)
			if (good(b.samples[i].second))
				insertImpl(b.samples[i].first, b.samples[i].second);

		total_values += b.total_values;
	}

	void read(DB::ReadBuffer & buf)
	{
		DB::readIntBinary<size_t>(sample_count, buf);
		DB::readIntBinary<size_t>(total_values, buf);
		samples.resize(std::min(total_values, sample_count));

		for (size_t i = 0; i < samples.size(); ++i)
			DB::readBinary(samples[i].first, buf);

		sorted = false;
	}

	void write(DB::WriteBuffer & buf) const
	{
		DB::writeIntBinary<size_t>(sample_count, buf);
		DB::writeIntBinary<size_t>(total_values, buf);

		for (size_t i = 0; i < std::min(sample_count, total_values); ++i)
			DB::writeBinary(samples[i].first, buf);
	}

private:
	friend void rs_perf_test();
	friend void qdigest_test(int, UInt64, const std::vector<UInt64> &, int, bool);

	size_t sample_count;
	size_t total_values{};
	bool sorted{};
	std::vector<std::pair<T, UInt32>> samples;
	UInt8 skip_degree{};

	void sortIfNeeded()
	{
		if (sorted)
			return;
		sorted = true;
		std::sort(samples.begin(), samples.end(), [] (const std::pair<T, UInt32> & lhs, const std::pair<T, UInt32> & rhs) {
			return lhs.first < rhs.first;
		});
	}

	template <typename ResultType>
	ResultType onEmpty() const
	{
		if (OnEmpty == ReservoirSamplerDeterministicOnEmpty::THROW)
			throw Poco::Exception("Quantile of empty ReservoirSamplerDeterministic");
		else
			return NanLikeValueConstructor<ResultType, std::is_floating_point<ResultType>::value>::getValue();
	}
};
