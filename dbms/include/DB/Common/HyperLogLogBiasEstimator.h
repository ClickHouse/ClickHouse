#pragma once

#include <DB/Common/Exception.h>

#include <algorithm>
#include <limits>
#include <tuple>
#include <type_traits>

/** Этот класс предоставляет способ, чтобы оценить погрешность результата применения алгоритма HyperLogLog.
  * Эмирические наблюдения показывают, что большие погрешности возникают при E < 5 * 2^precision, где
  * E - возвращаемое значение алгоритмом HyperLogLog, и precision - параметр точности HyperLogLog.
  * См. "HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm".
  * (S. Heule et al., Proceedings of the EDBT 2013 Conference).
  */
template <typename BiasData>
class HyperLogLogBiasEstimator
{
public:
	static constexpr bool isTrivial()
	{
		return false;
	}

	/// Предельное количество уникальных значений до которого должна примениться поправка
	/// из алгоритма LinearCounting.
	static double getThreshold()
	{
		return BiasData::getThreshold();
	}

	/// Вернуть оценку погрешности.
	static double getBias(double raw_estimate)
	{
		const auto & estimates = BiasData::getRawEstimates();
		const auto & biases = BiasData::getBiases();

		auto it = std::lower_bound(estimates.begin(), estimates.end(), raw_estimate);

		if (it == estimates.end())
		{
			return biases[estimates.size() - 1];
		}
		else if (*it == raw_estimate)
		{
			size_t index = std::distance(estimates.begin(), it);
			return biases[index];
		}
		else if (it == estimates.begin())
		{
			return biases[0];
		}
		else
		{
			/// Получаем оценку погрешности путём линейной интерполяции.
			size_t index = std::distance(estimates.begin(), it);

			double estimate1 = estimates[index - 1];
			double estimate2 = estimates[index];

			double bias1 = biases[index - 1];
			double bias2 = biases[index];
			/// Предполагается, что условие estimate1 < estimate2 всегда выполнено.
			double slope = (bias2 - bias1) / (estimate2 - estimate1);

			return bias1 + slope * (raw_estimate - estimate1);
		}
	}

private:
	/// Статические проверки.
	using TRawEstimatesRef = decltype(BiasData::getRawEstimates());
	using TRawEstimates = typename std::remove_reference<TRawEstimatesRef>::type;

	using TBiasDataRef = decltype(BiasData::getBiases());
	using TBiasData = typename std::remove_reference<TBiasDataRef>::type;

	static_assert(std::is_same<TRawEstimates, TBiasData>::value, "Bias estimator data have inconsistent types");
	static_assert(std::tuple_size<TRawEstimates>::value > 0, "Bias estimator has no raw estimate data");
	static_assert(std::tuple_size<TBiasData>::value > 0, "Bias estimator has no bias data");
	static_assert(std::tuple_size<TRawEstimates>::value == std::tuple_size<TBiasData>::value,
				  "Bias estimator has inconsistent data");
};

/** Тривиальный случай HyperLogLogBiasEstimator: употребляется, если не хотим исправить
  * погрешность. Это имеет смысль при маленьких значениях параметра точности, например 5 или 12.
  * Тогда применяются поправки из оригинальной версии алгоритма HyperLogLog.
  * См. "HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm"
  * (P. Flajolet et al., AOFA '07: Proceedings of the 2007 International Conference on Analysis
  * of Algorithms)
  */
struct TrivialBiasEstimator
{
	static constexpr bool isTrivial()
	{
		return true;
	}

	static double getThreshold()
	{
		return 0.0;
	}

	static double getBias(double raw_estimate)
	{
		return 0.0;
	}
};
