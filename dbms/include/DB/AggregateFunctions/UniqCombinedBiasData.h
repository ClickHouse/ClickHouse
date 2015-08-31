#pragma once

#include <array>

namespace DB
{

/** Данные для HyperLogLogBiasEstimator в функции uniqCombined.
  * Схема разработки следующая:
  * 1. Собрать clickhouse.
  * 2. Запустить скрипт gen-bias-data.py, который возвращает один массив для getRawEstimates()
  *    и другой массив для getBiases().
  * 3. Обновить getRawEstimates() и getBiases(). Также обновить размер массивов в InterpolatedData.
  * 4. Собрать clickhouse.
  * 5. Запустить скрипт linear-counting-threshold.py, который возвращает данные для gnuplot.
  * 6. Сгенерить график на основе этих данных.
  * 7. Определить минимальное количество уникальных значений, при котором лучше исправить погрешность
  *    с помощью её оценки, чем применить алгоритм LinearCounting.
  * 7. Соответственно обновить константу в функции getThreshold()
  * 8. Собрать clickhouse.
  */
struct UniqCombinedBiasData
{
	using InterpolatedData = std::array<double, 178>;

	static double getThreshold();
	/// Оценки количества уникальных значений по алгоритму HyperLogLog без применения каких-либо поправок.
	static const InterpolatedData & getRawEstimates();
	/// Соответствующие оценки погрешности.
	static const InterpolatedData & getBiases();
};

}
