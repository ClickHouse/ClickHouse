#pragma once

#include <array>

namespace DB
{

/** Данные для HyperLogLogBiasEstimator в функции uniqCombined.
  * Схема разработки следующая:
  * 1. Собрать clickhouse.
  * 2. Запустить скрипт src/dbms/scripts/gen-bias-data.py, который возвращает один массив для getRawEstimates()
  *    и другой массив для getBiases().
  * 3. Обновить getRawEstimates() и getBiases(). Также обновить размер массивов в InterpolatedData.
  * 4. Собрать clickhouse.
  * 5. Запустить скрипт src/dbms/scripts/linear-counting-threshold.py, который создаёт 3 файла:
  *    - raw_graph.txt (1-й столбец: настоящее количество уникальных значений;
  *      2-й столбец: относительная погрешность в случае HyperLogLog без применения каких-либо поправок)
  *    - linear_counting_graph.txt (1-й столбец: настоящее количество уникальных значений;
  *      2-й столбец: относительная погрешность в случае HyperLogLog с применением LinearCounting)
  *    - bias_corrected_graph.txt (1-й столбец: настоящее количество уникальных значений;
  *      2-й столбец: относительная погрешность в случае HyperLogLog с применением поправок из алгортима HyperLogLog++)
  * 6. Сгенерить график с gnuplot на основе этих данных.
  * 7. Определить минимальное количество уникальных значений, при котором лучше исправить погрешность
  *    с помощью её оценки (т.е. по алгоритму HyperLogLog++), чем применить алгоритм LinearCounting.
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
