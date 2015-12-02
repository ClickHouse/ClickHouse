#pragma once

#include <vector>

#include <DB/Core/Field.h>
#include <DB/Core/FieldVisitors.h>


namespace DB
{


/** Параметры разных функций quantilesSomething.
  * - список уровней квантилей.
  * Также необходимо вычислить массив индексов уровней, идущих по возрастанию.
  *
  * Пример: quantiles(0.5, 0.99, 0.95)(x).
  * levels: 0.5, 0.99, 0.95
  * levels_permutation: 0, 2, 1
  */
template <typename T>	/// float или double
struct QuantileLevels
{
	using Levels = std::vector<T>;
	using Permutation = std::vector<size_t>;

	Levels levels;
	Permutation permutation;	/// Индекс i-го по величине уровня в массиве levels.

	size_t size() const { return levels.size(); }

	void set(const Array & params)
	{
		if (params.empty())
			throw Exception("Aggregate function quantiles requires at least one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		size_t size = params.size();
		levels.resize(size);
		permutation.resize(size);

		for (size_t i = 0; i < size; ++i)
		{
			levels[i] = apply_visitor(FieldVisitorConvertToNumber<Float64>(), params[i]);
			permutation[i] = i;
		}

		std::sort(permutation.begin(), permutation.end(), [this] (size_t a, size_t b) { return levels[a] < levels[b]; });
	}
};


}
