#pragma once

#include <vector>


namespace DB
{

/// Описание правила сортировки по одному столбцу.
struct SortColumnDescription
{
	size_t column_number;	/// Номер столбца
	int direction;			/// 1 - по возрастанию, -1 - по убыванию.

	SortColumnDescription(size_t column_number_, int direction_)
		: column_number(column_number_), direction(direction_) {}
};

/// Описание правила сортировки по нескольким столбцам.
typedef std::vector<SortColumnDescription> SortDescription;

}

