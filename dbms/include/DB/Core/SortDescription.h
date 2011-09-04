#pragma once

#include <vector>

#include <DB/Core/Types.h>


namespace DB
{

/// Описание правила сортировки по одному столбцу.
struct SortColumnDescription
{
	String column_name;		/// Имя столбца.
	size_t column_number;	/// Номер столбца (используется, если не задано имя).
	int direction;			/// 1 - по возрастанию, -1 - по убыванию.

	SortColumnDescription(size_t column_number_, int direction_)
		: column_number(column_number_), direction(direction_) {}

	SortColumnDescription(String column_name_, int direction_)
		: column_name(column_name_), column_number(0), direction(direction_) {}
};

/// Описание правила сортировки по нескольким столбцам.
typedef std::vector<SortColumnDescription> SortDescription;

}

