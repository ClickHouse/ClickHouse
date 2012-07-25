#pragma once

#include <vector>

#include <DB/Core/Types.h>
#include <DB/Columns/IColumn.h>


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


/** Курсор, позволяющий сравнивать соответствующие строки в разных блоках.
  * Для использования в priority queue.
  */
struct SortCursor
{
	ConstColumnPlainPtrs * all_columns;
	ConstColumnPlainPtrs * sort_columns;
	size_t sort_columns_size;
	size_t pos;
	size_t rows;
	const SortDescription * desc;

	SortCursor(ConstColumnPlainPtrs * all_columns_, ConstColumnPlainPtrs * sort_columns_, const SortDescription * desc_, size_t pos_ = 0)
		: all_columns(all_columns_), sort_columns(sort_columns_), sort_columns_size(sort_columns->size()),
		pos(pos_), rows((*all_columns)[0]->size()), desc(desc_)
	{
	}

	/** Инвертировано, чтобы из priority queue элементы вынимались в нужном порядке.
	  */
	bool operator< (const SortCursor & rhs) const
	{
		for (size_t i = 0; i < sort_columns_size; ++i)
		{
			int res = (*desc)[i].direction * (*sort_columns)[i]->compareAt(pos, rhs.pos, *(*rhs.sort_columns)[i]);
			if (res > 0)
				return true;
			if (res < 0)
				return false;
		}
		return false;
	}

	bool isLast() const { return pos + 1 >= rows; }
	SortCursor next() const { return SortCursor(all_columns, sort_columns, desc, pos + 1); }
};

}

