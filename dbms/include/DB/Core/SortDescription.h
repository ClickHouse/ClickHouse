#pragma once

#include <vector>

#include <DB/Core/Types.h>
#include <DB/Core/Block.h>
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
  * Курсор двигается по одному блоку.
  * Для использования в priority queue.
  */
struct SortCursorImpl
{
	ConstColumnPlainPtrs all_columns;
	ConstColumnPlainPtrs sort_columns;
	SortDescription desc;
	size_t sort_columns_size;
	size_t pos;
	size_t rows;
	
	SortCursorImpl() {}

	SortCursorImpl(const Block & block, const SortDescription & desc_)
		: desc(desc_), sort_columns_size(desc.size())
	{
		reset(block);
	}

	/// Установить курсор в начало нового блока.
	void reset(const Block & block)
	{
		all_columns.clear();
		sort_columns.clear();
		
		size_t num_columns = block.columns();

		for (size_t j = 0; j < num_columns; ++j)
			all_columns.push_back(&*block.getByPosition(j).column);

		for (size_t j = 0, size = desc.size(); j < size; ++j)
		{
			size_t column_number = !desc[j].column_name.empty()
				? block.getPositionByName(desc[j].column_name)
				: desc[j].column_number;

			sort_columns.push_back(&*block.getByPosition(column_number).column);
		}

		pos = 0;
		rows = all_columns[0]->size();
	}

	bool isLast() const { return pos + 1 >= rows; }
	void next() { ++pos; }
};


/// Для лёгкости копирования.
struct SortCursor
{
	SortCursorImpl * impl;

	SortCursor(SortCursorImpl * impl_) : impl(impl_) {}
	SortCursorImpl * operator-> () { return impl; }
	const SortCursorImpl * operator-> () const { return impl; }

	/// Инвертировано, чтобы из priority queue элементы вынимались в нужном порядке.
	bool operator< (const SortCursor & rhs) const
	{
		for (size_t i = 0; i < impl->sort_columns_size; ++i)
		{
			int res = impl->desc[i].direction * impl->sort_columns[i]->compareAt(impl->pos, rhs.impl->pos, *(rhs.impl->sort_columns[i]));
			if (res > 0)
				return true;
			if (res < 0)
				return false;
		}
		return false;
	}
};

}

