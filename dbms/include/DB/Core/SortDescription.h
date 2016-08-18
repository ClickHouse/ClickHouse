#pragma once

#include <vector>

#include <DB/Core/Types.h>
#include <DB/Core/Block.h>
#include <DB/Columns/IColumn.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Common/Collator.h>


namespace DB
{

/// Описание правила сортировки по одному столбцу.
struct SortColumnDescription
{
	String column_name;						/// Имя столбца.
	size_t column_number;					/// Номер столбца (используется, если не задано имя).
	int direction;							/// 1 - по возрастанию, -1 - по убыванию.
	std::shared_ptr<Collator> collator;	/// Collator для locale-specific сортировки строк

	SortColumnDescription(size_t column_number_, int direction_, const std::shared_ptr<Collator> & collator_ = nullptr)
		: column_number(column_number_), direction(direction_), collator(collator_) {}

	SortColumnDescription(String column_name_, int direction_, const std::shared_ptr<Collator> & collator_ = nullptr)
		: column_name(column_name_), column_number(0), direction(direction_), collator(collator_) {}

	/// Для IBlockInputStream.
	String getID() const
	{
		std::stringstream res;
		res << column_name << ", " << column_number << ", " << direction;
		if (collator)
			res << ", collation locale: " << collator->getLocale();
		return res.str();
	}
};

/// Описание правила сортировки по нескольким столбцам.
using SortDescription = std::vector<SortColumnDescription>;


/** Курсор, позволяющий сравнивать соответствующие строки в разных блоках.
  * Курсор двигается по одному блоку.
  * Для использования в priority queue.
  */
struct SortCursorImpl
{
	ConstColumnPlainPtrs all_columns;
	ConstColumnPlainPtrs sort_columns;
	SortDescription desc;
	size_t sort_columns_size = 0;
	size_t pos = 0;
	size_t rows = 0;

	/** Порядок (что сравнивается), если сравниваемые столбцы равны.
	  * Даёт возможность предпочитать строки из нужного курсора.
	  */
	size_t order;

	using NeedCollationFlags = std::vector<UInt8>;

	/** Нужно ли использовать Collator для сортировки столбца */
	NeedCollationFlags need_collation;

	/** Есть ли хотя бы один столбец с Collator. */
	bool has_collation = false;

	SortCursorImpl() {}

	SortCursorImpl(const Block & block, const SortDescription & desc_, size_t order_ = 0)
		: desc(desc_), sort_columns_size(desc.size()), order(order_), need_collation(desc.size())
	{
		reset(block);
	}

	bool empty() const { return rows == 0; }

	/// Установить курсор в начало нового блока.
	void reset(const Block & block)
	{
		all_columns.clear();
		sort_columns.clear();

		size_t num_columns = block.columns();

		for (size_t j = 0; j < num_columns; ++j)
			all_columns.push_back(block.getByPosition(j).column.get());

		for (size_t j = 0, size = desc.size(); j < size; ++j)
		{
			size_t column_number = !desc[j].column_name.empty()
				? block.getPositionByName(desc[j].column_name)
				: desc[j].column_number;

			sort_columns.push_back(block.getByPosition(column_number).column.get());

			need_collation[j] = desc[j].collator != nullptr && sort_columns.back()->getName() == "ColumnString";
			has_collation |= need_collation[j];
		}

		pos = 0;
		rows = all_columns[0]->size();
	}

	bool isFirst() const { return pos == 0; }
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

	/// Указанная строка данного курсора больше указанной строки другого курсора.
	bool greaterAt(const SortCursor & rhs, size_t lhs_pos, size_t rhs_pos) const
	{
		for (size_t i = 0; i < impl->sort_columns_size; ++i)
		{
			int direction = impl->desc[i].direction;
			int res = direction * impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), direction);
			if (res > 0)
				return true;
			if (res < 0)
				return false;
		}
		return impl->order > rhs.impl->order;
	}

	/// Проверяет, что все строки в текущем блоке данного курсора меньше или равны, чем все строки текущего блока другого курсора.
	bool totallyLessOrEquals(const SortCursor & rhs) const
	{
		if (impl->rows == 0 || rhs.impl->rows == 0)
			return false;

		/// Последняя строка данного курсора не больше первой строки другого.
		return !greaterAt(rhs, impl->rows - 1, 0);
	}

	bool greater(const SortCursor & rhs) const
	{
		return greaterAt(rhs, impl->pos, rhs.impl->pos);
	}

	/// Инвертировано, чтобы из priority queue элементы вынимались в порядке по возрастанию.
	bool operator< (const SortCursor & rhs) const
	{
		return greater(rhs);
	}
};


/// Отдельный компаратор для locale-sensitive сравнения строк
struct SortCursorWithCollation
{
	SortCursorImpl * impl;

	SortCursorWithCollation(SortCursorImpl * impl_) : impl(impl_) {}
	SortCursorImpl * operator-> () { return impl; }
	const SortCursorImpl * operator-> () const { return impl; }

	bool greaterAt(const SortCursorWithCollation & rhs, size_t lhs_pos, size_t rhs_pos) const
	{
		for (size_t i = 0; i < impl->sort_columns_size; ++i)
		{
			int direction = impl->desc[i].direction;
			int res;
			if (impl->need_collation[i])
			{
				const ColumnString & column_string = typeid_cast<const ColumnString &>(*impl->sort_columns[i]);
				res = column_string.compareAtWithCollation(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), *impl->desc[i].collator);
			}
			else
				res = impl->sort_columns[i]->compareAt(lhs_pos, rhs_pos, *(rhs.impl->sort_columns[i]), direction);

			res *= direction;
			if (res > 0)
				return true;
			if (res < 0)
				return false;
		}
		return impl->order > rhs.impl->order;
	}

	bool totallyLessOrEquals(const SortCursorWithCollation & rhs) const
	{
		if (impl->rows == 0 || rhs.impl->rows == 0)
			return false;

		/// Последняя строка данного курсора не больше первой строки другого.
		return !greaterAt(rhs, impl->rows - 1, 0);
	}

	bool greater(const SortCursorWithCollation & rhs) const
	{
		return greaterAt(rhs, impl->pos, rhs.impl->pos);
	}

	bool operator< (const SortCursorWithCollation & rhs) const
	{
		return greater(rhs);
	}
};

}

