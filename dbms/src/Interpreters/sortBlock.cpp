#include <DB/Interpreters/sortBlock.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int BAD_COLLATION;
}


using ColumnsWithSortDescriptions = std::vector<std::pair<const IColumn *, SortColumnDescription>>;


static inline bool needCollation(const IColumn * column, const SortColumnDescription & description)
{
	if (description.collator.isNull())
		return false;

	if (column->getName() != "ColumnString")
		throw Exception("Collations could be specified only for String columns.", ErrorCodes::BAD_COLLATION);

	return true;
}


struct PartialSortingLess
{
	const ColumnsWithSortDescriptions & columns;

	PartialSortingLess(const ColumnsWithSortDescriptions & columns_) : columns(columns_) {}

	bool operator() (size_t a, size_t b) const
	{
		for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end(); ++it)
		{
			int res = it->second.direction * it->first->compareAt(a, b, *it->first, it->second.direction);
			if (res < 0)
				return true;
			else if (res > 0)
				return false;
		}
		return false;
	}
};

struct PartialSortingLessWithCollation
{
	const ColumnsWithSortDescriptions & columns;

	PartialSortingLessWithCollation(const ColumnsWithSortDescriptions & columns_) : columns(columns_) {}

	bool operator() (size_t a, size_t b) const
	{
		for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end(); ++it)
		{
			int res;
			if (needCollation(it->first, it->second))
			{
				const ColumnString & column_string = typeid_cast<const ColumnString &>(*it->first);
				res = column_string.compareAtWithCollation(a, b, *it->first, *it->second.collator);
			}
			else
				res = it->first->compareAt(a, b, *it->first, it->second.direction);

			res *= it->second.direction;
			if (res < 0)
				return true;
			else if (res > 0)
				return false;
		}
		return false;
	}
};


void sortBlock(Block & block, const SortDescription & description, size_t limit)
{
	if (!block)
		return;

	/// Если столбец сортировки один
	if (description.size() == 1)
	{
		bool reverse = description[0].direction == -1;

		IColumn * column = !description[0].column_name.empty()
			? block.getByName(description[0].column_name).column
			: block.getByPosition(description[0].column_number).column;

		IColumn::Permutation perm;
		if (needCollation(column, description[0]))
		{
			const ColumnString & column_string = typeid_cast<const ColumnString &>(*column);
			column_string.getPermutationWithCollation(*description[0].collator, reverse, limit, perm);
		}
		else
			column->getPermutation(reverse, limit, perm);

		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
	}
	else
	{
		size_t size = block.rows();
		IColumn::Permutation perm(size);
		for (size_t i = 0; i < size; ++i)
			perm[i] = i;

		if (limit >= size)
			limit = 0;

		bool need_collation = false;
		ColumnsWithSortDescriptions columns_with_sort_desc;

		for (size_t i = 0, size = description.size(); i < size; ++i)
		{
			IColumn * column = !description[i].column_name.empty()
				? block.getByName(description[i].column_name).column
				: block.getByPosition(description[i].column_number).column;

			columns_with_sort_desc.push_back(std::make_pair(column, description[i]));

			if (needCollation(column, description[i]))
				need_collation = true;
		}

		if (need_collation)
		{
			PartialSortingLessWithCollation less_with_collation(columns_with_sort_desc);

			if (limit)
				std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), less_with_collation);
			else
				std::sort(perm.begin(), perm.end(), less_with_collation);
		}
		else
		{
			PartialSortingLess less(columns_with_sort_desc);

			if (limit)
				std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), less);
			else
				std::sort(perm.begin(), perm.end(), less);
		}

		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, limit);
	}
}


void stableGetPermutation(const Block & block, const SortDescription & description, IColumn::Permutation & out_permutation)
{
	if (!block)
		return;

	size_t size = block.rows();
	out_permutation.resize(size);
	for (size_t i = 0; i < size; ++i)
		out_permutation[i] = i;

	ColumnsWithSortDescriptions columns_with_sort_desc;

	for (size_t i = 0, size = description.size(); i < size; ++i)
	{
		const IColumn * column = !description[i].column_name.empty()
			? block.getByName(description[i].column_name).column
			: block.getByPosition(description[i].column_number).column;

		columns_with_sort_desc.push_back(std::make_pair(column, description[i]));
	}

	std::stable_sort(out_permutation.begin(), out_permutation.end(), PartialSortingLess(columns_with_sort_desc));
}


bool isAlreadySorted(const Block & block, const SortDescription & description)
{
	if (!block)
		return true;

	size_t rows = block.rows();

	ColumnsWithSortDescriptions columns_with_sort_desc;

	for (size_t i = 0, size = description.size(); i < size; ++i)
	{
		const IColumn * column = !description[i].column_name.empty()
			? block.getByName(description[i].column_name).column
			: block.getByPosition(description[i].column_number).column;

		columns_with_sort_desc.push_back(std::make_pair(column, description[i]));
	}

	PartialSortingLess less(columns_with_sort_desc);

	/** Если строк не слишком мало, то предпримем быструю попытку проверить, что блок не сортирован.
	  * Константы - наугад.
	  */
	static constexpr size_t num_rows_to_try = 10;
	if (rows > num_rows_to_try * 5)
	{
		for (size_t i = 1; i < num_rows_to_try; ++i)
		{
			size_t prev_position = rows * (i - 1) / num_rows_to_try;
			size_t curr_position = rows * i / num_rows_to_try;

			if (less(curr_position, prev_position))
				return false;
		}
	}

	for (size_t i = 1; i < rows; ++i)
		if (less(i, i - 1))
			return false;

	return true;
}


void stableSortBlock(Block & block, const SortDescription & description)
{
	if (!block)
		return;

	IColumn::Permutation perm;
	stableGetPermutation(block, description, perm);

	size_t columns = block.columns();
	for (size_t i = 0; i < columns; ++i)
		block.getByPosition(i).column = block.getByPosition(i).column->permute(perm, 0);
}

}
