#include <DB/Interpreters/sortBlock.h>


namespace DB
{
	
typedef std::vector<std::pair<const IColumn *, SortColumnDescription> > ColumnsWithSortDescriptions;


static inline bool needCollation(const IColumn * column, const SortColumnDescription & description)
{
	return !description.collator.isNull() && column->getName() == "ColumnString";
}


struct PartialSortingLess
{
	const ColumnsWithSortDescriptions & columns;
	
	PartialSortingLess(const ColumnsWithSortDescriptions & columns_) : columns(columns_) {}

	bool operator() (size_t a, size_t b) const
	{
		for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end(); ++it)
		{
			int res = it->second.direction * it->first->compareAt(a, b, *it->first);
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
				const ColumnString & column_string = dynamic_cast<const ColumnString &>(*it->first);
				res = column_string.compareAt(a, b, *it->first, *it->second.collator);
			}
			else
				res = it->first->compareAt(a, b, *it->first);
			
			res *= it->second.direction;
			if (res < 0)
				return true;
			else if (res > 0)
				return false;
		}
		return false;
	}
};


void sortBlock(Block & block, const SortDescription & description)
{
	if (!block)
		return;
	
	/// Если столбец сортировки один
	if (description.size() == 1)
	{
		IColumn * column = !description[0].column_name.empty()
			? block.getByName(description[0].column_name).column
			: block.getByPosition(description[0].column_number).column;
		
		IColumn::Permutation perm;
		if (needCollation(column, description[0]))
		{
			const ColumnString & column_string = dynamic_cast<const ColumnString &>(*column);
			perm = column_string.getPermutation(*description[0].collator);
		}
		else
			perm = column->getPermutation();

		if (description[0].direction == -1)
			for (size_t i = 0, size = perm.size(); i < size / 2; ++i)
				std::swap(perm[i], perm[size - 1 - i]);

		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->permute(perm);
	}
	else
	{
		size_t size = block.rows();
		IColumn::Permutation perm(size);
		for (size_t i = 0; i < size; ++i)
			perm[i] = i;
		
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
			std::sort(perm.begin(), perm.end(), less_with_collation);
		}
		else
		{
			PartialSortingLess less(columns_with_sort_desc);
			std::sort(perm.begin(), perm.end(), less);
		}

		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
			block.getByPosition(i).column = block.getByPosition(i).column->permute(perm);
	}
}

}
