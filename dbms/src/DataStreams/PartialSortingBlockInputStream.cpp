#include <DB/DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


struct PartialSortingLess
{
	typedef std::vector<std::pair<const IColumn *, int> > Columns;
	Columns columns;

	PartialSortingLess(const Block & block, const SortDescription & description)
	{
		for (size_t i = 0, size = description.size(); i < size; ++i)
			columns.push_back(std::make_pair(
				!description[i].column_name.empty()
					? &*block.getByName(description[i].column_name).column
					: &*block.getByPosition(description[i].column_number).column,
				description[i].direction));
	}

	bool operator() (size_t a, size_t b) const
	{
		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
		{
			int res = it->second * it->first->compareAt(a, b, *it->first);
			if (res < 0)
				return true;
			else if (res > 0)
				return false;
		}
		return false;
	}
};


Block PartialSortingBlockInputStream::readImpl()
{
	Block res = input->read();
	if (!res)
		return res;

	/// Если столбец сортировки один
	if (description.size() == 1)
	{
		IColumn::Permutation perm = (!description[0].column_name.empty()
			? res.getByName(description[0].column_name).column
			: res.getByPosition(description[0].column_number).column)->getPermutation();
		
		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
			res.getByPosition(i).column->permute(perm);
	}
	else
	{
		size_t size = res.rows();
		IColumn::Permutation perm(size);
		for (size_t i = 0; i < size; ++i)
			perm[i] = i;

		PartialSortingLess less(res, description);
		std::sort(perm.begin(), perm.end(), less);

		size_t columns = res.columns();
		for (size_t i = 0; i < columns; ++i)
			res.getByPosition(i).column->permute(perm);
	}

	return res;
}


}
