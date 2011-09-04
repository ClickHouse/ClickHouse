#include <DB/DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{

struct PartialSortingLess
{
	typedef std::vector<const IColumn *> Columns;
	Columns columns;

	PartialSortingLess(const Block & block, const ColumnNumbers & column_numbers)
	{
		for (size_t i = 0, size = column_numbers.size(); i < size; ++i)
			columns.push_back(&*block.getByPosition(column_numbers[i]).column);
	}

	bool operator() (size_t a, size_t b) const
	{
		for (Columns::const_iterator it = columns.begin(); it != columns.end(); ++it)
		{
			int res = (*it)->compareAt(a, b, **it);
			if (res < 0)
				return true;
			else if (res > 0)
				return false;
		}
		return false;
	}
};


Block PartialSortingBlockInputStream::read()
{
	Block res = input->read();
	if (!res)
		return res;

	size_t size = res.rows();
	IColumn::Permutation perm(size);
	for (size_t i = 0; i < size; ++i)
		perm[i] = i;

	PartialSortingLess less(res, column_numbers);
	std::sort(perm.begin(), perm.end(), less);

	size_t columns = res.columns();
	for (size_t i = 0; i < columns; ++i)
		res.getByPosition(i).column->permute(perm);

	return res;
}

}
