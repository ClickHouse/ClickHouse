#include <DB/Interpreters/sortBlock.h>

#include <DB/DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


Block PartialSortingBlockInputStream::readImpl()
{
	Block res = children.back()->read();
	sortBlock(res, description, limit);

	if (limit && res.rowsInFirstColumn() > limit)
		for (size_t i = 0, size = res.columns(); i < size; ++i)
			res.getByPosition(i).column = res.getByPosition(i).column->cut(0, limit);

	return res;
}


}
