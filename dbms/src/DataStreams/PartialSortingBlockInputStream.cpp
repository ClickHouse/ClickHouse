#include <DB/Interpreters/sortBlock.h>

#include <DB/DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


Block PartialSortingBlockInputStream::readImpl()
{
	Block res = children.back()->read();
	sortBlock(res, description, limit);
	return res;
}


}
