#include <DB/Interpreters/sortBlock.h>

#include <DB/DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


Block PartialSortingBlockInputStream::readImpl()
{
	Block res = input->read();
	sortBlock(res, description);
	return res;
}


}
