#include <Interpreters/sortBlock.h>

#include <DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


Block PartialSortingBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    sortBlock(res, description, limit);
    return res;
}


}
