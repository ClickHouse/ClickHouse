#include <Interpreters/sortBlock.h>

#include <DataStreams/PartialSortingBlockInputStream.h>


namespace DB
{


Block PartialSortingBlockInputStream::readImpl()
{
    /// TODO. If there is a limit, maintain "greatest" value,
    /// then skip all blocks that are greater or equal
    /// and filter other blocks by this value.

    Block res = children.back()->read();
    sortBlock(res, description, limit);
    return res;
}


}
