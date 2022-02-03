#include <Processors/Sinks/SinkToStorage.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

SinkToStorage::SinkToStorage(const Block & header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::transform(Chunk & chunk)
{
    /** Throw an exception if the sizes of arrays - elements of nested data structures doesn't match.
      * We have to make this assertion before writing to table, because storage engine may assume that they have equal sizes.
      * NOTE It'd better to do this check in serialization of nested structures (in place when this assumption is required),
      * but currently we don't have methods for serialization of nested structures "as a whole".
      */
    Nested::validateArraySizes(getHeader().cloneWithColumns(chunk.getColumns()));

    consume(chunk.clone());
    if (lastBlockIsDuplicate())
        chunk.clear();
}

}
