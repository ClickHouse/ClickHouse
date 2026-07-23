#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RelativePathWithMetadataPtr ObjectStorageIteratorFromList::current()
{
    if (!isValid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to access invalid iterator");

    return *batch_iterator;
}

}
