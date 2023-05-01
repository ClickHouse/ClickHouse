#include <Disks/ObjectStorages/StoredObject.h>

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/logger_useful.h>


namespace DB
{

StoredObject::StoredObject(
    const std::string & absolute_path_,
    uint64_t bytes_size_,
    const std::string & mapped_path_)
    : absolute_path(absolute_path_)
    , mapped_path(mapped_path_)
    , bytes_size(bytes_size_)
{
}

const std::string & StoredObject::getMappedPath() const
{
    return mapped_path;
}

}
