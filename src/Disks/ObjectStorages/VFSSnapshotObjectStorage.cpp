#include "VFSSnapshotObjectStorage.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event VFSGcCumulativeSnapshotBytesRead;
}

namespace DB
{

static StoredObject makeSnapshotStoredObject(const String & name, const String & prefix)
{
    return StoredObject{ObjectStorageKey::createAsAbsolute(fmt::format("{}{}", prefix, name)).serialize()};
}

VFSSnapshotReadStreamFromObjectStorage::VFSSnapshotReadStreamFromObjectStorage(
    ObjectStoragePtr object_storage_, const StoredObject & snapshot_object_)
    : object_storage(object_storage_), snapshot_object(snapshot_object_)
{
}

VFSSnapshotReadStreamFromObjectStorage::entry_type VFSSnapshotReadStreamFromObjectStorage::nextImpl()
{
    if (!read_buffer.has_value())
        read_buffer.emplace(object_storage->readObject(snapshot_object));

    if (read_buffer->eof())
    {
        ProfileEvents::increment(ProfileEvents::VFSGcCumulativeSnapshotBytesRead, read_buffer->count());
        return {};
    }
    return VFSSnapshotEntryStringSerializer::deserialize(*read_buffer);
}

VFSSnapshotWriteStreamFromObjectStorage::VFSSnapshotWriteStreamFromObjectStorage(
    ObjectStoragePtr object_storage_, const StoredObject & snapshot_object_, int snapshot_lz4_compression_level_)
    : object_storage(object_storage_), snapshot_object(snapshot_object_), snapshot_lz4_compression_level(snapshot_lz4_compression_level_)
{
}

WriteBuffer & VFSSnapshotWriteStreamFromObjectStorage::getWriteBuffer()
{
    if (!write_buffer.has_value())
        write_buffer.emplace(object_storage->writeObject(snapshot_object, WriteMode::Rewrite), snapshot_lz4_compression_level);

    return *write_buffer;
}

void VFSSnapshotWriteStreamFromObjectStorage::writeImpl(VFSSnapshotEntry && entry)
{
    VFSSnapshotEntryStringSerializer::serialize(std::move(entry), getWriteBuffer());
}

void VFSSnapshotWriteStreamFromObjectStorage::finalizeImpl()
{
    getWriteBuffer().finalize();
}

VFSSnapshotObjectStorage::VFSSnapshotObjectStorage(ObjectStoragePtr object_storage_, const String & prefix_, const VFSSettings & settings)
    : object_storage(object_storage_), prefix(prefix_), snapshot_lz4_compression_level(settings.snapshot_lz4_compression_level)
{
}

VFSSnapshotReadStreamPtr VFSSnapshotObjectStorage::readSnapshot(const String & name)
{
    return std::make_unique<VFSSnapshotReadStreamFromObjectStorage>(object_storage, makeSnapshotStoredObject(name, prefix));
}
VFSSnapshotWriteStreamPtr VFSSnapshotObjectStorage::writeSnapshot(const String & name)
{
    return std::make_unique<VFSSnapshotWriteStreamFromObjectStorage>(
        object_storage, makeSnapshotStoredObject(name, prefix), snapshot_lz4_compression_level);
}
VFSSnapshotSortingWriteStreamPtr VFSSnapshotObjectStorage::writeSnapshotWithSorting(const String & name)
{
    return std::make_unique<VFSSnapshotSortingWriteStream>(writeSnapshot(name));
}

Strings VFSSnapshotObjectStorage::listSnapshots() const
{
    static constexpr int MAX_KEYS = 10;

    Strings snapshots;
    RelativePathsWithMetadata objects;
    object_storage->listObjects(prefix, objects, MAX_KEYS);

    for (const auto & obj : objects)
    {
        String name = obj.relative_path.substr(prefix.length());
        snapshots.push_back(name);
    }

    return snapshots;
}

size_t VFSSnapshotObjectStorage::removeSnapshots(Strings names)
{
    StoredObjects objects_to_remove;

    for (const auto & name : names)
        objects_to_remove.push_back(makeSnapshotStoredObject(name, prefix));
    object_storage->removeObjectsIfExist(objects_to_remove);

    return names.size();
}

}
