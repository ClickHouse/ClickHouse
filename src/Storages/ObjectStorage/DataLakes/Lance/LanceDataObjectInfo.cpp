#include "config.h"

#if USE_LANCE

#include <Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
extern const int UNKNOWN_PROTOCOL;
}

namespace DB::Lance
{

void LanceObjectSerializableInfo::checkVersion(size_t protocol_version) const
{
    if (protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_METADATA)
    {
        throw DB::Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Lance cluster task metadata requires protocol version >= {}, got {}",
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_METADATA,
            protocol_version);
    }
}

void LanceObjectSerializableInfo::serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const
{
    checkVersion(protocol_version);
    writeVarUInt(version, out);
    writeVarUInt(static_cast<UInt64>(fragment_ids.size()), out);
    for (UInt64 fragment_id : fragment_ids)
        writeVarUInt(fragment_id, out);
    writeVarUInt(static_cast<UInt64>(pack_index), out);
    writeVarUInt(static_cast<UInt64>(pack_count), out);
}

void LanceObjectSerializableInfo::deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);
    readVarUInt(version, in);
    UInt64 size = 0;
    readVarUInt(size, in);
    fragment_ids.resize(static_cast<size_t>(size));
    for (size_t i = 0; i < fragment_ids.size(); ++i)
        readVarUInt(fragment_ids[i], in);
    UInt64 pack_index_value = 0;
    UInt64 pack_count_value = 0;
    readVarUInt(pack_index_value, in);
    readVarUInt(pack_count_value, in);
    pack_index = static_cast<size_t>(pack_index_value);
    pack_count = static_cast<size_t>(pack_count_value);
}

}

namespace DB
{

ObjectMetadata LanceDatasetObjectInfo::createDatasetObjectMetadata()
{
    ObjectMetadata metadata;
    metadata.is_size_known = false;
    return metadata;
}

LanceDatasetObjectInfo::LanceDatasetObjectInfo(
    String synthetic_path_,
    Lance::TableStateSnapshot snapshot_,
    Lance::DatasetHandle dataset_,
    std::vector<UInt64> fragment_ids_,
    size_t pack_index_,
    size_t pack_count_)
    : ObjectInfo(RelativePathWithMetadata(std::move(synthetic_path_), createDatasetObjectMetadata()))
    , snapshot(std::move(snapshot_))
    , dataset(std::move(dataset_))
    , fragment_ids(std::move(fragment_ids_))
    , pack_index(pack_index_)
    , pack_count(pack_count_)
{
}

LanceDatasetObjectInfo::LanceDatasetObjectInfo(const RelativePathWithMetadata & path_, const Lance::LanceObjectSerializableInfo & info_)
    : ObjectInfo(RelativePathWithMetadata(path_.relative_path, createDatasetObjectMetadata()))
    , snapshot(Lance::TableStateSnapshot{.version = info_.version})
    , dataset()
    , fragment_ids(info_.fragment_ids)
    , pack_index(info_.pack_index)
    , pack_count(info_.pack_count)
{
    relative_path_with_metadata.read_source_index = path_.read_source_index;
}

Lance::LanceObjectSerializableInfo LanceDatasetObjectInfo::toSerializableInfo() const
{
    return Lance::LanceObjectSerializableInfo{
        .version = snapshot.version,
        .fragment_ids = fragment_ids,
        .pack_index = pack_index,
        .pack_count = pack_count,
    };
}

}

#endif
