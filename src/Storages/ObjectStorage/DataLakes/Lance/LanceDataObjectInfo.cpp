#include "config.h"

#if USE_LANCE

#include <Storages/ObjectStorage/DataLakes/Lance/LanceDataObjectInfo.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <filesystem>
#include <utility>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int UNKNOWN_PROTOCOL;
}

namespace DB::Lance
{

namespace
{
constexpr UInt64 MAX_FRAGMENT_IDS_PER_LANCE_TASK = 1'000'000;
}

void LanceObjectSerializableInfo::checkVersion(size_t protocol_version) const
{
    if (protocol_version < DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_SNAPSHOT_IDENTITY)
    {
        throw DB::Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Lance cluster task metadata requires protocol version >= {}, got {}",
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_LANCE_SNAPSHOT_IDENTITY,
            protocol_version);
    }
}

void LanceObjectSerializableInfo::serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const
{
    checkVersion(protocol_version);
    if (fragment_ids.size() > MAX_FRAGMENT_IDS_PER_LANCE_TASK)
        throw DB::Exception(ErrorCodes::INCORRECT_DATA, "Invalid Lance fragment count {}", fragment_ids.size());
    if (pack_count == 0 || pack_index >= pack_count)
        throw DB::Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid Lance fragment pack index {} or pack count {}",
            pack_index,
            pack_count);
    snapshot.serialize(out);
    writeVarUInt(static_cast<UInt64>(fragment_ids.size()), out);
    for (UInt64 fragment_id : fragment_ids)
        writeVarUInt(fragment_id, out);
    writeVarUInt(static_cast<UInt64>(pack_index), out);
    writeVarUInt(static_cast<UInt64>(pack_count), out);
}

void LanceObjectSerializableInfo::deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version)
{
    checkVersion(protocol_version);
    snapshot = TableStateSnapshot::deserialize(in, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);
    UInt64 size = 0;
    readVarUInt(size, in);
    if (size > MAX_FRAGMENT_IDS_PER_LANCE_TASK || !std::in_range<size_t>(size))
        throw DB::Exception(ErrorCodes::INCORRECT_DATA, "Invalid Lance fragment count {}", size);
    fragment_ids.resize(static_cast<size_t>(size));
    for (size_t i = 0; i < fragment_ids.size(); ++i)
        readVarUInt(fragment_ids[i], in);
    UInt64 pack_index_value = 0;
    UInt64 pack_count_value = 0;
    readVarUInt(pack_index_value, in);
    readVarUInt(pack_count_value, in);
    if (pack_count_value == 0 || pack_index_value >= pack_count_value
        || !std::in_range<size_t>(pack_index_value)
        || !std::in_range<size_t>(pack_count_value))
        throw DB::Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid Lance fragment pack index {} or pack count {}",
            pack_index_value,
            pack_count_value);
    pack_index = static_cast<size_t>(pack_index_value);
    pack_count = static_cast<size_t>(pack_count_value);
}

}

namespace DB
{

namespace
{

String datasetPathFromTaskPath(const String & task_path)
{
    const auto suffix = task_path.rfind("#v");
    return suffix == String::npos ? task_path : task_path.substr(0, suffix);
}

}

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
    , dataset_path(datasetPathFromTaskPath(relative_path_with_metadata.relative_path))
{
}

LanceDatasetObjectInfo::LanceDatasetObjectInfo(const RelativePathWithMetadata & path_, const Lance::LanceObjectSerializableInfo & info_)
    : ObjectInfo(RelativePathWithMetadata(path_.relative_path, createDatasetObjectMetadata()))
    , snapshot(info_.snapshot)
    , dataset()
    , fragment_ids(info_.fragment_ids)
    , pack_index(info_.pack_index)
    , pack_count(info_.pack_count)
    , dataset_path(datasetPathFromTaskPath(path_.relative_path))
{
    relative_path_with_metadata.read_source_index = path_.read_source_index;
}

std::optional<std::string> LanceDatasetObjectInfo::getFileNameForVirtualColumns() const
{
    return std::filesystem::path(dataset_path).filename().string();
}

Lance::LanceObjectSerializableInfo LanceDatasetObjectInfo::toSerializableInfo() const
{
    return Lance::LanceObjectSerializableInfo{
        .snapshot = snapshot,
        .fragment_ids = fragment_ids,
        .pack_index = pack_index,
        .pack_count = pack_count,
    };
}

}

#endif
