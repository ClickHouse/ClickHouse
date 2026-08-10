#include <Interpreters/ClusterFunctionReadTask.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/Context.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_PROTOCOL;
    extern const int LOGICAL_ERROR;
}
namespace Setting
{
    extern const SettingsBool cluster_function_process_archive_on_multiple_nodes;
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(ObjectInfoPtr object, const ContextPtr & context, bool read_pins_generation_)
    : read_pins_generation(read_pins_generation_)
{
    if (!object)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`object` cannot be null");

    if (object->data_lake_metadata.has_value())
        data_lake_metadata = object->data_lake_metadata.value();

#if USE_AVRO
    if (std::dynamic_pointer_cast<IcebergDataObjectInfo>(object))
    {
        iceberg_info = dynamic_cast<IcebergDataObjectInfo &>(*object).info;
    }
#endif

    const bool send_over_whole_archive = !context->getSettingsRef()[Setting::cluster_function_process_archive_on_multiple_nodes];
    path = send_over_whole_archive ? object->getPathOrPathToArchiveIfArchive() : object->getPath();
    read_source_index = object->relative_path_with_metadata.read_source_index;
    file_bucket_info = object->file_bucket_info;

    /// Propagate object metadata available from the List request to the worker to avoid re-fetching it
    /// again. Only real (fetched) metadata: the `skip_object_metadata` placeholder carries no usable
    /// size/time and the worker must fetch its own instead.
    if (auto object_metadata = object->getObjectMetadata(); object_metadata && object_metadata->is_fetched)
    {
        has_object_metadata = true;
        etag = object_metadata->etag;
        size_bytes = object_metadata->size_bytes;
        is_size_known = object_metadata->is_size_known;
        last_modified_epoch_us = static_cast<UInt64>(object_metadata->last_modified.epochMicroseconds());
        is_last_modified_known = object_metadata->is_last_modified_known;
    }
}

ClusterFunctionReadTaskResponse::ClusterFunctionReadTaskResponse(const std::string & path_)
    : path(path_)
{
}

ObjectInfoPtr ClusterFunctionReadTaskResponse::getObjectInfo() const
{
    if (isEmpty())
        return {};

    ObjectInfoPtr object;

    if (iceberg_info.has_value())
    {
#if USE_AVRO
        auto iceberg_object = std::make_shared<IcebergDataObjectInfo>(RelativePathWithMetadata{path});
        iceberg_object->info = iceberg_info.value();
        object = iceberg_object;
#else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg support is disabled");
#endif
    }
    else
    {
        object = std::make_shared<ObjectInfo>(path);
    }
    object->relative_path_with_metadata.read_source_index = read_source_index;
    object->data_lake_metadata = data_lake_metadata;
    object->file_bucket_info = file_bucket_info;

    /// Rebuild the propagated split-time metadata (see `etag` in the header) so the worker's ranged GETs
    /// validate against it.
    if (has_object_metadata)
    {
        ObjectMetadata object_metadata;
        object_metadata.etag = etag;
        object_metadata.size_bytes = size_bytes;
        object_metadata.is_size_known = is_size_known;
        object_metadata.last_modified = Poco::Timestamp(static_cast<Poco::Timestamp::TimeVal>(last_modified_epoch_us));
        object_metadata.is_last_modified_known = is_last_modified_known;
        object->setObjectMetadata(object_metadata);
        /// Mark it as coordinator-propagated (carries no tags) so the worker's read path reuses it -
        /// skipping its own HEAD - yet still fetches tags when `_tags` is requested.
        object->metadata_propagated_from_coordinator = true;
    }

    return object;
}

void ClusterFunctionReadTaskResponse::serialize(WriteBuffer & out, size_t worker_protocol_version) const
{
    auto protocol_version
        = std::min(static_cast<UInt64>(worker_protocol_version), static_cast<UInt64>(DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION));
    writeVarUInt(protocol_version, out);
    writeStringBinary(path, out);

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        SerializedSetsRegistry registry;
        if (data_lake_metadata.schema_transform)
            data_lake_metadata.schema_transform->serialize(out, registry);
        else
            ActionsDAG().serialize(out, registry);

        if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS)
        {
            if (data_lake_metadata.excluded_rows)
                data_lake_metadata.excluded_rows->write(out);
            else
                DataLakeObjectMetadata::ExcludedRows().write(out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO)
    {
        if (file_bucket_info)
        {
            /// Write format name so we can create appropriate file bucket info during deserialization.
            writeStringBinary(file_bucket_info->getFormatName(), out);
            file_bucket_info->serialize(out);
        }
        else
        {
            /// Write empty string as format name if file_bucket_info is not set.
            writeStringBinary("", out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        if (iceberg_info.has_value())
        {
            writeVarUInt(1, out);
            iceberg_info->serializeForClusterFunctionProtocol(out, protocol_version);
        }
        else
        {
            writeVarUInt(0, out);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_READ_SOURCE_INDEX)
    {
        writeVarUInt(read_source_index.has_value(), out);
        if (read_source_index)
            writeVarUInt(*read_source_index, out);
    }
    else if (read_source_index.has_value())
    {
        /// Fail closed: downgrading the task to a path-only `ObjectInfo` would make the worker treat all
        /// web URL shards as failover for that path, reading only the first available source and silently
        /// missing rows from the other shards. A worker that cannot carry `read_source_index` must not run
        /// such a task.
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Worker protocol version {} cannot carry `read_source_index`, which is required for distributed "
            "reads of wildcard URL shards (minimum protocol version: {})",
            protocol_version,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_READ_SOURCE_INDEX);
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_OBJECT_METADATA)
    {
        writeBinary(has_object_metadata, out);
        if (has_object_metadata)
        {
            writeStringBinary(etag, out);
            writeVarUInt(size_bytes, out);
            writeBinary(is_size_known, out);
            writeVarUInt(last_modified_epoch_us, out);
            writeBinary(is_last_modified_known, out);
        }
    }
    else if (file_bucket_info && !etag.empty() && read_pins_generation)
    {
        /// Fail closed: a bucket-split task carries offsets computed from the generation this ETag
        /// identifies. A worker that cannot receive the ETag would pin nothing (or its own fresh HEAD)
        /// and could apply those offsets to different bytes after a concurrent overwrite - the very
        /// misread the propagation exists to prevent. Only gated for backends whose read actually pins
        /// to the ETag (S3); a backend that never pins (Azure, HDFS, ...) loses nothing on an old worker,
        /// so rejecting it would needlessly break mixed-version rolling upgrades.
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Worker protocol version {} cannot carry the object metadata required to pin a bucket-split "
            "read to the coordinator's generation (minimum protocol version: {})",
            protocol_version,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_OBJECT_METADATA);
    }
}

void ClusterFunctionReadTaskResponse::deserialize(ReadBuffer & in)
{
    size_t protocol_version = 0;
    readVarUInt(protocol_version, in);
    if (protocol_version < DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION || protocol_version > DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_PROTOCOL,
            "Supported protocol versions are in range [{}, {}], got: {}",
            DBMS_CLUSTER_INITIAL_PROCESSING_PROTOCOL_VERSION,
            DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION,
            protocol_version);
    }

    readStringBinary(path, in);
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_DATA_LAKE_METADATA)
    {
        DeserializedSetsRegistry registry;
        /// Trusted intra-cluster metadata: decode types without the input complexity limit.
        auto transform = std::make_shared<ActionsDAG>(ActionsDAG::deserialize(in, registry, Context::getGlobalContextInstance(), 0));

        if (!path.empty() && !transform->getInputs().empty())
        {
            data_lake_metadata.schema_transform = std::move(transform);
        }
        if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_EXCLUDED_ROWS)
        {
            data_lake_metadata.excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
            data_lake_metadata.excluded_rows->read(in);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO)
    {
        String format;
        readStringBinary(format, in);
        if (!format.empty())
        {
            file_bucket_info = FormatFactory::instance().getFileBucketInfo(format);
            file_bucket_info->deserialize(in);
        }
    }
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_ICEBERG_METADATA)
    {
        auto has_iceberg_metadata = false;
        readVarUInt(has_iceberg_metadata, in);
        if (has_iceberg_metadata)
        {
            iceberg_info = Iceberg::IcebergObjectSerializableInfo{};
            iceberg_info->deserializeForClusterFunctionProtocol(in, protocol_version);
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_READ_SOURCE_INDEX)
    {
        bool has_read_source_index = false;
        readVarUInt(has_read_source_index, in);
        if (has_read_source_index)
        {
            UInt64 value = 0;
            readVarUInt(value, in);
            read_source_index = value;
        }
    }

    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_OBJECT_METADATA)
    {
        readBinary(has_object_metadata, in);
        if (has_object_metadata)
        {
            readStringBinary(etag, in);
            readVarUInt(size_bytes, in);
            readBinary(is_size_known, in);
            readVarUInt(last_modified_epoch_us, in);
            readBinary(is_last_modified_known, in);
        }
    }
}

}
