#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStatisticsNdv.h>

#if USE_AVRO

#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Databases/DataLake/ICatalog.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/Puffin/PuffinThetaSketch.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Formats/Impl/Puffin/PuffinReader.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool iceberg_write_table_statistics_ndv;
}

struct IcebergColumnSketches::ColumnSketch
{
    size_t position;
    Int32 field_id;
    PuffinColumnThetaSketch sketch;
};

IcebergColumnSketches::IcebergColumnSketches(const Block & header, const Poco::JSON::Array::Ptr & schema_fields)
    : runner((getParquetEncoderThreadPool().initializeWithDefaultSettingsIfNotInitialized(), getParquetEncoderThreadPool().get()), ThreadName::PARQUET_ENCODER)
{
    for (size_t position = 0; position < header.columns(); ++position)
    {
        const auto & column = header.getByPosition(position);
        if (!PuffinColumnThetaSketch::isSupportedType(column.type))
            continue;
        for (unsigned i = 0; i < schema_fields->size(); ++i)
        {
            const auto field = schema_fields->getObject(i);
            if (field->getValue<String>(Iceberg::f_name) == column.name)
            {
                sketches.push_back({position, field->getValue<Int32>(Iceberg::f_id), PuffinColumnThetaSketch(column.type)});
                break;
            }
        }
    }
}

IcebergColumnSketches::~IcebergColumnSketches() = default;

void IcebergColumnSketches::update(const Columns & columns)
{
    /// One task per column, running while the writer goes on with the next chunk, which waits for them here.
    /// A failure must not fail the write, so it only stops the sketches from being published.
    try
    {
        runner.waitForAllToFinishAndRethrowFirstError();
        if (failed)
            return;
        /// Makes the append in `enqueueAndKeepTrack` non-allocating, so a scheduled task is always tracked.
        runner.reserve(sketches.size());
        for (auto & column_sketch : sketches)
        {
            runner.enqueueAndKeepTrack([&column_sketch, column = columns[column_sketch.position]]
            {
                column_sketch.sketch.update(*column);
            });
        }
    }
    catch (...)
    {
        tryLogCurrentException(
            getLogger("IcebergTableStatisticsNdv"), "Cannot update the column sketches, no statistics will be written");
        failed = true;
    }
}

std::optional<std::map<Int32, String>> IcebergColumnSketches::finish()
{
    runner.waitForAllToFinishAndRethrowFirstError();
    if (failed)
        return {};
    std::map<Int32, String> result;
    for (const auto & column : sketches)
        result.emplace(column.field_id, column.sketch.serialize().bytes);
    return result;
}

namespace
{

using SketchByFieldId = std::map<Int32, PuffinColumnThetaSketch::Serialized>;

/// How the current top-level fields differ from the schema a snapshot was written with.
struct SchemaChanges
{
    /// Absent from it and without an `initial-default`: NULL in all its rows.
    std::unordered_set<Int32> null_fields;
    /// Absent from it, with an `initial-default`: the serialized sketch of that default, the value of all its rows.
    std::map<Int32, String> default_sketches;
    /// With another type in it (a promotion such as `int` to `long`): its sketches hash other bytes.
    std::unordered_set<Int32> retyped_fields;
};

struct ParentSketches
{
    bool has_rows = false;
    /// Serialized, by field id.
    std::map<Int32, String> sketches;
    /// Fields added after the parent without an `initial-default`: a sketch of the new rows is complete for them. A field
    /// added with one has the sketch of its default in `sketches`.
    std::unordered_set<Int32> fields_null_in_parent;
};

/// The NDV statistics file of a snapshot that a commit adds to the table metadata.
class SnapshotStatisticsNdv
{
public:
    SnapshotStatisticsNdv(
        const Poco::JSON::Object::Ptr & metadata_,
        const Poco::JSON::Object::Ptr & snapshot_,
        const Iceberg::IcebergPathResolver & resolver_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_)
        : metadata(metadata_)
        , snapshot(snapshot_)
        , resolver(resolver_)
        , object_storage(std::move(object_storage_))
        , context(std::move(context_))
    {
    }

    /// Nothing if the parent snapshot has rows but no sketches.
    [[nodiscard]] std::optional<ParentSketches> readParentSketches() const
    {
        const Int64 parent_snapshot_id
            = snapshot->has(Iceberg::f_parent_snapshot_id) ? snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id) : -1;
        if (parent_snapshot_id == -1)
            return ParentSketches{};
        const auto parent_snapshot = findSnapshot(parent_snapshot_id);
        const auto summary = parent_snapshot ? parent_snapshot->getObject(Iceberg::f_summary) : nullptr;
        if (summary && summary->has(Iceberg::f_total_records) && summary->getValue<String>(Iceberg::f_total_records) == "0")
            return ParentSketches{};

        const auto parent_entry = findStatisticsEntry(parent_snapshot_id);
        if (!parent_entry)
            return {};
        const auto parent_path
            = resolver.resolve(Iceberg::IcebergPathFromMetadata::deserialize(parent_entry->getValue<String>(Iceberg::f_statistics_path)));
        RelativePathWithMetadata object_info(parent_path);
        auto buf = createReadBuffer(object_info, object_storage, context, getLogger("IcebergTableStatisticsNdv"));
        PuffinReader reader(*buf);
        auto changes = schemaChanges(parent_snapshot);
        const auto listed_fields = listedSketchFields(parent_entry, parent_snapshot_id);
        ParentSketches parent{
            .has_rows = true, .sketches = std::move(changes.default_sketches), .fields_null_in_parent = std::move(changes.null_fields)};
        for (const auto & blob : reader.getBlobs())
        {
            /// A file can keep the blobs of an earlier snapshot (Trino's `ANALYZE` of some columns): they miss later rows.
            if (blob.type != PUFFIN_DATASKETCHES_THETA_BLOB_TYPE || blob.fields.size() != 1 || blob.snapshot_id != parent_snapshot_id
                || !listed_fields.contains(blob.fields[0]) || changes.retyped_fields.contains(blob.fields[0]))
                continue;
            if (auto bytes = reader.readBlob(blob))
                parent.sketches.emplace(blob.fields[0], std::move(*bytes));
        }
        return parent;
    }

    /// Writes `sketches` as the Puffin file `path` and registers it in the `statistics` of the metadata.
    void write(const SketchByFieldId & sketches, const Iceberg::IcebergPathFromMetadata & path)
    {
        const Int64 snapshot_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
        /// Format version 1 has no sequence numbers.
        const Int64 sequence_number
            = snapshot->has(Iceberg::f_metadata_sequence_number) ? snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number) : 0;

        WriteBufferFromOwnString out;
        PuffinWriter writer(out);
        Poco::JSON::Array::Ptr blob_metadata = new Poco::JSON::Array;
        for (const auto & [field_id, sketch] : sketches)
        {
            const auto blob = makeThetaSketchBlobMetadata(field_id, snapshot_id, sequence_number, sketch.ndv);
            writer.addBlob(blob, sketch.bytes);
            blob_metadata->add(blob.toJSON(/*in_footer=*/ false));
        }
        const size_t footer_size = writer.finalize();
        out.finalize();
        const String & file = out.str();

        Iceberg::writeMessageToFile(file, resolver.resolve(path), object_storage, context, /*write_if_none_match=*/ "");

        Poco::JSON::Object::Ptr statistics_entry = new Poco::JSON::Object;
        statistics_entry->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        statistics_entry->set(Iceberg::f_statistics_path, path.serialize());
        statistics_entry->set(Iceberg::f_metadata_file_size_in_bytes, file.size());
        statistics_entry->set(Iceberg::f_file_footer_size_in_bytes, footer_size);
        statistics_entry->set(Iceberg::f_blob_metadata, blob_metadata);

        auto statistics_entries = metadata->getArray(Iceberg::f_statistics);
        if (!statistics_entries)
        {
            statistics_entries = new Poco::JSON::Array;
            metadata->set(Iceberg::f_statistics, statistics_entries);
        }
        statistics_entries->add(statistics_entry);
    }

private:
    [[nodiscard]] Poco::JSON::Object::Ptr findStatisticsEntry(Int64 snapshot_id) const
    {
        const auto statistics_entries = metadata->getArray(Iceberg::f_statistics);
        for (unsigned i = 0; statistics_entries && i < statistics_entries->size(); ++i)
        {
            const auto entry = statistics_entries->getObject(i);
            if (entry->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == snapshot_id)
                return entry;
        }
        return nullptr;
    }

    [[nodiscard]] Poco::JSON::Object::Ptr findSnapshot(Int64 snapshot_id) const
    {
        const auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (unsigned i = 0; snapshots && i < snapshots->size(); ++i)
            if (const auto other = snapshots->getObject(i); other->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == snapshot_id)
                return other;
        return nullptr;
    }

    [[nodiscard]] Poco::JSON::Array::Ptr findSchemaFields(Int64 schema_id) const
    {
        const auto schemas = metadata->getArray(Iceberg::f_schemas);
        for (unsigned i = 0; schemas && i < schemas->size(); ++i)
            if (const auto schema = schemas->getObject(i); schema->getValue<Int64>(Iceberg::f_schema_id) == schema_id)
                return schema->getArray(Iceberg::f_fields);
        return nullptr;
    }

    [[nodiscard]] SchemaChanges schemaChanges(const Poco::JSON::Object::Ptr & other) const
    {
        if (!other || !other->has(Iceberg::f_schema_id) || !metadata->has(Iceberg::f_current_schema_id))
            return {};
        const auto snapshot_fields = findSchemaFields(other->getValue<Int64>(Iceberg::f_schema_id));
        const auto current_fields = findSchemaFields(metadata->getValue<Int64>(Iceberg::f_current_schema_id));
        if (!snapshot_fields || !current_fields)
            return {};
        std::unordered_map<Int32, String> snapshot_types;
        for (unsigned i = 0; i < snapshot_fields->size(); ++i)
        {
            const auto field = snapshot_fields->getObject(i);
            snapshot_types.emplace(field->getValue<Int32>(Iceberg::f_id), field->get(Iceberg::f_type).toString());
        }
        SchemaChanges changes;
        for (unsigned i = 0; i < current_fields->size(); ++i)
        {
            const auto field = current_fields->getObject(i);
            const auto id = field->getValue<Int32>(Iceberg::f_id);
            if (const auto type = snapshot_types.find(id); type == snapshot_types.end())
            {
                if (!field->has(Iceberg::f_initial_default) || field->get(Iceberg::f_initial_default).isEmpty())
                    changes.null_fields.insert(id);
                else if (auto sketch = initialDefaultSketch(field))
                    changes.default_sketches.emplace(id, std::move(*sketch));
            }
            else if (type->second != field->get(Iceberg::f_type).toString())
                changes.retyped_fields.insert(id);
        }
        return changes;
    }

    /// The sketch of the `initial-default` of the primitive field `field`, or nothing for another type.
    [[nodiscard]] static std::optional<String> initialDefaultSketch(const Poco::JSON::Object::Ptr & field)
    {
        const auto iceberg_type = field->get(Iceberg::f_type);
        if (!iceberg_type.isString())
            return {};
        const auto type = Iceberg::IcebergSchemaProcessor::getSimpleType(iceberg_type.extract<String>());
        if (!PuffinColumnThetaSketch::isSupportedType(type))
            return {};
        PuffinColumnThetaSketch sketch(type);
        sketch.update(*type->createColumnConst(1, *Iceberg::getIcebergInitialDefault(field, type)));
        return sketch.serialize().bytes;
    }

    /// The fields of the theta sketches of `snapshot_id` that the statistics entry lists, as readers take them.
    [[nodiscard]] static std::unordered_set<Int32> listedSketchFields(const Poco::JSON::Object::Ptr & entry, Int64 snapshot_id)
    {
        std::unordered_set<Int32> fields;
        const auto blobs = entry->getArray(Iceberg::f_blob_metadata);
        for (unsigned i = 0; blobs && i < blobs->size(); ++i)
        {
            const auto blob = blobs->getObject(i);
            const auto blob_fields = blob->getArray(Iceberg::f_fields);
            if (blob->getValue<String>(Iceberg::f_type) == PUFFIN_DATASKETCHES_THETA_BLOB_TYPE && blob_fields && blob_fields->size() == 1
                && blob->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == snapshot_id)
                fields.insert(blob_fields->getElement<Int32>(0));
        }
        return fields;
    }

    Poco::JSON::Object::Ptr metadata;
    Poco::JSON::Object::Ptr snapshot;
    const Iceberg::IcebergPathResolver & resolver;
    ObjectStoragePtr object_storage;
    ContextPtr context;
};

}

std::optional<String> writeIcebergTableStatisticsNdv(
    IcebergColumnSketches * column_sketches,
    bool union_with_parent,
    Poco::JSON::Object::Ptr metadata,
    const Poco::JSON::Object::Ptr & snapshot,
    FileNamesGenerator & generator,
    const Iceberg::IcebergPathResolver & resolver,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context)
{
    chassert(column_sketches || union_with_parent);
    const auto log = getLogger("IcebergTableStatisticsNdv");
    const Int64 snapshot_id = snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
    SnapshotStatisticsNdv snapshot_statistics(metadata, snapshot, resolver, object_storage, context);
    String statistics_file_path;
    try
    {
        ParentSketches parent;
        if (union_with_parent)
        {
            auto parent_sketches = snapshot_statistics.readParentSketches();
            if (!parent_sketches)
            {
                LOG_DEBUG(log, "Snapshot {}: no statistics written, its parent snapshot has rows but no sketches", snapshot_id);
                return {};
            }
            parent = std::move(*parent_sketches);
        }

        SketchByFieldId sketches;
        /// Sketches cannot subtract, so once a commit removes rows the number of distinct values is an upper bound.
        if (!column_sketches)
        {
            for (const auto & [field_id, bytes] : parent.sketches)
                sketches.emplace(field_id, unionThetaSketches({bytes}));
        }
        else
        {
            const auto new_sketches = column_sketches->finish();
            if (!new_sketches)
                return {};
            for (const auto & [field_id, bytes] : *new_sketches)
            {
                auto parent_sketch = parent.sketches.find(field_id);
                if (parent_sketch != parent.sketches.end())
                    sketches.emplace(field_id, unionThetaSketches({parent_sketch->second, bytes}));
                else if (!parent.has_rows || parent.fields_null_in_parent.contains(field_id))
                    sketches.emplace(field_id, unionThetaSketches({bytes}));
            }
        }
        if (sketches.empty())
        {
            LOG_DEBUG(log, "Snapshot {}: no statistics written, no column has a sketch", snapshot_id);
            return {};
        }

        const auto path = generator.generateStatisticsFileName(snapshot_id);
        statistics_file_path = resolver.resolve(path);
        snapshot_statistics.write(sketches, path);
        LOG_DEBUG(log, "Snapshot {}: wrote statistics file {} with {} columns", snapshot_id, statistics_file_path, sketches.size());
        return statistics_file_path;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Snapshot {}: cannot write the statistics file", snapshot_id));
        if (!statistics_file_path.empty())
        {
            try
            {
                object_storage->removeObjectIfExists(StoredObject(statistics_file_path));
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Best-effort cleanup failed for {}", statistics_file_path));
            }
        }
        return {};
    }
}

bool shouldWriteIcebergTableStatisticsNdv(const ContextPtr & context, const std::shared_ptr<DataLake::ICatalog> & catalog)
{
    /// TODO: send the statistics file to a transactional catalog as a `set-statistics` update.
    return USE_DATASKETCHES && context->getSettingsRef()[Setting::iceberg_write_table_statistics_ndv]
        && !(catalog && catalog->isTransactional());
}

}

#endif
