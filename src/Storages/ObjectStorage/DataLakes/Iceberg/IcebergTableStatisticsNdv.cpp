#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStatisticsNdv.h>

#if USE_AVRO

#include <Core/Settings.h>
#include <Databases/DataLake/ICatalog.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>

#if USE_DATASKETCHES
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/castColumn.h>
#include <Processors/Formats/Impl/PuffinFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Common/transformEndianness.h>
#include <base/sort.h>

#include <theta_sketch.hpp>
#include <theta_union.hpp>
#include <theta_update_sketch_base.hpp>
#endif

namespace DB
{

namespace Setting
{
    extern const SettingsBool iceberg_write_table_statistics_ndv;
}

#if USE_DATASKETCHES

namespace
{

constexpr UInt32 timestamp_scale = 6;

using SketchByFieldId = std::map<Int32, datasketches::compact_theta_sketch>;

struct ParentSketches
{
    bool has_rows = false;
    SketchByFieldId sketches;
};

/// The NDV statistics file of a snapshot that a commit adds to the table metadata.
class SnapshotStatisticsNdv
{
public:
    SnapshotStatisticsNdv(
        Poco::JSON::Object::Ptr metadata_,
        Poco::JSON::Object::Ptr snapshot_,
        const Iceberg::IcebergPathResolver & resolver_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_)
        : metadata(std::move(metadata_))
        , snapshot(std::move(snapshot_))
        , resolver(resolver_)
        , object_storage(std::move(object_storage_))
        , context(std::move(context_))
    {
    }

    /// Nothing if the parent snapshot has rows but no sketches.
    std::optional<ParentSketches> readParentSketches() const
    {
        const Int64 parent_snapshot_id
            = snapshot->has(Iceberg::f_parent_snapshot_id) ? snapshot->getValue<Int64>(Iceberg::f_parent_snapshot_id) : -1;
        if (parent_snapshot_id == -1 || !snapshotHasRows(parent_snapshot_id))
            return ParentSketches{};

        const auto parent_entry = findStatisticsEntry(parent_snapshot_id);
        if (!parent_entry)
            return {};
        const auto parent_path
            = resolver.resolve(Iceberg::IcebergPathFromMetadata::deserialize(parent_entry->getValue<String>(Iceberg::f_statistics_path)));
        RelativePathWithMetadata object_info(parent_path);
        auto buf = createReadBuffer(object_info, object_storage, context, getLogger("IcebergTableStatisticsNdv"));
        PuffinReader reader(*buf);
        ParentSketches parent{.has_rows = true, .sketches = {}};
        for (const auto & blob : reader.getBlobs())
        {
            /// A file can keep the blobs of an earlier snapshot (Trino's `ANALYZE` of some columns): they miss later rows.
            if (blob.type != PUFFIN_THETA_SKETCH_BLOB_TYPE || blob.fields.size() != 1 || blob.snapshot_id != parent_snapshot_id)
                continue;
            const auto bytes = reader.readBlob(blob);
            if (bytes)
                parent.sketches.emplace(blob.fields[0], datasketches::compact_theta_sketch::deserialize(bytes->data(), bytes->size()));
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
            PuffinBlob blob;
            blob.type = PUFFIN_THETA_SKETCH_BLOB_TYPE;
            blob.snapshot_id = snapshot_id;
            blob.sequence_number = sequence_number;
            blob.fields.push_back(field_id);
            blob.properties.emplace(Iceberg::f_ndv, std::to_string(static_cast<UInt64>(sketch.get_estimate())));
            const auto bytes = sketch.serialize();
            writer.addBlob(blob, std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()));
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
    Poco::JSON::Object::Ptr findStatisticsEntry(Int64 snapshot_id) const
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

    bool snapshotHasRows(Int64 snapshot_id) const
    {
        const auto snapshots = metadata->getArray(Iceberg::f_snapshots);
        for (unsigned i = 0; snapshots && i < snapshots->size(); ++i)
        {
            const auto other = snapshots->getObject(i);
            if (other->getValue<Int64>(Iceberg::f_metadata_snapshot_id) != snapshot_id)
                continue;
            const auto summary = other->getObject(Iceberg::f_summary);
            return !summary || !summary->has(Iceberg::f_total_records) || summary->getValue<String>(Iceberg::f_total_records) != "0";
        }
        return true;
    }

    Poco::JSON::Object::Ptr metadata;
    Poco::JSON::Object::Ptr snapshot;
    const Iceberg::IcebergPathResolver & resolver;
    ObjectStoragePtr object_storage;
    ContextPtr context;
};

}

/// The theta sketch of one column. Each value is hashed as its Iceberg single-value serialization, the bytes Iceberg's
/// own writers hash (`Conversions.toByteBuffer`), with the hash and seed of an `update_theta_sketch`, so the sketches
/// union with theirs. The sketch owns the hash table of an `update_theta_sketch` and inserts a chunk's hashes into it.
class IcebergColumnSketches::ColumnSketch
{
public:
    /// The type whose `getDataAt` bytes are the Iceberg single-value serialization of the values of `type`: null if
    /// `type` already has them or is a decimal, nothing if `type` has no such serialization.
    static std::optional<DataTypePtr> getCastType(const DataTypePtr & type)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::Float32:
            case TypeIndex::Float64:
            case TypeIndex::Date32:
            case TypeIndex::String:
            case TypeIndex::FixedString:
                return DataTypePtr{};
            case TypeIndex::UInt8:
                if (isBool(type))
                    return DataTypePtr{};
                return std::make_shared<DataTypeInt32>();
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::UInt16:
            case TypeIndex::Date:
                return std::make_shared<DataTypeInt32>();
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
                return std::make_shared<DataTypeInt64>();
            /// Iceberg `timestamp`: microseconds.
            case TypeIndex::DateTime:
                return std::make_shared<DataTypeDateTime64>(timestamp_scale);
            case TypeIndex::DateTime64:
            {
                const auto scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
                if (scale > timestamp_scale)
                    return {};
                if (scale == timestamp_scale)
                    return DataTypePtr{};
                return std::make_shared<DataTypeDateTime64>(timestamp_scale);
            }
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::Decimal256:
                return DataTypePtr{};
            default:
                return {};
        }
    }

    ColumnSketch(size_t position_, Int32 field_id_, DataTypePtr type_, DataTypePtr cast_to_)
        : position(position_)
        , field_id(field_id_)
        , type(std::move(type_))
        , cast_to(std::move(cast_to_))
        , table(TableBuilder().build())
    {
    }

    void update(const ColumnPtr & column, const NullMap * null_map)
    {
        const auto encoded = encode(column);
        PaddedPODArray<UInt64> hashes(encoded->size());
        encoded->computeMurmurHash3Into(0, encoded->size(), static_cast<UInt32>(datasketches::DEFAULT_SEED), hashes.data());
        /// Empty strings are skipped, as the Java `UpdateSketch` skips an empty buffer.
        const auto * strings = typeid_cast<const ColumnString *>(encoded.get());
        for (size_t i = 0; i < hashes.size(); ++i)
        {
            if ((null_map && (*null_map)[i]) || (strings && strings->getDataAt(i).empty()))
                continue;
            table.is_empty_ = false;
            /// The theta hash is the first half shifted right by one bit; zero marks an empty slot of the table.
            const UInt64 hash = hashes[i] >> 1;
            if (hash >= table.theta_ || hash == 0)
                continue;
            const auto [slot, found] = table.find(hash);
            if (!found)
                table.insert(slot, hash);
        }
    }

    /// The serialized compact sketch, as `update_theta_sketch::compact().serialize()` gives it.
    String serialize() const
    {
        Retained retained{.empty = table.is_empty_, .theta = table.theta_, .entries = {}};
        if (!retained.empty)
        {
            retained.entries.reserve(table.num_entries_);
            for (size_t i = 0; i < (1ULL << table.lg_cur_size_); ++i)
                if (table.entries_[i] != 0)
                    retained.entries.push_back(table.entries_[i]);
            ::sort(retained.entries.begin(), retained.entries.end());
        }
        const auto bytes = datasketches::compact_theta_sketch(retained, /*ordered=*/ true).serialize();
        return String(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    }

    size_t position;
    Int32 field_id;

private:
    using Table = datasketches::theta_update_sketch_base<UInt64, datasketches::trivial_extract_key, std::allocator<UInt64>>;

    /// The table of an `update_theta_sketch` with the builder's defaults.
    struct TableBuilder : datasketches::update_theta_sketch::builder
    {
        Table build() const { return Table(starting_lg_size(), lg_k_, rf_, p_, starting_theta(), seed_, allocator_); }
    };

    /// The retained hashes of the table, with the accessors the converting constructor of `compact_theta_sketch` reads.
    struct Retained
    {
        bool empty;
        UInt64 theta;
        std::vector<UInt64> entries;

        bool is_empty() const { return empty; }
        bool is_ordered() const { return true; }
        UInt16 get_seed_hash() const { return compute_seed_hash(datasketches::DEFAULT_SEED); }
        UInt64 get_theta64() const { return empty ? datasketches::theta_constants::MAX_THETA : theta; }
        std::allocator<UInt64> get_allocator() const { return {}; }
        size_t get_num_retained() const { return entries.size(); }
        auto begin() const { return entries.begin(); }
        auto end() const { return entries.end(); }
    };

    ColumnPtr encode(const ColumnPtr & column) const
    {
        if (cast_to)
            return castColumn({column, type, ""}, cast_to);
        switch (type->getTypeId())
        {
            case TypeIndex::Decimal32: return encodeDecimals<Decimal32>(*column);
            case TypeIndex::Decimal64: return encodeDecimals<Decimal64>(*column);
            case TypeIndex::Decimal128: return encodeDecimals<Decimal128>(*column);
            case TypeIndex::Decimal256: return encodeDecimals<Decimal256>(*column);
            default: return column;
        }
    }

    /// The bytes Spark hashes for a decimal (Iceberg's single-value serialization, Java's
    /// `BigDecimal.unscaledValue().toByteArray()`): the unscaled value as two's-complement big-endian in the minimum
    /// number of bytes. `getDataAt` gives it little-endian at full width, so no `IColumn` hash covers it.
    template <typename Decimal>
    static ColumnPtr encodeDecimals(const IColumn & column)
    {
        using Native = typename Decimal::NativeType;
        const auto & data = assert_cast<const ColumnDecimal<Decimal> &>(column).getData();
        auto result = ColumnString::create();
        result->reserve(data.size());
        UInt8 bytes[sizeof(Native)];
        for (const auto & value : data)
        {
            Native big_endian = value.value;
            transformEndianness<std::endian::big>(big_endian);
            memcpy(bytes, &big_endian, sizeof(Native));
            const size_t first = minimalTwosComplementOffset(bytes, sizeof(Native));
            result->insertData(reinterpret_cast<const char *>(bytes + first), sizeof(Native) - first);
        }
        return result;
    }

    DataTypePtr type;
    DataTypePtr cast_to;
    Table table;
};

IcebergColumnSketches::IcebergColumnSketches(const Block & header, const Poco::JSON::Array::Ptr & schema_fields)
    : runner((getParquetEncoderThreadPool().initializeWithDefaultSettingsIfNotInitialized(), getParquetEncoderThreadPool().get()), ThreadName::PARQUET_ENCODER)
{
    for (size_t position = 0; position < header.columns(); ++position)
    {
        const auto & column = header.getByPosition(position);
        auto type = removeNullable(column.type);
        auto cast_to = ColumnSketch::getCastType(type);
        if (!cast_to)
            continue;
        for (unsigned i = 0; i < schema_fields->size(); ++i)
        {
            const auto field = schema_fields->getObject(i);
            if (field->getValue<String>(Iceberg::f_name) == column.name)
            {
                sketches.emplace_back(position, field->getValue<Int32>(Iceberg::f_id), std::move(type), std::move(*cast_to));
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
        for (auto & column_sketch : sketches)
        {
            runner.enqueueAndKeepTrack([&column_sketch, column = columns[column_sketch.position]]
            {
                /// Owns the unwrapped column, which `null_map` points into.
                const ColumnPtr full_column = column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
                ColumnPtr nested = full_column;
                const NullMap * null_map = nullptr;
                if (const auto * nullable = typeid_cast<const ColumnNullable *>(full_column.get()))
                {
                    null_map = &nullable->getNullMapData();
                    nested = nullable->getNestedColumnPtr();
                }
                column_sketch.update(nested, null_map);
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
        result.emplace(column.field_id, column.serialize());
    return result;
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
            sketches = std::move(parent.sketches);
        else
        {
            const auto serialized_sketches = column_sketches->finish();
            if (!serialized_sketches)
                return {};
            for (const auto & [field_id, bytes] : *serialized_sketches)
            {
                auto sketch = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
                auto parent_sketch = parent.sketches.find(field_id);
                if (parent_sketch != parent.sketches.end())
                {
                    datasketches::theta_union sketch_union = datasketches::theta_union::builder().build();
                    sketch_union.update(parent_sketch->second);
                    sketch_union.update(sketch);
                    sketches.emplace(field_id, sketch_union.get_result());
                }
                else if (!parent.has_rows)
                    sketches.emplace(field_id, std::move(sketch));
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

#else

class IcebergColumnSketches::ColumnSketch
{
};

IcebergColumnSketches::IcebergColumnSketches(const Block &, const Poco::JSON::Array::Ptr &)
    : runner(getParquetEncoderThreadPool().get(), ThreadName::PARQUET_ENCODER)
{
}

IcebergColumnSketches::~IcebergColumnSketches() = default;

void IcebergColumnSketches::update(const Columns &)
{
}

std::optional<std::map<Int32, String>> IcebergColumnSketches::finish()
{
    return {};
}

std::optional<String> writeIcebergTableStatisticsNdv(
    IcebergColumnSketches *, bool, Poco::JSON::Object::Ptr, const Poco::JSON::Object::Ptr &, FileNamesGenerator &,
    const Iceberg::IcebergPathResolver &, const ObjectStoragePtr &, const ContextPtr &)
{
    return {};
}

#endif

bool shouldWriteIcebergTableStatisticsNdv(const ContextPtr & context, const std::shared_ptr<DataLake::ICatalog> & catalog)
{
    /// TODO: send the statistics file to a transactional catalog as a `set-statistics` update.
    return USE_DATASKETCHES && context->getSettingsRef()[Setting::iceberg_write_table_statistics_ndv]
        && !(catalog && catalog->isTransactional());
}

}

#endif
