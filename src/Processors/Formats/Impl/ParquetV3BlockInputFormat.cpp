#include <memory>
#include <Common/CurrentThread.h>
#include <optional>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

#if USE_PARQUET

#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Core/ProtocolDefines.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/SharedThreadPools.h>
#include <IO/VarInt.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <parquet/file_reader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FILE_CHANGED_WHILE_READING;
}

static Parquet::ReadOptions convertReadOptions(const FormatSettings & format_settings)
{
    Parquet::ReadOptions options;
    options.format = format_settings;

    options.schema_inference_force_nullable = format_settings.schema_inference_make_columns_nullable == 1;
    options.schema_inference_force_not_nullable = format_settings.schema_inference_make_columns_nullable == 0;

    return options;
}

/// Verify the file still has the same number of row groups as when the bucket (row-group)
/// assignment was computed. The assignment is an invariant: the ids - and their count - were
/// derived from a footer read at planning time. If the file diverged - e.g. an object overwritten
/// between the footer read and the per-bucket read on the object-storage path, which (unlike the
/// local `StorageFile` path) has no file-version guard - the assignment no longer maps to the file.
/// A shrunk file is caught by the per-row-group out-of-range checks, but a file that *grew* keeps
/// every old id in range while leaving the new row groups assigned to no bucket, which would
/// silently undercount. Comparing the total row-group count fails close in both directions.
/// `file_num_row_groups == 0` means the count is unknown (e.g. an older serialized bucket) and
/// skips the check.
static void checkFileMatchesBucketAssignment(size_t file_num_row_groups, size_t actual_num_row_groups)
{
    if (file_num_row_groups != 0 && actual_num_row_groups != file_num_row_groups)
        throw Exception(
            ErrorCodes::FILE_CHANGED_WHILE_READING,
            "The Parquet file has {} row groups, but the parallel single-file bucket assignment was computed for a file "
            "with {} row groups. The file was likely modified concurrently while a parallel single-file read was in progress",
            actual_num_row_groups, file_num_row_groups);
}

ParquetV3BlockInputFormat::ParquetV3BlockInputFormat(
    ReadBuffer & buf,
    SharedHeader header_,
    const FormatSettings & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    size_t min_bytes_for_seek,
    ParquetMetadataCachePtr metadata_cache_,
    const std::optional<RelativePathWithMetadata> & object_with_metadata_)
    : IInputFormat(header_, &buf)
    , format_settings(format_settings_)
    , read_options(convertReadOptions(format_settings))
    , parser_shared_resources(parser_shared_resources_)
    , format_filter_info(format_filter_info_)
    , metadata_cache(metadata_cache_)
    , object_with_metadata(object_with_metadata_)
{
    read_options.min_bytes_for_seek = min_bytes_for_seek;
    read_options.bytes_per_read_task = min_bytes_for_seek * 4;

    if (!format_filter_info)
        format_filter_info = std::make_shared<FormatFilterInfo>();
}

void ParquetV3BlockInputFormat::initializeIfNeeded()
{
    if (!reader)
    {
        format_filter_info->initKeyConditionOnce(getPort().getHeader());
        parser_shared_resources->initOnce([&]
            {
                if (format_settings.parquet.enable_row_group_prefetch && parser_shared_resources->max_io_threads > 0)
                    parser_shared_resources->io_runner.initThreadPool(
                        getFormatParsingThreadPool().get(), parser_shared_resources->max_io_threads, ThreadName::PARQUET_PREFETCH, CurrentThread::getGroup());

                /// Unfortunately max_parsing_threads setting doesn't have a value for
                /// "do parsing in the same thread as the rest of query processing
                /// (inside IInputFormat::read()), with no thread pool". But such mode seems
                /// useful, at least for testing performance. So we use max_parsing_threads = 1
                /// as a signal to disable thread pool altogether, sacrificing the ability to
                /// use thread pool with 1 thread. We could subtract 1 instead, but then
                /// by default the thread pool would use `num_cores - 1` threads, also bad.
                if (parser_shared_resources->max_parsing_threads <= 1)
                    parser_shared_resources->parsing_runner.initManual();
                else
                    parser_shared_resources->parsing_runner.initThreadPool(
                        getFormatParsingThreadPool().get(), parser_shared_resources->max_parsing_threads, ThreadName::PARQUET_DECODER, CurrentThread::getGroup());

                auto ext = std::make_shared<Parquet::SharedResourcesExt>();

                ext->total_memory_low_watermark = format_settings.parquet.memory_low_watermark;
                ext->total_memory_high_watermark = format_settings.parquet.memory_high_watermark;
                parser_shared_resources->opaque = ext;
            });

        {
            std::lock_guard lock(reader_mutex);
            reader.emplace();
            reader->reader.prefetcher.init(in, read_options, parser_shared_resources);
            reader->reader.file_metadata = getFileMetadata(reader->reader.prefetcher);
            if (buckets_to_read)
                checkFileMatchesBucketAssignment(buckets_to_read->file_num_row_groups, reader->reader.file_metadata.row_groups.size());
            reader->reader.init(read_options, getPort().getHeader(), format_filter_info);
            reader->init(parser_shared_resources, buckets_to_read ? std::optional(buckets_to_read->row_group_ids) : std::nullopt);
        }
    }
}

parquet::format::FileMetaData ParquetV3BlockInputFormat::getFileMetadata(Parquet::Prefetcher & prefetcher) const
{
    if (metadata_cache && object_with_metadata.has_value() && object_with_metadata->metadata.has_value())
    {
        String file_name = object_with_metadata->getPath();
        String etag = object_with_metadata->metadata->etag;
        ParquetMetadataCacheKey cache_key = ParquetMetadataCache::createKey(file_name, etag);
        return metadata_cache->getOrSetMetadata(
            cache_key, [&]() { return Parquet::Reader::readFileMetaData(prefetcher); });
    }
    else
    {
        return Parquet::Reader::readFileMetaData(prefetcher);
    }
}

Chunk ParquetV3BlockInputFormat::read()
{
    if (need_only_count)
    {
        if (reported_count)
            return {};

        /// Don't init Reader and ReadManager if we only need file metadata.
        Parquet::Prefetcher temp_prefetcher;
        temp_prefetcher.init(in, read_options, parser_shared_resources);
        parquet::format::FileMetaData file_metadata = getFileMetadata(temp_prefetcher);

        size_t num_rows = 0;
        if (buckets_to_read)
        {
            /// Only count rows in the assigned row groups. Otherwise multiple sources
            /// reading buckets of the same file would each report the file's total.
            ///
            /// The bucket (row-group) assignment is an invariant: every id in
            /// `row_group_ids` was computed from a footer read at planning time and must
            /// exist in the metadata read here. An out-of-range id means the underlying
            /// file diverged from the one the split was computed on (e.g. an object was
            /// overwritten between the footer read and this count on the object-storage
            /// path, which - unlike the local `StorageFile` path - has no file-version
            /// guard). Fail close rather than silently dropping a row group and returning
            /// an undercount. The out-of-range check below catches a shrunk file; the
            /// total-count check here also catches a file that grew (every old id still in
            /// range, but new row groups assigned to no bucket).
            checkFileMatchesBucketAssignment(buckets_to_read->file_num_row_groups, file_metadata.row_groups.size());
            for (size_t rg : buckets_to_read->row_group_ids)
            {
                if (rg >= file_metadata.row_groups.size())
                    throw Exception(
                        ErrorCodes::FILE_CHANGED_WHILE_READING,
                        "Row group {} from the bucket assignment is out of range: the file has only {} row groups. "
                        "The file was likely modified concurrently while a parallel single-file read was in progress",
                        rg, file_metadata.row_groups.size());
                num_rows += size_t(file_metadata.row_groups[rg].num_rows);
            }
        }
        else
        {
            num_rows = size_t(file_metadata.num_rows);
        }

        auto chunk = getChunkForCount(num_rows);
        chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(0));

        reported_count = true;
        return chunk;
    }

    initializeIfNeeded();
    auto res = reader->read();
    previous_block_missing_values = res.block_missing_values;
    previous_approx_bytes_read_for_chunk = res.virtual_bytes_read;
    return std::move(res.chunk);
}

std::optional<std::pair<std::vector<size_t>, size_t>> ParquetV3BlockInputFormat::getMatchedBuckets() const
{
    if (!reader)
        return std::nullopt;
    std::vector<size_t> matched;
    for (const auto & row_group : reader->reader.row_groups)
    {
        if (!row_group.need_to_process)
            continue;

        bool produced_rows = false;
        for (const auto & subgroup : row_group.subgroups)
        {
            if (subgroup.filter.rows_pass > 0)
            {
                produced_rows = true;
                break;
            }
        }

        if (produced_rows)
            matched.push_back(row_group.row_group_idx);
    }
    return std::make_pair(std::move(matched), reader->reader.file_metadata.row_groups.size());
}

void ParquetV3BlockInputFormat::setBucketsToRead(const FileBucketInfoPtr & buckets_to_read_)
{
    if (reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reader already initialized");
    buckets_to_read = std::static_pointer_cast<ParquetFileBucketInfo>(buckets_to_read_);
}

const BlockMissingValues * ParquetV3BlockInputFormat::getMissingValues() const
{
    return &previous_block_missing_values;
}

void ParquetV3BlockInputFormat::onCancel() noexcept
{
    std::lock_guard lock(reader_mutex);
    if (reader)
        reader->cancel();
}

void ParquetV3BlockInputFormat::resetParser()
{
    {
        std::lock_guard lock(reader_mutex);
        reader.reset();
    }
    previous_block_missing_values.clear();
    IInputFormat::resetParser();
}

NativeParquetSchemaReader::NativeParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_)
    , read_options(convertReadOptions(format_settings_))
{
}

void NativeParquetSchemaReader::initializeIfNeeded()
{
    if (initialized)
        return;
    Parquet::Prefetcher prefetcher;
    prefetcher.init(&in, read_options, /*parser_shared_resources_=*/ nullptr);
    file_metadata = Parquet::Reader::readFileMetaData(prefetcher);
    initialized = true;
}

NamesAndTypesList NativeParquetSchemaReader::readSchema()
{
    initializeIfNeeded();
    Parquet::SchemaConverter schemer(file_metadata, read_options, /*sample_block*/ nullptr);
    return schemer.inferSchema();
}

std::optional<size_t> NativeParquetSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return size_t(file_metadata.num_rows);
}

void ParquetFileBucketInfo::serialize(WriteBuffer & buffer, size_t protocol_version)
{
    writeVarUInt(row_group_ids.size(), buffer);
    for (auto chunk : row_group_ids)
        writeVarUInt(chunk, buffer);
    /// `file_num_row_groups` was added later, so it is only present from this protocol version on.
    /// Writing it unconditionally would misalign the stream when talking to an older peer that does
    /// not expect it (and would leave the field unread, breaking the following payload).
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT)
        writeVarUInt(file_num_row_groups, buffer);
}

void ParquetFileBucketInfo::deserialize(ReadBuffer & buffer, size_t protocol_version)
{
    size_t size_chunks = 0;
    readVarUInt(size_chunks, buffer);
    row_group_ids = std::vector<size_t>{};
    row_group_ids.resize(size_chunks);
    size_t bucket = 0;
    for (size_t i = 0; i < size_chunks; ++i)
    {
        readVarUInt(bucket, buffer);
        row_group_ids[i] = bucket;
    }
    /// An older peer does not send `file_num_row_groups`; leave it 0 ("unknown"), which disables the
    /// row-group-count check on the read path. See the comment on the field.
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT)
        readVarUInt(file_num_row_groups, buffer);
    else
        file_num_row_groups = 0;
}

String ParquetFileBucketInfo::getIdentifier() const
{
    String result;
    for (auto chunk : row_group_ids)
        result += "_" + std::to_string(chunk);
    return result;
}

ParquetFileBucketInfo::ParquetFileBucketInfo(const std::vector<size_t> & row_group_ids_, size_t file_num_row_groups_)
    : row_group_ids(row_group_ids_)
    , file_num_row_groups(file_num_row_groups_)
{
}

std::shared_ptr<FileBucketInfo> ParquetFileBucketInfo::filterByMatchingRowGroups(const std::vector<size_t> & matching_row_groups) const
{
    if (matching_row_groups.empty())
        return nullptr;
    if (row_group_ids.empty())
        return std::make_shared<ParquetFileBucketInfo>(matching_row_groups, file_num_row_groups);
    std::unordered_set<size_t> matching_set(matching_row_groups.begin(), matching_row_groups.end());
    std::vector<size_t> filtered;
    for (size_t rg : row_group_ids)
        if (matching_set.contains(rg))
            filtered.push_back(rg);
    if (filtered.empty())
        return nullptr;
    return std::make_shared<ParquetFileBucketInfo>(std::move(filtered), file_num_row_groups);
}

void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances);
void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances)
{
    instances.emplace("Parquet", std::make_shared<ParquetFileBucketInfo>());
}

std::vector<FileBucketInfoPtr> ParquetBucketSplitter::splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_)
{
    std::atomic<int> is_stopped = false;
    auto arrow_file = asArrowFile(buf, format_settings_, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true, nullptr);
    auto metadata = parquet::ReadMetaData(arrow_file);
    std::vector<size_t> bucket_sizes;
    for (int i = 0; i < metadata->num_row_groups(); ++i)
        bucket_sizes.push_back(metadata->RowGroup(i)->total_byte_size());

    std::vector<std::vector<size_t>> buckets;
    size_t current_weight = 0;
    for (size_t i = 0; i < bucket_sizes.size(); ++i)
    {
        if (current_weight + bucket_sizes[i] <= bucket_size)
        {
            if (buckets.empty())
                buckets.emplace_back();
            buckets.back().push_back(i);
            current_weight += bucket_sizes[i];
        }
        else
        {
            current_weight = 0;
            buckets.push_back({});
            buckets.back().push_back(i);
            current_weight += bucket_sizes[i];
        }
    }

    const size_t file_num_row_groups = size_t(metadata->num_row_groups());
    std::vector<FileBucketInfoPtr> result;
    for (const auto & bucket : buckets)
    {
        result.push_back(std::make_shared<ParquetFileBucketInfo>(bucket, file_num_row_groups));
    }
    return result;
}

namespace
{

/// Computes the bucket layout for one Parquet file from already-parsed metadata.
/// No I/O — the caller is responsible for getting the `FileMetaData`. Kept in
/// one place so the splitter (Arrow-style `ReadBuffer` API) and the cache-aware
/// helper share the same row-group-distribution policy.
///
/// Distributes row groups across at most `target_count` contiguous chunks. Each
/// chunk becomes a single `ParquetFileBucketInfo` containing several row groups,
/// so the caller gets one source per chunk and no row group is dropped.
///
/// We also require each chunk to cover at least `min_row_groups_per_chunk` row
/// groups: parallelising a file with very few row groups across all available
/// threads multiplies the per-bucket metadata-parse / prefetcher-setup overhead
/// without giving each source enough work to amortise it. For "short" queries
/// over a smallish single Parquet file this can be a >2x slowdown vs reading the
/// file with a single source (see `tests/performance/clickbench_parquet_short.xml`).
/// Large files (many row groups) still get max parallelism.
///
/// The floor is tuned empirically against `clickbench_parquet_short` on the
/// synthetic 20-row-group test file: splitting that file into 2 buckets cost
/// ~1-3 ms of per-bucket setup, which is 18-37 % of the single-source runtime
/// for these queries. A floor of 16 keeps that 20-row-group file as a single
/// source, while a real `hits.parquet` (hundreds of row groups) still gets
/// fan-out up to `max_threads`.
std::vector<FileBucketInfoPtr> computeBucketsByCount(size_t target_count, size_t num_row_groups)
{
    if (target_count == 0 || num_row_groups == 0)
        return {};

    static constexpr size_t min_row_groups_per_chunk = 16;
    const size_t max_chunks_by_row_groups = std::max<size_t>(1, num_row_groups / min_row_groups_per_chunk);
    const size_t num_chunks = std::min({target_count, num_row_groups, max_chunks_by_row_groups});
    std::vector<FileBucketInfoPtr> result;
    result.reserve(num_chunks);
    for (size_t g = 0; g < num_chunks; ++g)
    {
        size_t lo = g * num_row_groups / num_chunks;
        size_t hi = (g + 1) * num_row_groups / num_chunks;
        std::vector<size_t> ids;
        ids.reserve(hi - lo);
        for (size_t k = lo; k < hi; ++k)
            ids.push_back(k);
        result.push_back(std::make_shared<ParquetFileBucketInfo>(ids, num_row_groups));
    }
    return result;
}

/// Reads the Parquet footer via the native reader (the same path `ParquetV3BlockInputFormat`
/// takes). Returned metadata can be stored directly in `ParquetMetadataCache`.
parquet::format::FileMetaData parseFileMetadataNative(ReadBuffer & buf, const FormatSettings & format_settings)
{
    Parquet::Prefetcher prefetcher;
    auto read_options = convertReadOptions(format_settings);
    prefetcher.init(&buf, read_options, /*parser_shared_resources_=*/ nullptr);
    return Parquet::Reader::readFileMetaData(prefetcher);
}

}

std::vector<FileBucketInfoPtr> ParquetBucketSplitter::splitToBucketsByCount(size_t target_count, ReadBuffer & buf, const FormatSettings & format_settings_)
{
    auto file_metadata = parseFileMetadataNative(buf, format_settings_);
    return computeBucketsByCount(target_count, file_metadata.row_groups.size());
}

std::vector<FileBucketInfoPtr> splitParquetFileWithCache(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    ReadBuffer & buf,
    const FormatSettings & format_settings,
    ParquetMetadataCachePtr metadata_cache)
{
    size_t num_row_groups = 0;
    if (metadata_cache && !file_path.empty() && !cache_etag.empty())
    {
        auto key = ParquetMetadataCache::createKey(file_path, cache_etag);
        auto file_metadata = metadata_cache->getOrSetMetadata(
            key, [&] { return parseFileMetadataNative(buf, format_settings); });
        num_row_groups = file_metadata.row_groups.size();
    }
    else
    {
        auto file_metadata = parseFileMetadataNative(buf, format_settings);
        num_row_groups = file_metadata.row_groups.size();
    }
    return computeBucketsByCount(target_count, num_row_groups);
}

std::vector<FileBucketInfoPtr> trySplitParquetFileFromCacheOnly(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    const ParquetMetadataCachePtr & metadata_cache)
{
    if (!metadata_cache || file_path.empty() || cache_etag.empty())
        return {};
    auto key = ParquetMetadataCache::createKey(file_path, cache_etag);
    auto cached = metadata_cache->get(key);
    if (!cached)
        return {};
    return computeBucketsByCount(target_count, cached->metadata.row_groups.size());
}

void registerInputFormatParquet(FormatFactory & factory);
void registerInputFormatParquet(FormatFactory & factory)
{
    factory.registerFileBucketInfo(
        "Parquet",
        []
        {
            return std::make_shared<ParquetFileBucketInfo>();
        }
    );
    factory.registerRandomAccessInputFormatWithMetadata(
        "Parquet",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           FormatParserSharedResourcesPtr parser_shared_resources,
           FormatFilterInfoPtr format_filter_info,
           const std::optional<RelativePathWithMetadata> & object_with_metadata,
           const ContextPtr & context) -> InputFormatPtr
        {
            size_t min_bytes_for_seek
                = is_remote_fs ? read_settings.remote_fs_settings.min_bytes_for_seek : settings.parquet.local_read_min_bytes_for_seek;
            /// `tryGet` keeps the metadata-aware creator usable from contexts that don't
            /// initialise the cache (e.g. the client side of `INSERT ... FROM INFILE`).
            /// In such contexts we just don't memoise the footer — the format itself works
            /// correctly with a null cache.
            ParquetMetadataCachePtr metadata_cache = context->tryGetParquetMetadataCache();
            return std::make_shared<ParquetV3BlockInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                settings,
                std::move(parser_shared_resources),
                std::move(format_filter_info),
                min_bytes_for_seek,
                metadata_cache,
                object_with_metadata
            );
        });
    factory.registerRandomAccessInputFormat(
        "Parquet",
        [](ReadBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        const ReadSettings & read_settings,
        bool is_remote_fs,
        FormatParserSharedResourcesPtr parser_shared_resources,
        FormatFilterInfoPtr format_filter_info) -> InputFormatPtr
    {
        size_t min_bytes_for_seek
            = is_remote_fs ? read_settings.remote_fs_settings.min_bytes_for_seek : settings.parquet.local_read_min_bytes_for_seek;
        return std::make_shared<ParquetV3BlockInputFormat>(
            buf,
            std::make_shared<const Block>(sample),
            settings,
            std::move(parser_shared_resources),
            std::move(format_filter_info),
            min_bytes_for_seek,
            nullptr,
            std::nullopt
        );
    });
    factory.markFormatSupportsSubsetOfColumns("Parquet");
    factory.registerPrewhereSupportChecker("Parquet", [](const FormatSettings &)
    {
        return true;
    });

    factory.setDocumentation("Parquet", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format widespread in the Hadoop ecosystem. ClickHouse supports read and write operations for this format.

## Data types matching {#data-types-matching-parquet}

The table below shows how Parquet data types match ClickHouse [data types](/sql-reference/data-types/index.md).

| Parquet type (logical, converted, or physical) | ClickHouse data type |
|------------------------------------------------|----------------------|
| `BOOLEAN` | [Bool](/sql-reference/data-types/boolean.md) |
| `UINT_8` | [UInt8](/sql-reference/data-types/int-uint.md) |
| `INT_8` | [Int8](/sql-reference/data-types/int-uint.md) |
| `UINT_16` | [UInt16](/sql-reference/data-types/int-uint.md) |
| `INT_16` | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) |
| `UINT_32` | [UInt32](/sql-reference/data-types/int-uint.md) |
| `INT_32` | [Int32](/sql-reference/data-types/int-uint.md) |
| `UINT_64` | [UInt64](/sql-reference/data-types/int-uint.md) |
| `INT_64` | [Int64](/sql-reference/data-types/int-uint.md) |
| `DATE` | [Date32](/sql-reference/data-types/date.md) |
| `TIMESTAMP`, `TIME` | [DateTime64](/sql-reference/data-types/datetime64.md) |
| `FLOAT` | [Float32](/sql-reference/data-types/float.md) |
| `DOUBLE` | [Float64](/sql-reference/data-types/float.md) |
| `INT96` | [DateTime64(9, 'UTC')](/sql-reference/data-types/datetime64.md) |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON` | [String](/sql-reference/data-types/string.md) |
| `JSON` | [JSON](/sql-reference/data-types/newjson.md) |
| `FIXED_LEN_BYTE_ARRAY` | [FixedString](/sql-reference/data-types/fixedstring.md) |
| `DECIMAL` | [Decimal](/sql-reference/data-types/decimal.md) |
| `LIST` | [Array](/sql-reference/data-types/array.md) |
| `MAP` | [Map](/sql-reference/data-types/map.md) |
| struct | [Tuple](/sql-reference/data-types/tuple.md) |
| `FLOAT16` | [Float32](/sql-reference/data-types/float.md) |
| `UUID` | [FixedString(16)](/sql-reference/data-types/fixedstring.md) |
| `INTERVAL` | [FixedString(12)](/sql-reference/data-types/fixedstring.md) |

When writing Parquet file, data types that don't have a matching Parquet type are converted to the nearest available type:

| ClickHouse data type | Parquet type |
|----------------------|--------------|
| [IPv4](/sql-reference/data-types/ipv4.md) | `UINT_32` |
| [IPv6](/sql-reference/data-types/ipv6.md) | `FIXED_LEN_BYTE_ARRAY` (16 bytes) |
| [Date](/sql-reference/data-types/date.md) (16 bits) | `DATE` (32 bits) |
| [DateTime](/sql-reference/data-types/datetime.md) (32 bits, seconds) | `TIMESTAMP` (64 bits, milliseconds) |
| [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32 bytes, little-endian) |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to that data type which is set for the ClickHouse table column. E.g. a `UINT_32` Parquet column can be read into an [IPv4](/sql-reference/data-types/ipv4.md) ClickHouse column.

For some Parquet types there's no closely matching ClickHouse type. We read them as follows:
* `TIME` (time of day) is read as a timestamp. E.g. `10:23:13.000` becomes `1970-01-01 10:23:13.000`.
* `TIMESTAMP`/`TIME` with `isAdjustedToUTC=false` is a local wall-clock time (year, month, day, hour, minute, second and subsecond fields in a local timezone, regardless of what specific time zone is considered local), same as SQL `TIMESTAMP WITHOUT TIME ZONE`. ClickHouse reads it as if it were a UTC timestamp instead. E.g. `2025-09-29 18:42:13.000` (representing a reading of a local wall clock) becomes `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')` representing a point in time). If converted to String, it shows the correct year, month, day, hour, minute, second and subsecond, which can then be interpreted as being in some local timezone instead of UTC. Counterintuitively, changing the type from `DateTime64(3, 'UTC')` to `DateTime64(3)` would not help as both types represent a point in time rather than a clock reading, but `DateTime64(3)` would incorrectly be formatted using local timezone.
* `INTERVAL` is currently read as `FixedString(12)` with raw binary representation of the time interval, as encoded in Parquet file.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using a Parquet file with the following data, named as `football.parquet`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

### Reading data {#reading-data}

Read data using the `Parquet` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquet is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output Parquet files.
:::

To exchange data with Hadoop, you can use the [`HDFS table engine`](/engines/table-engines/integrations/hdfs.md).

## Format settings {#format-settings}

| Setting                                                                        | Description                                                                                                                                                                                                                       | Default     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_parquet_case_insensitive_column_matching`                        | Ignore case when matching Parquet columns with CH columns.                                                                                                                                                                          | `0`         |
| `input_format_parquet_preserve_order`                                          | Avoid reordering rows when reading from Parquet files. Usually makes it much slower.                                                                                                                                              | `0`         |
| `input_format_parquet_filter_push_down`                                        | When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.                                                                                          | `1`         |
| `input_format_parquet_bloom_filter_push_down`                                  | When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.                                                                                                          | `0`         |
| `input_format_parquet_allow_missing_columns`                                   | Allow missing columns while reading Parquet input formats                                                                                                                                                                          | `1`         |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format                                                                                                                          | `8192`      |
| `input_format_parquet_enable_row_group_prefetch`                               | Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.                                                                                                                          | `1`         |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Parquet                                                                                                                                                      | `0`         |
| `input_format_parquet_max_block_size`                                          | Max block size for parquet reader.                                                                                                                                                                                                | `65409`     |
| `input_format_parquet_prefer_block_bytes`                                      | Average block bytes output by parquet reader                                                                                                                                                                                      | `16744704`  |
| `input_format_parquet_enable_json_parsing`                                      | When reading Parquet files, parse JSON columns as ClickHouse JSON Column.                                                                                                                                                                                      | `1`  |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LEN_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `input_format_parquet_import_nested`                                           | Obsolete setting, does nothing.                                                                                                                                                                                                   | `0`         |
| `input_format_parquet_local_time_as_utc` | true | Determines the data type used by schema inference for Parquet timestamps with isAdjustedToUTC=false. If true: DateTime64(..., 'UTC'), if false: DateTime64(...). Neither behavior is fully correct as ClickHouse doesn't have a data type for local wall-clock time. Counterintuitively, 'true' is probably the less incorrect option, because formatting the 'UTC' timestamp as String will produce representation of the correct local time. |
)DOCS_MD"});
}

void registerParquetSchemaReader(FormatFactory & factory);
void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSplitter("Parquet", []
        {
            return std::make_shared<ParquetBucketSplitter>();
        });
    factory.registerSchemaReader(
        "Parquet", [](ReadBuffer & buf, const FormatSettings & settings) -> SchemaReaderPtr
        {
            return std::make_shared<NativeParquetSchemaReader>(buf, settings);
        }
    );

    factory.registerAdditionalInfoForSchemaCacheGetter(
        "Parquet",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "schema_inference_make_columns_nullable={};enable_json_parsing={}",
                settings.schema_inference_make_columns_nullable,
                settings.parquet.enable_json_parsing);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatParquet(FormatFactory &);
void registerParquetSchemaReader(FormatFactory &);
void registerInputFormatParquet(FormatFactory &)
{
}

void registerParquetSchemaReader(FormatFactory &) {}
}

#endif
