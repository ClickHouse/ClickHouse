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
#include <Common/SipHash.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>

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

    options.dictionary_filter_limit_bytes = format_settings.parquet.dictionary_filter_push_down;

    return options;
}

/// Verify the file still has the same number of row groups as when the bucket (row-group)
/// assignment was computed. The assignment is an invariant: the ids - and their count - were
/// derived from a footer read at planning time. If the file diverged - e.g. an object overwritten
/// between the footer read and the per-bucket read - the assignment no longer maps to the file.
/// A shrunk file is caught by the per-row-group out-of-range checks, but a file that *grew* keeps
/// every old id in range while leaving the new row groups assigned to no bucket, which would
/// silently undercount. Comparing the total row-group count fails close in both directions.
/// `file_num_row_groups == 0` means the count is unknown (e.g. an older serialized bucket) and
/// skips the check.
static void checkFileMatchesBucketAssignment(const ParquetFileBucketInfo & bucket, const parquet::format::FileMetaData & file_metadata)
{
    if (bucket.file_num_row_groups != 0 && file_metadata.row_groups.size() != bucket.file_num_row_groups)
        throw Exception(
            ErrorCodes::FILE_CHANGED_WHILE_READING,
            "The Parquet file has {} row groups, but the parallel single-file bucket assignment was computed for a file "
            "with {} row groups. The file was likely modified concurrently while a parallel single-file read was in progress",
            file_metadata.row_groups.size(), bucket.file_num_row_groups);

    /// A matching row-group count can still hide a rewrite. Locally, an in-place rewrite that keeps the
    /// inode, the byte size and the filesystem timestamp tick is invisible to the file-version token,
    /// so the assignment (or a cached footer it was computed from) may describe a previous generation of
    /// the file; on object storage, a read that is not pinned to the listed etag can return a different
    /// generation than the one the assignment was computed from. Comparing the digest of the footer
    /// actually read here against the one the assignment was computed from fails close in both cases
    /// on any footer-visible difference, including the per-column statistics that change with the
    /// data values (see `ParquetFileBucketInfo::footer_digest` for the exactness contract).
    if (bucket.footer_digest != 0 && computeParquetFooterDigest(file_metadata) != bucket.footer_digest)
        throw Exception(
            ErrorCodes::FILE_CHANGED_WHILE_READING,
            "The Parquet file's footer differs from the one the parallel single-file bucket assignment was computed from. "
            "The file was likely modified concurrently while a parallel single-file read was in progress");
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
                checkFileMatchesBucketAssignment(*buckets_to_read, reader->reader.file_metadata);
            reader->reader.init(read_options, getPort().getHeader(), format_filter_info);
            reader->init(
                parser_shared_resources,
                buckets_to_read ? std::optional(buckets_to_read->row_group_ids) : std::nullopt,
                buckets_to_read && buckets_to_read->omitted_row_groups_are_pruned);
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
            checkFileMatchesBucketAssignment(*buckets_to_read, file_metadata);
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
    /// `file_num_row_groups` and `footer_digest` were added later, so they are only present from this
    /// protocol version on. Writing them unconditionally would misalign the stream when talking to an
    /// older peer that does not expect them (and would leave the fields unread, breaking the
    /// following payload).
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT)
    {
        writeVarUInt(file_num_row_groups, buffer);
        writeVarUInt(footer_digest, buffer);
    }
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
    /// An older peer does not send `file_num_row_groups` / `footer_digest`; leave them 0 ("unknown"),
    /// which disables the corresponding checks on the read path. Such a peer never receives a bucket
    /// that has either of them set, because `getMinProtocolVersion` makes the task fail closed
    /// instead of being downgraded. See the comments on the fields.
    if (protocol_version >= DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT)
    {
        readVarUInt(file_num_row_groups, buffer);
        readVarUInt(footer_digest, buffer);
    }
    else
    {
        file_num_row_groups = 0;
        footer_digest = 0;
    }
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

std::shared_ptr<FileBucketInfo> ParquetFileBucketInfo::filterByMatchingRowGroups(
    const std::vector<size_t> & matching_row_groups, size_t caller_file_num_row_groups) const
{
    /// A caller that knows the file's total row-group count (e.g. the object-storage
    /// query-condition-cache read path, where it equals the number of cached marks) passes it here so
    /// the resulting bucket carries the same fail-close `checkFileMatchesBucketAssignment` guard as
    /// splitter- and cluster-derived buckets. 0 means "unknown"; keep whatever this prototype carries.
    const size_t result_file_num_row_groups = caller_file_num_row_groups != 0 ? caller_file_num_row_groups : file_num_row_groups;
    if (matching_row_groups.empty())
        return nullptr;
    if (row_group_ids.empty())
    {
        auto result = std::make_shared<ParquetFileBucketInfo>(matching_row_groups, result_file_num_row_groups);
        result->footer_digest = footer_digest;
        /// The row groups left out here were dropped by the query condition cache, i.e. pruned - no
        /// other reader picks them up. See `FileBucketInfo::omitted_row_groups_are_pruned`.
        result->omitted_row_groups_are_pruned = true;
        return result;
    }
    std::unordered_set<size_t> matching_set(matching_row_groups.begin(), matching_row_groups.end());
    std::vector<size_t> filtered;
    for (size_t rg : row_group_ids)
        if (matching_set.contains(rg))
            filtered.push_back(rg);
    if (filtered.empty())
        return nullptr;
    /// Filtering a real split bucket: the row groups left out are a mix of the other buckets' row
    /// groups and the ones the cache dropped, so this reader stays accountable for its own bucket
    /// only and `omitted_row_groups_are_pruned` stays false. Both current callers apply the cache
    /// filter to a fresh prototype (they are gated on there being no split), so this path only
    /// affects a hypothetical cache-filtered split.
    auto result = std::make_shared<ParquetFileBucketInfo>(std::move(filtered), result_file_num_row_groups);
    result->footer_digest = footer_digest;
    return result;
}

UInt64 ParquetFileBucketInfo::getMinProtocolVersion() const
{
    /// Once the file's row-group count or the footer digest is known, the worker must be able to carry
    /// it so it can run the `checkFileMatchesBucketAssignment` fail-close guard against a concurrent
    /// overwrite. A worker that only understands `file_bucket_info` but not these fields would
    /// deserialize them as 0 and silently disable the guard, so the task must fail closed instead of
    /// being downgraded to such a worker. When both are unknown (0) there is no guard to lose, so the
    /// base version is enough.
    return file_num_row_groups != 0 || footer_digest != 0
        ? DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_PARQUET_FILE_ROW_GROUP_COUNT
        : DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION_WITH_FILE_BUCKETS_INFO;
}

bool ParquetFileBucketInfo::coversWholeFile() const
{
    /// The bucket must hold every row group of the file: for `file_num_row_groups` total row
    /// groups that is exactly the ids `0 .. file_num_row_groups - 1`, which `splitToBuckets` /
    /// `computeBucketsByCount` emit in ascending order. A count of 0 means the total is unknown,
    /// so whole-file coverage cannot be proven.
    if (file_num_row_groups == 0 || row_group_ids.size() != file_num_row_groups)
        return false;
    for (size_t i = 0; i < row_group_ids.size(); ++i)
        if (row_group_ids[i] != i)
            return false;
    return true;
}

void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances);
void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances)
{
    instances.emplace("Parquet", std::make_shared<ParquetFileBucketInfo>());
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
///
/// `apply_row_group_floor == false` disables the floor: it is part of the same
/// heuristic as the byte-based gates, so turning both of those off (which
/// `compatibility` set to a pre-26.8 version does) must restore the old
/// row-group-count-only fan-out for every file, including one with fewer than
/// `min_row_groups_per_chunk` row groups.
std::vector<FileBucketInfoPtr> computeBucketsByCount(size_t target_count, size_t num_row_groups, bool apply_row_group_floor = true)
{
    if (target_count == 0 || num_row_groups == 0)
        return {};

    static constexpr size_t min_row_groups_per_chunk = 16;
    const size_t max_chunks_by_row_groups
        = apply_row_group_floor ? std::max<size_t>(1, num_row_groups / min_row_groups_per_chunk) : num_row_groups;
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

/// Maps every leaf's raw dotted `path_in_schema` to the logical dotted names the native Parquet
/// reader can give it. Mirrors `SchemaConverter`'s naming: an element contributes a name
/// component only outside List / Map wrapper levels, so `a.list.element.x` (an
/// `Array(Tuple(x ...))` element) becomes `a.x` and `a.list.element` (a plain array leaf)
/// becomes `a`. A Map leaf gets more than one logical name because the reader supports two
/// naming modes for the same footer shape: when the column is read as a `Map`, the `key_value`
/// wrapper is dropped and the key / value elements take the `keys` / `values` subcolumn names
/// `DataTypeMap` uses (`m.key_value.value` -> `m.values`); when it is explicitly requested as an
/// `Array(Tuple(...))` (`SchemaContext::MapTupleAsPlainTuple`), the wrapper is dropped but the
/// elements keep their footer names (`m.key_value.value` -> `m.value`). Which mode applies
/// depends on the requested type, unknown here, so every leaf carries all its possible spellings
/// and matching accepts any of them. Returns false on a malformed schema tree; the caller then
/// falls back to raw-path matching.
bool collectLogicalPaths(
    const std::vector<parquet::format::SchemaElement> & schema,
    size_t & idx,
    const String & raw_prefix,
    const std::vector<String> & logical_prefixes,
    bool append_name,
    std::unordered_map<String, std::vector<String>> & out)
{
    if (idx >= schema.size())
        return false;
    const parquet::format::SchemaElement & elem = schema[idx];
    ++idx;

    String raw = raw_prefix.empty() ? elem.name : raw_prefix + "." + elem.name;
    std::vector<String> logical = logical_prefixes;
    if (append_name)
        for (String & prefix : logical)
            prefix = prefix.empty() ? elem.name : prefix + "." + elem.name;

    const size_t num_children = elem.__isset.num_children ? static_cast<size_t>(elem.num_children) : 0;
    if (num_children == 0)
    {
        out.emplace(std::move(raw), std::move(logical));
        return true;
    }

    const bool is_list = elem.converted_type == parquet::format::ConvertedType::LIST || elem.logicalType.__isset.LIST;
    const bool is_map = elem.converted_type == parquet::format::ConvertedType::MAP
        || elem.converted_type == parquet::format::ConvertedType::MAP_KEY_VALUE || elem.logicalType.__isset.MAP;

    if ((is_list || is_map) && num_children == 1 && idx < schema.size()
        && schema[idx].repetition_type == parquet::format::FieldRepetitionType::REPEATED)
    {
        const parquet::format::SchemaElement & rep = schema[idx];
        const size_t rep_children = rep.__isset.num_children ? static_cast<size_t>(rep.num_children) : 0;
        if (is_map && rep_children == 2)
        {
            /// Map: the repeated `key_value` group is a wrapper, dropped from the logical name.
            /// When the column is read as a `Map`, the reader renames the key / value elements
            /// (whatever the footer calls them) to the `keys` / `values` subcolumn names
            /// `DataTypeMap` requires — `SchemaConverter` does this at the output-tuple level —
            /// so a direct map-subcolumn read requests `m.keys` / `m.values`. When the column is
            /// explicitly requested as an `Array(Tuple(...))`, the elements keep their footer
            /// names instead, so the same leaf is addressed as e.g. `m.key` / `m.value`. Both
            /// spellings are collected for each element. The whole-map name `m` is still a
            /// dotted prefix of all of them, so whole-map requests keep matching.
            ++idx;
            String raw_rep = raw + "." + rep.name;
            for (const char * map_name : {"keys", "values"})
            {
                if (idx >= schema.size())
                    return false;
                std::vector<String> child_logical;
                child_logical.reserve(logical.size() * 2);
                for (const String & prefix : logical)
                {
                    String renamed = prefix.empty() ? String(map_name) : prefix + "." + map_name;
                    String plain = prefix.empty() ? schema[idx].name : prefix + "." + schema[idx].name;
                    child_logical.push_back(std::move(renamed));
                    if (plain != child_logical.back())
                        child_logical.push_back(std::move(plain));
                }
                if (!collectLogicalPaths(schema, idx, raw_rep, child_logical, false, out))
                    return false;
            }
            return true;
        }
        if (is_list)
        {
            if (rep_children == 1)
            {
                /// Three-level list: both the repeated wrapper (`list`) and the element under it
                /// contribute no name component.
                ++idx;
                String raw_rep = raw + "." + rep.name;
                return collectLogicalPaths(schema, idx, raw_rep, logical, false, out);
            }
            /// Two-level list (e.g. hudi): the repeated element itself is the wrapper level.
            return collectLogicalPaths(schema, idx, raw, logical, false, out);
        }
        /// A MAP-annotated group without the expected key/value structure: fall through and treat
        /// it as a plain group, like the reader does.
    }

    /// Plain group (tuple): every field contributes its name.
    for (size_t i = 0; i < num_children; ++i)
        if (!collectLogicalPaths(schema, idx, raw, logical, true, out))
            return false;
    return true;
}

/// Whether `requested` contains the logical name itself or any of its dotted prefixes — a
/// requested prefix (the top-level name, or an inner tuple like `t.a`) reads every leaf below it.
bool anyDottedPrefixRequested(const String & logical, const std::unordered_set<String> & requested)
{
    for (size_t pos = logical.find('.'); pos != String::npos; pos = logical.find('.', pos + 1))
        if (requested.contains(logical.substr(0, pos)))
            return true;
    return requested.contains(logical);
}

/// Sum of the compressed sizes of the column chunks the query will actually read,
/// across all row groups. Used to decide whether a single-file split is worth its
/// per-source setup cost. An empty `requested_columns` set means "read everything"
/// (be conservative and let the split proceed). Chunks with no metadata / no path
/// are skipped.
///
/// `requested_columns` holds the logical names the reader understands: the full
/// dotted path (e.g. `t.x`) for a tuple element the reader addresses on its own, or
/// a top-level name for a column read only as a whole (whole Arrays, Maps, Tuples,
/// dynamic subcolumns). Raw footer paths keep List / Map wrapper segments the
/// logical names drop (`a.list.element.x` is addressed as `a.x` when `a` is an
/// `Array(Tuple(...))`), so each chunk's path is normalized through the same naming
/// the reader uses (`collectLogicalPaths`) and matches when any of its logical
/// names (a Map leaf has one per naming mode) or any dotted prefix of one is
/// requested — so `sum(a.x)` counts only the `a.x` leaf while
/// `sum(t)` counts every leaf under `t`. Matching only the top-level name would
/// over-count narrow subcolumn reads and split them anyway, defeating the point of
/// the size gate. If the schema tree cannot be walked, matching conservatively falls
/// back to the raw path and its top-level name.
size_t projectedCompressedBytes(const parquet::format::FileMetaData & md, const std::unordered_set<String> & requested_columns)
{
    std::unordered_map<String, std::vector<String>> logical_paths;
    if (!requested_columns.empty() && !md.schema.empty())
    {
        const size_t root_children
            = md.schema.front().__isset.num_children ? static_cast<size_t>(md.schema.front().num_children) : 0;
        size_t idx = 1;
        bool ok = true;
        for (size_t i = 0; ok && i < root_children; ++i)
            ok = collectLogicalPaths(md.schema, idx, "", {""}, true, logical_paths);
        if (!ok)
            logical_paths.clear();
    }

    size_t total = 0;
    for (const auto & rg : md.row_groups)
    {
        for (const auto & col : rg.columns)
        {
            if (!col.__isset.meta_data)
                continue;
            const auto & path = col.meta_data.path_in_schema;
            if (path.empty())
                continue;
            if (!requested_columns.empty())
            {
                bool matched = requested_columns.contains(path.front());
                if (!matched && path.size() > 1)
                {
                    String leaf_path = path.front();
                    for (size_t i = 1; i < path.size(); ++i)
                    {
                        leaf_path += '.';
                        leaf_path += path[i];
                    }
                    if (auto it = logical_paths.find(leaf_path); it != logical_paths.end())
                    {
                        for (const String & logical : it->second)
                        {
                            matched = anyDottedPrefixRequested(logical, requested_columns);
                            if (matched)
                                break;
                        }
                    }
                    else
                        matched = requested_columns.contains(leaf_path);
                }
                if (!matched)
                    continue;
            }
            if (col.meta_data.total_compressed_size > 0)
                total += static_cast<size_t>(col.meta_data.total_compressed_size);
        }
    }
    return total;
}

/// Like `computeBucketsByCount`, but query-aware: it refuses to split (returns a
/// single full-file chunk) when the query reads fewer than `min_bytes_to_split`
/// compressed bytes — too little to amortize the per-source setup — and otherwise
/// caps the number of chunks so each bucket carries at least `min_bytes_per_bucket`
/// of compressed data. This targets the short-query regression: fanning a
/// light/narrow query (few or small columns) out across many sources multiplies
/// the per-source open/reader-init overhead without any read-parallelism win.
/// `min_bytes_to_split == 0` disables the lower bound; `min_bytes_per_bucket == 0`
/// disables the per-bucket size cap (falling back to a pure row-group-count split).
/// With both at 0 - the values `compatibility` set to a pre-26.8 version restores -
/// the whole size heuristic is off, so `min_row_groups_per_chunk` is not applied
/// either and the fan-out is driven by the row-group count alone, exactly as before
/// the size gate existed. Otherwise the floor applies via the delegated
/// `computeBucketsByCount`.
std::vector<FileBucketInfoPtr> computeBucketsByCountAndBytes(
    size_t target_count, size_t num_row_groups, size_t projected_bytes, size_t min_bytes_to_split, size_t min_bytes_per_bucket)
{
    if (target_count == 0 || num_row_groups == 0)
        return {};
    const bool apply_row_group_floor = min_bytes_to_split > 0 || min_bytes_per_bucket > 0;
    if (min_bytes_to_split > 0 && projected_bytes < min_bytes_to_split)
        return computeBucketsByCount(1, num_row_groups, apply_row_group_floor);
    const size_t max_chunks_by_bytes
        = min_bytes_per_bucket > 0 ? std::max<size_t>(1, projected_bytes / min_bytes_per_bucket) : target_count;
    return computeBucketsByCount(std::min(target_count, max_chunks_by_bytes), num_row_groups, apply_row_group_floor);
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

/// Stamps every bucket of a freshly computed split with the digest of the footer the split was
/// computed from, so the per-bucket read fails close if the file it opens has a different footer
/// (see `ParquetFileBucketInfo::footer_digest`).
void setFooterDigest(std::vector<FileBucketInfoPtr> & buckets, const parquet::format::FileMetaData & file_metadata)
{
    if (buckets.empty())
        return;
    const UInt64 digest = computeParquetFooterDigest(file_metadata);
    for (auto & bucket : buckets)
        if (auto * parquet_bucket = dynamic_cast<ParquetFileBucketInfo *>(bucket.get()))
            parquet_bucket->footer_digest = digest;
}

}

UInt64 computeParquetFooterDigest(const parquet::format::FileMetaData & file_metadata)
{
    /// Hashes the footer's layout - the schema shape, and every row group's and column chunk's row
    /// counts, byte sizes and file offsets - plus its value-bearing fields: per-column statistics
    /// (min/max/null/distinct/NaN counts), key-value metadata and `created_by`. The layout is what a
    /// bucket assignment is computed from, and the statistics change whenever the data values do, so
    /// two generations of a file that differ in any of it produce different digests, while the same
    /// in-memory struct - whether freshly parsed or returned by `ParquetMetadataCache` - always
    /// produces the same one. A rewrite whose footer is identical in all hashed fields (same layout
    /// AND same statistics at the same offsets) is indistinguishable without re-reading the data
    /// pages, so the digest is a fail-close guard against footer-visible generation drift, not a
    /// content hash of the file; an exact byte-level pin additionally requires the read itself to be
    /// pinned to a generation (locally the file-version token bracket, on S3
    /// `s3_validate_etag_on_read`).
    ///
    /// Deliberately hand-rolled instead of re-serializing the thrift struct: `FileMetaData` carries
    /// thrift enums (`SchemaElement::type`, `ColumnMetaData::codec`, `PageEncodingStats::page_type`,
    /// ...) whose in-memory value can be out of range for a malformed or future-writer file, and the
    /// generated `write` loads them as enumerators - undefined behavior that `-fsanitize=enum`
    /// reports, aborting a read that otherwise succeeds. `Reader::columnChunkCanUseDictionaryFilter`
    /// reads such fields through `isValidThriftEnum` for the same reason. Only integer and string
    /// fields are hashed here, so advisory garbage in an enum cannot turn a readable file into a hard
    /// failure. Optional fields contribute a presence flag so a set-to-zero field cannot collide with
    /// an absent one.
    SipHash hash;
    auto update_optional = [&](bool is_set, Int64 value)
    {
        hash.update(is_set);
        if (is_set)
            hash.update(value);
    };
    auto update_optional_string = [&](bool is_set, const std::string & value)
    {
        hash.update(is_set);
        if (is_set)
            hash.update(value);
    };
    auto update_key_value_metadata = [&](bool is_set, const std::vector<parquet::format::KeyValue> & key_value_metadata)
    {
        hash.update(is_set);
        if (!is_set)
            return;
        hash.update(key_value_metadata.size());
        for (const auto & key_value : key_value_metadata)
        {
            hash.update(key_value.key);
            update_optional_string(key_value.__isset.value, key_value.value);
        }
    };

    hash.update(file_metadata.version);
    hash.update(file_metadata.num_rows);
    update_optional_string(file_metadata.__isset.created_by, file_metadata.created_by);
    update_key_value_metadata(file_metadata.__isset.key_value_metadata, file_metadata.key_value_metadata);
    hash.update(file_metadata.schema.size());
    for (const auto & element : file_metadata.schema)
    {
        hash.update(element.name);
        update_optional(element.__isset.num_children, element.num_children);
        update_optional(element.__isset.type_length, element.type_length);
        update_optional(element.__isset.precision, element.precision);
        update_optional(element.__isset.scale, element.scale);
        update_optional(element.__isset.field_id, element.field_id);
    }

    hash.update(file_metadata.row_groups.size());
    for (const auto & row_group : file_metadata.row_groups)
    {
        hash.update(row_group.num_rows);
        hash.update(row_group.total_byte_size);
        update_optional(row_group.__isset.file_offset, row_group.file_offset);
        update_optional(row_group.__isset.total_compressed_size, row_group.total_compressed_size);
        update_optional(row_group.__isset.ordinal, row_group.ordinal);
        hash.update(row_group.columns.size());
        for (const auto & column : row_group.columns)
        {
            hash.update(column.file_offset);
            hash.update(column.__isset.file_path);
            if (column.__isset.file_path)
                hash.update(column.file_path);
            update_optional(column.__isset.offset_index_offset, column.offset_index_offset);
            update_optional(column.__isset.offset_index_length, column.offset_index_length);
            update_optional(column.__isset.column_index_offset, column.column_index_offset);
            update_optional(column.__isset.column_index_length, column.column_index_length);
            hash.update(column.__isset.meta_data);
            if (!column.__isset.meta_data)
                continue;
            const auto & meta = column.meta_data;
            hash.update(meta.num_values);
            hash.update(meta.total_uncompressed_size);
            hash.update(meta.total_compressed_size);
            hash.update(meta.data_page_offset);
            update_optional(meta.__isset.index_page_offset, meta.index_page_offset);
            update_optional(meta.__isset.dictionary_page_offset, meta.dictionary_page_offset);
            update_optional(meta.__isset.bloom_filter_offset, meta.bloom_filter_offset);
            update_optional(meta.__isset.bloom_filter_length, meta.bloom_filter_length);
            hash.update(meta.path_in_schema.size());
            for (const auto & part : meta.path_in_schema)
                hash.update(part);
            update_key_value_metadata(meta.__isset.key_value_metadata, meta.key_value_metadata);
            /// The statistics are the footer's only fields whose values depend on the data pages'
            /// contents, so they are what distinguishes two generations whose layout happens to be
            /// identical (e.g. a same-size rewrite with different values). All hashed subfields are
            /// integers, booleans or byte strings - no thrift enums.
            hash.update(meta.__isset.statistics);
            if (meta.__isset.statistics)
            {
                const auto & stats = meta.statistics;
                update_optional_string(stats.__isset.max, stats.max);
                update_optional_string(stats.__isset.min, stats.min);
                update_optional(stats.__isset.null_count, stats.null_count);
                update_optional(stats.__isset.distinct_count, stats.distinct_count);
                update_optional_string(stats.__isset.max_value, stats.max_value);
                update_optional_string(stats.__isset.min_value, stats.min_value);
                update_optional(stats.__isset.is_max_value_exact, stats.is_max_value_exact);
                update_optional(stats.__isset.is_min_value_exact, stats.is_min_value_exact);
                update_optional(stats.__isset.nan_count, stats.nan_count);
            }
        }
    }

    const UInt64 digest = hash.get64();
    /// 0 is the "unknown" marker in `ParquetFileBucketInfo::footer_digest`; never return it.
    return digest == 0 ? 1 : digest;
}

std::vector<FileBucketInfoPtr> ParquetBucketSplitter::splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_)
{
    /// The footer is parsed through the native reader (rather than Arrow) so the split can be stamped
    /// with its digest: that is the generation token the per-bucket reads validate what they opened
    /// against (see `ParquetFileBucketInfo::footer_digest`). `RowGroup::total_byte_size` is the same
    /// field either way.
    auto file_metadata = parseFileMetadataNative(buf, format_settings_);

    std::vector<std::vector<size_t>> buckets;
    size_t current_weight = 0;
    for (size_t i = 0; i < file_metadata.row_groups.size(); ++i)
    {
        const size_t row_group_size = size_t(file_metadata.row_groups[i].total_byte_size);
        if (current_weight + row_group_size <= bucket_size)
        {
            if (buckets.empty())
                buckets.emplace_back();
            buckets.back().push_back(i);
            current_weight += row_group_size;
        }
        else
        {
            current_weight = 0;
            buckets.push_back({});
            buckets.back().push_back(i);
            current_weight += row_group_size;
        }
    }

    const size_t file_num_row_groups = file_metadata.row_groups.size();
    std::vector<FileBucketInfoPtr> result;
    for (const auto & bucket : buckets)
        result.push_back(std::make_shared<ParquetFileBucketInfo>(bucket, file_num_row_groups));
    setFooterDigest(result, file_metadata);
    return result;
}

std::vector<FileBucketInfoPtr> ParquetBucketSplitter::splitToBucketsByCount(size_t target_count, ReadBuffer & buf, const FormatSettings & format_settings_)
{
    auto file_metadata = parseFileMetadataNative(buf, format_settings_);
    auto buckets = computeBucketsByCount(target_count, file_metadata.row_groups.size());
    setFooterDigest(buckets, file_metadata);
    return buckets;
}

std::vector<FileBucketInfoPtr> splitParquetFileWithCache(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    ReadBuffer & buf,
    const FormatSettings & format_settings,
    ParquetMetadataCachePtr metadata_cache,
    const std::unordered_set<String> & requested_columns,
    size_t min_bytes_to_split,
    size_t min_bytes_per_bucket)
{
    parquet::format::FileMetaData file_metadata;
    if (metadata_cache && !file_path.empty() && !cache_etag.empty())
    {
        auto key = ParquetMetadataCache::createKey(file_path, cache_etag);
        file_metadata = metadata_cache->getOrSetMetadata(
            key, [&] { return parseFileMetadataNative(buf, format_settings); });
    }
    else
    {
        file_metadata = parseFileMetadataNative(buf, format_settings);
    }
    auto buckets = computeBucketsByCountAndBytes(
        target_count, file_metadata.row_groups.size(), projectedCompressedBytes(file_metadata, requested_columns),
        min_bytes_to_split, min_bytes_per_bucket);
    setFooterDigest(buckets, file_metadata);
    return buckets;
}

std::vector<FileBucketInfoPtr> trySplitParquetFileFromCacheOnly(
    size_t target_count,
    const String & file_path,
    const String & cache_etag,
    const ParquetMetadataCachePtr & metadata_cache,
    const std::unordered_set<String> & requested_columns,
    size_t min_bytes_to_split,
    size_t min_bytes_per_bucket)
{
    if (!metadata_cache || file_path.empty() || cache_etag.empty())
        return {};
    auto key = ParquetMetadataCache::createKey(file_path, cache_etag);
    auto cached = metadata_cache->get(key);
    if (!cached)
        return {};
    auto buckets = computeBucketsByCountAndBytes(
        target_count, cached->metadata.row_groups.size(), projectedCompressedBytes(cached->metadata, requested_columns),
        min_bytes_to_split, min_bytes_per_bucket);
    setFooterDigest(buckets, cached->metadata);
    return buckets;
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

The table below shows how Parquet data types match ClickHouse [data types](/reference/data-types/index).

| Parquet type (logical, converted, or physical) | ClickHouse data type |
|------------------------------------------------|----------------------|
| `BOOLEAN` | [Bool](/reference/data-types/boolean) |
| `UINT_8` | [UInt8](/reference/data-types/int-uint) |
| `INT_8` | [Int8](/reference/data-types/int-uint) |
| `UINT_16` | [UInt16](/reference/data-types/int-uint) |
| `INT_16` | [Int16](/reference/data-types/int-uint)/[Enum16](/reference/data-types/enum) |
| `UINT_32` | [UInt32](/reference/data-types/int-uint) |
| `INT_32` | [Int32](/reference/data-types/int-uint) |
| `UINT_64` | [UInt64](/reference/data-types/int-uint) |
| `INT_64` | [Int64](/reference/data-types/int-uint) |
| `DATE` | [Date32](/reference/data-types/date) |
| `TIMESTAMP`, `TIME` | [DateTime64](/reference/data-types/datetime64) |
| `FLOAT` | [Float32](/reference/data-types/float) |
| `DOUBLE` | [Float64](/reference/data-types/float) |
| `INT96` | [DateTime64(9, 'UTC')](/reference/data-types/datetime64) |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON` | [String](/reference/data-types/string) |
| `JSON` | [JSON](/reference/data-types/newjson) |
| `FIXED_LEN_BYTE_ARRAY` | [FixedString](/reference/data-types/fixedstring) |
| `DECIMAL` | [Decimal](/reference/data-types/decimal) |
| `LIST` | [Array](/reference/data-types/array) |
| `MAP` | [Map](/reference/data-types/map) |
| struct | [Tuple](/reference/data-types/tuple) |
| `FLOAT16` | [Float32](/reference/data-types/float) |
| `UUID` | [FixedString(16)](/reference/data-types/fixedstring) |
| `INTERVAL` | [FixedString(12)](/reference/data-types/fixedstring) |
| `Point` (GeoParquet) | [Point](/reference/data-types/geo#point) |
| `MultiPoint` (GeoParquet) | [MultiPoint](/reference/data-types/geo#multipoint) |
| `LineString` (GeoParquet) | [LineString](/reference/data-types/geo#linestring) |
| `Polygon` (GeoParquet) | [Polygon](/reference/data-types/geo#polygon) |
| `MultiLineString` (GeoParquet) | [MultiLineString](/reference/data-types/geo#multilinestring) |
| `MultiPolygon` (GeoParquet) | [MultiPolygon](/reference/data-types/geo#multipolygon) |
| mixed/unknown geometry (GeoParquet) | [Geometry](/reference/data-types/geo#geometry) |

When writing Parquet file, data types that don't have a matching Parquet type are converted to the nearest available type:

| ClickHouse data type | Parquet type |
|----------------------|--------------|
| [IPv4](/reference/data-types/ipv4) | `UINT_32` |
| [IPv6](/reference/data-types/ipv6) | `FIXED_LEN_BYTE_ARRAY` (16 bytes) |
| [Date](/reference/data-types/date) (16 bits) | `DATE` (32 bits) |
| [DateTime](/reference/data-types/datetime) (32 bits, seconds) | `TIMESTAMP` (64 bits, milliseconds) |
| [Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint) | `FIXED_LEN_BYTE_ARRAY` (16/32 bytes, little-endian) |
| [Point](/reference/data-types/geo#point) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [MultiPoint](/reference/data-types/geo#multipoint) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [LineString](/reference/data-types/geo#linestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [Polygon](/reference/data-types/geo#polygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [MultiLineString](/reference/data-types/geo#multilinestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |
| [MultiPolygon](/reference/data-types/geo#multipolygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to that data type which is set for the ClickHouse table column. E.g. a `UINT_32` Parquet column can be read into an [IPv4](/reference/data-types/ipv4) ClickHouse column.

For some Parquet types there's no closely matching ClickHouse type. We read them as follows:
* `TIME` (time of day) is read as a timestamp. E.g. `10:23:13.000` becomes `1970-01-01 10:23:13.000`.
* `TIMESTAMP`/`TIME` with `isAdjustedToUTC=false` is a local wall-clock time (year, month, day, hour, minute, second and subsecond fields in a local timezone, regardless of what specific time zone is considered local), same as SQL `TIMESTAMP WITHOUT TIME ZONE`. ClickHouse reads it as if it were a UTC timestamp instead. E.g. `2025-09-29 18:42:13.000` (representing a reading of a local wall clock) becomes `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')` representing a point in time). If converted to String, it shows the correct year, month, day, hour, minute, second and subsecond, which can then be interpreted as being in some local timezone instead of UTC. Counterintuitively, changing the type from `DateTime64(3, 'UTC')` to `DateTime64(3)` would not help as both types represent a point in time rather than a clock reading, but `DateTime64(3)` would incorrectly be formatted using local timezone.
* `INTERVAL` is currently read as `FixedString(12)` with raw binary representation of the time interval, as encoded in Parquet file.

## Geo types (GeoParquet) {#geo-types}

ClickHouse supports reading and writing geometry columns according to the [GeoParquet](https://geoparquet.org/) specification. Geometry columns are stored as `BYTE_ARRAY` payloads encoded in [WKB](https://libgeos.org/specifications/wkb/) (or WKT on read), with a JSON `geo` key in the file-level Parquet metadata describing each geometry column's encoding, geometry type and CRS.

### Read behavior {#read}

On read, geometry columns are mapped to the corresponding ClickHouse [geo data types](/reference/data-types/geo):
* A column declared as `Point`, `MultiPoint`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` is read into the matching ClickHouse geo type.
* A column with multiple or unknown geometry types is read into the [`Geometry`](/reference/data-types/geo#geometry) type, which is a `Variant` over all supported geo types.
* If the requested column type is `String`, the GeoParquet metadata is ignored and the raw encoded geometry payload is returned as-is — WKB or WKT bytes, matching whichever encoding the GeoParquet column declares. This is also true if the setting [`input_format_parquet_allow_geoparquet_parser`](/reference/settings/formats/input-format#input_format_parquet_allow_geoparquet_parser) is set to `0`.

### Write behavior {#write}

On write, top-level columns of type `Point`, `MultiPoint`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` are encoded as `BYTE_ARRAY` (WKB) and the appropriate `geo` JSON metadata is appended to the Parquet file footer. A top-level [`Geometry`](/reference/data-types/geo#geometry) `Variant` is also encoded as a WKB `BYTE_ARRAY` payload (its sub-values are converted to WKB and stored as a `Nullable(String)` column), but no `geo` metadata is emitted for it, so the result is not recognized as a GeoParquet geometry column on read. Other geo-related types, such as [`Ring`](/reference/data-types/geo#ring), are written using their native underlying representation with no GeoParquet metadata. This behavior can be disabled entirely by setting [`output_format_parquet_geometadata`](/reference/settings/formats/output-format#output_format_parquet_geometadata) to `0`, in which case even the supported geo types are written using their native underlying representation (`Point` as `Tuple(Float64, Float64)`, `LineString` as `Array(Point)`, `Polygon` as `Array(Array(Point))`, etc.) and no GeoParquet metadata is emitted.

Geometry columns must appear at the root of the schema or nested inside `Tuple` (`struct`); nesting them inside `Array` or `Map` is not supported. `Nullable` is not supported for geo columns either.

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

<Tip>
Parquet is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output Parquet files.
</Tip>

To exchange data with Hadoop, you can use the [`HDFS table engine`](/reference/engines/table-engines/integrations/hdfs).

## Format settings {#format-settings}

| Setting                                                                        | Description                                                                                                                                                                                                                       | Default     |
|--------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `input_format_parquet_case_insensitive_column_matching`                        | Ignore case when matching Parquet columns with CH columns.                                                                                                                                                                          | `0`         |
| `input_format_parquet_preserve_order`                                          | Avoid reordering rows when reading from Parquet files. Usually makes it much slower.                                                                                                                                              | `0`         |
| `input_format_parquet_filter_push_down`                                        | When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.                                                                                          | `1`         |
| `input_format_parquet_bloom_filter_push_down`                                  | When reading Parquet files, skip whole row groups based on the WHERE expressions and bloom filter in the Parquet metadata.                                                                                                          | `0`         |
| `input_format_parquet_dictionary_filter_push_down`                             | When reading Parquet files (with reader v3), skip whole row groups based on the WHERE/PREWHERE expressions and the dictionary page contents, for equality and `IN` conditions, when all data pages of a column chunk are dictionary-encoded. The value is the maximum dictionary page size (in bytes) for which this optimization is applied; set to `0` to disable. Takes precedence over the bloom filter when both are available. | `1048576`   |
| `input_format_parquet_allow_missing_columns`                                   | Allow missing columns while reading Parquet input formats                                                                                                                                                                          | `1`         |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format                                                                                                                          | `8192`      |
| `input_format_parquet_enable_row_group_prefetch`                               | Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch.                                                                                                                          | `1`         |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Skip columns with unsupported types while schema inference for format Parquet                                                                                                                                                      | `0`         |
| `input_format_parquet_max_block_size`                                          | Max block size for parquet reader.                                                                                                                                                                                                | `65409`     |
| `input_format_parquet_prefer_block_bytes`                                      | Average block bytes output by parquet reader                                                                                                                                                                                      | `16744704`  |
| `input_format_parquet_enable_json_parsing`                                      | When reading Parquet files, parse JSON columns as ClickHouse JSON Column.                                                                                                                                                                                      | `1`  |
| `input_format_parquet_allow_geoparquet_parser`                                  | When reading Parquet files, recognize the GeoParquet `geo` metadata and decode geometry columns (WKB or WKT, per the column's declared encoding) as ClickHouse geo data types. If `0`, geometry columns are exposed as their raw physical (`String`) representation.                                                                                                                                              | `1`         |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LEN_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `output_format_parquet_geometadata`                                            | Write GeoParquet `geo` metadata into the Parquet file footer and encode top-level ClickHouse geo columns ([`Point`](/reference/data-types/geo#point), [`MultiPoint`](/reference/data-types/geo#multipoint), [`LineString`](/reference/data-types/geo#linestring), [`Polygon`](/reference/data-types/geo#polygon), [`MultiLineString`](/reference/data-types/geo#multilinestring), [`MultiPolygon`](/reference/data-types/geo#multipolygon)) as WKB. If `0`, those columns are written using their native underlying representation (e.g. `Point` as `Tuple(Float64, Float64)`) and no GeoParquet metadata is emitted.                                                                                                                                                                          | `1`         |
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
