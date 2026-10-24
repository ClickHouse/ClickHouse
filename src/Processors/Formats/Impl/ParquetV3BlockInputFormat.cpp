#include <memory>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
#include <optional>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

#if USE_PARQUET

#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/ThreadPool.h>
#include <Core/Settings.h>
#include <Common/setThreadName.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <IO/SharedThreadPools.h>
#include <IO/VarInt.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/AggregateFunctionStatesFromValuesInputFormat.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Storages/MergeTree/ColumnsCache.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace ProfileEvents
{
    extern const Event ParquetReadMinMaxFromStatistics;
    extern const Event ParquetFilesSkippedByCachedStatistics;
    extern const Event ParquetMetadataCacheHits;
    extern const Event ParquetPrunedRowGroups;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool use_parquet_metadata_cache;
    extern const SettingsBool use_columns_cache;
    extern const SettingsBool enable_reads_from_columns_cache;
    extern const SettingsBool enable_writes_to_columns_cache;
    extern const SettingsUInt64 columns_cache_max_bytes_to_write_to_cache;
    extern const SettingsUInt64 columns_cache_max_estimated_bytes_to_write_to_cache;
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

/// The source whose `READ` grant is required to see the cached entries of a file in `system.columns_cache`.
static std::optional<AccessTypeObjects::Source> getColumnsCacheSource(ObjectStorageType storage_type)
{
    switch (storage_type)
    {
        case ObjectStorageType::S3: return AccessTypeObjects::Source::S3;
        case ObjectStorageType::Azure: return AccessTypeObjects::Source::AZURE;
        case ObjectStorageType::HDFS: return AccessTypeObjects::Source::HDFS;
        case ObjectStorageType::Web: return AccessTypeObjects::Source::URL;
        case ObjectStorageType::Local: return AccessTypeObjects::Source::FILE;
        case ObjectStorageType::None:
        case ObjectStorageType::Max:
            return std::nullopt;
    }
}

/// The columns cache options for reading a file, or nullptr if the read does not use the cache.
/// The cache is used only for a file whose content is identified by a strong version token:
/// an ETag of an object in an object storage, the identity of an immutable file recorded by its
/// table, or the version token of a local file once it has settled (see `StorageFile`), the same
/// as for the userspace page cache. The file must also be
/// identified among all storages, see `RelativePathWithMetadata::path_for_content_cache`.
static std::shared_ptr<const Parquet::ColumnsCacheOptions> getColumnsCacheOptions(
    const ContextPtr & context, const FormatSettings & format_settings, const std::optional<RelativePathWithMetadata> & object_with_metadata)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::use_columns_cache])
        return nullptr;

    const bool enable_reads = settings[Setting::enable_reads_from_columns_cache];
    const bool enable_writes = settings[Setting::enable_writes_to_columns_cache];
    if (!enable_reads && !enable_writes)
        return nullptr;

    if (!object_with_metadata || !object_with_metadata->metadata)
        return nullptr;
    const auto content_token = object_with_metadata->metadata->getContentCacheToken();
    if (!content_token)
        return nullptr;

    if (!object_with_metadata->path_for_content_cache)
        return nullptr;
    const auto source = getColumnsCacheSource(object_with_metadata->storage_type_for_content_cache);
    if (!source)
        return nullptr;

    /// A zero-sized cache accepts nothing, so it is not even looked up.
    ColumnsCachePtr cache = context->getColumnsCache();
    if (!cache || cache->maxSizeInBytes() == 0)
        return nullptr;

    auto options = std::make_shared<Parquet::ColumnsCacheOptions>();
    options->cache = std::move(cache);
    options->file_path = object_with_metadata->getPath();
    options->table_uuid = object_with_metadata->table_uuid_for_content_cache;
    options->source = *source;
    options->enable_reads = enable_reads;
    options->enable_writes = enable_writes;
    options->max_bytes_to_write = settings[Setting::columns_cache_max_bytes_to_write_to_cache];
    options->max_estimated_bytes_to_write = settings[Setting::columns_cache_max_estimated_bytes_to_write_to_cache];
    options->write_budget = context->getColumnsCacheWriteBudget();
    if (!options->write_budget)
        options->write_budget = std::make_shared<ColumnsCacheWriteBudget>();

    const ObjectMetadata & metadata = *object_with_metadata->metadata;
    SipHash hash;
    hash.update(*object_with_metadata->path_for_content_cache);
    hash.update(*content_token);
    hash.update(metadata.size_bytes);
    /// The format settings that change the values decoded from the file without changing their
    /// types. The types are part of the identity of every column, see `Reader::initColumnsCache`.
    hash.update(format_settings.null_as_default);
    hash.update(format_settings.parquet.allow_geoparquet_parser);
    hash.update(format_settings.parquet.enable_json_parsing);
    hash.update(format_settings.parquet.local_time_as_utc);
    hash.update(format_settings.parquet.detect_variant_by_structure);
    hash.update(format_settings.json.type_json_use_partial_match_to_skip_paths_by_regexp);
    hash.update(format_settings.precise_float_parsing);
    hash.update(static_cast<UInt64>(format_settings.date_time_overflow_behavior));
    options->file_identity = hash.get64();

    return options;
}

ParquetV3BlockInputFormat::ParquetV3BlockInputFormat(
    ReadBuffer & buf,
    SharedHeader header_,
    const FormatSettings & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    FormatFilterInfoPtr format_filter_info_,
    size_t min_bytes_for_seek,
    ParquetMetadataCachePtr metadata_cache_,
    const std::optional<RelativePathWithMetadata> & object_with_metadata_,
    bool is_remote_fs_)
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
    read_options.is_remote_fs = is_remote_fs_;

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
            auto cache_key = getMetadataCacheKey();
            ParquetMetadataCache::MappedPtr cache_cell;
            bool cache_hit = false;
            if (cache_key)
            {
                std::tie(cache_cell, cache_hit) = metadata_cache->getOrSetMetadata(
                    *cache_key, [&] { return readFileFooter(reader->reader.prefetcher); });
                reader->reader.setFileFooter(cache_cell->footer, cache_cell->column_chunks);
                if (cache_hit && object_with_metadata->metadata->isEtagUsableAsCacheKey())
                {
                    metadata_cache_cell = cache_cell;
                    metadata_cache_key = *cache_key;
                }
            }
            else
            {
                reader->reader.setFileFooter(readFileFooter(reader->reader.prefetcher));
            }
            reader->reader.init(read_options, getPort().getHeader(), format_filter_info);
            if (metadata_cache_cell && format_filter_info->keep_filter_column_statistics && format_filter_info->key_condition)
            {
                /// Ranges are collected only for the columns not kept yet, and only while they can still be kept.
                Block key_block = getPort().getHeader();
                for (const auto & column : format_filter_info->additional_columns)
                    key_block.insert(column);
                const auto & kept_statistics = *metadata_cache_cell->kept_statistics;
                for (size_t idx : format_filter_info->key_condition->getUsedColumnsInOrder())
                {
                    if (idx >= key_block.columns())
                        continue;
                    const auto & column = key_block.getByPosition(idx);
                    if (!kept_statistics.get(ParquetCachedStatisticsFilter::makeColumnKey(column.name, *column.type, format_settings)))
                        reader->reader.collect_key_condition_column_ranges.push_back(idx);
                }
                reader->reader.max_key_condition_column_ranges_bytes = kept_statistics.remainingBytes();
            }
            reader->init(parser_shared_resources, buckets_to_read ? std::optional(buckets_to_read->row_group_ids) : std::nullopt);
            /// Not after the miss that inserted the cell: replacing it would move it to SLRU's protected queue.
            if (cache_hit)
                metadata_cache->addColumnChunks(*cache_key, cache_cell, reader->reader.getColumnChunks());
        }
    }
}

std::optional<ParquetMetadataCacheKey> ParquetV3BlockInputFormat::getMetadataCacheKey() const
{
    if (metadata_cache && object_with_metadata.has_value() && object_with_metadata->metadata.has_value())
        return ParquetMetadataCache::createKey(
            object_with_metadata->getPath(),
            object_with_metadata->metadata->getContentCacheToken().value_or(object_with_metadata->metadata->etag));
    return std::nullopt;
}

std::shared_ptr<const Parquet::FileFooter> ParquetV3BlockInputFormat::readFileFooter(Parquet::Prefetcher & prefetcher) const
{
    return Parquet::Reader::readFileFooter(prefetcher, read_options.format.parquet.footer_read_size);
}

std::shared_ptr<const Parquet::FileFooter> ParquetV3BlockInputFormat::getFileFooter(Parquet::Prefetcher & prefetcher) const
{
    if (auto cache_key = getMetadataCacheKey())
        return metadata_cache->getOrSetMetadata(*cache_key, [&] { return readFileFooter(prefetcher); }).first->footer;
    return readFileFooter(prefetcher);
}

void ParquetV3BlockInputFormat::cacheFilterColumnStatistics()
{
    std::lock_guard lock(reader_mutex);
    if (!reader || !metadata_cache_cell || min_max_from_statistics)
        return;
    const auto & parquet_reader = reader->reader;
    /// With a column mapper (data lakes) the column names do not identify the columns of the file.
    if (!parquet_reader.key_condition_column_ranges_complete || format_filter_info->column_mapper)
        return;
    for (const auto & column_ranges : parquet_reader.key_condition_column_ranges)
    {
        if (std::ranges::all_of(column_ranges.ranges, [](const Range & range) { return range.isInfinite(); }))
            continue;
        const auto & column = parquet_reader.extended_sample_block.getByPosition(column_ranges.idx);
        String column_key = ParquetCachedStatisticsFilter::makeColumnKey(column.name, *column.type, format_settings);
        if (metadata_cache && metadata_cache_key)
            metadata_cache->keepStatistics(
                *metadata_cache_key, metadata_cache_cell, column_key, column_ranges.ranges, column_ranges.bytes);
    }
}

String ParquetCachedStatisticsFilter::makeColumnKey(
    const String & column_name, const IDataType & type, const FormatSettings & format_settings)
{
    const String type_name = type.getName();
    const auto & parquet = format_settings.parquet;
    return fmt::format(
        "{}:{}{}:{}{}{}{}{}{}{}{}:{}:{}:{}:{}",
        column_name.size(), column_name, type_name.size(), type_name,
        format_settings.null_as_default, parquet.case_insensitive_column_matching, parquet.allow_missing_columns,
        parquet.allow_geoparquet_parser, parquet.local_time_as_utc, parquet.enable_json_parsing, parquet.detect_variant_by_structure,
        static_cast<int>(format_settings.date_time_overflow_behavior), format_settings.schema_inference_make_columns_nullable,
        format_settings.schema_inference_make_json_columns_nullable, format_settings.schema_inference_allow_nullable_tuple_type);
}

std::shared_ptr<const ParquetCachedStatisticsFilter> ParquetCachedStatisticsFilter::create(
    const FormatFilterInfoPtr & format_filter_info, const Block & header, const FormatSettings & format_settings)
{
    /// The reader builds the key condition for its header, which is `header` only if `getHeaderToParse` keeps it.
    if (!format_filter_info || !format_filter_info->filter_actions_dag || format_filter_info->column_mapper
        || !format_settings.parquet.filter_push_down
        || AggregateFunctionStatesFromValuesInputFormat::getHeaderToParse(header, format_settings.aggregate_function_input_format))
        return nullptr;

    format_filter_info->initKeyConditionOnce(header);
    const auto & key_condition = format_filter_info->key_condition;
    if (!key_condition || key_condition->alwaysUnknownOrTrue())
        return nullptr;

    Block key_block = header;
    for (const auto & column : format_filter_info->additional_columns)
        key_block.insert(column);

    auto filter = std::make_shared<ParquetCachedStatisticsFilter>();
    filter->key_condition = key_condition;
    for (size_t idx : key_condition->getUsedColumnsInOrder())
    {
        if (idx >= key_block.columns())
            return nullptr;
        const auto & column = key_block.getByPosition(idx);
        filter->column_keys.emplace_back(idx, makeColumnKey(column.name, *column.type, format_settings));
    }
    if (filter->column_keys.empty())
        return nullptr;
    filter->key_types = key_block.getDataTypes();
    return filter;
}

std::optional<size_t> ParquetCachedStatisticsFilter::canSkipFile(
    ParquetMetadataCache & metadata_cache, const String & path, const String & version) const
{
    auto cell = metadata_cache.get(ParquetMetadataCache::createKey(path, version));
    if (!cell)
        return std::nullopt;

    std::vector<std::pair<size_t, ParquetKeptStatistics::RangesPtr>> columns;
    columns.reserve(column_keys.size());
    for (const auto & [idx, key] : column_keys)
        /// A column without kept ranges is unknown, like a column without statistics.
        if (auto ranges = cell->kept_statistics->get(key))
            columns.emplace_back(idx, std::move(ranges));
    if (columns.empty())
        return std::nullopt;

    const size_t num_kept_row_groups = columns.front().second->size();
    for (const auto & column : columns)
        if (column.second->size() != num_kept_row_groups)
            return std::nullopt;

    Hyperrectangle hyperrectangle(key_types.size(), Range::createWholeUniverse());
    for (size_t row_group = 0; row_group < num_kept_row_groups; ++row_group)
    {
        for (const auto & [idx, ranges] : columns)
            hyperrectangle[idx] = (*ranges)[row_group];
        if (key_condition->checkInHyperrectangle(hyperrectangle, key_types).can_be_true)
            return std::nullopt;
    }
    return cell->footer->metadata.row_groups.size();
}

void ParquetCachedStatisticsFilter::countSkippedFile(size_t num_row_groups)
{
    ProfileEvents::increment(ProfileEvents::ParquetFilesSkippedByCachedStatistics);
    ProfileEvents::increment(ProfileEvents::ParquetPrunedRowGroups, num_row_groups);
    ProfileEvents::increment(ProfileEvents::ParquetMetadataCacheHits);
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
        auto footer = getFileFooter(temp_prefetcher);


        auto chunk = getChunkForCount(size_t(footer->metadata.num_rows));
        chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(0));

        reported_count = true;
        return chunk;
    }

    initializeIfNeeded();
    if (reader->reader.min_max_chunk.has_value())
    {
        Chunk chunk = std::move(*reader->reader.min_max_chunk);
        reader->reader.min_max_chunk.reset();
        min_max_from_statistics = true;
        ProfileEvents::increment(ProfileEvents::ParquetReadMinMaxFromStatistics);
        return chunk;
    }
    auto res = reader->read();
    previous_block_missing_values = res.block_missing_values;
    previous_approx_bytes_read_for_chunk = res.virtual_bytes_read;
    return std::move(res.chunk);
}

std::optional<std::pair<std::vector<size_t>, size_t>> ParquetV3BlockInputFormat::getMatchedBuckets() const
{
    if (!reader || min_max_from_statistics)
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
    return std::make_pair(std::move(matched), reader->reader.file_metadata().row_groups.size());
}

std::vector<std::pair<size_t, Field>> ParquetV3BlockInputFormat::getTopKBestValuesOfBuckets() const
{
    std::vector<std::pair<size_t, Field>> res;
    if (!reader)
        return res;
    for (const auto & row_group : reader->reader.row_groups)
        if (row_group.need_to_process && row_group.top_k_best_value)
            res.emplace_back(row_group.row_group_idx, (*row_group.top_k_best_value)[0]);
    return res;
}

bool ParquetV3BlockInputFormat::isTopKFilterApplied() const
{
    /// The reader installs `__topKFilter` only for a file that stores the sort column.
    return reader && reader->reader.format_filter_info && reader->reader.format_filter_info->top_k_filter
        && reader->reader.top_k_column_is_read;
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

void ParquetV3BlockInputFormat::resetReadBuffer()
{
    {
        /// Background tasks read through a non-owning pointer to the buffers the base class is
        /// about to release, so they have to be stopped first. `reader` stays alive:
        /// getMatchedBuckets() reads row group metadata after the source is exhausted.
        std::lock_guard lock(reader_mutex);
        if (reader)
            reader->shutdownTasks();
    }
    IInputFormat::resetReadBuffer();
}

void ParquetV3BlockInputFormat::resetParser()
{
    {
        std::lock_guard lock(reader_mutex);
        reader.reset();
        min_max_from_statistics = false;
        metadata_cache_cell.reset();
        metadata_cache_key.reset();
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
    file_footer = Parquet::Reader::readFileFooter(prefetcher, read_options.format.parquet.footer_read_size);
    initialized = true;
}

NamesAndTypesList NativeParquetSchemaReader::readSchema()
{
    initializeIfNeeded();
    Parquet::SchemaConverter schemer(*file_footer, read_options, /*sample_block*/ nullptr);
    return schemer.inferSchema();
}

std::optional<size_t> NativeParquetSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return size_t(file_footer->metadata.num_rows);
}

void ParquetFileBucketInfo::serialize(WriteBuffer & buffer)
{
    writeVarUInt(row_group_ids.size(), buffer);
    for (auto chunk : row_group_ids)
        writeVarUInt(chunk, buffer);
}

void ParquetFileBucketInfo::deserialize(ReadBuffer & buffer)
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
}

String ParquetFileBucketInfo::getIdentifier() const
{
    String result;
    for (auto chunk : row_group_ids)
        result += "_" + std::to_string(chunk);
    return result;
}

ParquetFileBucketInfo::ParquetFileBucketInfo(const std::vector<size_t> & row_group_ids_)
    : row_group_ids(row_group_ids_)
{
}

std::shared_ptr<FileBucketInfo> ParquetFileBucketInfo::filterByMatchingRowGroups(const std::vector<size_t> & matching_row_groups) const
{
    if (matching_row_groups.empty())
        return nullptr;
    if (row_group_ids.empty())
        return std::make_shared<ParquetFileBucketInfo>(matching_row_groups);
    std::unordered_set<size_t> matching_set(matching_row_groups.begin(), matching_row_groups.end());
    std::vector<size_t> filtered;
    for (size_t rg : row_group_ids)
        if (matching_set.contains(rg))
            filtered.push_back(rg);
    if (filtered.empty())
        return nullptr;
    return std::make_shared<ParquetFileBucketInfo>(std::move(filtered));
}

void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances);
void registerParquetFileBucketInfo(std::unordered_map<String, FileBucketInfoPtr> & instances)
{
    instances.emplace("Parquet", std::make_shared<ParquetFileBucketInfo>());
}

std::vector<FileBucketInfoPtr> ParquetBucketSplitter::splitToBuckets(size_t bucket_size, ReadBuffer & buf, const FormatSettings & format_settings_)
{
    Parquet::ReadOptions read_options = convertReadOptions(format_settings_);
    Parquet::Prefetcher prefetcher;
    prefetcher.init(&buf, read_options, /*parser_shared_resources_=*/ nullptr);
    auto footer = Parquet::Reader::readFileFooter(prefetcher, read_options.format.parquet.footer_read_size);
    std::vector<size_t> bucket_sizes;
    for (const auto & row_group : footer->metadata.row_groups)
        bucket_sizes.push_back(size_t(row_group.total_byte_size));

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

    std::vector<FileBucketInfoPtr> result;
    for (const auto & bucket : buckets)
    {
        result.push_back(std::make_shared<ParquetFileBucketInfo>(bucket));
    }
    return result;
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
            ParquetMetadataCachePtr metadata_cache;
            if (context->getSettingsRef()[Setting::use_parquet_metadata_cache])
                metadata_cache = context->tryGetParquetMetadataCache();
            auto format = std::make_shared<ParquetV3BlockInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                settings,
                std::move(parser_shared_resources),
                std::move(format_filter_info),
                min_bytes_for_seek,
                metadata_cache,
                object_with_metadata,
                is_remote_fs
            );
            format->setColumnsCacheOptions(getColumnsCacheOptions(context, settings, object_with_metadata));
            return format;
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
            std::nullopt,
            is_remote_fs
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
| `TIMESTAMP` | [DateTime64](/reference/data-types/datetime64) |
| `TIME` | [Time64](/reference/data-types/time64) |
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
| `GEOMETRY`, `GEOGRAPHY` | [Point](/reference/data-types/geo#point), [MultiPoint](/reference/data-types/geo#multipoint), [LineString](/reference/data-types/geo#linestring), [Polygon](/reference/data-types/geo#polygon), [MultiLineString](/reference/data-types/geo#multilinestring), [MultiPolygon](/reference/data-types/geo#multipolygon) or [Geometry](/reference/data-types/geo#geometry), see [Geo types](#geo-types) |
| `VARIANT` | [Dynamic](/reference/data-types/dynamic) |
| `FILE` | [Tuple](/reference/data-types/tuple) of `Nullable` fields, see [below](#file-type) |

When writing Parquet file, data types that don't have a matching Parquet type are converted to the nearest available type:

| ClickHouse data type | Parquet type |
|----------------------|--------------|
| [IPv4](/reference/data-types/ipv4) | `UINT_32` |
| [IPv6](/reference/data-types/ipv6) | `FIXED_LEN_BYTE_ARRAY` (16 bytes) |
| [Date](/reference/data-types/date) (16 bits) | `DATE` (32 bits) |
| [DateTime](/reference/data-types/datetime) (32 bits, seconds) | `TIMESTAMP` (64 bits, milliseconds) |
| [Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint) | `FIXED_LEN_BYTE_ARRAY` (16/32 bytes, little-endian) |
| [Point](/reference/data-types/geo#point) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [MultiPoint](/reference/data-types/geo#multipoint) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [LineString](/reference/data-types/geo#linestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [Polygon](/reference/data-types/geo#polygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [MultiLineString](/reference/data-types/geo#multilinestring) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [MultiPolygon](/reference/data-types/geo#multipolygon) | `BYTE_ARRAY` (WKB) + GeoParquet metadata, or `GEOMETRY` with `output_format_parquet_geometry_logical_type` |
| [Geometry](/reference/data-types/geo#geometry) | `BYTE_ARRAY` (WKB), or `GEOMETRY` + GeoParquet metadata with `output_format_parquet_geometry_logical_type` |

Arrays can be nested and can have a value of `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

### Wide integers {#wide-integers}

By default, ClickHouse writes `Int128`, `UInt128`, `Int256`, and `UInt256` as unannotated `FIXED_LEN_BYTE_ARRAY(16/32)` values in little-endian order. This legacy representation remains the default so files can be read by older ClickHouse versions.

Set `output_format_parquet_wide_integer_as_decimal = 1` to use the standard Parquet `DECIMAL` representation instead:

| ClickHouse type | Parquet logical type | Physical type | Value encoding |
|-----------------|----------------------|---------------|----------------|
| `UInt128` | `DECIMAL(39, 0)` | `FIXED_LEN_BYTE_ARRAY(17)` | A zero sign byte followed by the 16-byte unsigned value in big-endian order |
| `UInt256` | `DECIMAL(78, 0)` | `FIXED_LEN_BYTE_ARRAY(33)` | A zero sign byte followed by the 32-byte unsigned value in big-endian order |
| `Int128` | `DECIMAL(39, 0)` | `FIXED_LEN_BYTE_ARRAY(17)` | The 16-byte two's-complement value in big-endian order, sign-extended to 17 bytes |
| `Int256` | `DECIMAL(77, 0)` | `FIXED_LEN_BYTE_ARRAY(33)` | The 32-byte two's-complement value in big-endian order, sign-extended to 33 bytes |

The decimal representation provides numeric ordering for standard Parquet column-chunk statistics and page indexes, which lets ClickHouse use row-group and page min/max pruning. ClickHouse reads both encodings. For decimal precision 39, schema inference returns `Decimal(39, 0)`, backed by `Decimal256`; specify an explicit `Int128` or `UInt128` structure to recover a wide-integer type. Decimal precision 77 or 78 exceeds the range of ClickHouse `Decimal256`, so reading those files requires an explicit compatible `Int256` or `UInt256` structure. With an explicit wide-integer structure, ClickHouse accepts standard decimal values stored as either `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY` of any valid width, and range-checks every value after removing sign extension.

Some Parquet clients support decimal precision only up to 38, while Arrow's high-level decimal types support precision only up to 76. Such clients can reject the precision-77/78 representation even though its schema and 33-byte physical width conform to Parquet. Keep the setting disabled when files must be read by older ClickHouse versions or clients with smaller decimal limits.

Data types of ClickHouse table columns can differ from the corresponding fields of the Parquet data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/reference/functions/regular-functions/type-conversion-functions#CAST) the data to that data type which is set for the ClickHouse table column. E.g. a `UINT_32` Parquet column can be read into an [IPv4](/reference/data-types/ipv4) ClickHouse column.

For some Parquet types there's no closely matching ClickHouse type. We read them as follows:
* `TIMESTAMP` with `isAdjustedToUTC=false` is a local wall-clock time (year, month, day, hour, minute, second and subsecond fields in a local timezone, regardless of what specific time zone is considered local), same as SQL `TIMESTAMP WITHOUT TIME ZONE`. ClickHouse reads it as if it were a UTC timestamp instead. E.g. `2025-09-29 18:42:13.000` (representing a reading of a local wall clock) becomes `2025-09-29 18:42:13.000` (`DateTime64(3, 'UTC')` representing a point in time). If converted to String, it shows the correct year, month, day, hour, minute, second and subsecond, which can then be interpreted as being in some local timezone instead of UTC. Counterintuitively, changing the type from `DateTime64(3, 'UTC')` to `DateTime64(3)` would not help as both types represent a point in time rather than a clock reading, but `DateTime64(3)` would incorrectly be formatted using local timezone.
* `INTERVAL` is currently read as `FixedString(12)` with raw binary representation of the time interval, as encoded in Parquet file.

## Variant type {#variant-type}

ClickHouse reads columns stored in the Parquet [`VARIANT`](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) encoding, such as those written by Spark 4.0, into the [`Dynamic`](/reference/data-types/dynamic) type. A column is recognized as a variant if it is a group annotated with the `VARIANT` logical type (specification version 1). Spark 4.0 writes variant columns without this annotation, so a group with just a `metadata` and a `value` field of type `BYTE_ARRAY` is also read as a variant. This can be disabled with the setting `input_format_parquet_detect_variant_by_structure`, then such a group is read as a `Tuple` of the two raw fields, as before; requesting the `Dynamic` or `JSON` type for the column explicitly reads it as a variant regardless of the setting.

Each value keeps its own type. Variant primitive types are read as the matching ClickHouse types (for example, `int32` as `Int32`, `decimal8` as `Decimal(18, S)`, `timestamp` as `DateTime64(6, 'UTC')`, `timestamp_ntz` as `DateTime64(6)`, `binary` and `string` as `String`, `uuid` as `UUID`), the variant `null` is read as `NULL`, variant objects are read as [`JSON`](/reference/data-types/newjson) with nested objects flattened into dot-separated paths (a `null` field is treated as absent), variant arrays whose elements are all objects are read as `Array(JSON)`, and other variant arrays are read as `Array(Dynamic)`. The top-level type is `Dynamic` rather than `JSON`, because a variant value is not necessarily an object.

A variant column can also be read as [`JSON`](/reference/data-types/newjson) by requesting that type explicitly, for example in the table structure or in the structure argument of the `file` table function. Then every value must be an object, otherwise an exception is thrown. A variant `null` and a `null` of the whole column are read as an empty object. Typed paths of the requested `JSON` type are converted to their types, and paths listed in `SKIP` and `SKIP REGEXP` are skipped.

Shredded variants (groups with a `typed_value` field) are not supported yet: reading such a column throws an exception, while the other columns of the file can still be read, and schema inference skips the column if `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` is enabled. Writing `Dynamic` or `JSON` columns to Parquet as `VARIANT` is not supported either.

## File references {#file-type}

A group annotated with the Parquet [`FILE`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#file) logical type represents a reference to a range of bytes, stored inline or in an external file. It is read as a named `Tuple` of the fields present in the file, each of them `Nullable`: `uri`, `content_type` and `checksum` as `Nullable(String)`, `offset` and `size` as `Nullable(Int64)`, and `inline` as `Nullable(String)` with raw bytes. A `null` file reference is read as a tuple with all elements `NULL`. As for any `Tuple`, a subset of the fields can be read by requesting a `Tuple` with fewer elements, or individual fields as subcolumns, such as `file.uri`. The schema of the group is validated: it must contain only these fields, all `OPTIONAL`, with the types defined by the specification, otherwise an exception is thrown. The values are not validated: a value that does not form a valid reference (for example, `offset` without `uri`) is returned as stored. ClickHouse does not resolve the references, that is, it does not read the external files.

On write, such a `Tuple` (with element names being a non-empty subset of the `FILE` fields, integer `offset` and `size`, and `String` or `FixedString` other elements) is written as a `FILE`-annotated group if the setting [`output_format_parquet_write_file_logical_type`](/reference/settings/formats/output-format#output_format_parquet_write_file_logical_type) is enabled. Otherwise it is written as an ordinary group.

## Geo types (GeoParquet) {#geo-types}

ClickHouse supports reading and writing geometry columns according to the [GeoParquet](https://geoparquet.org/) specification and the Parquet [`GEOMETRY` and `GEOGRAPHY` logical types](https://github.com/apache/parquet-format/blob/master/Geospatial.md). Geometry columns are stored as `BYTE_ARRAY` payloads encoded in [WKB](https://libgeos.org/specifications/wkb/) (or WKT on read for GeoParquet). GeoParquet uses a JSON `geo` key in the file-level Parquet metadata describing each geometry column's encoding, geometry type and CRS, while the logical types annotate the column itself, and the column chunk metadata contains `GeospatialStatistics` (a bounding box and the list of geometry types) instead of min/max statistics. A file can have both.

### Read behavior {#read}

On read, geometry columns are mapped to the corresponding ClickHouse [geo data types](/reference/data-types/geo):
* A column declared as `Point`, `MultiPoint`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` is read into the matching ClickHouse geo type.
* A column with multiple or unknown geometry types is read into the [`Geometry`](/reference/data-types/geo#geometry) type, which is a `Variant` over all supported geo types.
* For a column with the `GEOMETRY` or `GEOGRAPHY` logical type, the geometry type is taken from the GeoParquet metadata if the file has it. Otherwise, if the `GeospatialStatistics` of all column chunks list the same single 2D geometry type, the column is read into the matching geo type, and into `Geometry` otherwise. If a geo type is requested explicitly, the values are decoded directly into that type.
* ClickHouse has no separate type for geography, so `GEOGRAPHY` columns are read into the same geo types as `GEOMETRY` (the coordinates are longitude and latitude), and the edge interpolation algorithm (the `algorithm` parameter) is not represented. The CRS (the `crs` parameter, `OGC:CRS84` by default) is not represented either. Geometries with Z or M coordinates are not supported.
* The sort order of the `GEOMETRY` and `GEOGRAPHY` logical types is undefined, so min/max statistics and the column index of such columns are ignored. With [`input_format_parquet_spatial_filter_push_down`](/reference/settings/formats/input-format#input_format_parquet_spatial_filter_push_down), the bounding box from `GeospatialStatistics` is used to skip row groups.
* If the requested column type is `String`, the GeoParquet metadata and the logical type are ignored and the raw encoded geometry payload is returned as-is — WKB or WKT bytes, matching whichever encoding the GeoParquet column declares. This is also true if the setting [`input_format_parquet_allow_geoparquet_parser`](/reference/settings/formats/input-format#input_format_parquet_allow_geoparquet_parser) is set to `0` and no geo type is requested explicitly.

### Write behavior {#write}

On write, top-level columns of type `Point`, `MultiPoint`, `LineString`, `Polygon`, `MultiLineString` or `MultiPolygon` are encoded as `BYTE_ARRAY` (WKB) and the appropriate `geo` JSON metadata is appended to the Parquet file footer. A top-level [`Geometry`](/reference/data-types/geo#geometry) `Variant` is also encoded as a WKB `BYTE_ARRAY` payload (its sub-values are converted to WKB and stored as a `Nullable(String)` column), but by default no `geo` metadata is emitted for it, so it is read back as `String`. These columns are written as `BYTE_ARRAY` with min/max statistics by default. With the setting [`output_format_parquet_geometry_logical_type`](/reference/settings/formats/output-format#output_format_parquet_geometry_logical_type) enabled, all these columns are annotated with the `GEOMETRY` logical type with the default CRS (`OGC:CRS84`), their column chunk metadata contains `GeospatialStatistics` (the bounding box and the geometry types) but no min/max statistics and no column index, as required by the specification, and a `Geometry` column is described in the `geo` metadata with an empty list of geometry types, so it is read back as `Geometry`. The setting is disabled by default because readers that do not know this logical type (e.g. Arrow C++ and `pyarrow` before version 20) cannot open the whole file. Other geo-related types, such as [`Ring`](/reference/data-types/geo#ring), are written using their native underlying representation with no GeoParquet metadata. This behavior can be disabled entirely by setting [`output_format_parquet_geometadata`](/reference/settings/formats/output-format#output_format_parquet_geometadata) to `0`, in which case even the supported geo types are written using their native underlying representation (`Point` as `Tuple(Float64, Float64)`, `LineString` as `Array(Point)`, `Polygon` as `Array(Array(Point))`, etc.) and no GeoParquet metadata is emitted.

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
| `input_format_parquet_allow_geoparquet_parser`                                  | When reading Parquet files, recognize the GeoParquet `geo` metadata and the `GEOMETRY` and `GEOGRAPHY` logical types, and decode geometry columns (WKB or WKT, per the column's declared encoding) as ClickHouse geo data types. If `0`, geometry columns are exposed as their raw physical (`String`) representation.                                                                                                                                              | `1`         |
| `output_format_parquet_row_group_size`                                         | Target row group size in rows.                                                                                                                                                                                                      | `1000000`   |
| `output_format_parquet_row_group_size_bytes`                                   | Target row group size in bytes, before compression.                                                                                                                                                                                  | `536870912` |
| `output_format_parquet_string_as_string`                                       | Use Parquet String type instead of Binary for String columns.                                                                                                                                                                      | `1`         |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | Use Parquet FIXED_LEN_BYTE_ARRAY type instead of Binary for FixedString columns.                                                                                                                                                  | `1`         |
| `output_format_parquet_wide_integer_as_decimal`                                | Write `Int128`, `UInt128`, `Int256`, and `UInt256` as standard big-endian Parquet `DECIMAL` values instead of legacy little-endian fixed byte arrays.                                                                            | `0`         |
| `output_format_parquet_compression_method`                                     | Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)                                                                                                              | `zstd`      |
| `output_format_parquet_parallel_encoding`                                      | Do Parquet encoding in multiple threads.                                                                                                                                          | `1`         |
| `output_format_parquet_data_page_size`                                         | Target page size in bytes, before compression.                                                                                                                                                                                      | `1048576`   |
| `output_format_parquet_batch_size`                                             | Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.                                                                                                              | `1024`      |
| `output_format_parquet_write_page_index`                                       | Add a possibility to write page index into parquet files.                                                                                                                                                                          | `1`         |
| `output_format_parquet_geometadata`                                            | Write GeoParquet `geo` metadata into the Parquet file footer and encode top-level ClickHouse geo columns ([`Point`](/reference/data-types/geo#point), [`MultiPoint`](/reference/data-types/geo#multipoint), [`LineString`](/reference/data-types/geo#linestring), [`Polygon`](/reference/data-types/geo#polygon), [`MultiLineString`](/reference/data-types/geo#multilinestring), [`MultiPolygon`](/reference/data-types/geo#multipolygon)) as WKB. If `0`, those columns are written using their native underlying representation (e.g. `Point` as `Tuple(Float64, Float64)`) and no GeoParquet metadata is emitted.                                                                                                                                                                          | `1`         |
| `output_format_parquet_geometry_logical_type`                                  | Annotate the geo columns written as WKB with the Parquet `GEOMETRY` logical type and write `GeospatialStatistics` instead of min/max statistics for them. Has no effect if `output_format_parquet_geometadata` is `0`.                                                                                                                                                                                                  | `0`         |
| `output_format_parquet_write_file_logical_type` | Write a named `Tuple` whose element names are fields of the Parquet `FILE` logical type as a `FILE`-annotated group, see [File references](#file-type). | `0` |
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
                "schema_inference_make_columns_nullable={};schema_inference_make_json_columns_nullable={};"
                "schema_inference_allow_nullable_tuple_type={};"
                "enable_json_parsing={};max_parser_depth={};"
                "local_time_as_utc={};allow_geoparquet_parser={};skip_columns_with_unsupported_types={}",
                settings.schema_inference_make_columns_nullable,
                settings.schema_inference_make_json_columns_nullable,
                settings.schema_inference_allow_nullable_tuple_type,
                settings.parquet.enable_json_parsing,
                settings.max_parser_depth,
                settings.parquet.local_time_as_utc,
                settings.parquet.allow_geoparquet_parser,
                settings.parquet.skip_columns_with_unsupported_types_in_schema_inference);
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
