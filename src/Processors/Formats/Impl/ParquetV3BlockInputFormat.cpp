#include <memory>
#include <Common/CurrentThread.h>
#include <optional>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>

#if USE_PARQUET

#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
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
}

static Parquet::ReadOptions convertReadOptions(const FormatSettings & format_settings)
{
    Parquet::ReadOptions options;
    options.format = format_settings;

    options.schema_inference_force_nullable = format_settings.schema_inference_make_columns_nullable == 1;
    options.schema_inference_force_not_nullable = format_settings.schema_inference_make_columns_nullable == 0;

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


        auto chunk = getChunkForCount(size_t(file_metadata.num_rows));
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
