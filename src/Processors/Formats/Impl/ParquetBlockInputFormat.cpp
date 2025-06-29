#include "ParquetBlockInputFormat.h"

#if USE_PARQUET

#include <Columns/ColumnNullable.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SharedThreadPools.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/bloom_filter.h>
#include <parquet/bloom_filter_reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "ArrowFieldIndexUtil.h"
#include <base/scope_guard.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/FieldAccurateComparison.h>
#include <Processors/Formats/Impl/Parquet/ParquetRecordReader.h>
#include <Processors/Formats/Impl/Parquet/parquetBloomFilterHash.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <shared_mutex>
#include <boost/algorithm/string/case_conv.hpp>

namespace ProfileEvents
{
    extern const Event ParquetFetchWaitTimeMicroseconds;
    extern const Event ParquetReadRowGroups;
    extern const Event ParquetPrunedRowGroups;
}

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
    extern const Metric IOThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int LOGICAL_ERROR;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
        {                                                              \
            throw Exception::createDeprecated(_s.ToString(),           \
                _s.IsOutOfMemory() ? ErrorCodes::CANNOT_ALLOCATE_MEMORY : ErrorCodes::INCORRECT_DATA); \
        }                                                              \
    } while (false)

/// Decode min/max value from column chunk statistics. Returns Null if missing or unsupported.
///
/// There are two questionable decisions in this implementation:
///  * We parse the value from the encoded byte string instead of casting the parquet::Statistics
///    to parquet::TypedStatistics and taking the value from there.
///  * We dispatch based on the parquet logical+converted+physical type instead of the ClickHouse type.
/// The idea is that this is similar to what we'll have to do when reimplementing Parquet parsing in
/// ClickHouse instead of using Arrow (for speed). So, this is an exercise in parsing Parquet manually.
static Field decodePlainParquetValueSlow(const std::string & data, parquet::Type::type physical_type, const parquet::ColumnDescriptor & descr, TypeIndex type_hint)
{
    using namespace parquet;

    auto decode_integer = [&](bool signed_) -> UInt64 {
        size_t size;
        switch (physical_type)
        {
            case parquet::Type::type::BOOLEAN: size = 1; break;
            case parquet::Type::type::INT32: size = 4; break;
            case parquet::Type::type::INT64: size = 8; break;
            default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected physical type for number");
        }
        if (data.size() != size)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected size: {}", data.size());

        UInt64 val = 0;
        memcpy(&val, data.data(), size);

        /// Sign-extend.
        if (signed_ && size < 8 && (val >> (size * 8 - 1)) != 0)
            val |= 0 - (1ul << (size * 8));

        return val;
    };

    /// Decimal.
    do // while (false)
    {
        Int32 scale;
        if (descr.logical_type() && descr.logical_type()->is_decimal())
            scale = assert_cast<const DecimalLogicalType &>(*descr.logical_type()).scale();
        else if (descr.converted_type() == ConvertedType::type::DECIMAL)
            scale = descr.type_scale();
        else
            break;

        size_t size;
        bool big_endian = false;
        switch (physical_type)
        {
            case Type::type::BOOLEAN: size = 1; break;
            case Type::type::INT32: size = 4; break;
            case Type::type::INT64: size = 8; break;

            case Type::type::FIXED_LEN_BYTE_ARRAY:
                big_endian = true;
                size = data.size();
                break;
            default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal physical type");
        }
        /// Note that size is not necessarily a power of two.
        /// E.g. spark turns 8-byte unsigned integers into 9-byte signed decimals.
        if (data.size() != size || size < 1 || size > 32)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal size: {} (actual {})", size, data.size());

        Int256 val = 0;
        memcpy(&val, data.data(), size);
        if (big_endian)
            std::reverse(reinterpret_cast<char *>(&val), reinterpret_cast<char *>(&val) + size);
        /// Sign-extend.
        if (size < 32 && (val >> (size * 8 - 1)) != 0)
            val |= ~((Int256(1) << (size * 8)) - 1);

        auto narrow = [&](auto x) -> Field
        {
            memcpy(&x, &val, sizeof(x));
            return Field(DecimalField<decltype(x)>(x, static_cast<UInt32>(scale)));
        };
        if (size <= 4)
            return narrow(Decimal32(0));
        if (size <= 8)
            return narrow(Decimal64(0));
        if (size <= 16)
            return narrow(Decimal128(0));
        return narrow(Decimal256(0));
    }
    while (false);

    /// Timestamp (decimal).
    {
        Int32 scale = -1;
        bool is_timestamp = true;
        if (descr.logical_type() && (descr.logical_type()->is_time() || descr.logical_type()->is_timestamp()))
        {
            LogicalType::TimeUnit::unit unit = descr.logical_type()->is_time()
                ? assert_cast<const TimeLogicalType &>(*descr.logical_type()).time_unit()
                : assert_cast<const TimestampLogicalType &>(*descr.logical_type()).time_unit();
            switch (unit)
            {
                case LogicalType::TimeUnit::unit::MILLIS: scale = 3; break;
                case LogicalType::TimeUnit::unit::MICROS: scale = 6; break;
                case LogicalType::TimeUnit::unit::NANOS: scale = 9; break;
                default: throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unknown time unit");
            }
        }
        else switch (descr.converted_type())
        {
            case ConvertedType::type::TIME_MILLIS: scale = 3; break;
            case ConvertedType::type::TIME_MICROS: scale = 6; break;
            case ConvertedType::type::TIMESTAMP_MILLIS: scale = 3; break;
            case ConvertedType::type::TIMESTAMP_MICROS: scale = 6; break;
            default: is_timestamp = false;
        }

        if (is_timestamp)
        {
            Int64 val = static_cast<Int64>(decode_integer(/* signed */ true));
            return Field(DecimalField<Decimal64>(Decimal64(val), scale));
        }
    }

    /// Floats.

    if (physical_type == Type::type::FLOAT)
    {
        if (data.size() != 4)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
        Float32 val;
        memcpy(&val, data.data(), data.size());
        return Field(val);
    }

    if (physical_type == Type::type::DOUBLE)
    {
        if (data.size() != 8)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
        Float64 val;
        memcpy(&val, data.data(), data.size());
        return Field(val);
    }

    if (physical_type == Type::type::BYTE_ARRAY || physical_type == Type::type::FIXED_LEN_BYTE_ARRAY)
    {
        /// Arrow's parquet decoder handles missing min/max values slightly incorrectly.
        /// In a parquet file, min and max have separate is_set flags, i.e. one may be missing even
        /// if the other is set. Arrow decoder ORs (!) these two flags together into one: HasMinMax().
        /// So, if exactly one of {min, max} is missing, Arrow reports it as empty string, with no
        /// indication that it's actually missing.
        ///
        /// How can exactly one of {min, max} be missing? This happens if one of the two strings
        /// exceeds the length limit for stats. Repro:
        ///
        ///   insert into function file('t.parquet') select arrayStringConcat(range(number*1000000)) from numbers(2) settings output_format_parquet_use_custom_encoder=0
        ///   select tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') from file('t.parquet', ParquetMetadata)
        ///
        /// Here the row group contains two strings: one empty, one very long. But the statistics
        /// reported by arrow are indistinguishable from statistics if all strings were empty.
        /// (Min and max are the last two tuple elements in the output of the second query. Notice
        /// how they're empty strings instead of NULLs.)
        ///
        /// So we have to be conservative and treat empty string as unknown.
        /// This is unfortunate because it's probably common for string columns to have lots of empty
        /// values, and filter pushdown would probably often be useful in that case.
        ///
        /// TODO: Remove this workaround either when we implement our own Parquet decoder that
        ///       doesn't have this bug, or if it's fixed in Arrow.
        if (data.empty())
            return Field();

        /// Long integers, encoded either as text or as little-endian bytes.
        /// The parquet file doesn't know that it's numbers, so the min/max are produced by comparing
        /// strings lexicographically. So these min and max are mostly useless to us.
        /// There's one case where they're not useless: min == max; currently we don't make use of this.
        switch (type_hint)
        {
            case TypeIndex::UInt128:
            case TypeIndex::UInt256:
            case TypeIndex::Int128:
            case TypeIndex::Int256:
            case TypeIndex::IPv6:
                return Field();
            default: break;
        }

        /// Strings.
        return Field(data);
    }

    /// This type is deprecated in Parquet.
    /// TODO: But turns out it's still used in practice, we should support it.
    if (physical_type == Type::type::INT96)
        return Field();

    /// Integers.

    bool signed_ = true;
    if (descr.logical_type() && descr.logical_type()->is_int())
        signed_ = assert_cast<const IntLogicalType &>(*descr.logical_type()).is_signed();
    else
        signed_ = descr.converted_type() != ConvertedType::type::UINT_8 &&
                  descr.converted_type() != ConvertedType::type::UINT_16 &&
                  descr.converted_type() != ConvertedType::type::UINT_32 &&
                  descr.converted_type() != ConvertedType::type::UINT_64;

    UInt64 val = decode_integer(signed_);
    Field field = signed_ ? Field(static_cast<Int64>(val)) : Field(val);
    return field;
}

struct ParquetBloomFilter final : public KeyCondition::BloomFilter
{
    explicit ParquetBloomFilter(std::unique_ptr<parquet::BloomFilter> && parquet_bf_)
        : parquet_bf(std::move(parquet_bf_)) {}

    bool findAnyHash(const std::vector<uint64_t> & hashes) override
    {
        for (const auto hash : hashes)
        {
            if (parquet_bf->FindHash(hash))
            {
                return true;
            }
        }

        return false;
    }

private:
    std::unique_ptr<parquet::BloomFilter> parquet_bf;
};

static KeyCondition::ColumnIndexToBloomFilter buildColumnIndexToBF(
    parquet::BloomFilterReader & bf_reader,
    int row_group,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    const std::unordered_set<std::size_t> & filtering_columns
)
{
    auto rg_bf = bf_reader.RowGroup(row_group);

    if (!rg_bf)
    {
        return {};
    }

    KeyCondition::ColumnIndexToBloomFilter index_to_column_bf;

    for (const auto & [clickhouse_index, parquet_indexes] : clickhouse_column_index_to_parquet_index)
    {
        if (!filtering_columns.contains(clickhouse_index))
        {
            continue;
        }

        // Complex / nested types contain more than one index. We don't support those.
        if (parquet_indexes.size() > 1)
        {
            continue;
        }

        auto parquet_index = parquet_indexes[0];

        auto parquet_bf = rg_bf->GetColumnBloomFilter(parquet_index);

        if (!parquet_bf)
        {
            continue;
        }

        index_to_column_bf[clickhouse_index] = std::make_unique<ParquetBloomFilter>(std::move(parquet_bf));
    }

    return index_to_column_bf;
}

/// Range of values for each column, based on statistics in the Parquet metadata.
/// This is lower/upper bounds, not necessarily exact min and max, e.g. the min/max can be just
/// missing in the metadata.
static std::vector<Range> getHyperrectangleForRowGroup(const parquet::FileMetaData & file, int row_group_idx, const Block & header, const FormatSettings & format_settings)
{
    auto column_name_for_lookup = [&](std::string column_name) -> std::string
    {
        if (format_settings.parquet.case_insensitive_column_matching)
            boost::to_lower(column_name);
        return column_name;
    };

    std::unique_ptr<parquet::RowGroupMetaData> row_group = file.RowGroup(row_group_idx);

    std::unordered_map<std::string, std::shared_ptr<parquet::Statistics>> name_to_statistics;
    for (int i = 0; i < row_group->num_columns(); ++i)
    {
        auto c = row_group->ColumnChunk(i);
        auto s = c->statistics();
        if (!s)
            continue;

        if (s->descr()->schema_node()->is_repeated())
            continue;

        auto path = c->path_in_schema()->ToDotVector();
        if (path.size() != 1)
            continue; // compound types not supported

        name_to_statistics.emplace(column_name_for_lookup(path[0]), s);
    }

    ///    +-----+
    ///   /     /|
    ///  +-----+ |
    ///  |     | +
    ///  |     |/
    ///  +-----+
    std::vector<Range> hyperrectangle(header.columns(), Range::createWholeUniverse());

    for (size_t idx = 0; idx < header.columns(); ++idx)
    {
        const std::string & name = header.getByPosition(idx).name;
        auto it = name_to_statistics.find(column_name_for_lookup(name));
        if (it == name_to_statistics.end())
            continue;
        auto stats = it->second;

        DataTypePtr type = header.getByPosition(idx).type;
        if (type->lowCardinality())
            type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
        if (type->isNullable())
            type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
        Field default_value = type->getDefault();
        TypeIndex type_index = type->getTypeId();

        /// Only primitive fields are supported, not arrays, maps, tuples, or Nested.
        /// Arrays, maps, and Nested can't be meaningfully supported because Parquet only has min/max
        /// across all *elements* of the array, not min/max array itself.
        /// Same limitation for tuples, but maybe it would make sense to have some kind of tuple
        /// expansion in KeyCondition to accept ranges per element instead of whole tuple.

        Field min;
        Field max;
        if (stats->HasMinMax())
        {
            try
            {
                min = decodePlainParquetValueSlow(stats->EncodeMin(), stats->physical_type(), *stats->descr(), type_index);
                max = decodePlainParquetValueSlow(stats->EncodeMax(), stats->physical_type(), *stats->descr(), type_index);

                /// If the data type in parquet file substantially differs from the requested data type,
                /// it's sometimes correct to just typecast the min/max values.
                /// Other times it's incorrect, e.g.:
                ///   INSERT INTO FUNCTION file('t.parquet', Parquet, 'x String') VALUES ('1'), ('100'), ('2');
                ///   SELECT * FROM file('t.parquet', Parquet, 'x Int64') WHERE x >= 3;
                /// If we just typecast min/max from string to integer, this query will incorrectly return empty result.
                /// Allow conversion in some simple cases, otherwise ignore the min/max values.
                auto min_type = min.getType();
                auto max_type = max.getType();
                min = tryConvertFieldToType(min, *type);
                max = tryConvertFieldToType(max, *type);
                auto ok_cast = [&](Field::Types::Which from, Field::Types::Which to) -> bool
                {
                    if (from == to)
                        return true;
                    /// Decimal -> wider decimal.
                    if (Field::isDecimal(from) || Field::isDecimal(to))
                        return Field::isDecimal(from) && Field::isDecimal(to) && to >= from;
                    /// Integer -> IP.
                    if (to == Field::Types::IPv4)
                        return from == Field::Types::UInt64;
                    /// Disable index for everything else, especially string <-> number.
                    return false;
                };
                if (!(ok_cast(min_type, min.getType()) && ok_cast(max_type, max.getType())) &&
                    !(min == max) &&
                    !(min_type == Field::Types::Int64 && min.getType() == Field::Types::UInt64 && min.safeGet<Int64>() >= 0) &&
                    !(max_type == Field::Types::UInt64 && max.getType() == Field::Types::Int64 && max.safeGet<UInt64>() <= UInt64(INT64_MAX)))
                {
                    min = Field();
                    max = Field();
                }
            }
            catch (Exception & e)
            {
                e.addMessage(" (When parsing Parquet statistics for column {}, physical type {}, {}. Please report an issue and use input_format_parquet_filter_push_down = false to work around.)", name, static_cast<int>(stats->physical_type()), stats->descr()->ToString());
                throw;
            }
        }

        /// In Range, NULL is represented as positive or negative infinity (represented by a special
        /// kind of Field, different from floating-point infinities).

        bool always_null = stats->descr()->max_definition_level() != 0 &&
            stats->HasNullCount() && stats->num_values() == 0;
        bool can_be_null = stats->descr()->max_definition_level() != 0 &&
            (!stats->HasNullCount() || stats->null_count() != 0);
        bool null_as_default = format_settings.null_as_default && !isNullableOrLowCardinalityNullable(header.getByPosition(idx).type);

        if (always_null)
        {
            /// Single-point range containing either the default value of one of the infinities.
            if (null_as_default)
                hyperrectangle[idx].right = hyperrectangle[idx].left = default_value;
            else
                hyperrectangle[idx].right = hyperrectangle[idx].left;
            continue;
        }

        if (can_be_null)
        {
            if (null_as_default)
            {
                /// Make sure the range contains the default value.
                if (!min.isNull() && accurateLess(default_value, min))
                    min = default_value;
                if (!max.isNull() && accurateLess(max, default_value))
                    max = default_value;
            }
            else
            {
                /// Make sure the range reaches infinity on at least one side.
                if (!min.isNull() && !max.isNull())
                    min = Field();
            }
        }
        else
        {
            /// If the column doesn't have nulls, exclude both infinities.
            if (min.isNull())
                hyperrectangle[idx].left_included = false;
            if (max.isNull())
                hyperrectangle[idx].right_included = false;
        }

        if (!min.isNull())
            hyperrectangle[idx].left = std::move(min);
        if (!max.isNull())
            hyperrectangle[idx].right = std::move(max);
    }

    return hyperrectangle;
}

std::unordered_set<std::size_t> getBloomFilterFilteringColumnKeys(const KeyCondition::RPN & rpn)
{
    std::unordered_set<std::size_t> column_keys;

    for (const auto & element : rpn)
    {
        if (auto bf_data = element.bloom_filter_data)
        {
            for (const auto index : bf_data->key_columns)
            {
                column_keys.insert(index);
            }
        }
    }

    return column_keys;
}

const parquet::ColumnDescriptor * getColumnDescriptorIfBloomFilterIsPresent(
    const std::unique_ptr<parquet::RowGroupMetaData> & parquet_rg_metadata,
    const std::vector<ArrowFieldIndexUtil::ClickHouseIndexToParquetIndex> & clickhouse_column_index_to_parquet_index,
    std::size_t clickhouse_column_index)
{
    if (clickhouse_column_index_to_parquet_index.size() <= clickhouse_column_index)
    {
        return nullptr;
    }

    const auto & parquet_indexes = clickhouse_column_index_to_parquet_index[clickhouse_column_index].parquet_indexes;

    // complex types like structs, tuples and maps will have more than one index.
    // we don't support those for now
    if (parquet_indexes.size() > 1)
    {
        return nullptr;
    }

    if (parquet_indexes.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column maps to 0 parquet leaf columns, raise an issue and try the query with `input_format_parquet_bloom_filter_push_down=false`");
    }

    auto parquet_column_index = parquet_indexes[0];

    const auto * parquet_column_descriptor = parquet_rg_metadata->schema()->Column(parquet_column_index);

    bool column_has_bloom_filter = parquet_rg_metadata->ColumnChunk(parquet_column_index)->bloom_filter_offset().has_value();
    if (!column_has_bloom_filter)
    {
        return nullptr;
    }

    return parquet_column_descriptor;
}

ParquetBlockInputFormat::ParquetBlockInputFormat(
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings & format_settings_,
    FormatParserGroupPtr parser_group_,
    size_t min_bytes_for_seek_)
    : IInputFormat(header_, &buf)
    , format_settings(format_settings_)
    , skip_row_groups(format_settings.parquet.skip_row_groups)
    , parser_group(std::move(parser_group_))
    , min_bytes_for_seek(min_bytes_for_seek_)
    , pending_chunks(PendingChunk::Compare { .row_group_first = format_settings_.parquet.preserve_order })
    , previous_block_missing_values(getPort().getHeader().columns())
{
    use_thread_pool = parser_group->max_parsing_threads > 1;

    bool row_group_prefetch =
        !use_thread_pool && parser_group->max_io_threads > 0 &&
        format_settings.parquet.enable_row_group_prefetch &&
        !format_settings.parquet.use_native_reader;
    if (row_group_prefetch)
        io_pool = std::make_shared<ThreadPool>(
            CurrentMetrics::IOThreads, CurrentMetrics::IOThreadsActive, CurrentMetrics::IOThreadsScheduled,
            parser_group->getIOThreadsPerReader());
}

ParquetBlockInputFormat::~ParquetBlockInputFormat()
{
    is_stopped = true;
    if (use_thread_pool)
        shutdown->shutdown();
    if (io_pool)
        io_pool->wait();
}

void ParquetBlockInputFormat::initializeIfNeeded()
{
    if (std::exchange(is_initialized, true))
        return;

    std::call_once(parser_group->init_flag, [&]
        {
            parser_group->initKeyCondition(getPort().getHeader());

            if (use_thread_pool)
                parser_group->parsing_runner.initThreadPool(
                    getFormatParsingThreadPool().get(), parser_group->max_parsing_threads, "ParquetDecoder", CurrentThread::getGroup());
        });

    // Create arrow file adapter.
    arrow_file = asArrowFile(*in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true, io_pool);

    if (is_stopped)
        return;

    metadata = parquet::ReadMetaData(arrow_file);
    const bool prefetch_group = io_pool != nullptr;

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    ArrowFieldIndexUtil field_util(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns);

    auto index_mapping = field_util.findRequiredIndices(getPort().getHeader(), *schema, *metadata);

    for (const auto & [clickhouse_header_index, parquet_indexes] : index_mapping)
    {
        for (auto parquet_index : parquet_indexes)
        {
            column_indices.push_back(parquet_index);
        }
    }

    int num_row_groups = metadata->num_row_groups();
    if (num_row_groups == 0)
    {
        return;
    }

    const auto bf_reader_properties = parquet::default_reader_properties();
    std::unique_ptr<parquet::BloomFilterReader> bf_reader;

    prefetch_group ? row_group_batches.reserve(1) : row_group_batches.reserve(num_row_groups);

    auto adaptive_chunk_size = [&](int row_group_idx) -> size_t
    {
        size_t total_size = 0;
        auto row_group_meta = metadata->RowGroup(row_group_idx);
        for (int column_index : column_indices)
        {
            total_size += row_group_meta->ColumnChunk(column_index)->total_uncompressed_size();
        }
        if (!total_size || !format_settings.parquet.prefer_block_bytes) return 0;
        auto average_row_bytes = floor(static_cast<double>(total_size) / row_group_meta->num_rows());
        // avoid inf preferred_num_rows;
        if (average_row_bytes < 1) return 0;
        const size_t preferred_num_rows = static_cast<size_t>(floor(format_settings.parquet.prefer_block_bytes/average_row_bytes));
        const size_t MIN_ROW_NUM = 128;
        // size_t != UInt64 in darwin
        return std::min(std::max(preferred_num_rows, MIN_ROW_NUM), static_cast<size_t>(format_settings.parquet.max_block_size));
    };

    std::unordered_set<std::size_t> filtering_columns;

    std::unique_ptr<KeyCondition> key_condition_with_bloom_filter_data;

    if (parser_group->key_condition)
    {
        key_condition_with_bloom_filter_data = std::make_unique<KeyCondition>(*parser_group->key_condition);

        if (format_settings.parquet.bloom_filter_push_down)
        {
            bf_reader = parquet::BloomFilterReader::Make(arrow_file, metadata, bf_reader_properties, nullptr);

            auto hash_one = [&](size_t column_idx, const Field & f) -> std::optional<uint64_t>
            {
                const auto * parquet_column_descriptor
                    = getColumnDescriptorIfBloomFilterIsPresent(metadata->RowGroup(0), index_mapping, column_idx);

                if (!parquet_column_descriptor)
                {
                    return std::nullopt;
                }

                return parquetTryHashField(f, parquet_column_descriptor);
            };

            auto hash_many = [&](size_t column_idx, const ColumnPtr & column) -> std::optional<std::vector<uint64_t>>
            {
                const auto * parquet_column_descriptor
                    = getColumnDescriptorIfBloomFilterIsPresent(metadata->RowGroup(0), index_mapping, column_idx);

                if (!parquet_column_descriptor)
                {
                    return std::nullopt;
                }

                auto nested_column = column;

                if (const auto & nullable_column = checkAndGetColumn<ColumnNullable>(column.get()))
                {
                    nested_column = nullable_column->getNestedColumnPtr();
                }

                return parquetTryHashColumn(nested_column.get(), parquet_column_descriptor);
            };

            key_condition_with_bloom_filter_data->prepareBloomFilterData(hash_one, hash_many);

            filtering_columns = getBloomFilterFilteringColumnKeys(key_condition_with_bloom_filter_data->getRPN());
        }
    }

    auto skip_row_group_based_on_filters = [&](int row_group)
    {
        if (!format_settings.parquet.filter_push_down && !format_settings.parquet.bloom_filter_push_down)
        {
            return false;
        }

        KeyCondition::ColumnIndexToBloomFilter column_index_to_bloom_filter;

        const auto & header = getPort().getHeader();

        std::vector<Range> hyperrectangle(header.columns(), Range::createWholeUniverse());

        if (format_settings.parquet.filter_push_down)
        {
            hyperrectangle = getHyperrectangleForRowGroup(*metadata, row_group, header, format_settings);
        }

        if (format_settings.parquet.bloom_filter_push_down)
        {
            column_index_to_bloom_filter = buildColumnIndexToBF(*bf_reader, row_group, index_mapping, filtering_columns);
        }

        bool maybe_exists = key_condition_with_bloom_filter_data->checkInHyperrectangle(hyperrectangle, getPort().getHeader().getDataTypes(), column_index_to_bloom_filter).can_be_true;

        return !maybe_exists;
    };

    for (int row_group = 0; row_group < num_row_groups; ++row_group)
    {
        if (skip_row_groups.contains(row_group))
            continue;

        if (key_condition_with_bloom_filter_data && skip_row_group_based_on_filters(row_group))
        {
            ProfileEvents::increment(ProfileEvents::ParquetPrunedRowGroups);
            continue;
        }

        // When single-threaded parsing, can prefetch row groups, so need to put all row groups in the same row_group_batch
        if (row_group_batches.empty() || (!prefetch_group && row_group_batches.back().total_bytes_compressed >= min_bytes_for_seek))
            row_group_batches.emplace_back();

        ProfileEvents::increment(ProfileEvents::ParquetReadRowGroups);
        row_group_batches.back().row_groups_idxs.push_back(row_group);
        row_group_batches.back().total_rows += metadata->RowGroup(row_group)->num_rows();
        auto row_group_size = metadata->RowGroup(row_group)->total_compressed_size();
        row_group_batches.back().row_group_sizes.push_back(row_group_size);
        row_group_batches.back().total_bytes_compressed += row_group_size;
        auto rows = adaptive_chunk_size(row_group);
        row_group_batches.back().adaptive_chunk_size = rows ? rows : format_settings.parquet.max_block_size;
    }
}

void ParquetBlockInputFormat::initializeRowGroupBatchReader(size_t row_group_batch_idx)
{
    const bool row_group_prefetch = io_pool != nullptr;
    auto & row_group_batch = row_group_batches[row_group_batch_idx];

    parquet::ArrowReaderProperties arrow_properties;
    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    arrow_properties.set_use_threads(false);
    arrow_properties.set_batch_size(row_group_batch.adaptive_chunk_size);

    // When reading a row group, arrow will:
    //  1. Look at `metadata` to get all byte ranges it'll need to read from the file (typically one
    //     per requested column in the row group).
    //  2. Coalesce ranges that are close together, trading off seeks vs read amplification.
    //     This is controlled by CacheOptions.
    //  3. Process the columns one by one, issuing the corresponding (coalesced) range reads as
    //     needed. Each range gets its own memory buffer allocated. These buffers stay in memory
    //     (in arrow::io::internal::ReadRangeCache) until the whole row group reading is done.
    //     So the memory usage of a "SELECT *" will be at least the compressed size of a row group
    //     (typically hundreds of MB).
    //
    // With this coalescing, we don't need any readahead on our side, hence avoid_buffering in
    // asArrowFile().
    //
    // This adds one unnecessary copy. We should probably do coalescing and prefetch scheduling on
    // our side instead.
    arrow::io::CacheOptions cache_options;

    if (row_group_prefetch)
    {
        // Manual prefetch via RowGroupPrefetchIterator
        arrow_properties.set_pre_buffer(false);
        cache_options = arrow::io::CacheOptions::Defaults();
    }
    else
    {
        arrow_properties.set_pre_buffer(true);
        cache_options = arrow::io::CacheOptions::LazyDefaults();
    }
    cache_options.hole_size_limit = min_bytes_for_seek;
    cache_options.range_size_limit = 1l << 40; // reading the whole row group at once is fine
    arrow_properties.set_cache_options(cache_options);

    // Workaround for a workaround in the parquet library.
    //
    // From ComputeColumnChunkRange() in contrib/arrow/cpp/src/parquet/file_reader.cc:
    //  > The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    //  > dictionary page header size in total_compressed_size and total_uncompressed_size
    //  > (see IMPALA-694). We add padding to compensate.
    //
    // That padding breaks the pre-buffered mode because the padded read ranges may overlap each
    // other, failing an assert. So we disable pre-buffering in this case.
    // That version is >10 years old, so this is not very important.
    if (metadata->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
        arrow_properties.set_pre_buffer(false);

    if (format_settings.parquet.use_native_reader)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
        if constexpr (std::endian::native != std::endian::little)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "parquet native reader only supports little endian system currently");
#pragma clang diagnostic pop

        row_group_batch.native_record_reader = std::make_shared<ParquetRecordReader>(
            getPort().getHeader(),
            arrow_properties,
            reader_properties,
            arrow_file,
            format_settings,
            row_group_batch.row_groups_idxs);
    }
    else
    {
        parquet::arrow::FileReaderBuilder builder;
        THROW_ARROW_NOT_OK(builder.Open(arrow_file, reader_properties, metadata));
        builder.properties(arrow_properties);
        builder.memory_pool(ArrowMemoryPool::instance());
        // should get raw reader before build, raw_reader will set null after build
        auto * parquet_file_reader = builder.raw_reader();
        THROW_ARROW_NOT_OK(builder.Build(&row_group_batch.file_reader));
        if (row_group_prefetch)
        {
            row_group_batch.prefetch_iterator = std::make_unique<RowGroupPrefetchIterator>(parquet_file_reader, row_group_batch, column_indices, min_bytes_for_seek);
            row_group_batch.record_batch_reader = row_group_batch.prefetch_iterator->nextRowGroupReader();
        }
        else
        {
            Stopwatch fetch_wait_time;
            THROW_ARROW_NOT_OK(
                row_group_batch.file_reader->GetRecordBatchReader(row_group_batch.row_groups_idxs, column_indices, &row_group_batch.record_batch_reader));
            increment(ProfileEvents::ParquetFetchWaitTimeMicroseconds, fetch_wait_time.elapsedMicroseconds());
        }
        row_group_batch.arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
            getPort().getHeader(),
            "Parquet",
            format_settings,
            format_settings.parquet.allow_missing_columns,
            format_settings.null_as_default,
            format_settings.date_time_overflow_behavior,
            format_settings.parquet.allow_geoparquet_parser,
            format_settings.parquet.case_insensitive_column_matching,
            false, /* is_stream_ */
            format_settings.parquet.enable_json_parsing);
    }
}

void ParquetBlockInputFormat::scheduleRowGroup(size_t row_group_batch_idx)
{
    chassert(!mutex.try_lock());

    auto & status = row_group_batches[row_group_batch_idx].status;
    chassert(status == RowGroupBatchState::Status::NotStarted || status == RowGroupBatchState::Status::Paused);

    status = RowGroupBatchState::Status::Running;

    parser_group->parsing_runner(
        [this, row_group_batch_idx, shutdown_ = shutdown]()
        {
            std::shared_lock shutdown_lock(*shutdown_, std::try_to_lock);
            if (!shutdown_lock.owns_lock())
                return;

            try
            {
                threadFunction(row_group_batch_idx);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                background_exception = std::current_exception();
                condvar.notify_all();
            }
        });
}

void ParquetBlockInputFormat::threadFunction(size_t row_group_batch_idx)
{
    std::unique_lock lock(mutex);

    auto & row_group_batch = row_group_batches[row_group_batch_idx];
    chassert(row_group_batch.status == RowGroupBatchState::Status::Running);

    while (true)
    {
        if (is_stopped || row_group_batch.num_pending_chunks >= max_pending_chunks_per_row_group_batch)
        {
            row_group_batch.status = RowGroupBatchState::Status::Paused;
            return;
        }

        decodeOneChunk(row_group_batch_idx, lock);

        if (row_group_batch.status == RowGroupBatchState::Status::Done)
            return;
    }
}
std::shared_ptr<arrow::RecordBatchReader> ParquetBlockInputFormat::RowGroupPrefetchIterator::nextRowGroupReader()
{
    if (prefetched_row_groups.empty()) return nullptr;
    std::shared_ptr<arrow::RecordBatchReader> reader;
    Stopwatch fetch_wait_time;
    // GetRecordBatchReader will block until the data is ready.
    // Only the corresponding objects will be created, and no data parsing will be performed.
    THROW_ARROW_NOT_OK(row_group_batch.file_reader->GetRecordBatchReader(prefetched_row_groups, column_indices, &reader));
    prefetched_row_groups.clear();
    // Start to prefetch next row groups
    prefetchNextRowGroups();
    increment(ProfileEvents::ParquetFetchWaitTimeMicroseconds, fetch_wait_time.elapsedMicroseconds());
    return reader;
}

void ParquetBlockInputFormat::RowGroupPrefetchIterator::prefetchNextRowGroups()
{
    if (next_row_group_idx < row_group_batch.row_groups_idxs.size())
    {
        size_t total_bytes_compressed = 0;
        // Merge small row groups
        while (next_row_group_idx < row_group_batch.row_groups_idxs.size() &&
               total_bytes_compressed < min_bytes_for_seek)
        {
            total_bytes_compressed += row_group_batch.row_group_sizes[next_row_group_idx];
            prefetched_row_groups.emplace_back(row_group_batch.row_groups_idxs[next_row_group_idx]);
            ++next_row_group_idx;
        }
        file_reader->PreBuffer(prefetched_row_groups, column_indices,
            row_group_batch.file_reader->properties().io_context(), row_group_batch.file_reader->properties().cache_options());
    }
}

void ParquetBlockInputFormat::decodeOneChunk(size_t row_group_batch_idx, std::unique_lock<std::mutex> & lock)
{
    auto & row_group_batch = row_group_batches[row_group_batch_idx];
    chassert(row_group_batch.status != RowGroupBatchState::Status::Done);
    chassert(lock.owns_lock());
    SCOPE_EXIT({ chassert(lock.owns_lock() || std::uncaught_exceptions()); });

    lock.unlock();

    auto end_of_row_group = [&] {
        row_group_batch.native_record_reader.reset();
        row_group_batch.arrow_column_to_ch_column.reset();
        row_group_batch.record_batch_reader.reset();
        row_group_batch.file_reader.reset();

        lock.lock();
        row_group_batch.status = RowGroupBatchState::Status::Done;

        // We may be able to schedule more work now, but can't call scheduleMoreWorkIfNeeded() right
        // here because we're running on the same thread pool, so it'll deadlock if thread limit is
        // reached. Wake up read() instead.
        condvar.notify_all();
    };

    auto get_approx_original_chunk_size = [&](size_t num_rows)
    {
        return static_cast<size_t>(std::ceil(static_cast<double>(row_group_batch.total_bytes_compressed) / row_group_batch.total_rows * num_rows));
    };

    if (!row_group_batch.record_batch_reader && !row_group_batch.native_record_reader)
        initializeRowGroupBatchReader(row_group_batch_idx);

    PendingChunk res(getPort().getHeader().columns());
    res.chunk_idx = row_group_batch.next_chunk_idx;
    res.row_group_batch_idx = row_group_batch_idx;

    if (format_settings.parquet.use_native_reader)
    {
        auto chunk = row_group_batch.native_record_reader->readChunk();
        if (!chunk)
        {
            end_of_row_group();
            return;
        }

        /// TODO: support defaults_for_omitted_fields feature when supporting nested columns
        res.approx_original_chunk_size = get_approx_original_chunk_size(chunk.getNumRows());
        res.chunk = std::move(chunk);
    }
    else
    {
        auto fetchBatch = [&]
        {
            chassert(row_group_batch.record_batch_reader);
            auto batch = row_group_batch.record_batch_reader->Next();
            if (!batch.ok())
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());
            return batch;
        };

        auto batch = fetchBatch();
        if (!*batch && row_group_batch.prefetch_iterator)
        {
            row_group_batch.record_batch_reader = row_group_batch.prefetch_iterator->nextRowGroupReader();
            if (row_group_batch.record_batch_reader)
            {
                batch = fetchBatch();
            }
        }

        if (!*batch || !row_group_batch.record_batch_reader)
        {
            end_of_row_group();
            return;
        }

        auto tmp_table = arrow::Table::FromRecordBatches({*batch});

        /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
        /// Otherwise fill the missing columns with zero values of its type.
        BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &res.block_missing_values : nullptr;
        res.approx_original_chunk_size = get_approx_original_chunk_size((*tmp_table)->num_rows());
        res.chunk = row_group_batch.arrow_column_to_ch_column->arrowTableToCHChunk(*tmp_table, (*tmp_table)->num_rows(), metadata->key_value_metadata(), block_missing_values_ptr);
    }

    lock.lock();

    ++row_group_batch.next_chunk_idx;
    ++row_group_batch.num_pending_chunks;
    pending_chunks.push(std::move(res));
    condvar.notify_all();
}

void ParquetBlockInputFormat::scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_batch_touched)
{
    while (row_group_batches_completed < row_group_batches.size())
    {
        auto & row_group = row_group_batches[row_group_batches_completed];
        if (row_group.status != RowGroupBatchState::Status::Done || row_group.num_pending_chunks != 0)
            break;
        ++row_group_batches_completed;
    }

    if (use_thread_pool)
    {
        size_t max_decoding_threads = parser_group->getParsingThreadsPerReader();
        while (row_group_batches_started - row_group_batches_completed < max_decoding_threads &&
               row_group_batches_started < row_group_batches.size())
            scheduleRowGroup(row_group_batches_started++);

        if (row_group_batch_touched)
        {
            auto & row_group = row_group_batches[*row_group_batch_touched];
            if (row_group.status == RowGroupBatchState::Status::Paused &&
                row_group.num_pending_chunks < max_pending_chunks_per_row_group_batch)
                scheduleRowGroup(*row_group_batch_touched);
        }
    }
}

Chunk ParquetBlockInputFormat::read()
{
    initializeIfNeeded();

    if (is_stopped || row_group_batches_completed == row_group_batches.size())
        return {};

    if (need_only_count)
        return getChunkForCount(row_group_batches[row_group_batches_completed++].total_rows);

    std::unique_lock lock(mutex);

    while (true)
    {
        if (background_exception)
        {
            is_stopped = true;
            std::rethrow_exception(background_exception);
        }
        if (is_stopped)
            return {};

        scheduleMoreWorkIfNeeded();

        if (!pending_chunks.empty() &&
            (!format_settings.parquet.preserve_order ||
             pending_chunks.top().row_group_batch_idx == row_group_batches_completed))
        {
            PendingChunk chunk = std::move(const_cast<PendingChunk&>(pending_chunks.top()));
            pending_chunks.pop();

            auto & row_group = row_group_batches[chunk.row_group_batch_idx];
            chassert(row_group.num_pending_chunks != 0);
            chassert(chunk.chunk_idx == row_group.next_chunk_idx - row_group.num_pending_chunks);
            --row_group.num_pending_chunks;

            scheduleMoreWorkIfNeeded(chunk.row_group_batch_idx);

            previous_block_missing_values = std::move(chunk.block_missing_values);
            previous_approx_bytes_read_for_chunk = chunk.approx_original_chunk_size;
            return std::move(chunk.chunk);
        }

        if (row_group_batches_completed == row_group_batches.size())
            return {};

        if (use_thread_pool)
            condvar.wait(lock);
        else
            decodeOneChunk(row_group_batches_completed, lock);
    }
}

void ParquetBlockInputFormat::resetParser()
{
    is_stopped = true;
    if (use_thread_pool)
    {
        shutdown->shutdown();
        shutdown = std::make_shared<ShutdownHelper>();
    }

    arrow_file.reset();
    metadata.reset();
    column_indices.clear();
    row_group_batches.clear();
    while (!pending_chunks.empty())
        pending_chunks.pop();
    row_group_batches_completed = 0;
    previous_block_missing_values.clear();
    row_group_batches_started = 0;
    background_exception = nullptr;

    is_stopped = false;
    is_initialized = false;

    IInputFormat::resetParser();
}

const BlockMissingValues * ParquetBlockInputFormat::getMissingValues() const
{
    return &previous_block_missing_values;
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

void ParquetSchemaReader::initializeIfNeeded()
{
    if (arrow_file)
        return;

    std::atomic<int> is_stopped{0};
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
    metadata = parquet::ReadMetaData(arrow_file);
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    initializeIfNeeded();

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    /// When Parquet's schema is converted to Arrow's schema, logical types are lost (at least in
    /// the currently used Arrow 11 version). Therefore, we manually add the logical types as metadata
    /// to Arrow's schema. Logical types are useful for determining which ClickHouse column type to convert to.
    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    new_fields.reserve(schema->num_fields());

    for (int i = 0; i < schema->num_fields(); ++i)
    {
        auto field = schema->field(i);
        const auto * parquet_node = metadata->schema()->Column(i);
        const auto * lt = parquet_node->logical_type().get();

        if (lt and !lt->is_invalid())
        {
            std::shared_ptr<arrow::KeyValueMetadata> kv = field->HasMetadata() ? field->metadata()->Copy() : arrow::key_value_metadata({}, {});
            THROW_ARROW_NOT_OK(kv->Set("PARQUET:logical_type", lt->ToString()));

            field = field->WithMetadata(std::move(kv));
        }
        new_fields.emplace_back(std::move(field));
    }

    schema = arrow::schema(std::move(new_fields));

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        metadata->key_value_metadata(),
        "Parquet",
        format_settings,
        format_settings.parquet.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_geoparquet_parser,
        format_settings.parquet.enable_json_parsing);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> ParquetSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return metadata->num_rows();
}

void registerInputFormatParquet(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
            "Parquet",
            [](ReadBuffer & buf,
               const Block & sample,
               const FormatSettings & settings,
               const ReadSettings & read_settings,
               bool is_remote_fs,
               FormatParserGroupPtr parser_group) -> InputFormatPtr
            {
                size_t min_bytes_for_seek = is_remote_fs ? read_settings.remote_read_min_bytes_for_seek : settings.parquet.local_read_min_bytes_for_seek;
                auto ptr = std::make_shared<ParquetBlockInputFormat>(
                    buf,
                    sample,
                    settings,
                    std::move(parser_group),
                    min_bytes_for_seek);
                return ptr;
            });
    factory.markFormatSupportsSubsetOfColumns("Parquet");
}

void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ParquetSchemaReader>(buf, settings);
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
void registerInputFormatParquet(FormatFactory &)
{
}

void registerParquetSchemaReader(FormatFactory &) {}
}

#endif
