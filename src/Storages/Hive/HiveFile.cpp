#include <Storages/Hive/HiveFile.h>

#if USE_HIVE

#include <boost/algorithm/string/case_conv.hpp>
#include <arrow/io/memory.h>
#include <arrow/io/api.h>
#include <arrow/api.h>
#include <arrow/status.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include <orc/Statistics.hh>

#include <fmt/core.h>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (const ::arrow::Status & _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)


template <class FieldType, class StatisticsType>
Range createRangeFromOrcStatistics(const StatisticsType * stats)
{
    /// Null values or NaN/Inf values of double type.
    if (stats->hasMinimum() && stats->hasMaximum())
    {
        return Range(FieldType(stats->getMinimum()), true, FieldType(stats->getMaximum()), true);
    }
    else if (stats->hasMinimum())
    {
        return Range::createLeftBounded(FieldType(stats->getMinimum()), true);
    }
    else if (stats->hasMaximum())
    {
        return Range::createRightBounded(FieldType(stats->getMaximum()), true);
    }
    else
    {
        return Range();
    }
}

template <class FieldType, class StatisticsType>
Range createRangeFromParquetStatistics(std::shared_ptr<StatisticsType> stats)
{
    /// We must check if there are minimum or maximum values in statistics in case of
    /// null values or NaN/Inf values of double type.
    if (!stats->HasMinMax())
        return Range();
    return Range(FieldType(stats->min()), true, FieldType(stats->max()), true);
}

Range createRangeFromParquetStatistics(std::shared_ptr<parquet::ByteArrayStatistics> stats)
{
    if (!stats->HasMinMax())
        return Range();
    String min_val(reinterpret_cast<const char *>(stats->min().ptr), stats->min().len);
    String max_val(reinterpret_cast<const char *>(stats->max().ptr), stats->max().len);
    return Range(min_val, true, max_val, true);
}

std::optional<size_t> IHiveFile::getRows()
{
    if (!has_init_rows)
    {
        std::lock_guard lock(mutex);
        if (!has_init_rows)
        {
            rows = getRowsImpl();
            has_init_rows = true;
        }
    }
    return rows;
}

void IHiveFile::loadFileMinMaxIndex()
{
    if (file_minmax_idx_loaded)
        return;
    std::lock_guard lock(mutex);
    if (file_minmax_idx_loaded)
        return;
    loadFileMinMaxIndexImpl();
    file_minmax_idx_loaded = true;
}

void IHiveFile::loadSplitMinMaxIndexes()
{
    if (split_minmax_idxes_loaded)
        return;
    std::lock_guard lock(mutex);
    if (split_minmax_idxes_loaded)
        return;
    loadSplitMinMaxIndexesImpl();
    split_minmax_idxes_loaded = true;
}

Range HiveORCFile::buildRange(const orc::ColumnStatistics * col_stats)
{
    if (!col_stats || col_stats->hasNull())
        return {};

    if (const auto * int_stats = dynamic_cast<const orc::IntegerColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<Int64>(int_stats);
    }
    else if (const auto * double_stats = dynamic_cast<const orc::DoubleColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<Float64>(double_stats);
    }
    else if (const auto * string_stats = dynamic_cast<const orc::StringColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<String>(string_stats);
    }
    else if (const auto * bool_stats = dynamic_cast<const orc::BooleanColumnStatistics *>(col_stats))
    {
        auto false_cnt = bool_stats->getFalseCount();
        auto true_cnt = bool_stats->getTrueCount();
        if (false_cnt && true_cnt)
        {
            return Range(UInt8(0), true, UInt8(1), true);
        }
        else if (false_cnt)
        {
            return Range::createLeftBounded(UInt8(0), true);
        }
        else if (true_cnt)
        {
            return Range::createRightBounded(UInt8(1), true);
        }
    }
    else if (const auto * timestamp_stats = dynamic_cast<const orc::TimestampColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<UInt32>(timestamp_stats);
    }
    else if (const auto * date_stats = dynamic_cast<const orc::DateColumnStatistics *>(col_stats))
    {
        return createRangeFromOrcStatistics<UInt16>(date_stats);
    }
    return {};
}

void HiveORCFile::prepareReader()
{
    in = std::make_unique<ReadBufferFromHDFS>(namenode_url, path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
    auto format_settings = getFormatSettings(getContext());
    std::atomic<int> is_stopped{0};
    auto result = arrow::adapters::orc::ORCFileReader::Open(asArrowFile(*in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES), arrow::default_memory_pool());
    THROW_ARROW_NOT_OK(result.status());
    reader = std::move(result).ValueOrDie();
}

void HiveORCFile::prepareColumnMapping()
{
    const orc::Type & type = reader->GetRawORCReader()->getType();
    size_t count = type.getSubtypeCount();
    for (size_t pos = 0; pos < count; pos++)
    {
        /// Column names in hive is case-insensitive.
        String column{type.getFieldName(pos)};
        boost::to_lower(column);
        orc_column_positions[column] = pos;
    }
}

bool HiveORCFile::useFileMinMaxIndex() const
{
    return storage_settings->enable_orc_file_minmax_index;
}


std::unique_ptr<IMergeTreeDataPart::MinMaxIndex> HiveORCFile::buildMinMaxIndex(const orc::Statistics * statistics)
{
    if (!statistics)
        return nullptr;

    size_t range_num = index_names_and_types.size();
    auto idx = std::make_unique<IMergeTreeDataPart::MinMaxIndex>();
    idx->hyperrectangle.resize(range_num);

    size_t i = 0;
    for (const auto & name_type : index_names_and_types)
    {
        String column{name_type.name};
        boost::to_lower(column);
        auto it = orc_column_positions.find(column);
        if (it == orc_column_positions.end())
        {
            idx->hyperrectangle[i] = buildRange(nullptr);
        }
        else
        {
            size_t pos = it->second;
            /// Attention: column statistics start from 1. 0 has special purpose.
            const orc::ColumnStatistics * col_stats = statistics->getColumnStatistics(pos + 1);
            idx->hyperrectangle[i] = buildRange(col_stats);
        }
        ++i;
    }
    idx->initialized = true;
    return idx;
}

void HiveORCFile::loadFileMinMaxIndexImpl()
{
    if (!reader)
    {
        prepareReader();
        prepareColumnMapping();
    }

    auto statistics = reader->GetRawORCReader()->getStatistics();
    file_minmax_idx = buildMinMaxIndex(statistics.get());
}

bool HiveORCFile::useSplitMinMaxIndex() const
{
    return storage_settings->enable_orc_stripe_minmax_index;
}


void HiveORCFile::loadSplitMinMaxIndexesImpl()
{
    if (!reader)
    {
        prepareReader();
        prepareColumnMapping();
    }

    auto * raw_reader = reader->GetRawORCReader();
    auto stripe_num = raw_reader->getNumberOfStripes();
    auto stripe_stats_num = raw_reader->getNumberOfStripeStatistics();
    if (stripe_num != stripe_stats_num)
        throw Exception(
            fmt::format("orc file:{} has different strip num {} and strip statistics num {}", path, stripe_num, stripe_stats_num),
            ErrorCodes::BAD_ARGUMENTS);

    split_minmax_idxes.resize(stripe_num);
    for (size_t i = 0; i < stripe_num; ++i)
    {
        auto stripe_stats = raw_reader->getStripeStatistics(i);
        split_minmax_idxes[i] = buildMinMaxIndex(stripe_stats.get());
    }
}

std::optional<size_t> HiveORCFile::getRowsImpl()
{
    if (!reader)
    {
        prepareReader();
        prepareColumnMapping();
    }

    auto * raw_reader = reader->GetRawORCReader();
    return raw_reader->getNumberOfRows();
}

bool HiveParquetFile::useSplitMinMaxIndex() const
{
    return storage_settings->enable_parquet_rowgroup_minmax_index;
}

void HiveParquetFile::prepareReader()
{
    in = std::make_unique<ReadBufferFromHDFS>(namenode_url, path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
    auto format_settings = getFormatSettings(getContext());
    std::atomic<int> is_stopped{0};
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(asArrowFile(*in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES), arrow::default_memory_pool(), &reader));
}

void HiveParquetFile::loadSplitMinMaxIndexesImpl()
{
    if (!reader)
        prepareReader();

    auto meta = reader->parquet_reader()->metadata();
    size_t num_cols = meta->num_columns();
    size_t num_row_groups = meta->num_row_groups();
    const auto * schema = meta->schema();
    for (size_t pos = 0; pos < num_cols; ++pos)
    {
        String column{schema->Column(pos)->name()};
        boost::to_lower(column);
        parquet_column_positions[column] = pos;
    }


    split_minmax_idxes.resize(num_row_groups);
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        auto row_group_meta = meta->RowGroup(i);
        split_minmax_idxes[i] = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
        split_minmax_idxes[i]->hyperrectangle.resize(num_cols);

        size_t j = 0;
        auto it = index_names_and_types.begin();
        for (; it != index_names_and_types.end(); ++j, ++it)
        {
            String column{it->name};
            boost::to_lower(column);
            auto mit = parquet_column_positions.find(column);
            if (mit == parquet_column_positions.end())
                continue;

            size_t pos = mit->second;
            auto col_chunk = row_group_meta->ColumnChunk(pos);
            if (!col_chunk->is_stats_set())
                continue;

            auto stats = col_chunk->statistics();
            if (stats->HasNullCount() && stats->null_count() > 0)
                continue;

            if (auto bool_stats = std::dynamic_pointer_cast<parquet::BoolStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<UInt8>(bool_stats);
            }
            else if (auto int32_stats = std::dynamic_pointer_cast<parquet::Int32Statistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Int32>(int32_stats);
            }
            else if (auto int64_stats = std::dynamic_pointer_cast<parquet::Int64Statistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Int64>(int64_stats);
            }
            else if (auto float_stats = std::dynamic_pointer_cast<parquet::FloatStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Float64>(float_stats);
            }
            else if (auto double_stats = std::dynamic_pointer_cast<parquet::FloatStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics<Float64>(double_stats);
            }
            else if (auto string_stats = std::dynamic_pointer_cast<parquet::ByteArrayStatistics>(stats))
            {
                split_minmax_idxes[i]->hyperrectangle[j] = createRangeFromParquetStatistics(string_stats);
            }
            /// Other types are not supported for minmax index, skip
        }
        split_minmax_idxes[i]->initialized = true;
    }
}

std::optional<size_t> HiveParquetFile::getRowsImpl()
{
    if (!reader)
        prepareReader();

    auto meta = reader->parquet_reader()->metadata();
    return meta->num_rows();
}

}
#endif
