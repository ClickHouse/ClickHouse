#include <Storages/Hive/HiveFile.h>

#if USE_HIVE

#include <boost/algorithm/string/case_conv.hpp>
#include <arrow/io/memory.h>
#include <arrow/api.h>
#include <arrow/status.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <fmt/core.h>
#include <Common/Exception.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace HiveSetting
{
    extern const HiveSettingsBool enable_orc_file_minmax_index;
    extern const HiveSettingsBool enable_orc_stripe_minmax_index;
    extern const HiveSettingsBool enable_parquet_rowgroup_minmax_index;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <class FieldType, class T>
Range createRangeFromOrcStatistics(const std::optional<T> & minimum, const std::optional<T> & maximum)
{
    /// Null values or NaN/Inf values of double type.
    if (minimum && maximum)
        return Range(FieldType(*minimum), true, FieldType(*maximum), true);
    if (minimum)
        return Range::createLeftBounded(FieldType(*minimum), true);
    if (maximum)
        return Range::createRightBounded(FieldType(*maximum), true);
    return Range::createWholeUniverseWithoutNull();
}

template <class FieldType, class StatisticsType>
Range createRangeFromParquetStatistics(std::shared_ptr<StatisticsType> stats)
{
    /// We must check if there are minimum or maximum values in statistics in case of
    /// null values or NaN/Inf values of double type.
    if (!stats->HasMinMax())
        return Range::createWholeUniverseWithoutNull();
    return Range(FieldType(stats->min()), true, FieldType(stats->max()), true);
}

static Range createRangeFromParquetStatistics(std::shared_ptr<parquet::ByteArrayStatistics> stats)
{
    if (!stats->HasMinMax())
        return Range::createWholeUniverseWithoutNull();
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

Range HiveORCFile::buildRange(const ORC::Proto::ColumnStatistics * col_stats)
{
    if (!col_stats || col_stats->has_null.value_or(false))
        return Range::createWholeUniverseWithoutNull();

    if (const auto & int_stats = col_stats->int_statistics)
        return createRangeFromOrcStatistics<Int64>(int_stats->minimum, int_stats->maximum);
    if (const auto & double_stats = col_stats->double_statistics)
        return createRangeFromOrcStatistics<Float64>(double_stats->minimum, double_stats->maximum);
    if (const auto & string_stats = col_stats->string_statistics)
        return createRangeFromOrcStatistics<String>(string_stats->minimum, string_stats->maximum);
    if (const auto & bool_stats = col_stats->bucket_statistics)
    {
        if (bool_stats->count.empty())
            return Range::createWholeUniverseWithoutNull();
        UInt64 true_cnt = bool_stats->count[0];
        UInt64 false_cnt = col_stats->number_of_values.value_or(0) - true_cnt;
        if (false_cnt && true_cnt)
            return Range(UInt8(0), true, UInt8(1), true);
        if (false_cnt)
            return Range::createLeftBounded(UInt8(0), true);
        if (true_cnt)
            return Range::createRightBounded(UInt8(1), true);
        return Range::createWholeUniverseWithoutNull();
    }
    if (const auto & timestamp_stats = col_stats->timestamp_statistics)
    {
        auto minimum = timestamp_stats->minimum_utc ? timestamp_stats->minimum_utc : timestamp_stats->minimum;
        auto maximum = timestamp_stats->maximum_utc ? timestamp_stats->maximum_utc : timestamp_stats->maximum;
        return createRangeFromOrcStatistics<UInt32>(minimum, maximum);
    }
    if (const auto & date_stats = col_stats->date_statistics)
        return createRangeFromOrcStatistics<UInt16>(date_stats->minimum, date_stats->maximum);
    return Range::createWholeUniverseWithoutNull();
}

void HiveORCFile::prepareReader()
{
    in = std::make_unique<ReadBufferFromHDFS>(namenode_url, path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
    auto format_settings = getFormatSettings(getContext());
    std::atomic<int> is_stopped{0};
    reader = createORCReader(asORCInputStream(*in, format_settings, /*use_prefetch=*/false, is_stopped), ORC::ReaderOptions{});
}

void HiveORCFile::prepareColumnMapping()
{
    const ORC::Type & type = reader->getType();
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
    return (*storage_settings)[HiveSetting::enable_orc_file_minmax_index];
}


std::unique_ptr<IMergeTreeDataPart::MinMaxIndex> HiveORCFile::buildMinMaxIndex(const std::vector<ORC::Proto::ColumnStatistics> * statistics)
{
    if (!statistics)
        return nullptr;

    size_t range_num = index_names_and_types.size();
    auto idx = std::make_unique<IMergeTreeDataPart::MinMaxIndex>();
    idx->hyperrectangle.resize(range_num, Range::createWholeUniverseWithoutNull());

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
            const ORC::Proto::ColumnStatistics * col_stats = pos + 1 < statistics->size() ? &(*statistics)[pos + 1] : nullptr;
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

    file_minmax_idx = buildMinMaxIndex(&reader->getStatistics());
}

bool HiveORCFile::useSplitMinMaxIndex() const
{
    return (*storage_settings)[HiveSetting::enable_orc_stripe_minmax_index];
}


void HiveORCFile::loadSplitMinMaxIndexesImpl()
{
    if (!reader)
    {
        prepareReader();
        prepareColumnMapping();
    }

    auto * raw_reader = reader.get();
    auto stripe_num = raw_reader->getNumberOfStripes();
    const auto & stripe_statistics = raw_reader->getMetadata().stripe_stats;
    auto stripe_stats_num = stripe_statistics.size();
    if (stripe_num != stripe_stats_num)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "orc file:{} has different strip num {} and strip statistics num {}", path, stripe_num, stripe_stats_num);

    split_minmax_idxes.resize(stripe_num);
    for (size_t i = 0; i < stripe_num; ++i)
    {
        split_minmax_idxes[i] = buildMinMaxIndex(&stripe_statistics[i].col_stats);
    }
}

std::optional<size_t> HiveORCFile::getRowsImpl()
{
    if (!reader)
    {
        prepareReader();
        prepareColumnMapping();
    }

    auto * raw_reader = reader.get();
    return raw_reader->getNumberOfRows();
}

bool HiveParquetFile::useSplitMinMaxIndex() const
{
    return (*storage_settings)[HiveSetting::enable_parquet_rowgroup_minmax_index];
}

void HiveParquetFile::prepareReader()
{
    in = std::make_unique<ReadBufferFromHDFS>(namenode_url, path, getContext()->getGlobalContext()->getConfigRef(), getContext()->getReadSettings());
    auto format_settings = getFormatSettings(getContext());
    std::atomic<int> is_stopped{0};
    auto open_file_res = parquet::arrow::OpenFile(asArrowFile(*in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES), ArrowMemoryPool::instance());
    if (!open_file_res.ok())
        throwFromArrowStatus(open_file_res.status(), ErrorCodes::BAD_ARGUMENTS, "Failed to open Parquet file");
    reader = *std::move(open_file_res);
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
        String column{schema->Column(static_cast<int>(pos))->name()};
        boost::to_lower(column);
        parquet_column_positions[column] = pos;
    }


    split_minmax_idxes.resize(num_row_groups);
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        auto row_group_meta = meta->RowGroup(static_cast<int>(i));
        split_minmax_idxes[i] = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
        split_minmax_idxes[i]->hyperrectangle.resize(num_cols, Range::createWholeUniverseWithoutNull());

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
            auto col_chunk = row_group_meta->ColumnChunk(static_cast<int>(pos));
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
