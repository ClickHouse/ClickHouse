#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/config.h>

#if USE_HDFS
#include <memory>

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/core.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <orc/OrcFile.hh>
#include <orc/Reader.hh>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/Hive/HiveFile.h>

namespace DB
{

template <class T, class S>
Range createRangeFromOrcStatistics(const S * stats)
{
    if (stats->hasMinimum() && stats->hasMaximum())
    {
        return Range(T(stats->getMinimum()), true, T(stats->getMaximum()), true);
    }
    else if (stats->hasMinimum())
    {
        return Range::createLeftBounded(T(stats->getMinimum()), true);
    }
    else if (stats->hasMaximum())
    {
        return Range::createRightBounded(T(stats->getMaximum()), true);
    }
    else
    {
        return Range();
    }
}

template <class T, class S>
Range createRangeFromParquetStatistics(std::shared_ptr<S> stats)
{
    if (!stats->HasMinMax())
        return Range();
    return Range(T(stats->min()), true, T(stats->max()), true);
}

Range createRangeFromParquetStatistics(std::shared_ptr<parquet::ByteArrayStatistics> stats)
{
    if (!stats->HasMinMax())
        return Range();
    String min_val(reinterpret_cast<const char *>(stats->min().ptr), stats->min().len);
    String max_val(reinterpret_cast<const char *>(stats->max().ptr), stats->max().len);
    return Range(min_val, true, max_val, true);
}

Range HiveOrcFile::buildRange(const orc::ColumnStatistics * col_stats)
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

void HiveOrcFile::prepareReader()
{
    // TODO To be implemented
}

void HiveOrcFile::prepareColumnMapping()
{
    // TODO To be implemented
}

bool HiveOrcFile::hasMinMaxIndex() const
{
    return false;
}


std::unique_ptr<IMergeTreeDataPart::MinMaxIndex> HiveOrcFile::buildMinMaxIndex(const orc::Statistics * /*statistics*/)
{
    // TODO To be implemented
    return {};
}


void HiveOrcFile::loadMinMaxIndex()
{
    // TODO To be implemented
}

bool HiveOrcFile::hasSubMinMaxIndex() const
{
    return false;
}


void HiveOrcFile::loadSubMinMaxIndex()
{
    // TODO To be implemented
}

bool HiveParquetFile::hasSubMinMaxIndex() const
{
    // TODO To be implemented
    return false;
}

void HiveParquetFile::prepareReader()
{
    // TODO To be implemented
}


void HiveParquetFile::loadSubMinMaxIndex()
{
    // TODO To be implemented
}

}
#endif
