#include <Storages/Hive/HiveFile.h>

#if USE_HIVE

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/core.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/filesystem/filesystem.h>
#include <orc/OrcFile.hh>
#include <orc/Reader.hh>
#include <orc/Statistics.hh>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB
{
String IHiveFile::hiveMetaStoreFileFormattoHiveFileFormat(const String & format_class)
{
    using FileFormat = IHiveFile::FileFormat;
    FileFormat hdfs_file_format = IHiveFile::toFileFormat(format_class);
    String format_name;
    switch (hdfs_file_format)
    {
        case FileFormat::TEXT:
        case FileFormat::LZO_TEXT:
            format_name = "HiveText";
            break;
        case FileFormat::RC_FILE:
            /// TODO to be implemented
            throw Exception("Unsopported hive format rc_file", ErrorCodes::NOT_IMPLEMENTED);
        case FileFormat::SEQUENCE_FILE:
            /// TODO to be implemented
            throw Exception("Unsopported hive format sequence_file", ErrorCodes::NOT_IMPLEMENTED);
        case FileFormat::AVRO:
            format_name = "Avro";
            break;
        case FileFormat::PARQUET:
            format_name = "Parquet";
            break;
        case FileFormat::ORC:
            format_name = "ORC";
            break;
    }
    return format_name;
}

HiveFilePtr createHiveFile(
    const String & format_name,
    const FieldVector & fields,
    const String & namenode_url,
    const String & path,
    UInt64 ts,
    size_t size,
    const NamesAndTypesList & index_names_and_types,
    const std::shared_ptr<HiveSettings> & hive_settings,
    ContextPtr context)
{
    HiveFilePtr hive_file;
    if (format_name == "HiveText")
    {
        hive_file = std::make_shared<HiveTextFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "ORC")
    {
        hive_file = std::make_shared<HiveOrcFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else if (format_name == "Parquet")
    {
        hive_file = std::make_shared<HiveParquetFile>(fields, namenode_url, path, ts, size, index_names_and_types, hive_settings, context);
    }
    else
    {
        throw Exception("IHiveFile not implemented for format " + format_name, ErrorCodes::NOT_IMPLEMENTED);
    }
    return hive_file;

}

template <class FieldType, class StatisticsType>
Range createRangeFromOrcStatistics(const StatisticsType * stats)
{
    /// We must check if there are minimum or maximum values in statistics in case of
    /// null values or NaN/Inf values of double type.
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
    throw Exception("Unimplemented HiveOrcFile::prepareReader", ErrorCodes::NOT_IMPLEMENTED);
}

void HiveOrcFile::prepareColumnMapping()
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveOrcFile::prepareColumnMapping", ErrorCodes::NOT_IMPLEMENTED);
}

bool HiveOrcFile::hasMinMaxIndex() const
{
    return false;
}


std::unique_ptr<IMergeTreeDataPart::MinMaxIndex> HiveOrcFile::buildMinMaxIndex(const orc::Statistics * /*statistics*/)
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveOrcFile::buildMinMaxIndex", ErrorCodes::NOT_IMPLEMENTED);
}


void HiveOrcFile::loadMinMaxIndex()
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveOrcFile::loadMinMaxIndex", ErrorCodes::NOT_IMPLEMENTED);
}

bool HiveOrcFile::hasSubMinMaxIndex() const
{
    // TODO To be implemented
    return false;
}


void HiveOrcFile::loadSubMinMaxIndex()
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveOrcFile::loadSubMinMaxIndex", ErrorCodes::NOT_IMPLEMENTED);
}

bool HiveParquetFile::hasSubMinMaxIndex() const
{
    // TODO To be implemented
    return false;
}

void HiveParquetFile::prepareReader()
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveParquetFile::prepareReader", ErrorCodes::NOT_IMPLEMENTED);
}


void HiveParquetFile::loadSubMinMaxIndex()
{
    // TODO To be implemented
    throw Exception("Unimplemented HiveParquetFile::loadSubMinMaxIndex", ErrorCodes::NOT_IMPLEMENTED);
}

}
#endif
