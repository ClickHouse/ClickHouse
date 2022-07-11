#pragma once
#include <Common/config.h>

#if USE_HIVE

#include <vector>
#include <memory>

#include <boost/algorithm/string/join.hpp>

#include <Core/Field.h>
#include <Core/Block.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Hive/HiveSettings.h>

namespace orc
{
class Reader;
class Statistics;
class ColumnStatistics;
}

namespace parquet
{
class ParquetFileReader;
namespace arrow
{
    class FileReader;
}
}

namespace arrow
{
namespace io
{
    class RandomAccessFile;
}

namespace fs
{
    class FileSystem;
}

class Buffer;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IHiveFile : public WithContext
{
public:
    using MinMaxIndex = IMergeTreeDataPart::MinMaxIndex;
    using MinMaxIndexPtr = std::shared_ptr<MinMaxIndex>;

    enum class FileFormat
    {
        RC_FILE,
        TEXT,
        LZO_TEXT,
        SEQUENCE_FILE,
        AVRO,
        PARQUET,
        ORC,
    };

    inline static const String RCFILE_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    inline static const String TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    inline static const String LZO_TEXT_INPUT_FORMAT = "com.hadoop.mapred.DeprecatedLzoTextInputFormat";
    inline static const String SEQUENCE_INPUT_FORMAT = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    inline static const String PARQUET_INPUT_FORMAT = "com.cloudera.impala.hive.serde.ParquetInputFormat";
    inline static const String MR_PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    inline static const String AVRO_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    inline static const String ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    inline static const std::map<String, FileFormat> VALID_HDFS_FORMATS = {
        {RCFILE_INPUT_FORMAT, FileFormat::RC_FILE},
        {TEXT_INPUT_FORMAT, FileFormat::TEXT},
        {LZO_TEXT_INPUT_FORMAT, FileFormat::LZO_TEXT},
        {SEQUENCE_INPUT_FORMAT, FileFormat::SEQUENCE_FILE},
        {PARQUET_INPUT_FORMAT, FileFormat::PARQUET},
        {MR_PARQUET_INPUT_FORMAT, FileFormat::PARQUET},
        {AVRO_INPUT_FORMAT, FileFormat::AVRO},
        {ORC_INPUT_FORMAT, FileFormat::ORC},
    };

    static inline bool isFormatClass(const String & format_class) { return VALID_HDFS_FORMATS.count(format_class) > 0; }
    static inline FileFormat toFileFormat(const String & format_class)
    {
        if (isFormatClass(format_class))
        {
            return VALID_HDFS_FORMATS.find(format_class)->second;
        }
        throw Exception("Unsupported hdfs file format " + format_class, ErrorCodes::NOT_IMPLEMENTED);
    }

    IHiveFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & storage_settings_,
        ContextPtr context_)
        : WithContext(context_)
        , partition_values(values_)
        , namenode_url(namenode_url_)
        , path(path_)
        , last_modify_time(last_modify_time_)
        , size(size_)
        , index_names_and_types(index_names_and_types_)
        , storage_settings(storage_settings_)
    {
    }
    virtual ~IHiveFile() = default;

    virtual FileFormat getFormat() const = 0;

    virtual String getName() const = 0;

    virtual String getPath() const { return path; }

    virtual FieldVector getPartitionValues() const { return partition_values; }

    virtual String getNamenodeUrl() { return namenode_url; }

    virtual bool hasMinMaxIndex() const { return false; }

    virtual void loadMinMaxIndex()
    {
        throw Exception("Method loadMinMaxIndex is not supported by hive file:" + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual MinMaxIndexPtr getMinMaxIndex() const { return minmax_idx; }

    // Do hive file contains sub-file level minmax index?
    virtual bool hasSubMinMaxIndex() const { return false; }

    virtual void loadSubMinMaxIndex()
    {
        throw Exception("Method loadSubMinMaxIndex is not supported by hive file:" + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual const std::vector<MinMaxIndexPtr> & getSubMinMaxIndexes() const { return sub_minmax_idxes; }

    virtual void setSkipSplits(const std::set<int> & splits) { skip_splits = splits; }

    virtual const std::set<int> & getSkipSplits() const { return skip_splits; }

    inline std::string describeMinMaxIndex(const MinMaxIndexPtr & idx) const
    {
        if (!idx)
            return "";

        std::vector<std::string> strs;
        strs.reserve(index_names_and_types.size());
        size_t i = 0;
        for (const auto & name_type : index_names_and_types)
        {
            strs.push_back(name_type.name + ":" + name_type.type->getName() + idx->hyperrectangle[i++].toString());
        }
        return boost::algorithm::join(strs, "|");
    }

    inline UInt64 getLastModTs() const { return last_modify_time; }
    inline size_t getSize() const { return size; }

protected:
    FieldVector partition_values;
    String namenode_url;
    String path;
    UInt64 last_modify_time;
    size_t size;
    NamesAndTypesList index_names_and_types;
    MinMaxIndexPtr minmax_idx;
    std::vector<MinMaxIndexPtr> sub_minmax_idxes;
    std::set<int> skip_splits; // skip splits for this file after applying minmax index (if any)
    std::shared_ptr<HiveSettings> storage_settings;
};

using HiveFilePtr = std::shared_ptr<IHiveFile>;
using HiveFiles = std::vector<HiveFilePtr>;

class HiveTextFile : public IHiveFile
{
public:
    HiveTextFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    virtual FileFormat getFormat() const override { return FileFormat::TEXT; }
    virtual String getName() const override { return "TEXT"; }
};

class HiveOrcFile : public IHiveFile
{
public:
    HiveOrcFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    virtual FileFormat getFormat() const override { return FileFormat::ORC; }
    virtual String getName() const override { return "ORC"; }
    virtual bool hasMinMaxIndex() const override;
    virtual void loadMinMaxIndex() override;

    virtual bool hasSubMinMaxIndex() const override;
    virtual void loadSubMinMaxIndex() override;

protected:
    virtual std::unique_ptr<MinMaxIndex> buildMinMaxIndex(const orc::Statistics * statistics);
    virtual Range buildRange(const orc::ColumnStatistics * col_stats);
    virtual void prepareReader();
    virtual void prepareColumnMapping();

    std::shared_ptr<orc::Reader> reader;
    std::map<String, size_t> orc_column_positions;
};

class HiveParquetFile : public IHiveFile
{
public:
    HiveParquetFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    virtual FileFormat getFormat() const override { return FileFormat::PARQUET; }
    virtual String getName() const override { return "PARQUET"; }

    virtual bool hasSubMinMaxIndex() const override;
    virtual void loadSubMinMaxIndex() override;

protected:
    virtual void prepareReader();

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::shared_ptr<parquet::ParquetFileReader> reader;
    std::map<String, size_t> parquet_column_positions;
};
}


#endif
