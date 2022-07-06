#pragma once
#include <Common/config.h>

#if USE_HIVE

#include <vector>
#include <memory>

#include <boost/algorithm/string/join.hpp>
#include <arrow/adapters/orc/adapter.h>
#include <parquet/arrow/reader.h>

#include <Core/Field.h>
#include <Core/Block.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Hive/HiveSettings.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>

namespace orc
{
class Statistics;
class ColumnStatistics;
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
        const FieldVector & partition_values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & storage_settings_,
        const ContextPtr & context_)
        : WithContext(context_)
        , partition_values(partition_values_)
        , namenode_url(namenode_url_)
        , path(path_)
        , last_modify_time(last_modify_time_)
        , size(size_)
        , index_names_and_types(index_names_and_types_)
        , storage_settings(storage_settings_)
    {
    }
    virtual ~IHiveFile() = default;

    String getFormatName() const { return String(magic_enum::enum_name(getFormat())); }
    const String & getPath() const { return path; }
    UInt64 getLastModTs() const { return last_modify_time; }
    size_t getSize() const { return size; }
    std::optional<size_t> getRows();
    const FieldVector & getPartitionValues() const { return partition_values; }
    const String & getNamenodeUrl() { return namenode_url; }
    MinMaxIndexPtr getMinMaxIndex() const { return file_minmax_idx; }
    const std::vector<MinMaxIndexPtr> & getSubMinMaxIndexes() const { return split_minmax_idxes; }

    const std::unordered_set<int> & getSkipSplits() const { return skip_splits; }
    void setSkipSplits(const std::unordered_set<int> & skip_splits_) { skip_splits = skip_splits_; }

    String describeMinMaxIndex(const MinMaxIndexPtr & idx) const
    {
        if (!idx)
            return "";
        std::vector<String> strs;
        strs.reserve(index_names_and_types.size());
        size_t i = 0;
        for (const auto & name_type : index_names_and_types)
            strs.push_back(name_type.name + ":" + name_type.type->getName() + idx->hyperrectangle[i++].toString());
        return boost::algorithm::join(strs, "|");
    }

    virtual FileFormat getFormat() const = 0;

    /// If hive query could use file level minmax index?
    virtual bool useFileMinMaxIndex() const { return false; }
    void loadFileMinMaxIndex();

    /// If hive query could use sub-file level minmax index?
    virtual bool useSplitMinMaxIndex() const { return false; }
    void loadSplitMinMaxIndexes();

protected:
    virtual void loadFileMinMaxIndexImpl()
    {
        throw Exception("Method loadFileMinMaxIndexImpl is not supported by hive file:" + getFormatName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void loadSplitMinMaxIndexesImpl()
    {
        throw Exception("Method loadSplitMinMaxIndexesImpl is not supported by hive file:" + getFormatName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual std::optional<size_t> getRowsImpl() = 0;

    FieldVector partition_values;
    String namenode_url;
    String path;
    UInt64 last_modify_time;
    size_t size;
    std::atomic<bool> has_init_rows = false;
    std::optional<size_t> rows;

    NamesAndTypesList index_names_and_types;

    MinMaxIndexPtr file_minmax_idx;
    std::atomic<bool> file_minmax_idx_loaded{false};

    std::vector<MinMaxIndexPtr> split_minmax_idxes;
    std::atomic<bool> split_minmax_idxes_loaded{false};

    /// Skip splits for this file after applying minmax index (if any)
    std::unordered_set<int> skip_splits;
    std::shared_ptr<HiveSettings> storage_settings;

    /// IHiveFile would be shared among multi threads, need lock's protection to update min/max indexes.
    std::mutex mutex;
};

using HiveFilePtr = std::shared_ptr<IHiveFile>;
using HiveFiles = std::vector<HiveFilePtr>;
using HiveFilesCache = LRUCache<String, IHiveFile>;
using HiveFilesCachePtr = std::shared_ptr<HiveFilesCache>;

class HiveTextFile : public IHiveFile
{
public:
    HiveTextFile(
        const FieldVector & partition_values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        const ContextPtr & context_)
        : IHiveFile(partition_values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    FileFormat getFormat() const override { return FileFormat::TEXT; }

private:
    std::optional<size_t> getRowsImpl() override { return {}; }
};

class HiveORCFile : public IHiveFile
{
public:
    HiveORCFile(
        const FieldVector & partition_values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        const ContextPtr & context_)
        : IHiveFile(partition_values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    FileFormat getFormat() const override { return FileFormat::ORC; }
    bool useFileMinMaxIndex() const override;
    bool useSplitMinMaxIndex() const override;

private:
    static Range buildRange(const orc::ColumnStatistics * col_stats);

    void loadFileMinMaxIndexImpl() override;
    void loadSplitMinMaxIndexesImpl() override;
    std::unique_ptr<MinMaxIndex> buildMinMaxIndex(const orc::Statistics * statistics);
    void prepareReader();
    void prepareColumnMapping();

    std::optional<size_t> getRowsImpl() override;

    std::unique_ptr<ReadBufferFromHDFS> in;
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> reader;
    std::map<String, size_t> orc_column_positions;
};

class HiveParquetFile : public IHiveFile
{
public:
    HiveParquetFile(
        const FieldVector & partition_values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        const ContextPtr & context_)
        : IHiveFile(partition_values_, namenode_url_, path_, last_modify_time_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    FileFormat getFormat() const override { return FileFormat::PARQUET; }
    bool useSplitMinMaxIndex() const override;

private:
    void loadSplitMinMaxIndexesImpl() override;
    std::optional<size_t> getRowsImpl() override;
    void prepareReader();

    std::unique_ptr<ReadBufferFromHDFS> in;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::map<String, size_t> parquet_column_positions;
};
}


#endif
