#pragma once
#include <Common/config.h>
#include "Core/Block.h"

#if USE_HDFS
#include <vector>
#include <memory>

#include <boost/algorithm/string/join.hpp>
#include <arrow/filesystem/filesystem.h>
#include <orc/Statistics.hh>

#include <Core/Field.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Hive/StorageHive.h>
#include <Storages/Hive/HiveSettings.h>

namespace orc
{
class Reader;
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
    IHiveFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 ts_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & storage_settings_,
        ContextPtr context_)
        : WithContext(context_)
        , partition_values(values_)
        , namenode_url(namenode_url_)
        , path(path_)
        , last_mod_ts(ts_)
        , size(size_)
        , index_names_and_types(index_names_and_types_)
        , storage_settings(storage_settings_)
    {
        // std::cout << "1delim:" << storage_settings->hive_text_field_delimeter << std::endl;
        // std::cout << "1disable orc:" << storage_settings->disable_orc_stripe_minmax_index << std::endl;
        // std::cout << "1disable parquet:" << storage_settings->disable_parquet_rowgroup_minmax_index << std::endl;
    }
    virtual ~IHiveFile() = default;

    using FileFormat = StorageHive::FileFormat;
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

    virtual std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> getMinMaxIndex() const { return minmax_idx; }

    // Do hive file contains sub-file level minmax index?
    virtual bool hasSubMinMaxIndex() const { return false; }

    virtual void loadSubMinMaxIndex()
    {
        throw Exception("Method loadSubMinMaxIndex is not supported by hive file:" + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual const std::vector<std::shared_ptr<IMergeTreeDataPart::MinMaxIndex>> & getSubMinMaxIndexes() const { return sub_minmax_idxes; }

    virtual void setSkipSplits(const std::set<int> & splits) { skip_splits = splits; }

    virtual const std::set<int> & getSkipSplits() const { return skip_splits; }

    inline std::string describeMinMaxIndex(const std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> & idx) const
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

    inline UInt64 getLastModTs() const { return last_mod_ts; }
    inline size_t getSize() const { return size; }

protected:
    FieldVector partition_values;
    String namenode_url;
    String path;
    UInt64 last_mod_ts;
    size_t size;
    NamesAndTypesList index_names_and_types;
    std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> minmax_idx;
    std::vector<std::shared_ptr<IMergeTreeDataPart::MinMaxIndex>> sub_minmax_idxes;
    std::set<int> skip_splits; // skip splits for this file after applying minmax index (if any)
    std::shared_ptr<HiveSettings> storage_settings;
};

class HiveTextFile : public IHiveFile
{
public:
    HiveTextFile(
        const FieldVector & values_,
        const String & namenode_url_,
        const String & path_,
        UInt64 ts_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, ts_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    using FileFormat = StorageHive::FileFormat;
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
        UInt64 ts_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, ts_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    using FileFormat = StorageHive::FileFormat;
    virtual FileFormat getFormat() const override { return FileFormat::ORC; }
    virtual String getName() const override { return "ORC"; }
    virtual bool hasMinMaxIndex() const override;
    virtual void loadMinMaxIndex() override;

    virtual bool hasSubMinMaxIndex() const override;
    virtual void loadSubMinMaxIndex() override;

protected:
    virtual std::unique_ptr<IMergeTreeDataPart::MinMaxIndex> buildMinMaxIndex(const orc::Statistics * statistics);
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
        UInt64 ts_,
        size_t size_,
        const NamesAndTypesList & index_names_and_types_,
        const std::shared_ptr<HiveSettings> & hive_settings_,
        ContextPtr context_)
        : IHiveFile(values_, namenode_url_, path_, ts_, size_, index_names_and_types_, hive_settings_, context_)
    {
    }

    using FileFormat = StorageHive::FileFormat;
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
