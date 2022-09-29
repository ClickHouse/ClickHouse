#pragma once

#include <chrono>
#include <filesystem>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <Processors/ISource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/logger_useful.h>
#include <base/shared_ptr_helper.h>
#include <boost/filesystem.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/sync_queue.hpp>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>

#include <Parsers/ASTLiteral.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageGenerateRandom.h>

#include <base/unaligned.h>
#include <Common/SipHash.h>
#include <Common/randomSeed.h>

#include <Functions/FunctionFactory.h>

#include <Processors/ConcatProcessor.h>
#include <pcg_random.hpp>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
}

namespace fs = std::filesystem;

String fileTypeToString(std::filesystem::file_type type)
{
    switch (type)
    {
        case std::filesystem::file_type::none:
            return "none";
        case std::filesystem::file_type::not_found:
            return "not found";
        case std::filesystem::file_type::regular:
            return "regular file";
        case std::filesystem::file_type::directory:
            return "directory";
        case std::filesystem::file_type::symlink:
            return "symlink";
        case std::filesystem::file_type::block:
            return "block";
        case std::filesystem::file_type::character:
            return "character";
        case std::filesystem::file_type::fifo:
            return "fifo";
        case std::filesystem::file_type::socket:
            return "socket";
        case std::filesystem::file_type::unknown:
            return "unknown";
    }
}

String permissionsToString(fs::perms perms)
{
    using permissions = std::filesystem::perms;
    String result = "rwxrwxrwx";
    for (uint32_t i = 0; i < 9; ++i)
    {
        if (!(static_cast<uint32_t>(perms) & (1 << i)))
        {
            result[8 - i] = '-';
        }
    }

    if ((perms & permissions::set_uid) != permissions::none)
    {
        result[2] = 's';
    }

    if ((perms & permissions::set_gid) != permissions::none)
    {
        result[5] = 's';
    }

    if ((perms & permissions::sticky_bit) != permissions::none)
    {
        result[8] = 't';
    }

    return result;
}

class StorageDirectorySource final : public ISource
{
public:
    struct PathInfo
    {
        boost::sync_queue<fs::directory_entry> queue;
        fs::recursive_directory_iterator directory_iterator;
        std::mutex mutex;
        const String path;
        unsigned curr_row = 0;

        PathInfo(String path_) : directory_iterator(path_), path(path_) {
            queue.push(fs::directory_entry(path));
        }
    };
    using PathInfoPtr = std::shared_ptr<PathInfo>;

    String getName() const override { return DB::String(); }

    Chunk generate() override
    {
        printf("\nmaxblocksize = %lu\n", max_block_size);
        // TODO change to vectors?
        auto permissions = ColumnString::create();
        auto type = ColumnString::create();
        auto symlink = ColumnUInt8::create();
        auto path_column = ColumnString::create();
        //        auto permissions = ColumnString::create();
        auto size = ColumnUInt32::create();
        auto last_write_time = std::shared_ptr<DataTypeDateTime>()->createColumn();
        auto name = ColumnString::create();


        fs::directory_entry file;

        while (true)
        {
            if (std::lock_guard<std::mutex> lock(path_info->mutex);
                path_info->curr_row < max_block_size && path_info->directory_iterator != end(path_info->directory_iterator))
            {
                ++path_info->curr_row;
                file = *path_info->directory_iterator++;
            }
            else
            {
                break;
            }
            permissions->insert(permissionsToString(file.status().permissions()));
            type->insert(fileTypeToString(file.status().type()));
            symlink->insert(file.is_symlink());
            path_column->insert(file.path().string());
            //            permissions->insert(directory_iterator->status().permissions())
            size->insert(file.is_regular_file() ? file.file_size() : 0);

            //            time_t time;
            //            try
            //            {
            //                 time = std::chrono::file_clock::to_time_t();
            //            } catch (const boost::filesystem::filesystem_error&  e) {
            //                printf("%s\n", e.what());
            //            }
            printf("beforetime\n");
            printf("%lu", static_cast<UInt64>(fs::last_write_time(file.path().string()).time_since_epoch().count()));
            last_write_time->insert(std::chrono::file_clock::to_time_t(fs::last_write_time(file.path().string())));
            printf("aftertime\n");

            name->insert(file.path().filename().string());
            printf(
                "\n\n%s %lu %s\n",
                file.path().string().c_str(),
                file.is_regular_file() ? file.file_size() : 0,
                file.path().filename().string().c_str());
        }

        printf("\n read completed \n");

        auto num_rows = path_column->size();

        if (num_rows == 0)
        {
            printf("done");
            return {};
        }

        printf("num rows %lu\n", num_rows);

        Columns columns
            = {std::move(permissions),
               std::move(type),
               std::move(symlink),
               std::move(path_column),
               std::move(size),
               std::move(last_write_time),
               std::move(name)};

        printf("debugpnt\n");

        return {std::move(columns), num_rows};
    }

    StorageDirectorySource(
        //        std::shared_ptr<StorageDirectory> storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        //        ContextPtr context_,
        UInt64 max_block_size_,
        PathInfoPtr path_info_,
        ColumnsDescription columns_description_)
        : ISource(metadata_snapshot_->getSampleBlock())
        //        , storage(std::move(storage_))
        , metadata_snapshot(metadata_snapshot_)
        , path_info(path_info_)
        , columns_description(std::move(columns_description_))
        //        , context(context_)
        , max_block_size(max_block_size_)
    {
    }

private:
    //    std::shared_ptr<StorageDirectory> storage;
    StorageMetadataPtr metadata_snapshot;
    PathInfoPtr path_info;
    ColumnsDescription columns_description;
    //    ContextPtr context; /// TODO Untangle potential issues with context lifetime.
    UInt64 max_block_size;
};


class StorageDirectory final : public shared_ptr_helper<StorageDirectory>, public IStorage
{
    friend struct shared_ptr_helper<StorageDirectory>;

public:
    std::string getName() const override { return "Directory"; }

    Pipe read(
        const Names & /* column_names */,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo &,
        ContextPtr /* context */,
        QueryProcessingStage::Enum /* processed_stage */,
        size_t max_block_size,
        unsigned num_streams) override
    {
        printf("\nread1\n");
        auto this_ptr = std::static_pointer_cast<StorageDirectory>(shared_from_this());
        printf("\nread2 path %s \n", path.c_str());
        if (!fs::exists(path))
        {
            printf("\nexception\n");
            throw Exception("Directory " + path + " doesn't exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);
        }
        printf("\nread3\n");

        auto path_info = std::make_shared<StorageDirectorySource::PathInfo>(path);
        Pipes pipes;
        for (unsigned i = 0; i < num_streams; ++i) {
            pipes.emplace_back(std::make_shared<StorageDirectorySource>(metadata_snapshot, max_block_size, path_info, metadata_snapshot->getColumns()));
        }
        printf("\nread4\n");
        auto pipe = Pipe::unitePipes(std::move(pipes));
        pipe.addTransform(std::make_shared<ConcatProcessor>(pipe.getHeader(), pipe.numOutputPorts()));
        return pipe;
    }

    bool storesDataOnDisk() const override { return true; }

    Strings getDataPaths() const override { return {path}; }

    NamesAndTypesList getVirtuals() const override { return {}; }

protected:
    friend class StorageDirectorySource;

private:
    String path;

    StorageDirectory(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        String path_,
        ConstraintsDescription constraints_,
        const String & comment)
        : IStorage(table_id_), path(path_)
    {
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(columns_description_);
        metadata_.setConstraints(constraints_);
        metadata_.setComment(comment);
        setInMemoryMetadata(metadata_);
    }
};
}
