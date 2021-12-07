#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <optional>

#include <Common/escapeForFileName.h>
#include <Common/Exception.h>

#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>

#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageStripeLog.h>
#include "StorageLogSettings.h"
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Pipe.h>

#include <cassert>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int TIMEOUT_EXCEEDED;
}


class StripeLogSource final : public SourceWithProgress
{
public:
    static Block getHeader(
        StorageStripeLog & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const Names & column_names,
        IndexForNativeFormat::Blocks::const_iterator index_begin,
        IndexForNativeFormat::Blocks::const_iterator index_end)
    {
        if (index_begin == index_end)
            return metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());

        /// TODO: check if possible to always return storage.getSampleBlock()

        Block header;

        for (const auto & column : index_begin->columns)
        {
            auto type = DataTypeFactory::instance().get(column.type);
            header.insert(ColumnWithTypeAndName{ type, column.name });
        }

        return header;
    }

    StripeLogSource(
        StorageStripeLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const Names & column_names,
        size_t max_read_buffer_size_,
        std::shared_ptr<const IndexForNativeFormat> & index_,
        IndexForNativeFormat::Blocks::const_iterator index_begin_,
        IndexForNativeFormat::Blocks::const_iterator index_end_)
        : SourceWithProgress(
            getHeader(storage_, metadata_snapshot_, column_names, index_begin_, index_end_))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , max_read_buffer_size(max_read_buffer_size_)
        , index(index_)
        , index_begin(index_begin_)
        , index_end(index_end_)
    {
    }

    String getName() const override { return "StripeLog"; }

protected:
    Chunk generate() override
    {
        Block res;
        start();

        if (block_in)
        {
            res = block_in->read();

            /// Freeing memory before destroying the object.
            if (!res)
            {
                block_in.reset();
                data_in.reset();
                index.reset();
            }
        }

        return Chunk(res.getColumns(), res.rows());
    }

private:
    StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_read_buffer_size;

    std::shared_ptr<const IndexForNativeFormat> index;
    IndexForNativeFormat::Blocks::const_iterator index_begin;
    IndexForNativeFormat::Blocks::const_iterator index_end;
    Block header;

    /** optional - to create objects only on first reading
      *  and delete objects (release buffers) after the source is exhausted
      * - to save RAM when using a large number of sources.
      */
    bool started = false;
    std::optional<CompressedReadBufferFromFile> data_in;
    std::optional<NativeBlockInputStream> block_in;

    void start()
    {
        if (!started)
        {
            started = true;

            String data_file_path = storage.table_path + "data.bin";
            size_t buffer_size = std::min(max_read_buffer_size, storage.disk->getFileSize(data_file_path));

            data_in.emplace(storage.disk->readFile(data_file_path, buffer_size));
            block_in.emplace(*data_in, 0, index_begin, index_end);
        }
    }
};


class StripeLogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit StripeLogBlockOutputStream(
        StorageStripeLog & storage_, const StorageMetadataPtr & metadata_snapshot_, std::unique_lock<std::shared_timed_mutex> && lock_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
        , data_out_file(storage.table_path + "data.bin")
        , data_out_compressed(storage.disk->writeFile(data_out_file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append))
        , data_out(std::make_unique<CompressedWriteBuffer>(
            *data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), storage.max_compress_block_size))
        , index_out_file(storage.table_path + "index.mrk")
        , index_out_compressed(storage.disk->writeFile(index_out_file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append))
        , index_out(std::make_unique<CompressedWriteBuffer>(*index_out_compressed))
        , block_out(*data_out, 0, metadata_snapshot->getSampleBlock(), false, index_out.get(), storage.disk->getFileSize(data_out_file))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        if (storage.file_checker.empty())
        {
            storage.file_checker.setEmpty(storage.table_path + "data.bin");
            storage.file_checker.setEmpty(storage.table_path + "index.mrk");
            storage.file_checker.save();
        }
    }

    ~StripeLogBlockOutputStream() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.
                data_out.reset();
                data_out_compressed.reset();
                index_out.reset();
                index_out_compressed.reset();

                storage.file_checker.repair();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        block_out.write(block);
    }

    void writeSuffix() override
    {
        if (done)
            return;

        block_out.writeSuffix();
        data_out->next();
        data_out_compressed->next();
        data_out_compressed->finalize();
        index_out->next();
        index_out_compressed->next();
        index_out_compressed->finalize();

        storage.file_checker.update(data_out_file);
        storage.file_checker.update(index_out_file);
        storage.file_checker.save();

        done = true;

        /// unlock should be done from the same thread as lock, and dtor may be
        /// called from different thread, so it should be done here (at least in
        /// case of no exceptions occurred)
        lock.unlock();
    }

private:
    StorageStripeLog & storage;
    StorageMetadataPtr metadata_snapshot;
    std::unique_lock<std::shared_timed_mutex> lock;

    String data_out_file;
    std::unique_ptr<WriteBuffer> data_out_compressed;
    std::unique_ptr<CompressedWriteBuffer> data_out;
    String index_out_file;
    std::unique_ptr<WriteBuffer> index_out_compressed;
    std::unique_ptr<CompressedWriteBuffer> index_out;
    NativeBlockOutputStream block_out;

    bool done = false;
};


StorageStripeLog::StorageStripeLog(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage(table_id_)
    , disk(std::move(disk_))
    , table_path(relative_path_)
    , max_compress_block_size(max_compress_block_size_)
    , file_checker(disk, table_path + "sizes.json")
    , log(&Poco::Logger::get("StorageStripeLog"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

    if (!attach)
    {
        /// create directories if they do not exist
        disk->createDirectories(table_path);
    }
    else
    {
        try
        {
            file_checker.repair();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


void StorageStripeLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        file_checker.setPath(table_path + "sizes.json");
    }
    renameInMemory(new_table_id);
}


static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    Int64 lock_timeout = settings.lock_acquire_timeout.totalSeconds();
    if (settings.max_execution_time.totalSeconds() != 0 && settings.max_execution_time.totalSeconds() < lock_timeout)
        lock_timeout = settings.max_execution_time.totalSeconds();
    return std::chrono::seconds{lock_timeout};
}


Pipe StorageStripeLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    unsigned num_streams)
{
    std::shared_lock lock(rwlock, getLockTimeout(context));
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    NameSet column_names_set(column_names.begin(), column_names.end());

    Pipes pipes;

    String index_file = table_path + "index.mrk";
    if (file_checker.empty() || !disk->exists(index_file))
    {
        return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));
    }

    CompressedReadBufferFromFile index_in(disk->readFile(index_file, 4096));
    std::shared_ptr<const IndexForNativeFormat> index{std::make_shared<IndexForNativeFormat>(index_in, column_names_set)};

    size_t size = index->blocks.size();
    if (num_streams > size)
        num_streams = size;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        IndexForNativeFormat::Blocks::const_iterator begin = index->blocks.begin();
        IndexForNativeFormat::Blocks::const_iterator end = index->blocks.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<StripeLogSource>(
            *this, metadata_snapshot, column_names, context->getSettingsRef().max_read_buffer_size, index, begin, end));
    }

    /// We do not keep read lock directly at the time of reading, because we read ranges of data that do not change.

    return Pipe::unitePipes(std::move(pipes));
}


BlockOutputStreamPtr StorageStripeLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    std::unique_lock lock(rwlock, getLockTimeout(context));
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return std::make_shared<StripeLogBlockOutputStream>(*this, metadata_snapshot, std::move(lock));
}


CheckResults StorageStripeLog::checkData(const ASTPtr & /* query */, ContextPtr context)
{
    std::shared_lock lock(rwlock, getLockTimeout(context));
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return file_checker.check();
}

void StorageStripeLog::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    disk->clearDirectory(table_path);
    file_checker = FileChecker{disk, table_path + "sizes.json"};
}


void registerStorageStripeLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    factory.registerStorage("StripeLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return StorageStripeLog::create(
            disk,
            args.relative_data_path,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.attach,
            args.getContext()->getSettings().max_compress_block_size);
    }, features);
}

}
