#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <cassert>

#include <Poco/Util/XMLConfiguration.h>

#include <Common/escapeForFileName.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/LimitReadBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/NestedUtils.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/CheckResults.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTinyLog.h>
#include "StorageLogSettings.h"

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>

#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION ".bin"


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int DUPLICATE_COLUMN;
    extern const int INCORRECT_FILE_NAME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class TinyLogSource final : public SourceWithProgress
{
public:
    static Block getHeader(const NamesAndTypesList & columns)
    {
        Block res;

        for (const auto & name_type : columns)
            res.insert({ name_type.type->createColumn(), name_type.type, name_type.name });

        return res;
    }

    TinyLogSource(
        size_t block_size_,
        const NamesAndTypesList & columns_,
        StorageTinyLog & storage_,
        size_t max_read_buffer_size_,
        FileChecker::Map file_sizes_)
        : SourceWithProgress(getHeader(columns_))
        , block_size(block_size_), columns(columns_), storage(storage_)
        , max_read_buffer_size(max_read_buffer_size_), file_sizes(std::move(file_sizes_))
    {
    }

    String getName() const override { return "TinyLog"; }

protected:
    Chunk generate() override;

private:
    size_t block_size;
    NamesAndTypesList columns;
    StorageTinyLog & storage;
    bool is_finished = false;
    size_t max_read_buffer_size;
    FileChecker::Map file_sizes;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, size_t max_read_buffer_size_, size_t file_size)
            : plain(file_size ? disk->readFile(data_path, std::min(max_read_buffer_size_, file_size)) : std::make_unique<ReadBuffer>(nullptr, 0)),
            limited(std::make_unique<LimitReadBuffer>(*plain, file_size, false)),
            compressed(*limited)
        {
        }

        std::unique_ptr<ReadBuffer> plain;
        std::unique_ptr<ReadBuffer> limited;
        CompressedReadBuffer compressed;
    };

    using FileStreams = std::map<String, std::unique_ptr<Stream>>;
    FileStreams streams;

    using DeserializeState = ISerialization::DeserializeBinaryBulkStatePtr;
    using DeserializeStates = std::map<String, DeserializeState>;
    DeserializeStates deserialize_states;

    void readData(const NameAndTypePair & name_and_type, ColumnPtr & column, UInt64 limit, ISerialization::SubstreamsCache & cache);
};


Chunk TinyLogSource::generate()
{
    Block res;

    if (is_finished || file_sizes.empty() || (!streams.empty() && streams.begin()->second->compressed.eof()))
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        is_finished = true;
        streams.clear();
        return {};
    }

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    for (const auto & name_type : columns)
    {
        ColumnPtr column;
        try
        {
            column = name_type.type->createColumn();
            readData(name_type, column, block_size, caches[name_type.getNameInStorage()]);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name_type.name + " at " + fullPath(storage.disk, storage.table_path));
            throw;
        }

        if (!column->empty())
            res.insert(ColumnWithTypeAndName(std::move(column), name_type.type, name_type.name));
    }

    if (!res || streams.begin()->second->compressed.eof())
    {
        is_finished = true;
        streams.clear();
    }

    return Chunk(res.getColumns(), res.rows());
}


void TinyLogSource::readData(const NameAndTypePair & name_and_type,
    ColumnPtr & column, UInt64 limit, ISerialization::SubstreamsCache & cache)
{
    ISerialization::DeserializeBinaryBulkSettings settings; /// TODO Use avg_value_size_hint.
    const auto & [name, type] = name_and_type;
    auto serialization = IDataType::getSerialization(name_and_type);

    settings.getter = [&] (const ISerialization::SubstreamPath & path) -> ReadBuffer *
    {
        if (cache.count(ISerialization::getSubcolumnNameForStream(path)))
            return nullptr;

        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto & stream = streams[stream_name];
        if (!stream)
        {
            String file_path = storage.files[stream_name].data_file_path;
            stream = std::make_unique<Stream>(
                storage.disk, file_path, max_read_buffer_size, file_sizes[fileName(file_path)]);
        }

        return &stream->compressed;
    };

    if (deserialize_states.count(name) == 0)
         serialization->deserializeBinaryBulkStatePrefix(settings, deserialize_states[name]);

    serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, deserialize_states[name], &cache);
}


class TinyLogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit TinyLogBlockOutputStream(
        StorageTinyLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::unique_lock<std::shared_timed_mutex> && lock_)
        : storage(storage_), metadata_snapshot(metadata_snapshot_), lock(std::move(lock_))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        /// If there were no files, add info to rollback in case of error.
        if (storage.file_checker.empty())
        {
            for (const auto & file : storage.files)
                storage.file_checker.setEmpty(file.second.data_file_path);
            storage.file_checker.save();
        }
    }

    ~TinyLogBlockOutputStream() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.
                LOG_WARNING(storage.log, "Rollback partial writes");
                streams.clear();
                storage.file_checker.repair();
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageTinyLog & storage;
    StorageMetadataPtr metadata_snapshot;
    std::unique_lock<std::shared_timed_mutex> lock;
    bool done = false;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, CompressionCodecPtr codec, size_t max_compress_block_size) :
            plain(disk->writeFile(data_path, max_compress_block_size, WriteMode::Append)),
            compressed(*plain, std::move(codec), max_compress_block_size)
        {
        }

        std::unique_ptr<WriteBuffer> plain;
        CompressedWriteBuffer compressed;

        void finalize()
        {
            compressed.next();
            plain->finalize();
        }
    };

    using FileStreams = std::map<String, std::unique_ptr<Stream>>;
    FileStreams streams;

    using SerializeState = ISerialization::SerializeBinaryBulkStatePtr;
    using SerializeStates = std::map<String, SerializeState>;
    SerializeStates serialize_states;

    using WrittenStreams = std::set<String>;

    ISerialization::OutputStreamGetter createStreamGetter(const NameAndTypePair & column, WrittenStreams & written_streams);
    void writeData(const NameAndTypePair & name_and_type, const IColumn & column, WrittenStreams & written_streams);
};


ISerialization::OutputStreamGetter TinyLogBlockOutputStream::createStreamGetter(
    const NameAndTypePair & column,
    WrittenStreams & written_streams)
{
    return [&] (const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        String stream_name = ISerialization::getFileNameForStream(column, path);

        if (!written_streams.insert(stream_name).second)
            return nullptr;

        const auto & columns = metadata_snapshot->getColumns();
        if (!streams.count(stream_name))
            streams[stream_name] = std::make_unique<Stream>(
                storage.disk,
                storage.files[stream_name].data_file_path,
                columns.getCodecOrDefault(column.name),
                storage.max_compress_block_size);

        return &streams[stream_name]->compressed;
    };
}


void TinyLogBlockOutputStream::writeData(const NameAndTypePair & name_and_type, const IColumn & column, WrittenStreams & written_streams)
{
    ISerialization::SerializeBinaryBulkSettings settings;
    const auto & [name, type] = name_and_type;
    auto serialization = type->getDefaultSerialization();

    if (serialize_states.count(name) == 0)
    {
        /// Some stream getters may be called form `serializeBinaryBulkStatePrefix`.
        /// Use different WrittenStreams set, or we get nullptr for them in `serializeBinaryBulkWithMultipleStreams`
        WrittenStreams prefix_written_streams;
        settings.getter = createStreamGetter(name_and_type, prefix_written_streams);
        serialization->serializeBinaryBulkStatePrefix(settings, serialize_states[name]);
    }

    settings.getter = createStreamGetter(name_and_type, written_streams);
    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, 0, settings, serialize_states[name]);
}


void TinyLogBlockOutputStream::writeSuffix()
{
    if (done)
        return;

    /// If nothing was written - leave the table in initial state.
    if (streams.empty())
    {
        done = true;
        return;
    }

    WrittenStreams written_streams;
    ISerialization::SerializeBinaryBulkSettings settings;
    for (const auto & column : getHeader())
    {
        auto it = serialize_states.find(column.name);
        if (it != serialize_states.end())
        {
            settings.getter = createStreamGetter(NameAndTypePair(column.name, column.type), written_streams);
            auto serialization = column.type->getDefaultSerialization();
            serialization->serializeBinaryBulkStateSuffix(settings, it->second);
        }
    }

    /// Finish write.
    for (auto & stream : streams)
        stream.second->finalize();

    Strings column_files;
    for (auto & pair : streams)
        column_files.push_back(storage.files[pair.first].data_file_path);

    streams.clear();
    done = true;

    for (const auto & file : column_files)
        storage.file_checker.update(file);
    storage.file_checker.save();

    /// unlock should be done from the same thread as lock, and dtor may be
    /// called from different thread, so it should be done here (at least in
    /// case of no exceptions occurred)
    lock.unlock();
}


void TinyLogBlockOutputStream::write(const Block & block)
{
    metadata_snapshot->check(block, true);

    /// The set of written offset columns so that you do not write shared columns for nested structures multiple times
    WrittenStreams written_streams;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(NameAndTypePair(column.name, column.type), *column.column, written_streams);
    }
}


StorageTinyLog::StorageTinyLog(
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
    , log(&Poco::Logger::get("StorageTinyLog"))
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

    for (const auto & col : storage_metadata.getColumns().getAllPhysical())
        addFiles(col);
}


void StorageTinyLog::addFiles(const NameAndTypePair & column)
{
    const auto & [name, type] = column;
    if (files.end() != files.find(name))
        throw Exception("Duplicate column with name " + name + " in constructor of StorageTinyLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    ISerialization::StreamCallback stream_callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(column, substream_path);
        if (!files.count(stream_name))
        {
            ColumnData column_data;
            files.insert(std::make_pair(stream_name, column_data));
            files[stream_name].data_file_path = table_path + stream_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION;
        }
    };

    ISerialization::SubstreamPath substream_path;
    auto serialization = type->getDefaultSerialization();
    serialization->enumerateStreams(stream_callback, substream_path);
}


void StorageTinyLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        file_checker.setPath(table_path + "sizes.json");

        for (auto & file : files)
            file.second.data_file_path = table_path + fileName(file.second.data_file_path);
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


Pipe StorageTinyLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    auto all_columns = metadata_snapshot->getColumns().getByNames(ColumnsDescription::All, column_names, true);

    // When reading, we lock the entire storage, because we only have one file
    // per column and can't modify it concurrently.
    const Settings & settings = context->getSettingsRef();

    std::shared_lock lock{rwlock, getLockTimeout(context)};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    /// No need to hold lock while reading because we read fixed range of data that does not change while appending more data.
    return Pipe(std::make_shared<TinyLogSource>(
        max_block_size,
        Nested::convertToSubcolumns(all_columns),
        *this,
        settings.max_read_buffer_size,
        file_checker.getFileSizes()));
}


BlockOutputStreamPtr StorageTinyLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<TinyLogBlockOutputStream>(*this, metadata_snapshot, std::unique_lock{rwlock, getLockTimeout(context)});
}


CheckResults StorageTinyLog::checkData(const ASTPtr & /* query */, ContextPtr context)
{
    std::shared_lock lock(rwlock, getLockTimeout(context));
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return file_checker.check();
}

void StorageTinyLog::truncate(
    const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    disk->clearDirectory(table_path);

    files.clear();
    file_checker = FileChecker{disk, table_path + "sizes.json"};

    for (const auto & column : metadata_snapshot->getColumns().getAllPhysical())
        addFiles(column);
}


void registerStorageTinyLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    factory.registerStorage("TinyLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return StorageTinyLog::create(
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
