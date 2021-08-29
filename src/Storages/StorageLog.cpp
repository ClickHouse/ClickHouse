#include <Storages/StorageLog.h>
#include <Storages/StorageFactory.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <DataTypes/NestedUtils.h>

#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include "StorageLogSettings.h"
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/IBackup.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <cassert>
#include <chrono>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION ".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_NAME "__marks.mrk"


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int SIZES_OF_MARKS_FILES_ARE_INCONSISTENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
}

class LogSource final : public SourceWithProgress
{
public:
    static Block getHeader(const NamesAndTypesList & columns)
    {
        Block res;

        for (const auto & name_type : columns)
            res.insert({ name_type.type->createColumn(), name_type.type, name_type.name });

        return res;
    }

    LogSource(
        size_t block_size_, const NamesAndTypesList & columns_, StorageLog & storage_,
        size_t mark_number_, size_t rows_limit_, size_t max_read_buffer_size_)
        : SourceWithProgress(getHeader(columns_)),
        block_size(block_size_),
        columns(columns_),
        storage(storage_),
        mark_number(mark_number_),
        rows_limit(rows_limit_),
        max_read_buffer_size(max_read_buffer_size_)
    {
    }

    String getName() const override { return "Log"; }

protected:
    Chunk generate() override;

private:
    size_t block_size;
    NamesAndTypesList columns;
    StorageLog & storage;
    size_t mark_number;     /// from what mark to read data
    size_t rows_limit;      /// The maximum number of rows that can be read
    size_t rows_read = 0;
    size_t max_read_buffer_size;

    std::unordered_map<String, SerializationPtr> serializations;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, size_t offset, size_t max_read_buffer_size_)
            : plain(disk->readFile(data_path, std::min(max_read_buffer_size_, disk->getFileSize(data_path))))
            , compressed(*plain)
        {
            if (offset)
                plain->seek(offset, SEEK_SET);
        }

        std::unique_ptr<ReadBufferFromFileBase> plain;
        CompressedReadBuffer compressed;
    };

    using FileStreams = std::map<String, Stream>;
    FileStreams streams;

    using DeserializeState = ISerialization::DeserializeBinaryBulkStatePtr;
    using DeserializeStates = std::map<String, DeserializeState>;
    DeserializeStates deserialize_states;

    void readData(const NameAndTypePair & name_and_type, ColumnPtr & column, size_t max_rows_to_read, ISerialization::SubstreamsCache & cache);
};


Chunk LogSource::generate()
{
    Block res;

    if (rows_read == rows_limit)
        return {};

    if (storage.file_checker.empty())
        return {};

    /// How many rows to read for the next block.
    size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);
    std::unordered_map<String, ISerialization::SubstreamsCache> caches;

    for (const auto & name_type : columns)
    {
        ColumnPtr column;
        try
        {
            column = name_type.type->createColumn();
            readData(name_type, column, max_rows_to_read, caches[name_type.getNameInStorage()]);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name_type.name + " at " + fullPath(storage.disk, storage.table_path));
            throw;
        }

        if (!column->empty())
            res.insert(ColumnWithTypeAndName(std::move(column), name_type.type, name_type.name));
    }

    if (res)
        rows_read += res.rows();

    if (!res || rows_read == rows_limit)
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        streams.clear();
    }

    UInt64 num_rows = res.rows();
    return Chunk(res.getColumns(), num_rows);
}


void LogSource::readData(const NameAndTypePair & name_and_type, ColumnPtr & column,
    size_t max_rows_to_read, ISerialization::SubstreamsCache & cache)
{
    ISerialization::DeserializeBinaryBulkSettings settings; /// TODO Use avg_value_size_hint.
    const auto & [name, type] = name_and_type;
    auto serialization = IDataType::getSerialization(name_and_type);

    auto create_stream_getter = [&](bool stream_for_prefix)
    {
        return [&, stream_for_prefix] (const ISerialization::SubstreamPath & path) -> ReadBuffer * //-V1047
        {
            if (cache.count(ISerialization::getSubcolumnNameForStream(path)))
                return nullptr;

            String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
            const auto & file_it = storage.files_by_stream_name.find(stream_name);
            if (storage.files_by_stream_name.end() == file_it)
                throw Exception("Logical error: no information about file " + stream_name + " in StorageLog", ErrorCodes::LOGICAL_ERROR);

            UInt64 offset = 0;
            if (!stream_for_prefix && mark_number)
                offset = file_it->second.marks[mark_number].offset;

            auto & data_file_path = file_it->second.data_file_path;
            auto it = streams.try_emplace(stream_name, storage.disk, data_file_path, offset, max_read_buffer_size).first;

            return &it->second.compressed;
        };
    };

    if (deserialize_states.count(name) == 0)
    {
        settings.getter = create_stream_getter(true);
        serialization->deserializeBinaryBulkStatePrefix(settings, deserialize_states[name]);
    }

    settings.getter = create_stream_getter(false);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, settings, deserialize_states[name], &cache);
}


class LogSink final : public SinkToStorage
{
public:
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    explicit LogSink(
        StorageLog & storage_, const StorageMetadataPtr & metadata_snapshot_, WriteLock && lock_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
    {
        if (!lock)
            throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

        /// Load the marks if not loaded yet. We have to do that now because we're going to update these marks.
        storage.loadMarks(lock);

        /// If there were no files, save zero file sizes to be able to rollback in case of error.
        storage.saveFileSizes(lock);
    }

    String getName() const override { return "LogSink"; }

    ~LogSink() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.

                /// No more writing.
                streams.clear();

                /// Truncate files to the older sizes.
                storage.file_checker.repair();

                /// Remove excessive marks.
                storage.removeUnsavedMarks(lock);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void consume(Chunk chunk) override;
    void onFinish() override;

private:
    StorageLog & storage;
    StorageMetadataPtr metadata_snapshot;
    WriteLock lock;
    bool done = false;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, CompressionCodecPtr codec, size_t max_compress_block_size) :
            plain(disk->writeFile(data_path, max_compress_block_size, WriteMode::Append)),
            compressed(*plain, std::move(codec), max_compress_block_size),
            plain_offset(disk->getFileSize(data_path))
        {
        }

        std::unique_ptr<WriteBuffer> plain;
        CompressedWriteBuffer compressed;

        /// How many bytes were in the file at the time the Stream was created.
        size_t plain_offset;

        /// Used to not write shared offsets of columns for nested structures multiple times.
        bool written = false;

        void finalize()
        {
            compressed.next();
            plain->next();
        }
    };

    using FileStreams = std::map<String, Stream>;
    FileStreams streams;

    using SerializeState = ISerialization::SerializeBinaryBulkStatePtr;
    using SerializeStates = std::map<String, SerializeState>;
    SerializeStates serialize_states;

    ISerialization::OutputStreamGetter createStreamGetter(const NameAndTypePair & name_and_type);

    void writeData(const NameAndTypePair & name_and_type, const IColumn & column);
};


void LogSink::consume(Chunk chunk)
{
    auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
    metadata_snapshot->check(block, true);

    for (auto & stream : streams | boost::adaptors::map_values)
        stream.written = false;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(NameAndTypePair(column.name, column.type), *column.column);
    }
}


void LogSink::onFinish()
{
    if (done)
        return;

    for (auto & stream : streams | boost::adaptors::map_values)
        stream.written = false;

    ISerialization::SerializeBinaryBulkSettings settings;
    for (const auto & column : getPort().getHeader())
    {
        auto it = serialize_states.find(column.name);
        if (it != serialize_states.end())
        {
            settings.getter = createStreamGetter(NameAndTypePair(column.name, column.type));
            auto serialization = column.type->getDefaultSerialization();
            serialization->serializeBinaryBulkStateSuffix(settings, it->second);
        }
    }

    /// Finish write.
    for (auto & stream : streams | boost::adaptors::map_values)
        stream.finalize();
    streams.clear();

    storage.saveMarks(lock);
    storage.saveFileSizes(lock);

    done = true;

    /// unlock should be done from the same thread as lock, and dtor may be
    /// called from different thread, so it should be done here (at least in
    /// case of no exceptions occurred)
    lock.unlock();
}


ISerialization::OutputStreamGetter LogSink::createStreamGetter(const NameAndTypePair & name_and_type)
{
    return [&] (const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto it = streams.find(stream_name);
        if (streams.end() == it)
            throw Exception("Logical error: stream was not created when writing data in LogSink",
                            ErrorCodes::LOGICAL_ERROR);

        Stream & stream = it->second;
        if (stream.written)
            return nullptr;

        return &stream.compressed;
    };
}


void LogSink::writeData(const NameAndTypePair & name_and_type, const IColumn & column)
{
    ISerialization::SerializeBinaryBulkSettings settings;
    const auto & [name, type] = name_and_type;
    auto serialization = type->getDefaultSerialization();

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        const auto & columns = metadata_snapshot->getColumns();

        streams.try_emplace(
            stream_name,
            storage.disk,
            storage.files_by_stream_name.at(stream_name).data_file_path,
            columns.getCodecOrDefault(name_and_type.name),
            storage.max_compress_block_size);
    }, settings.path);

    settings.getter = createStreamGetter(name_and_type);

    if (serialize_states.count(name) == 0)
         serialization->serializeBinaryBulkStatePrefix(settings, serialize_states[name]);

    if (storage.use_marks_file)
    {
        serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
        {
            String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
            const auto & stream = streams.at(stream_name);
            if (stream.written)
                return;

            auto & marks = storage.files_by_stream_name.at(stream_name).marks;
            auto & mark = marks.emplace_back();
            mark.rows = (marks.empty() ? 0 : marks.back().rows) + column.size();
            mark.offset = stream.plain_offset + stream.plain->count();
        }, settings.path);
    }

    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, 0, settings, serialize_states[name]);

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto & stream = streams.at(stream_name);
        if (stream.written)
            return;

        stream.written = true;
        stream.compressed.next();
    }, settings.path);
}


void StorageLog::Mark::write(WriteBuffer & out) const
{
    writeIntBinary(rows, out);
    writeIntBinary(offset, out);
}

void StorageLog::Mark::read(ReadBuffer & in)
{
    readIntBinary(rows, in);
    readIntBinary(offset, in);
}


StorageLog::StorageLog(
    const String & engine_name_,
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage(table_id_)
    , engine_name(engine_name_)
    , disk(std::move(disk_))
    , table_path(relative_path_)
    , use_marks_file(engine_name == "Log")
    , marks_file_path(table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME)
    , file_checker(disk, table_path + "sizes.json")
    , max_compress_block_size(max_compress_block_size_)
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

    for (const auto & column : storage_metadata.getColumns().getAllPhysical())
        addFiles(column);
}


void StorageLog::addFiles(const NameAndTypePair & column)
{
    if (files_by_stream_name.contains(column.name))
        throw Exception("Duplicate column with name " + column.name + " in constructor of StorageLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    ISerialization::StreamCallback stream_callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(column, substream_path);

        if (!files_by_stream_name.contains(stream_name))
        {
            ColumnData column_data;
            column_data.data_file_path = table_path + stream_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION;
            auto it = files_by_stream_name.emplace(stream_name, column_data).first;
            files.emplace_back(&it->second);
            ++file_count;
        }
    };

    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams(stream_callback);
}


void StorageLog::loadMarks(std::chrono::seconds lock_timeout)
{
    if (marks_loaded)
        return;

    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    loadMarks(lock);
}

void StorageLog::loadMarks(const WriteLock & /* already locked for writing */)
{
    if (marks_loaded)
        return;

    if (!use_marks_file)
    {
        /// Use a generated mark.
        Mark mark;
        mark.offset = 0; /// starting from the beginning of the file
        mark.rows = std::numeric_limits<UInt64>::max(); /// number of rows is unknown
        for (auto * file : files)
        {
            file->marks.resize(1);
            file->marks[0] = mark;
        }
        marks_loaded = true;
        return;
    }

    size_t num_marks = 0;
    if (disk->exists(marks_file_path))
    {
        size_t file_size = disk->getFileSize(marks_file_path);
        if (file_size % (file_count * sizeof(Mark)) != 0)
            throw Exception("Size of marks file is inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

        num_marks = file_size / (file_count * sizeof(Mark));

        for (auto * file : files)
            file->marks.resize(num_marks);

        std::unique_ptr<ReadBuffer> marks_rb = disk->readFile(marks_file_path, 32768);
        for (size_t i = 0; i != num_marks; ++i)
        {
            for (auto * file : files)
            {
                Mark mark;
                mark.read(*marks_rb);
                file->marks[i] = mark;
            }
        }
    }

    marks_loaded = true;
    num_marks_saved = num_marks;
}

void StorageLog::saveMarks(const WriteLock & /* already locked for writing */)
{
    if (!use_marks_file)
        return;

    size_t num_marks = file_count ? files[0]->marks.size() : 0;
    if (num_marks_saved == num_marks)
        return;

    for (const auto * file : files)
    {
        if (file->marks.size() != num_marks)
            throw Exception("Wrong number of marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);
    }

    size_t start = num_marks_saved;
    auto write_mode = start ? WriteMode::Append : WriteMode::Rewrite;
    auto marks_stream = disk->writeFile(marks_file_path, 4096, write_mode);

    for (size_t i = start; i != num_marks; ++i)
    {
        for (const auto * file : files)
        {
            const auto & mark = file->marks[i];
            mark.write(*marks_stream);
        }
    }

    marks_stream->next();
    marks_stream->finalize();

    num_marks_saved = num_marks;
}


void StorageLog::removeUnsavedMarks(const WriteLock & /* already locked for writing */)
{
    if (!use_marks_file)
        return;

    for (auto * file : files)
    {
        if (file->marks.size() > num_marks_saved)
            file->marks.resize(num_marks_saved);
    }
}


void StorageLog::saveFileSizes(const WriteLock & /* already locked for writing */)
{
    if (file_checker.empty())
    {
        /// If there were no files, save zero file sizes.
        for (const auto * file : files)
            file_checker.setEmpty(file->data_file_path);
        if (use_marks_file)
            file_checker.setEmpty(marks_file_path);
    }
    else
    {
        for (const auto * file : files)
            file_checker.update(file->data_file_path);
        if (use_marks_file)
            file_checker.update(marks_file_path);
    }
    file_checker.save();
}


void StorageLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        file_checker.setPath(table_path + "sizes.json");

        for (auto * file : files)
            file->data_file_path = table_path + fileName(file->data_file_path);

        marks_file_path = table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME;
    }
    renameInMemory(new_table_id);
}

void StorageLog::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    disk->clearDirectory(table_path);
    file_checker = FileChecker{disk, table_path + "sizes.json"};

    for (auto * file : files)
        file->marks.clear();
    marks_loaded = false;
    num_marks_saved = 0;
}


static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    Int64 lock_timeout = settings.lock_acquire_timeout.totalSeconds();
    if (settings.max_execution_time.totalSeconds() != 0 && settings.max_execution_time.totalSeconds() < lock_timeout)
        lock_timeout = settings.max_execution_time.totalSeconds();
    return std::chrono::seconds{lock_timeout};
}


Pipe StorageLog::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    auto lock_timeout = getLockTimeout(context);
    loadMarks(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    if (!file_count || files[0]->marks.empty())
        return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));

    auto all_columns = metadata_snapshot->getColumns().getByNames(ColumnsDescription::All, column_names, true);
    all_columns = Nested::convertToSubcolumns(all_columns);

    Pipes pipes;

    const Marks & marks = files[0]->marks;
    size_t marks_size = marks.size();

    if (num_streams > marks_size)
        num_streams = marks_size;

    size_t max_read_buffer_size = context->getSettingsRef().max_read_buffer_size;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        size_t mark_begin = stream * marks_size / num_streams;
        size_t mark_end = (stream + 1) * marks_size / num_streams;

        size_t rows_begin = mark_begin ? marks[mark_begin - 1].rows : 0;
        size_t rows_end = mark_end ? marks[mark_end - 1].rows : 0;

        pipes.emplace_back(std::make_shared<LogSource>(
            max_block_size,
            all_columns,
            *this,
            mark_begin,
            rows_end - rows_begin,
            max_read_buffer_size));
    }

    /// No need to hold lock while reading because we read fixed range of data that does not change while appending more data.
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    auto lock_timeout = getLockTimeout(context);
    loadMarks(lock_timeout);

    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return std::make_shared<LogSink>(*this, metadata_snapshot, std::move(lock));
}

CheckResults StorageLog::checkData(const ASTPtr & /* query */, ContextPtr context)
{
    ReadLock lock{rwlock, getLockTimeout(context)};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return file_checker.check();
}


IStorage::ColumnSizeByName StorageLog::getColumnSizes() const
{
    ReadLock lock{rwlock, std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC)};
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    ColumnSizeByName column_sizes;
    FileChecker::Map file_sizes = file_checker.getFileSizes();

    for (const auto & column : getInMemoryMetadata().getColumns().getAllPhysical())
    {
        ISerialization::StreamCallback stream_callback = [&, this] (const ISerialization::SubstreamPath & substream_path)
        {
            String stream_name = ISerialization::getFileNameForStream(column, substream_path);
            auto it = files_by_stream_name.find(stream_name);
            if (it != files_by_stream_name.end())
            {
                const auto & file = it->second;
                column_sizes[column.name].data_compressed += file_sizes[fileName(file.data_file_path)];
            }
        };

        ISerialization::SubstreamPath substream_path;
        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams(stream_callback, substream_path);
    }

    return column_sizes;
}


BackupEntries StorageLog::backup(const ASTs & partitions, ContextPtr context) const
{
    if (!partitions.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support partitions", getName());

    ReadLock lock{rwlock, getLockTimeout(context)};

    if (!file_count)
        return {};

    BackupEntries backup_entries;

    auto temp_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, "tmp/backup_");
    auto temp_dir = temp_dir_owner->getPath();
    disk->createDirectories(temp_dir);

    FileChecker::Map file_sizes = file_checker.getFileSizes();
    for (const auto & [file_name, file_size] : file_sizes)
    {
        String temp_file_path = temp_dir + "/" + file_name;
        disk->copy(table_path + file_name, disk, temp_file_path);
        backup_entries.emplace_back(
            file_name, std::make_unique<BackupEntryFromImmutableFile>(disk, temp_file_path, file_size, std::nullopt, temp_dir_owner));
    }

    backup_entries.emplace_back("sizes.json", std::make_unique<BackupEntryFromSmallFile>(disk, table_path + "sizes.json"));

    return backup_entries;
}


RestoreDataTasks StorageLog::restoreFromBackup(const BackupPtr & backup, const String & data_path_in_backup, const ASTs & partitions, ContextMutablePtr context)
{
    if (!partitions.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table engine {} doesn't support partitions", getName());

    auto restore_task = [this, backup, data_path_in_backup, context]
    {
        if (backup->list(data_path_in_backup).empty())
            return; /// no files in backup

        WriteLock lock{rwlock, getLockTimeout(context)};
        loadMarks(lock);
        saveFileSizes(lock);

        bool done = false;
        SCOPE_EXIT({
            if (!done)
            {
                file_checker.repair();
                removeUnsavedMarks(lock);
            }
        });

        for (auto * file : files)
        {
            auto entry = backup->read(data_path_in_backup + fileName(file->data_file_path));
            auto read_buffer = entry->getReadBuffer();
            auto write_buffer = disk->writeFile(file->data_file_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
            copyData(*read_buffer, *write_buffer);
        }

        if (use_marks_file)
        {
            auto entry = backup->read(data_path_in_backup + fileName(marks_file_path));
            auto read_buffer = entry->getReadBuffer();

            const auto & file_sizes = file_checker.getFileSizes();
            std::vector<size_t> initial_offset(file_count);
            for (size_t i = 0; i != file_count; ++i)
                initial_offset[i] = file_sizes.at(fileName(files[i]->data_file_path));
            size_t initial_rows = file_count ? files[0]->marks.back().rows : 0;

            while (!read_buffer->eof())
            {
                for (size_t i = 0; i != file_count; ++i)
                {
                    Mark mark;
                    mark.read(*read_buffer);
                    if (num_marks_saved)
                    {
                        mark.offset += initial_offset[i];
                        mark.rows += initial_rows;
                    }
                    files[i]->marks.emplace_back(mark);
                }
            }
        }

        saveMarks(lock);
        saveFileSizes(lock);
        done = true;
    };

    return {restore_task};
}


void registerStorageLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    auto create_fn = [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return StorageLog::create(
            args.engine_name,
            disk,
            args.relative_data_path,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.attach,
            args.getContext()->getSettings().max_compress_block_size);
    };

    factory.registerStorage("Log", create_fn, features);
    factory.registerStorage("TinyLog", create_fn, features);
}

}
