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

#include <DataTypes/NestedUtils.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include "StorageLogSettings.h"
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Pipe.h>

#include <cassert>


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
            const auto & file_it = storage.files.find(stream_name);
            if (storage.files.end() == file_it)
                throw Exception("Logical error: no information about file " + stream_name + " in StorageLog", ErrorCodes::LOGICAL_ERROR);

            UInt64 offset = 0;
            if (!stream_for_prefix && mark_number)
            {
                std::lock_guard marks_lock(file_it->second.marks_mutex);
                offset = file_it->second.marks[mark_number].offset;
            }

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


class LogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit LogBlockOutputStream(
        StorageLog & storage_, const StorageMetadataPtr & metadata_snapshot_, std::unique_lock<std::shared_timed_mutex> && lock_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , lock(std::move(lock_))
        , marks_stream(
            storage.disk->writeFile(storage.marks_file_path, 4096, WriteMode::Rewrite))
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

    ~LogBlockOutputStream() override
    {
        try
        {
            if (!done)
            {
                /// Rollback partial writes.
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
    StorageLog & storage;
    StorageMetadataPtr metadata_snapshot;
    std::unique_lock<std::shared_timed_mutex> lock;
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

        size_t plain_offset;    /// How many bytes were in the file at the time the LogBlockOutputStream was created.

        void finalize()
        {
            compressed.next();
            plain->next();
        }
    };

    using Mark = StorageLog::Mark;
    using MarksForColumns = std::vector<std::pair<size_t, Mark>>;

    using FileStreams = std::map<String, Stream>;
    FileStreams streams;

    using WrittenStreams = std::set<String>;

    std::unique_ptr<WriteBuffer> marks_stream; /// Declared below `lock` to make the file open when rwlock is captured.

    using SerializeState = ISerialization::SerializeBinaryBulkStatePtr;
    using SerializeStates = std::map<String, SerializeState>;
    SerializeStates serialize_states;

    ISerialization::OutputStreamGetter createStreamGetter(const NameAndTypePair & name_and_type, WrittenStreams & written_streams);

    void writeData(
        const NameAndTypePair & name_and_type,
        const IColumn & column,
        MarksForColumns & out_marks,
        WrittenStreams & written_streams);

    void writeMarks(MarksForColumns && marks);
};


void LogBlockOutputStream::write(const Block & block)
{
    metadata_snapshot->check(block, true);

    /// The set of written offset columns so that you do not write shared offsets of columns for nested structures multiple times
    WrittenStreams written_streams;

    MarksForColumns marks;
    marks.reserve(storage.file_count);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(NameAndTypePair(column.name, column.type), *column.column, marks, written_streams);
    }

    writeMarks(std::move(marks));
}


void LogBlockOutputStream::writeSuffix()
{
    if (done)
        return;

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
    marks_stream->next();
    marks_stream->finalize();

    for (auto & name_stream : streams)
        name_stream.second.finalize();

    Strings column_files;
    for (const auto & name_stream : streams)
        column_files.push_back(storage.files[name_stream.first].data_file_path);
    column_files.push_back(storage.marks_file_path);

    for (const auto & file : column_files)
        storage.file_checker.update(file);
    storage.file_checker.save();

    streams.clear();
    done = true;

    /// unlock should be done from the same thread as lock, and dtor may be
    /// called from different thread, so it should be done here (at least in
    /// case of no exceptions occurred)
    lock.unlock();
}


ISerialization::OutputStreamGetter LogBlockOutputStream::createStreamGetter(const NameAndTypePair & name_and_type,
                                                                       WrittenStreams & written_streams)
{
    return [&] (const ISerialization::SubstreamPath & path) -> WriteBuffer *
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        if (written_streams.count(stream_name))
            return nullptr;

        auto it = streams.find(stream_name);
        if (streams.end() == it)
            throw Exception("Logical error: stream was not created when writing data in LogBlockOutputStream",
                            ErrorCodes::LOGICAL_ERROR);
        return &it->second.compressed;
    };
}


void LogBlockOutputStream::writeData(const NameAndTypePair & name_and_type, const IColumn & column,
    MarksForColumns & out_marks, WrittenStreams & written_streams)
{
    ISerialization::SerializeBinaryBulkSettings settings;
    const auto & [name, type] = name_and_type;
    auto serialization = type->getDefaultSerialization();

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        if (written_streams.count(stream_name))
            return;

        const auto & columns = metadata_snapshot->getColumns();
        streams.try_emplace(
            stream_name,
            storage.disk,
            storage.files[stream_name].data_file_path,
            columns.getCodecOrDefault(name_and_type.name),
            storage.max_compress_block_size);
    }, settings.path);

    settings.getter = createStreamGetter(name_and_type, written_streams);

    if (serialize_states.count(name) == 0)
         serialization->serializeBinaryBulkStatePrefix(settings, serialize_states[name]);

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        if (written_streams.count(stream_name))
            return;

        const auto & file = storage.files[stream_name];
        const auto stream_it = streams.find(stream_name);

        Mark mark;
        mark.rows = (file.marks.empty() ? 0 : file.marks.back().rows) + column.size();
        mark.offset = stream_it->second.plain_offset + stream_it->second.plain->count();

        out_marks.emplace_back(file.column_index, mark);
    }, settings.path);

    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, 0, settings, serialize_states[name]);

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, path);
        if (!written_streams.emplace(stream_name).second)
            return;

        auto it = streams.find(stream_name);
        if (streams.end() == it)
            throw Exception("Logical error: stream was not created when writing data in LogBlockOutputStream", ErrorCodes::LOGICAL_ERROR);
        it->second.compressed.next();
    }, settings.path);
}


void LogBlockOutputStream::writeMarks(MarksForColumns && marks)
{
    if (marks.size() != storage.file_count)
        throw Exception("Wrong number of marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);

    std::sort(marks.begin(), marks.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

    for (const auto & mark : marks)
    {
        writeIntBinary(mark.second.rows, *marks_stream);
        writeIntBinary(mark.second.offset, *marks_stream);

        size_t column_index = mark.first;

        auto & file = storage.files[storage.column_names_by_idx[column_index]];
        std::lock_guard marks_lock(file.marks_mutex);
        file.marks.push_back(mark.second);
    }
}

StorageLog::StorageLog(
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

    marks_file_path = table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME;
}


void StorageLog::addFiles(const NameAndTypePair & column)
{
    if (files.end() != files.find(column.name))
        throw Exception("Duplicate column with name " + column.name + " in constructor of StorageLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    ISerialization::StreamCallback stream_callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(column, substream_path);

        if (!files.count(stream_name))
        {
            ColumnData & column_data = files[stream_name];
            column_data.column_index = file_count;
            column_data.data_file_path = table_path + stream_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION;

            column_names_by_idx.push_back(stream_name);
            ++file_count;
        }
    };

    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams(stream_callback);
}


void StorageLog::loadMarks(std::chrono::seconds lock_timeout)
{
    std::unique_lock lock(rwlock, lock_timeout);
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    if (loaded_marks)
        return;

    using FilesByIndex = std::vector<Files::iterator>;

    FilesByIndex files_by_index(file_count);
    for (Files::iterator it = files.begin(); it != files.end(); ++it)
        files_by_index[it->second.column_index] = it;

    if (disk->exists(marks_file_path))
    {
        size_t file_size = disk->getFileSize(marks_file_path);
        if (file_size % (file_count * sizeof(Mark)) != 0)
            throw Exception("Size of marks file is inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

        size_t marks_count = file_size / (file_count * sizeof(Mark));

        for (auto & file : files_by_index)
            file->second.marks.reserve(marks_count);

        std::unique_ptr<ReadBuffer> marks_rb = disk->readFile(marks_file_path, 32768);
        while (!marks_rb->eof())
        {
            for (auto & file : files_by_index)
            {
                Mark mark;
                readIntBinary(mark.rows, *marks_rb);
                readIntBinary(mark.offset, *marks_rb);
                file->second.marks.push_back(mark);
            }
        }
    }

    loaded_marks = true;
}


void StorageLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        file_checker.setPath(table_path + "sizes.json");

        for (auto & file : files)
            file.second.data_file_path = table_path + fileName(file.second.data_file_path);

        marks_file_path = table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME;
    }
    renameInMemory(new_table_id);
}

void StorageLog::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    files.clear();
    file_count = 0;
    loaded_marks = false;

    disk->clearDirectory(table_path);

    for (const auto & column : metadata_snapshot->getColumns().getAllPhysical())
        addFiles(column);

    file_checker = FileChecker{disk, table_path + "sizes.json"};
    marks_file_path = table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME;
}


const StorageLog::Marks & StorageLog::getMarksWithRealRowCount(const StorageMetadataPtr & metadata_snapshot) const
{
    /// There should be at least one physical column
    auto column = *metadata_snapshot->getColumns().getAllPhysical().begin();
    String filename;

    /** We take marks from first column.
      * If this is a data type with multiple stream, get the first stream, that we assume have real row count.
      * (Example: for Array data type, first stream is array sizes; and number of array sizes is the number of arrays).
      */
    ISerialization::SubstreamPath substream_root_path;
    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        if (filename.empty())
            filename = ISerialization::getFileNameForStream(column, substream_path);
    }, substream_root_path);

    Files::const_iterator it = files.find(filename);
    if (files.end() == it)
        throw Exception("Cannot find file " + filename, ErrorCodes::LOGICAL_ERROR);

    return it->second.marks;
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

    auto all_columns = metadata_snapshot->getColumns().getByNames(ColumnsDescription::All, column_names, true);
    all_columns = Nested::convertToSubcolumns(all_columns);

    std::shared_lock lock(rwlock, lock_timeout);
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    if (file_checker.empty())
        return Pipe(std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));

    Pipes pipes;

    const Marks & marks = getMarksWithRealRowCount(metadata_snapshot);
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

BlockOutputStreamPtr StorageLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    auto lock_timeout = getLockTimeout(context);
    loadMarks(lock_timeout);

    std::unique_lock lock(rwlock, lock_timeout);
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return std::make_shared<LogBlockOutputStream>(*this, metadata_snapshot, std::move(lock));
}

CheckResults StorageLog::checkData(const ASTPtr & /* query */, ContextPtr context)
{
    std::shared_lock lock(rwlock, getLockTimeout(context));
    if (!lock)
        throw Exception("Lock timeout exceeded", ErrorCodes::TIMEOUT_EXCEEDED);

    return file_checker.check();
}


void registerStorageLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    factory.registerStorage("Log", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String disk_name = getDiskName(*args.storage_def);
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return StorageLog::create(
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
