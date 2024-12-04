#include <Storages/StorageLog.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageLogSettings.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTCheckQuery.h>

#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>

#include <DataTypes/NestedUtils.h>

#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <cassert>
#include <chrono>

#include <boost/range/adaptor/map.hpp>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION ".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_NAME "__marks.mrk"


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
    extern const int DUPLICATE_COLUMN;
    extern const int SIZES_OF_MARKS_FILES_ARE_INCONSISTENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int NOT_IMPLEMENTED;
}

/// NOTE: The lock `StorageLog::rwlock` is NOT kept locked while reading,
/// because we read ranges of data that do not change.
class LogSource final : public ISource
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
        size_t block_size_,
        const NamesAndTypesList & columns_,
        const StorageLog & storage_,
        size_t rows_limit_,
        const std::vector<size_t> & offsets_,
        const std::vector<size_t> & file_sizes_,
        bool limited_by_file_sizes_,
        ReadSettings read_settings_)
        : ISource(getHeader(columns_))
        , block_size(block_size_)
        , columns(columns_)
        , storage(storage_)
        , rows_limit(rows_limit_)
        , offsets(offsets_)
        , file_sizes(file_sizes_)
        , limited_by_file_sizes(limited_by_file_sizes_)
        , read_settings(std::move(read_settings_))
    {
    }

    String getName() const override { return "Log"; }

protected:
    Chunk generate() override;

private:
    NameAndTypePair getColumnOnDisk(const NameAndTypePair & column) const;

    const size_t block_size;
    const NamesAndTypesList columns;
    const StorageLog & storage;
    const size_t rows_limit;      /// The maximum number of rows that can be read
    size_t rows_read = 0;
    bool is_finished = false;
    const std::vector<size_t> offsets;
    const std::vector<size_t> file_sizes;
    const bool limited_by_file_sizes;
    const ReadSettings read_settings;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, size_t offset, size_t file_size, bool limited_by_file_size, ReadSettings read_settings_)
        {
            plain = disk->readFile(data_path, read_settings_.adjustBufferSize(file_size));

            if (offset)
                plain->seek(offset, SEEK_SET);

            if (limited_by_file_size)
            {
                limited.emplace(*plain, file_size - offset, /* trow_exception */ false, /* exact_limit */ std::optional<size_t>());
                compressed.emplace(*limited);
            }
            else
                compressed.emplace(*plain);
        }

        std::unique_ptr<ReadBufferFromFileBase> plain;
        std::optional<LimitReadBuffer> limited;
        std::optional<CompressedReadBuffer> compressed;
    };

    using FileStreams = std::map<String, Stream>;
    FileStreams streams;

    using DeserializeState = ISerialization::DeserializeBinaryBulkStatePtr;
    using DeserializeStates = std::map<String, DeserializeState>;
    DeserializeStates deserialize_states;

    void readData(const NameAndTypePair & name_and_type, ColumnPtr & column, size_t max_rows_to_read, ISerialization::SubstreamsCache & cache);
    bool isFinished();
};

NameAndTypePair LogSource::getColumnOnDisk(const NameAndTypePair & column) const
{
    const auto & storage_columns = storage.columns_with_collected_nested;

    /// A special case when we read subcolumn of shared offsets of Nested.
    /// E.g. instead of requested column "n.arr1.size0" we must read column "n.size0" from disk.
    auto name_in_storage = column.getNameInStorage();
    if (column.getSubcolumnName() == "size0" && Nested::isSubcolumnOfNested(name_in_storage, storage_columns))
    {
        auto nested_name_in_storage = Nested::splitName(name_in_storage).first;
        auto new_name = Nested::concatenateName(nested_name_in_storage, column.getSubcolumnName());
        return storage_columns.getColumnOrSubcolumn(GetColumnsOptions::All, new_name);
    }

    return column;
}

Chunk LogSource::generate()
{
    if (isFinished())
    {
        /// Close the files (before destroying the object).
        /// When many sources are created, but simultaneously reading only a few of them,
        /// buffers don't waste memory.
        streams.clear();
        return {};
    }

    /// How many rows to read for the next block.
    size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);
    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    Block res;

    for (const auto & name_type : columns)
    {
        ColumnPtr column;
        auto name_type_on_disk = getColumnOnDisk(name_type);

        try
        {
            column = name_type_on_disk.type->createColumn();
            readData(name_type_on_disk, column, max_rows_to_read, caches[name_type_on_disk.getNameInStorage()]);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name_type_on_disk.name + " at " + fullPath(storage.disk, storage.table_path));
            throw;
        }

        if (!column->empty())
            res.insert(ColumnWithTypeAndName(column, name_type_on_disk.type, name_type_on_disk.name));
    }

    if (res)
        rows_read += res.rows();

    if (!res)
        is_finished = true;

    if (isFinished())
    {
        /// Close the files (before destroying the object).
        /// When many sources are created, but simultaneously reading only a few of them,
        /// buffers don't waste memory.
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
        return [&, stream_for_prefix] (const ISerialization::SubstreamPath & path) -> ReadBuffer *
        {
            if (cache.contains(ISerialization::getSubcolumnNameForStream(path)))
                return nullptr;

            String data_file_name = ISerialization::getFileNameForStream(name_and_type, path);

            const auto & data_file_it = storage.data_files_by_names.find(data_file_name);
            if (data_file_it == storage.data_files_by_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No information about file {} in StorageLog", data_file_name);
            const auto & data_file = *data_file_it->second;

            size_t offset = stream_for_prefix ? 0 : offsets[data_file.index];
            size_t file_size = file_sizes[data_file.index];

            auto it = streams.try_emplace(data_file_name, storage.disk, data_file.path, offset, file_size, limited_by_file_sizes, read_settings).first;
            return &it->second.compressed.value();
        };
    };

    if (!deserialize_states.contains(name))
    {
        settings.getter = create_stream_getter(true);
        serialization->deserializeBinaryBulkStatePrefix(settings, deserialize_states[name], nullptr);
    }

    settings.getter = create_stream_getter(false);
    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, settings, deserialize_states[name], &cache);
}

bool LogSource::isFinished()
{
    if (is_finished)
        return true;

    /// Check for row limit.
    if (rows_read == rows_limit)
    {
        is_finished = true;
        return true;
    }

    if (limited_by_file_sizes)
    {
        /// Check for EOF.
        if (!streams.empty() && streams.begin()->second.compressed->eof())
        {
            is_finished = true;
            return true;
        }
    }

    return false;
}


/// NOTE: The lock `StorageLog::rwlock` is kept locked in exclusive mode while writing.
class LogSink final : public SinkToStorage
{
public:
    using WriteLock = std::unique_lock<std::shared_timed_mutex>;

    explicit LogSink(
        StorageLog & storage_, const StorageMetadataPtr & metadata_snapshot_, WriteLock && lock_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , storage_snapshot(std::make_shared<StorageSnapshot>(storage, metadata_snapshot))
        , lock(std::move(lock_))
    {
        if (!lock)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

        /// Ensure that marks are loaded because we're going to update them.
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
                for (auto & [_, stream] : streams)
                {
                    stream.cancel();
                }
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

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    StorageLog & storage;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    WriteLock lock;
    bool done = false;

    struct Stream
    {
        Stream(const DiskPtr & disk, const String & data_path, size_t initial_data_size, CompressionCodecPtr codec, size_t max_compress_block_size) :
            plain(disk->writeFile(data_path, max_compress_block_size, WriteMode::Append)),
            compressed(*plain, std::move(codec), max_compress_block_size),
            plain_offset(initial_data_size)
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
            compressed.finalize();

            plain->next();
            plain->finalize();
        }

        void cancel()
        {
            compressed.cancel();
            plain->cancel();
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


void LogSink::consume(Chunk & chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.getColumns());
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
    for (const auto & column : getHeader())
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
    storage.updateTotalRows(lock);

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
        String data_file_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto it = streams.find(data_file_name);
        if (it == streams.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream was not created when writing data in LogSink");

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
        String data_file_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto it = streams.find(data_file_name);
        if (it == streams.end())
        {
            const auto & data_file_it = storage.data_files_by_names.find(data_file_name);
            if (data_file_it == storage.data_files_by_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No information about file {} in StorageLog", data_file_name);

            const auto & data_file = *data_file_it->second;
            auto compression = storage_snapshot->getCodecOrDefault(name_and_type.name);

            it = streams.try_emplace(data_file.name, storage.disk, data_file.path,
                                     storage.file_checker.getFileSize(data_file.path),
                                     compression, storage.max_compress_block_size).first;
        }

        auto & stream = it->second;
        if (stream.written)
            return;
    });

    settings.getter = createStreamGetter(name_and_type);

    if (!serialize_states.contains(name))
         serialization->serializeBinaryBulkStatePrefix(column, settings, serialize_states[name]);

    if (storage.use_marks_file)
    {
        serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
        {
            String data_file_name = ISerialization::getFileNameForStream(name_and_type, path);
            const auto & stream = streams.at(data_file_name);
            if (stream.written)
                return;

            auto & data_file = *storage.data_files_by_names.at(data_file_name);
            auto & marks = data_file.marks;
            size_t prev_num_rows = marks.empty() ? 0 : marks.back().rows;
            auto & mark = marks.emplace_back();
            mark.rows = prev_num_rows + column.size();
            mark.offset = stream.plain_offset + stream.plain->count();
        });
    }

    serialization->serializeBinaryBulkWithMultipleStreams(column, 0, 0, settings, serialize_states[name]);

    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & path)
    {
        String data_file_name = ISerialization::getFileNameForStream(name_and_type, path);
        auto & stream = streams.at(data_file_name);
        if (stream.written)
            return;

        stream.written = true;
        stream.compressed.next();
    });
}


void StorageLog::Mark::write(WriteBuffer & out) const
{
    writeBinaryLittleEndian(rows, out);
    writeBinaryLittleEndian(offset, out);
}


void StorageLog::Mark::read(ReadBuffer & in)
{
    readBinaryLittleEndian(rows, in);
    readBinaryLittleEndian(offset, in);
}


namespace
{
    /// NOTE: We extract the number of rows from the marks.
    /// For normal columns, the number of rows in the block is specified in the marks.
    /// For array columns and nested structures, there are more than one group of marks that correspond to different files
    ///  - for elements (file name.bin) - the total number of array elements in the block is specified,
    ///  - for array sizes (file name.size0.bin) - the number of rows (the whole arrays themselves) in the block is specified.
    /// So for Array data type, first stream is array sizes; and number of array sizes is the number of arrays.
    /// Thus we assume we can always get the real number of rows from the first column.
    constexpr size_t INDEX_WITH_REAL_ROW_COUNT = 0;
}


StorageLog::~StorageLog() = default;

StorageLog::StorageLog(
    const String & engine_name_,
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_)
    : IStorage(table_id_)
    , WithMutableContext(context_)
    , engine_name(engine_name_)
    , disk(std::move(disk_))
    , table_path(relative_path_)
    , use_marks_file(engine_name == "Log")
    , marks_file_path(table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME)
    , file_checker(disk, table_path + "sizes.json")
    , max_compress_block_size(context_->getSettingsRef()[Setting::max_compress_block_size])
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Storage {} requires data path", getName());

    /// Enumerate data files.
    for (const auto & column : storage_metadata.getColumns().getAllPhysical())
        addDataFiles(column);

    /// Ensure the file checker is initialized.
    if (file_checker.empty())
    {
        for (const auto & data_file : data_files)
            file_checker.setEmpty(data_file.path);
        if (use_marks_file)
            file_checker.setEmpty(marks_file_path);
    }

    if (mode < LoadingStrictnessLevel::ATTACH)
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

    columns_with_collected_nested = ColumnsDescription{Nested::collect(columns_.getAll())};
    total_bytes = file_checker.getTotalSize();
}


void StorageLog::addDataFiles(const NameAndTypePair & column)
{
    if (data_files_by_names.contains(column.name))
        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Duplicate column with name {} in constructor of StorageLog.",
            column.name);

    ISerialization::StreamCallback stream_callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String data_file_name = ISerialization::getFileNameForStream(column, substream_path);
        if (!data_files_by_names.contains(data_file_name))
        {
            DataFile & data_file = data_files.emplace_back();
            data_file.name = data_file_name;
            data_file.path = table_path + data_file_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION;
            data_file.index = num_data_files++;
            data_files_by_names.emplace(data_file_name, nullptr);
        }
    };

    column.type->getDefaultSerialization()->enumerateStreams(stream_callback);

    for (auto & data_file : data_files)
        data_files_by_names[data_file.name] = &data_file;
}


void StorageLog::loadMarks(std::chrono::seconds lock_timeout)
{
    if (!use_marks_file || marks_loaded)
        return;

    /// We load marks with an exclusive lock (i.e. the write lock) because we don't want
    /// a data race between two threads trying to load marks simultaneously.
    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    loadMarks(lock);
}

void StorageLog::loadMarks(const WriteLock & lock /* already locked exclusively */)
{
    if (!use_marks_file || marks_loaded)
        return;

    size_t num_marks = 0;
    if (disk->exists(marks_file_path))
    {
        size_t file_size = disk->getFileSize(marks_file_path);
        if (file_size % (num_data_files * sizeof(Mark)) != 0)
            throw Exception(ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT, "Size of marks file is inconsistent");

        num_marks = file_size / (num_data_files * sizeof(Mark));

        for (auto & data_file : data_files)
            data_file.marks.resize(num_marks);

        std::unique_ptr<ReadBuffer> marks_rb = disk->readFile(marks_file_path, ReadSettings().adjustBufferSize(32768));
        for (size_t i = 0; i != num_marks; ++i)
        {
            for (auto & data_file : data_files)
            {
                Mark mark;
                mark.read(*marks_rb);
                data_file.marks[i] = mark;
            }
        }
    }

    marks_loaded = true;
    num_marks_saved = num_marks;

    /// We need marks to calculate the number of rows, and now we have the marks.
    updateTotalRows(lock);
}

void StorageLog::saveMarks(const WriteLock & /* already locked for writing */)
{
    if (!use_marks_file)
        return;

    size_t num_marks = num_data_files ? data_files[0].marks.size() : 0;
    if (num_marks_saved == num_marks)
        return;

    for (const auto & data_file : data_files)
    {
        if (data_file.marks.size() != num_marks)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of marks generated from block. Makes no sense.");
    }

    size_t start = num_marks_saved;
    auto marks_stream = disk->writeFile(marks_file_path, 4096, WriteMode::Append);

    for (size_t i = start; i != num_marks; ++i)
    {
        for (const auto & data_file : data_files)
        {
            const auto & mark = data_file.marks[i];
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

    for (auto & data_file : data_files)
    {
        if (data_file.marks.size() > num_marks_saved)
            data_file.marks.resize(num_marks_saved);
    }
}


void StorageLog::saveFileSizes(const WriteLock & /* already locked for writing */)
{
    for (const auto & data_file : data_files)
        file_checker.update(data_file.path);

    if (use_marks_file)
        file_checker.update(marks_file_path);

    file_checker.save();
    total_bytes = file_checker.getTotalSize();
}


void StorageLog::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    assert(table_path != new_path_to_table_data);
    {
        disk->createDirectories(new_path_to_table_data);
        disk->moveDirectory(table_path, new_path_to_table_data);

        table_path = new_path_to_table_data;
        file_checker.setPath(table_path + "sizes.json");

        for (auto & data_file : data_files)
            data_file.path = table_path + fileName(data_file.path);

        marks_file_path = table_path + DBMS_STORAGE_LOG_MARKS_FILE_NAME;
    }
    renameInMemory(new_table_id);
}

static std::chrono::seconds getLockTimeout(ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    Int64 lock_timeout = settings[Setting::lock_acquire_timeout].totalSeconds();
    if (settings[Setting::max_execution_time].totalSeconds() != 0 && settings[Setting::max_execution_time].totalSeconds() < lock_timeout)
        lock_timeout = settings[Setting::max_execution_time].totalSeconds();
    return std::chrono::seconds{lock_timeout};
}

void StorageLog::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    WriteLock lock{rwlock, getLockTimeout(local_context)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    disk->clearDirectory(table_path);

    for (auto & data_file : data_files)
    {
        data_file.marks.clear();
        file_checker.setEmpty(data_file.path);
    }

    if (use_marks_file)
        file_checker.setEmpty(marks_file_path);

    marks_loaded = true;
    num_marks_saved = 0;
    total_rows = 0;
    total_bytes = 0;
    getContext()->clearMMappedFileCache();
}


Pipe StorageLog::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    auto lock_timeout = getLockTimeout(local_context);
    loadMarks(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    if (!num_data_files || !file_checker.getFileSize(data_files[INDEX_WITH_REAL_ROW_COUNT].path))
        return Pipe(std::make_shared<NullSource>(storage_snapshot->getSampleBlockForColumns(column_names)));

    const Marks & marks_with_real_row_count = data_files[INDEX_WITH_REAL_ROW_COUNT].marks;
    size_t num_marks = marks_with_real_row_count.size();

    size_t max_streams = use_marks_file ? num_marks : 1;
    num_streams = std::min(num_streams, max_streams);

    std::vector<size_t> offsets;
    offsets.resize(num_data_files, 0);

    std::vector<size_t> file_sizes;
    file_sizes.resize(num_data_files, 0);
    for (const auto & data_file : data_files)
        file_sizes[data_file.index] = file_checker.getFileSize(data_file.path);

    /// For TinyLog (use_marks_file == false) there is no row limit and we just read
    /// the data files up to their sizes.
    bool limited_by_file_sizes = !use_marks_file;
    size_t row_limit = std::numeric_limits<size_t>::max();

    ReadSettings read_settings = local_context->getReadSettings();
    Pipes pipes;

    /// Converting to subcolumns of Nested is needed for
    /// correct reading of parts of Nested with shared offsets.
    auto options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns();
    auto all_columns = storage_snapshot->getColumnsByNames(options, column_names);
    all_columns = Nested::convertToSubcolumns(all_columns);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        if (use_marks_file)
        {
            size_t mark_begin = stream * num_marks / num_streams;
            size_t mark_end = (stream + 1) * num_marks / num_streams;
            size_t start_row = mark_begin ? marks_with_real_row_count[mark_begin - 1].rows : 0;
            size_t end_row = mark_end ? marks_with_real_row_count[mark_end - 1].rows : 0;
            row_limit = end_row - start_row;
            for (const auto & data_file : data_files)
                offsets[data_file.index] = data_file.marks[mark_begin].offset;
        }

        pipes.emplace_back(std::make_shared<LogSource>(
            max_block_size,
            all_columns,
            *this,
            row_limit,
            offsets,
            file_sizes,
            limited_by_file_sizes,
            read_settings));
    }

    /// No need to hold lock while reading because we read fixed range of data that does not change while appending more data.
    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr StorageLog::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    WriteLock lock{rwlock, getLockTimeout(local_context)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    return std::make_shared<LogSink>(*this, metadata_snapshot, std::move(lock));
}

IStorage::DataValidationTasksPtr StorageLog::getCheckTaskList(
    const std::variant<std::monostate, ASTPtr, String> & check_task_filter, ContextPtr local_context)
{
    if (!std::holds_alternative<std::monostate>(check_task_filter))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CHECK PART/PARTITION are not supported for {}", getName());

    ReadLock lock{rwlock, getLockTimeout(local_context)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    return std::make_unique<DataValidationTasks>(file_checker.getDataValidationTasks(), std::move(lock));
}

std::optional<CheckResult> StorageLog::checkDataNext(DataValidationTasksPtr & check_task_list)
{
    return file_checker.checkNextEntry(assert_cast<DataValidationTasks *>(check_task_list.get())->file_checker_tasks);
}

IStorage::ColumnSizeByName StorageLog::getColumnSizes() const
{
    ReadLock lock{rwlock, std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC)};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    ColumnSizeByName column_sizes;

    for (const auto & column : getInMemoryMetadata().getColumns().getAllPhysical())
    {
        ISerialization::StreamCallback stream_callback = [&, this] (const ISerialization::SubstreamPath & substream_path)
        {
            String data_file_name = ISerialization::getFileNameForStream(column, substream_path);
            auto it = data_files_by_names.find(data_file_name);
            if (it != data_files_by_names.end())
            {
                const auto & data_file = *it->second;
                column_sizes[column.name].data_compressed += file_checker.getFileSize(data_file.path);
            }
        };

        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams(stream_callback);
    }

    return column_sizes;
}

void StorageLog::updateTotalRows(const WriteLock &)
{
    if (!use_marks_file || !marks_loaded)
        return;

    if (num_data_files)
        total_rows = data_files[INDEX_WITH_REAL_ROW_COUNT].marks.empty() ? 0 : data_files[INDEX_WITH_REAL_ROW_COUNT].marks.back().rows;
    else
        total_rows = 0;
}

std::optional<UInt64> StorageLog::totalRows(const Settings &) const
{
    if (use_marks_file && marks_loaded)
        return total_rows;

    if (!total_bytes)
        return 0;

    return {};
}

std::optional<UInt64> StorageLog::totalBytes(const Settings &) const
{
    return total_bytes;
}

void StorageLog::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto lock_timeout = getLockTimeout(backup_entries_collector.getContext());

    loadMarks(lock_timeout);

    ReadLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    if (!num_data_files || !file_checker.getFileSize(data_files[INDEX_WITH_REAL_ROW_COUNT].path))
        return;

    fs::path data_path_in_backup_fs = data_path_in_backup;
    auto temp_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, "tmp/");
    fs::path temp_dir = temp_dir_owner->getRelativePath();
    disk->createDirectories(temp_dir);

    const auto & read_settings = backup_entries_collector.getReadSettings();
    bool copy_encrypted = !backup_entries_collector.getBackupSettings().decrypt_files_from_encrypted_disks;

    /// *.bin
    for (const auto & data_file : data_files)
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String data_file_name = fileName(data_file.path);
        String hardlink_file_path = temp_dir / data_file_name;
        disk->createHardLink(data_file.path, hardlink_file_path);
        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromAppendOnlyFile>(
            disk, hardlink_file_path, copy_encrypted, file_checker.getFileSize(data_file.path));
        backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);
        backup_entries_collector.addBackupEntry(data_path_in_backup_fs / data_file_name, std::move(backup_entry));
    }

    /// __marks.mrk
    if (use_marks_file)
    {
        /// We make a copy of the data file because it can be changed later in write() or in truncate().
        String marks_file_name = fileName(marks_file_path);
        String hardlink_file_path = temp_dir / marks_file_name;
        disk->createHardLink(marks_file_path, hardlink_file_path);
        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromAppendOnlyFile>(
            disk, hardlink_file_path, copy_encrypted, file_checker.getFileSize(marks_file_path));
        backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);
        backup_entries_collector.addBackupEntry(data_path_in_backup_fs / marks_file_name, std::move(backup_entry));
    }

    /// sizes.json
    String files_info_path = file_checker.getPath();
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / fileName(files_info_path), std::make_unique<BackupEntryFromSmallFile>(disk, files_info_path, read_settings, copy_encrypted));

    /// columns.txt
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / "columns.txt",
        std::make_unique<BackupEntryFromMemory>(getInMemoryMetadata().getColumns().getAllPhysical().toString()));

    /// count.txt
    if (use_marks_file)
    {
        size_t num_rows = data_files[INDEX_WITH_REAL_ROW_COUNT].marks.empty() ? 0 : data_files[INDEX_WITH_REAL_ROW_COUNT].marks.back().rows;
        backup_entries_collector.addBackupEntry(
            data_path_in_backup_fs / "count.txt", std::make_unique<BackupEntryFromMemory>(toString(num_rows)));
    }
}

void StorageLog::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!num_data_files)
        return;

    if (!restorer.isNonEmptyTableAllowed() && total_bytes)
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    auto lock_timeout = getLockTimeout(restorer.getContext());
    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageLog>(shared_from_this()), backup, data_path_in_backup, lock_timeout]
        { storage->restoreDataImpl(backup, data_path_in_backup, lock_timeout); });
}

void StorageLog::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup, std::chrono::seconds lock_timeout)
{
    WriteLock lock{rwlock, lock_timeout};
    if (!lock)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Lock timeout exceeded");

    /// Load the marks if not loaded yet. We have to do that now because we're going to update these marks.
    loadMarks(lock);

    /// If there were no files, save zero file sizes to be able to rollback in case of error.
    saveFileSizes(lock);

    try
    {
        fs::path data_path_in_backup_fs = data_path_in_backup;

        /// Append data files.
        for (const auto & data_file : data_files)
        {
            String file_path_in_backup = data_path_in_backup_fs / fileName(data_file.path);
            if (!backup->fileExists(file_path_in_backup))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", file_path_in_backup);

            backup->copyFileToDisk(file_path_in_backup, disk, data_file.path, WriteMode::Append);
        }

        if (use_marks_file)
        {
            /// Append marks.
            size_t num_extra_marks = 0;
            String file_path_in_backup = data_path_in_backup_fs / fileName(marks_file_path);
            if (!backup->fileExists(file_path_in_backup))
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", file_path_in_backup);

            size_t file_size = backup->getFileSize(file_path_in_backup);
            if (file_size % (num_data_files * sizeof(Mark)) != 0)
                throw Exception(ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT, "Size of marks file is inconsistent");

            num_extra_marks = file_size / (num_data_files * sizeof(Mark));

            size_t num_marks = data_files[0].marks.size();
            for (auto & data_file : data_files)
                data_file.marks.reserve(num_marks + num_extra_marks);

            std::vector<size_t> old_data_sizes;
            std::vector<size_t> old_num_rows;
            old_data_sizes.resize(num_data_files);
            old_num_rows.resize(num_data_files);
            for (size_t i = 0; i != num_data_files; ++i)
            {
                old_data_sizes[i] = file_checker.getFileSize(data_files[i].path);
                old_num_rows[i] = num_marks ? data_files[i].marks[num_marks - 1].rows : 0;
            }

            auto marks_rb = backup->readFile(file_path_in_backup);

            for (size_t i = 0; i != num_extra_marks; ++i)
            {
                for (size_t j = 0; j != num_data_files; ++j)
                {
                    Mark mark;
                    mark.read(*marks_rb);
                    mark.rows += old_num_rows[j];     /// Adjust the number of rows.
                    mark.offset += old_data_sizes[j]; /// Adjust the offset.
                    data_files[j].marks.push_back(mark);
                }
            }
        }

        /// Finish writing.
        saveMarks(lock);
        saveFileSizes(lock);
        updateTotalRows(lock);
    }
    catch (...)
    {
        /// Rollback partial writes.
        file_checker.repair();
        removeUnsavedMarks(lock);
        throw;
    }
}

void registerStorageLog(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true
    };

    auto create_fn = [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        String disk_name = getDiskName(*args.storage_def, args.getContext());
        DiskPtr disk = args.getContext()->getDisk(disk_name);

        return std::make_shared<StorageLog>(
            args.engine_name,
            disk,
            args.relative_data_path,
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.mode,
            args.getContext());
    };

    factory.registerStorage("Log", create_fn, features);
    factory.registerStorage("TinyLog", create_fn, features);
}

}
