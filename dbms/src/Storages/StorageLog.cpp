#include <Storages/StorageLog.h>
#include <Storages/StorageFactory.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/NestedUtils.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>

#include <Common/typeid_cast.h>

#include <Interpreters/Context.h>

#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>


#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION ".bin"
#define DBMS_STORAGE_LOG_MARKS_FILE_NAME "__marks.mrk"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int DUPLICATE_COLUMN;
    extern const int SIZES_OF_MARKS_FILES_ARE_INCONSISTENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
}


class LogBlockInputStream final : public IProfilingBlockInputStream
{
public:
    LogBlockInputStream(
        size_t block_size_, const NamesAndTypesList & columns_, StorageLog & storage_,
        size_t mark_number_, size_t rows_limit_, size_t max_read_buffer_size_)
        : block_size(block_size_),
        columns(columns_),
        storage(storage_),
        mark_number(mark_number_),
        rows_limit(rows_limit_),
        max_read_buffer_size(max_read_buffer_size_)
    {
    }

    String getName() const override { return "Log"; }

    Block getHeader() const override
    {
        Block res;

        for (const auto & name_type : columns)
            res.insert({ name_type.type->createColumn(), name_type.type, name_type.name });

        return Nested::flatten(res);
    };

protected:
    Block readImpl() override;

private:
    size_t block_size;
    NamesAndTypesList columns;
    StorageLog & storage;
    size_t mark_number;     /// from what mark to read data
    size_t rows_limit;      /// The maximum number of rows that can be read
    size_t rows_read = 0;
    size_t max_read_buffer_size;

    struct Stream
    {
        Stream(const std::string & data_path, size_t offset, size_t max_read_buffer_size)
            : plain(data_path, std::min(static_cast<Poco::File::FileSize>(max_read_buffer_size), Poco::File(data_path).getSize())),
            compressed(plain)
        {
            if (offset)
                plain.seek(offset);
        }

        ReadBufferFromFile plain;
        CompressedReadBuffer compressed;
    };

    using FileStreams = std::map<std::string, Stream>;
    FileStreams streams;

    void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read);
};


class LogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit LogBlockOutputStream(StorageLog & storage_)
        : storage(storage_),
        lock(storage.rwlock),
        marks_stream(storage.marks_file.path(), 4096, O_APPEND | O_CREAT | O_WRONLY)
    {
    }

    ~LogBlockOutputStream() override
    {
        try
        {
            writeSuffix();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    Block getHeader() const override { return storage.getSampleBlock(); }
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageLog & storage;
    std::unique_lock<std::shared_mutex> lock;
    bool done = false;

    struct Stream
    {
        Stream(const std::string & data_path, size_t max_compress_block_size) :
            plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
            compressed(plain, CompressionSettings(CompressionMethod::LZ4), max_compress_block_size)
        {
            plain_offset = Poco::File(data_path).getSize();
        }

        WriteBufferFromFile plain;
        CompressedWriteBuffer compressed;

        size_t plain_offset;    /// How many bytes were in the file at the time the LogBlockOutputStream was created.

        void finalize()
        {
            compressed.next();
            plain.next();
        }
    };

    using Mark = StorageLog::Mark;
    using MarksForColumns = std::vector<std::pair<size_t, Mark>>;

    using FileStreams = std::map<std::string, Stream>;
    FileStreams streams;

    using WrittenStreams = std::set<std::string>;

    WriteBufferFromFile marks_stream; /// Declared below `lock` to make the file open when rwlock is captured.

    void writeData(const String & name, const IDataType & type, const IColumn & column,
        MarksForColumns & out_marks,
        WrittenStreams & written_streams);

    void writeMarks(MarksForColumns && marks);
};


Block LogBlockInputStream::readImpl()
{
    Block res;

    if (rows_read == rows_limit)
        return res;

    /// If there are no files in the folder, the table is empty.
    if (Poco::DirectoryIterator(storage.getFullPath()) == Poco::DirectoryIterator())
        return res;

    /// How many rows to read for the next block.
    size_t max_rows_to_read = std::min(block_size, rows_limit - rows_read);

    for (const auto & name_type : columns)
    {
        MutableColumnPtr column = name_type.type->createColumn();

        try
        {
            readData(name_type.name, *name_type.type, *column, max_rows_to_read);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name_type.name + " at " + storage.path + escapeForFileName(storage.name));
            throw;
        }

        if (column->size())
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

    return Nested::flatten(res);
}


void LogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read)
{
    IDataType::InputStreamGetter stream_getter = [&] (const IDataType::SubstreamPath & path) -> ReadBuffer *
    {
        String stream_name = IDataType::getFileNameForStream(name, path);

        const auto & file_it = storage.files.find(stream_name);
        if (storage.files.end() == file_it)
            throw Exception("Logical error: no information about file " + stream_name + " in StorageLog", ErrorCodes::LOGICAL_ERROR);

        auto it = streams.try_emplace(stream_name,
            file_it->second.data_file.path(),
            mark_number
                ? file_it->second.marks[mark_number].offset
                : 0,
            max_read_buffer_size).first;

        return &it->second.compressed;
    };

    type.deserializeBinaryBulkWithMultipleStreams(column, stream_getter, max_rows_to_read, 0, true, {}); /// TODO Use avg_value_size_hint.
}


void LogBlockOutputStream::write(const Block & block)
{
    storage.check(block, true);

    /// The set of written offset columns so that you do not write shared offsets of columns for nested structures multiple times
    WrittenStreams written_streams;

    MarksForColumns marks;
    marks.reserve(storage.file_count);

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(column.name, *column.type, *column.column, marks, written_streams);
    }

    writeMarks(std::move(marks));
}


void LogBlockOutputStream::writeSuffix()
{
    if (done)
        return;
    done = true;

    /// Finish write.
    marks_stream.next();

    for (auto & name_stream : streams)
        name_stream.second.finalize();

    std::vector<Poco::File> column_files;
    for (const auto & name_stream : streams)
        column_files.push_back(storage.files[name_stream.first].data_file);
    column_files.push_back(storage.marks_file);

    storage.file_checker.update(column_files.begin(), column_files.end());

    streams.clear();
}


void LogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column,
    MarksForColumns & out_marks,
    WrittenStreams & written_streams)
{
    type.enumerateStreams([&] (const IDataType::SubstreamPath & path)
    {
        String stream_name = IDataType::getFileNameForStream(name, path);
        if (written_streams.count(stream_name))
            return;

        const auto & file = storage.files[stream_name];
        const auto stream_it = streams.try_emplace(stream_name, storage.files[stream_name].data_file.path(), storage.max_compress_block_size).first;

        Mark mark;
        mark.rows = (file.marks.empty() ? 0 : file.marks.back().rows) + column.size();
        mark.offset = stream_it->second.plain_offset + stream_it->second.plain.count();

        out_marks.emplace_back(file.column_index, mark);
    }, {});

    IDataType::OutputStreamGetter stream_getter = [&] (const IDataType::SubstreamPath & path) -> WriteBuffer *
    {
        String stream_name = IDataType::getFileNameForStream(name, path);
        if (written_streams.count(stream_name))
            return nullptr;

        auto it = streams.find(stream_name);
        if (streams.end() == it)
            throw Exception("Logical error: stream was not created when writing data in LogBlockOutputStream", ErrorCodes::LOGICAL_ERROR);
        return &it->second.compressed;
    };

    type.serializeBinaryBulkWithMultipleStreams(column, stream_getter, 0, 0, true, {});

    type.enumerateStreams([&] (const IDataType::SubstreamPath & path)
    {
        String stream_name = IDataType::getFileNameForStream(name, path);
        if (!written_streams.emplace(stream_name).second)
            return;

        auto it = streams.find(stream_name);
        if (streams.end() == it)
            throw Exception("Logical error: stream was not created when writing data in LogBlockOutputStream", ErrorCodes::LOGICAL_ERROR);
        it->second.compressed.next();
    }, {});
}


void LogBlockOutputStream::writeMarks(MarksForColumns && marks)
{
    if (marks.size() != storage.file_count)
        throw Exception("Wrong number of marks generated from block. Makes no sense.", ErrorCodes::LOGICAL_ERROR);

    std::sort(marks.begin(), marks.end(), [](const auto & a, const auto & b) { return a.first < b.first; });

    for (const auto & mark : marks)
    {
        writeIntBinary(mark.second.rows, marks_stream);
        writeIntBinary(mark.second.offset, marks_stream);

        size_t column_index = mark.first;
        storage.files[storage.column_names[column_index]].marks.push_back(mark.second);
    }
}

StorageLog::StorageLog(
    const std::string & path_,
    const std::string & name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    size_t max_compress_block_size_)
    : IStorage{columns_, materialized_columns_, alias_columns_, column_defaults_},
    path(path_), name(name_),
    max_compress_block_size(max_compress_block_size_),
    file_checker(path + escapeForFileName(name) + '/' + "sizes.json")
{
    if (path.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

     /// create files if they do not exist
    Poco::File(path + escapeForFileName(name) + '/').createDirectories();

    for (const auto & column : getColumnsList())
        addFiles(column.name, *column.type);

    marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);
}


void StorageLog::addFiles(const String & column_name, const IDataType & type)
{
    if (files.end() != files.find(column_name))
        throw Exception("Duplicate column with name " + column_name + " in constructor of StorageLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    IDataType::StreamCallback stream_callback = [&] (const IDataType::SubstreamPath & substream_path)
    {
        String stream_name = IDataType::getFileNameForStream(column_name, substream_path);

        if (!files.count(stream_name))
        {
            ColumnData & column_data = files[stream_name];
            column_data.column_index = file_count;
            column_data.data_file = Poco::File{
                path + escapeForFileName(name) + '/' + stream_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION};

            column_names.push_back(stream_name);
            ++file_count;
        }
    };

    type.enumerateStreams(stream_callback, {});
}


void StorageLog::loadMarks()
{
    std::unique_lock<std::shared_mutex> lock(rwlock);

    if (loaded_marks)
        return;

    using FilesByIndex = std::vector<Files_t::iterator>;

    FilesByIndex files_by_index(file_count);
    for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
        files_by_index[it->second.column_index] = it;

    if (marks_file.exists())
    {
        size_t file_size = marks_file.getSize();
        if (file_size % (file_count * sizeof(Mark)) != 0)
            throw Exception("Size of marks file is inconsistent", ErrorCodes::SIZES_OF_MARKS_FILES_ARE_INCONSISTENT);

        size_t marks_count = file_size / (file_count * sizeof(Mark));

        for (auto & file : files_by_index)
            file->second.marks.reserve(marks_count);

        ReadBufferFromFile marks_rb(marks_file.path(), 32768);
        while (!marks_rb.eof())
        {
            for (size_t i = 0; i < files_by_index.size(); ++i)
            {
                Mark mark;
                readIntBinary(mark.rows, marks_rb);
                readIntBinary(mark.offset, marks_rb);
                files_by_index[i]->second.marks.push_back(mark);
            }
        }
    }

    loaded_marks = true;
}


void StorageLog::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    std::unique_lock<std::shared_mutex> lock(rwlock);

    /// Rename directory with data.
    Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

    path = new_path_to_db;
    name = new_table_name;
    file_checker.setPath(path + escapeForFileName(name) + '/' + "sizes.json");

    for (auto & file : files)
        file.second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(file.second.data_file.path()).getFileName());

    marks_file = Poco::File(path + escapeForFileName(name) + '/' + DBMS_STORAGE_LOG_MARKS_FILE_NAME);
}


const StorageLog::Marks & StorageLog::getMarksWithRealRowCount() const
{
    const String & column_name = columns.front().name;
    const IDataType & column_type = *columns.front().type;
    String filename;

    /** We take marks from first column.
      * If this is a data type with multiple stream, get the first stream, that we assume have real row count.
      * (Example: for Array data type, first stream is array sizes; and number of array sizes is the number of arrays).
      */
    column_type.enumerateStreams([&](const IDataType::SubstreamPath & substream_path)
    {
        if (filename.empty())
            filename = IDataType::getFileNameForStream(column_name, substream_path);
    },  {});

    Files_t::const_iterator it = files.find(filename);
    if (files.end() == it)
        throw Exception("Cannot find file " + filename, ErrorCodes::LOGICAL_ERROR);

    return it->second.marks;
}


BlockInputStreams StorageLog::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    loadMarks();

    NamesAndTypesList columns = Nested::collect(getColumnsList().addTypes(column_names));

    std::shared_lock<std::shared_mutex> lock(rwlock);

    BlockInputStreams res;

    const Marks & marks = getMarksWithRealRowCount();
    size_t marks_size = marks.size();

    if (num_streams > marks_size)
        num_streams = marks_size;

    size_t max_read_buffer_size = context.getSettingsRef().max_read_buffer_size;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        size_t mark_begin = stream * marks_size / num_streams;
        size_t mark_end = (stream + 1) * marks_size / num_streams;

        size_t rows_begin = mark_begin ? marks[mark_begin - 1].rows : 0;
        size_t rows_end = mark_end ? marks[mark_end - 1].rows : 0;

        res.emplace_back(std::make_shared<LogBlockInputStream>(
            max_block_size,
            columns,
            *this,
            mark_begin,
            rows_end - rows_begin,
            max_read_buffer_size));
    }

    return res;
}


BlockOutputStreamPtr StorageLog::write(
    const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    loadMarks();
    return std::make_shared<LogBlockOutputStream>(*this);
}

bool StorageLog::checkData() const
{
    std::shared_lock<std::shared_mutex> lock(rwlock);
    return file_checker.check();
}


void registerStorageLog(StorageFactory & factory)
{
    factory.registerStorage("Log", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageLog::create(
            args.data_path, args.table_name, args.columns,
            args.materialized_columns, args.alias_columns, args.column_defaults,
            args.context.getSettings().max_compress_block_size);
    });
}

}
