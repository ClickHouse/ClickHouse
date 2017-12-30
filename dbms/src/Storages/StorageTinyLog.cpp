#include <sys/stat.h>
#include <sys/types.h>

#include <map>

#include <Poco/Path.h>
#include <Poco/Util/XMLConfiguration.h>

#include <Common/escapeForFileName.h>

#include <Common/Exception.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypesNumber.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>

#include <Common/typeid_cast.h>

#include <Interpreters/Context.h>

#include <Storages/StorageTinyLog.h>
#include <Storages/StorageFactory.h>

#include <Poco/DirectoryIterator.h>

#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION ".bin"


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int DUPLICATE_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class TinyLogBlockInputStream final : public IProfilingBlockInputStream
{
public:
    TinyLogBlockInputStream(size_t block_size_, const Names & column_names_, StorageTinyLog & storage_, size_t max_read_buffer_size_)
        : block_size(block_size_), column_names(column_names_), column_types(column_names.size()),
        storage(storage_), max_read_buffer_size(max_read_buffer_size_) {}

    String getName() const override { return "TinyLog"; }

    String getID() const override;

protected:
    Block readImpl() override;
private:
    size_t block_size;
    Names column_names;
    DataTypes column_types;
    StorageTinyLog & storage;
    bool finished = false;
    size_t max_read_buffer_size;

    struct Stream
    {
        Stream(const std::string & data_path, size_t max_read_buffer_size)
            : plain(data_path, std::min(static_cast<Poco::File::FileSize>(max_read_buffer_size), Poco::File(data_path).getSize())),
            compressed(plain)
        {
        }

        ReadBufferFromFile plain;
        CompressedReadBuffer compressed;
    };

    using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
    FileStreams streams;

    void readData(const String & name, const IDataType & type, IColumn & column, size_t limit, bool read_offsets = true);
};


class TinyLogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit TinyLogBlockOutputStream(StorageTinyLog & storage_)
        : storage(storage_)
    {
    }

    ~TinyLogBlockOutputStream() override
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

    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageTinyLog & storage;
    bool done = false;

    struct Stream
    {
        Stream(const std::string & data_path, size_t max_compress_block_size) :
            plain(data_path, max_compress_block_size, O_APPEND | O_CREAT | O_WRONLY),
            compressed(plain, CompressionSettings(CompressionMethod::LZ4), max_compress_block_size)
        {
        }

        WriteBufferFromFile plain;
        CompressedWriteBuffer compressed;

        void finalize()
        {
            compressed.next();
            plain.next();
        }
    };

    using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
    FileStreams streams;

    using WrittenStreams = std::set<std::string>;

    void writeData(const String & name, const IDataType & type, const IColumn & column, WrittenStreams & written_streams);
};


String TinyLogBlockInputStream::getID() const
{
    std::stringstream res;
    res << "TinyLog(" << storage.getTableName() << ", " << &storage;

    for (const auto & name : column_names)
        res << ", " << name;

    res << ")";
    return res.str();
}


Block TinyLogBlockInputStream::readImpl()
{
    Block res;

    if (finished || (!streams.empty() && streams.begin()->second->compressed.eof()))
    {
        /** Close the files (before destroying the object).
          * When many sources are created, but simultaneously reading only a few of them,
          * buffers don't waste memory.
          */
        finished = true;
        streams.clear();
        return res;
    }

    {
        /// if there are no files in the folder, it means that the table is empty
        if (Poco::DirectoryIterator(storage.full_path()) == Poco::DirectoryIterator())
            return res;
    }

    /// If the files are not open, then open them.
    if (streams.empty())
        for (size_t i = 0, size = column_names.size(); i < size; ++i)
            column_types[i] = storage.getDataTypeByName(column_names[i]);

    /// Pointers to offset columns, shared for columns from nested data structures
    using OffsetColumns = std::map<std::string, ColumnPtr>;
    OffsetColumns offset_columns;

    for (size_t i = 0, size = column_names.size(); i < size; ++i)
    {
        const auto & name = column_names[i];

        MutableColumnPtr column;

        bool read_offsets = true;

        /// For nested structures, remember pointers to columns with offsets
        if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(column_types[i].get()))
        {
            String nested_name = Nested::extractTableName(name);

            if (offset_columns.count(nested_name) == 0)
                offset_columns[nested_name] = ColumnArray::ColumnOffsets::create();
            else
                read_offsets = false; /// on previous iterations, the offsets were already calculated by `readData`

            column = ColumnArray::create(type_arr->getNestedType()->createColumn(), offset_columns[nested_name]);
        }
        else
            column = column_types[i]->createColumn();

        try
        {
            readData(name, *column_types[i], *column, block_size, read_offsets);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name + " at " + storage.full_path());
            throw;
        }

        if (column->size())
            res.insert(ColumnWithTypeAndName(std::move(column), column_types[i], name));
    }

    if (!res || streams.begin()->second->compressed.eof())
    {
        finished = true;
        streams.clear();
    }

    return res;
}


void TinyLogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t limit, bool with_offsets)
{
    IDataType::InputStreamGetter stream_getter = [&] (const IDataType::SubstreamPath & path) -> ReadBuffer *
    {
        if (!with_offsets && !path.empty() && path.back().type == IDataType::Substream::ArraySizes)
            return nullptr;

        String stream_name = IDataType::getFileNameForStream(name, path);

        if (!streams.count(stream_name))
            streams[stream_name] = std::make_unique<Stream>(storage.files[stream_name].data_file.path(), max_read_buffer_size);

        return &streams[stream_name]->compressed;
    };

    type.deserializeBinaryBulkWithMultipleStreams(column, stream_getter, limit, 0, true, {}); /// TODO Use avg_value_size_hint.
}


void TinyLogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column, WrittenStreams & written_streams)
{
    IDataType::OutputStreamGetter stream_getter = [&] (const IDataType::SubstreamPath & path) -> WriteBuffer *
    {
        String stream_name = IDataType::getFileNameForStream(name, path);

        if (!streams.count(stream_name))
            streams[stream_name] = std::make_unique<Stream>(storage.files[stream_name].data_file.path(), storage.max_compress_block_size);
        else if (!written_streams.insert(stream_name).second)
            return nullptr;

        return &streams[stream_name]->compressed;
    };

    type.serializeBinaryBulkWithMultipleStreams(column, stream_getter, 0, 0, true, {});
}


void TinyLogBlockOutputStream::writeSuffix()
{
    if (done)
        return;
    done = true;

    /// Finish write.
    for (auto & stream : streams)
        stream.second->finalize();

    std::vector<Poco::File> column_files;
    for (auto & pair : streams)
        column_files.push_back(storage.files[pair.first].data_file);

    storage.file_checker.update(column_files.begin(), column_files.end());

    streams.clear();
}


void TinyLogBlockOutputStream::write(const Block & block)
{
    storage.check(block, true);

    /// The set of written offset columns so that you do not write shared columns for nested structures multiple times
    WrittenStreams written_streams;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(column.name, *column.type, *column.column, written_streams);
    }
}


StorageTinyLog::StorageTinyLog(
    const std::string & path_,
    const std::string & name_,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    path(path_), name(name_), columns(columns_),
    max_compress_block_size(max_compress_block_size_),
    file_checker(path + escapeForFileName(name) + '/' + "sizes.json"),
    log(&Logger::get("StorageTinyLog"))
{
    if (columns.empty())
        throw Exception("Empty list of columns passed to StorageTinyLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    String full_path = path + escapeForFileName(name) + '/';
    if (!attach)
    {
        /// create files if they do not exist
        if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
            throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
    }

    for (const auto & col : getColumnsList())
        addFiles(col.name, *col.type);
}


void StorageTinyLog::addFiles(const String & column_name, const IDataType & type)
{
    if (files.end() != files.find(column_name))
        throw Exception("Duplicate column with name " + column_name + " in constructor of StorageTinyLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    IDataType::StreamCallback stream_callback = [&] (const IDataType::SubstreamPath & substream_path)
    {
        String stream_name = IDataType::getFileNameForStream(column_name, substream_path);
        if (!files.count(stream_name))
        {
            ColumnData column_data;
            files.insert(std::make_pair(stream_name, column_data));
            files[stream_name].data_file = Poco::File(
                path + escapeForFileName(name) + '/' + stream_name + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
        }
    };

    type.enumerateStreams(stream_callback, {});
}


void StorageTinyLog::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    /// Rename directory with data.
    Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

    path = new_path_to_db;
    name = new_table_name;
    file_checker.setPath(path + escapeForFileName(name) + "/" + "sizes.json");

    for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
        it->second.data_file = Poco::File(path + escapeForFileName(name) + '/' + Poco::Path(it->second.data_file.path()).getFileName());
}


BlockInputStreams StorageTinyLog::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    return BlockInputStreams(1, std::make_shared<TinyLogBlockInputStream>(
        max_block_size, column_names, *this, context.getSettingsRef().max_read_buffer_size));
}


BlockOutputStreamPtr StorageTinyLog::write(
    const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<TinyLogBlockOutputStream>(*this);
}


bool StorageTinyLog::checkData() const
{
    return file_checker.check();
}


void registerStorageTinyLog(StorageFactory & factory)
{
    factory.registerStorage("TinyLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageTinyLog::create(
            args.data_path, args.table_name, args.columns,
            args.materialized_columns, args.alias_columns, args.column_defaults,
            args.attach, args.context.getSettings().max_compress_block_size);
    });
}

}
