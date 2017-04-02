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
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <Interpreters/Settings.h>

#include <Storages/StorageTinyLog.h>
#include <Poco/DirectoryIterator.h>

#define DBMS_STORAGE_LOG_DATA_FILE_EXTENSION     ".bin"
#define DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION ".null.bin"


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int DUPLICATE_COLUMN;
}


class TinyLogBlockInputStream : public IProfilingBlockInputStream
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
            : plain(data_path, std::min(max_read_buffer_size, Poco::File(data_path).getSize())),
            compressed(plain)
        {
        }

        ReadBufferFromFile plain;
        CompressedReadBuffer compressed;
    };

    using FileStreams = std::map<std::string, std::unique_ptr<Stream>>;
    FileStreams streams;

    void addStream(const String & name, const IDataType & type, size_t level = 0);
    void readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level = 0, bool read_offsets = true);
};


class TinyLogBlockOutputStream : public IBlockOutputStream
{
public:
    TinyLogBlockOutputStream(StorageTinyLog & storage_)
        : storage(storage_)
    {
        for (const auto & col : storage.getColumnsList())
            addStream(col.name, *col.type);
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
            compressed(plain, CompressionMethod::LZ4, max_compress_block_size)
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

    using OffsetColumns = std::set<std::string>;

    void addStream(const String & name, const IDataType & type, size_t level = 0);
    void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, size_t level = 0);
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
    {
        for (size_t i = 0, size = column_names.size(); i < size; ++i)
        {
            const auto & name = column_names[i];
            column_types[i] = storage.getDataTypeByName(name);
            addStream(name, *column_types[i]);
        }
    }

    /// Pointers to offset columns, mutual for columns from nested data structures
    using OffsetColumns = std::map<std::string, ColumnPtr>;
    OffsetColumns offset_columns;

    for (size_t i = 0, size = column_names.size(); i < size; ++i)
    {
        const auto & name = column_names[i];

        ColumnWithTypeAndName column;
        column.name = name;
        column.type = column_types[i];

        bool read_offsets = true;

        const IDataType * observed_type;
        bool is_nullable;

        if (column.type->isNullable())
        {
            const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*column.type);
            observed_type = nullable_type.getNestedType().get();
            is_nullable = true;
        }
        else
        {
            observed_type = column.type.get();
            is_nullable = false;
        }

        /// For nested structures, remember pointers to columns with offsets
        if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(observed_type))
        {
            String nested_name = DataTypeNested::extractNestedTableName(column.name);

            if (offset_columns.count(nested_name) == 0)
                offset_columns[nested_name] = std::make_shared<ColumnArray::ColumnOffsets_t>();
            else
                read_offsets = false; /// on previous iterations, the offsets were already calculated by `readData`

            column.column = std::make_shared<ColumnArray>(type_arr->getNestedType()->createColumn(), offset_columns[nested_name]);
            if (is_nullable)
                column.column = std::make_shared<ColumnNullable>(column.column, std::make_shared<ColumnUInt8>());
        }
        else
            column.column = column.type->createColumn();

        try
        {
            readData(name, *column.type, *column.column, block_size, 0, read_offsets);
        }
        catch (Exception & e)
        {
            e.addMessage("while reading column " + name + " at " + storage.full_path());
            throw;
        }

        if (column.column->size())
            res.insert(std::move(column));
    }

    if (!res || streams.begin()->second->compressed.eof())
    {
        finished = true;
        streams.clear();
    }

    return res;
}


void TinyLogBlockInputStream::addStream(const String & name, const IDataType & type, size_t level)
{
    if (type.isNullable())
    {
        /// First create the stream that handles the null map of the given column.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();
        std::string filename = name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;
        streams.emplace(filename, std::make_unique<Stream>(storage.files[filename].data_file.path(), max_read_buffer_size));

        /// Then create the stream that handles the data of the given column.
        addStream(name, nested_type, level);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /// For arrays separate threads are used for sizes.
        String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
        if (!streams.count(size_name))
            streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(storage.files[size_name].data_file.path(), max_read_buffer_size)));

        addStream(name, *type_arr->getNestedType(), level + 1);
    }
    else
        streams[name] = std::make_unique<Stream>(storage.files[name].data_file.path(), max_read_buffer_size);
}

void TinyLogBlockInputStream::readData(const String & name, const IDataType & type, IColumn & column, size_t limit, size_t level, bool read_offsets)
{
    if (type.isNullable())
    {
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        if (!column.isNullable())
            throw Exception{"Internal error: the column " + name + " is not nullable", ErrorCodes::LOGICAL_ERROR};

        ColumnNullable & nullable_col = static_cast<ColumnNullable &>(column);
        IColumn & nested_col = *nullable_col.getNestedColumn();

        /// First read from the null map.
        DataTypeUInt8{}.deserializeBinaryBulk(nullable_col.getNullMapConcreteColumn(),
            streams[name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION]->compressed, limit, 0);

        /// Then read data.
        readData(name, nested_type, nested_col, limit, level, read_offsets);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /// For arrays you first need to deserialize dimensions, and then the values.
        if (read_offsets)
        {
            type_arr->deserializeOffsets(
                column,
                streams[DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level)]->compressed,
                limit);
        }

        if (column.size())
        {
            IColumn & nested_column = typeid_cast<ColumnArray &>(column).getData();
            size_t nested_limit = typeid_cast<ColumnArray &>(column).getOffsets()[column.size() - 1];
            readData(name, *type_arr->getNestedType(), nested_column, nested_limit, level + 1);

            if (nested_column.size() != nested_limit)
                throw Exception("Cannot read array data for all offsets", ErrorCodes::CANNOT_READ_ALL_DATA);
        }
    }
    else
        type.deserializeBinaryBulk(column, streams[name]->compressed, limit, 0);    /// TODO Use avg_value_size_hint.
}


void TinyLogBlockOutputStream::addStream(const String & name, const IDataType & type, size_t level)
{
    if (type.isNullable())
    {
        /// First create the stream that handles the null map of the given column.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        std::string filename = name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;
        streams.emplace(filename, std::make_unique<Stream>(storage.files[filename].data_file.path(), storage.max_compress_block_size));

        /// Then create the stream that handles the data of the given column.
        addStream(name, nested_type, level);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /// For arrays separate threads are used for sizes.
        String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
        if (!streams.count(size_name))
            streams.emplace(size_name, std::unique_ptr<Stream>(new Stream(storage.files[size_name].data_file.path(), storage.max_compress_block_size)));

        addStream(name, *type_arr->getNestedType(), level + 1);
    }
    else
        streams[name] = std::make_unique<Stream>(storage.files[name].data_file.path(), storage.max_compress_block_size);
}


void TinyLogBlockOutputStream::writeData(const String & name, const IDataType & type, const IColumn & column,
                                            OffsetColumns & offset_columns, size_t level)
{
    if (type.isNullable())
    {
        /// First write to the null map.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(column);
        const IColumn & nested_col = *nullable_col.getNestedColumn();

        DataTypeUInt8{}.serializeBinaryBulk(nullable_col.getNullMapConcreteColumn(),
            streams[name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION]->compressed, 0, 0);

        /// Then write data.
        writeData(name, nested_type, nested_col, offset_columns, level);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /// For arrays, you first need to serialize the dimensions, and then the values.
        String size_name = DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

        if (offset_columns.count(size_name) == 0)
        {
            offset_columns.insert(size_name);
            type_arr->serializeOffsets(column, streams[size_name]->compressed, 0, 0);
        }

        writeData(name, *type_arr->getNestedType(), typeid_cast<const ColumnArray &>(column).getData(), offset_columns, level + 1);
    }
    else
        type.serializeBinaryBulk(column, streams[name]->compressed, 0, 0);
}


void TinyLogBlockOutputStream::writeSuffix()
{
    if (done)
        return;
    done = true;

    /// Finish write.
    for (FileStreams::iterator it = streams.begin(); it != streams.end(); ++it)
        it->second->finalize();

    std::vector<Poco::File> column_files;
    for (auto & pair : streams)
        column_files.push_back(storage.files[pair.first].data_file);

    storage.file_checker.update(column_files.begin(), column_files.end());

    streams.clear();
}


void TinyLogBlockOutputStream::write(const Block & block)
{
    storage.check(block, true);

    /// The set of written offset columns so that you do not write mutual columns for nested structures multiple times
    OffsetColumns offset_columns;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(column.name, *column.type, *column.column, offset_columns);
    }
}


StorageTinyLog::StorageTinyLog(
    const std::string & path_,
    const std::string & name_,
    NamesAndTypesListPtr columns_,
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
    if (columns->empty())
        throw Exception("Empty list of columns passed to StorageTinyLog constructor", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    String full_path = path + escapeForFileName(name) + '/';
    if (!attach)
    {
        /// create files if they do not exist
        if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
            throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
    }

    for (const auto & col : getColumnsList())
        addFile(col.name, *col.type);
}


StoragePtr StorageTinyLog::create(
    const std::string & path_,
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    bool attach,
    size_t max_compress_block_size_)
{
    return make_shared(
        path_, name_, columns_,
        materialized_columns_, alias_columns_, column_defaults_,
        attach, max_compress_block_size_
    );
}


void StorageTinyLog::addFile(const String & column_name, const IDataType & type, size_t level)
{
    if (files.end() != files.find(column_name))
        throw Exception("Duplicate column with name " + column_name + " in constructor of StorageTinyLog.",
            ErrorCodes::DUPLICATE_COLUMN);

    if (type.isNullable())
    {
        /// First add the file describing the null map of the column.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & actual_type = *nullable_type.getNestedType();

        std::string filename = column_name + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION;
        ColumnData & column_data = files.emplace(filename, ColumnData{}).first->second;
        column_data.data_file = Poco::File{
            path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_BINARY_NULL_MAP_EXTENSION};

        /// Then add the file describing the column data.
        addFile(column_name, actual_type, level);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        String size_column_suffix = ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
        String size_name = DataTypeNested::extractNestedTableName(column_name) + size_column_suffix;

        if (files.end() == files.find(size_name))
        {
            ColumnData column_data;
            files.insert(std::make_pair(size_name, column_data));
            files[size_name].data_file = Poco::File(
                path + escapeForFileName(name) + '/' + escapeForFileName(DataTypeNested::extractNestedTableName(column_name)) + size_column_suffix + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
        }

        addFile(column_name, *type_arr->getNestedType(), level + 1);
    }
    else
    {
        ColumnData column_data;
        files.insert(std::make_pair(column_name, column_data));
        files[column_name].data_file = Poco::File(
            path + escapeForFileName(name) + '/' + escapeForFileName(column_name) + DBMS_STORAGE_LOG_DATA_FILE_EXTENSION);
    }
}


void StorageTinyLog::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
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
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;
    return BlockInputStreams(1, std::make_shared<TinyLogBlockInputStream>(max_block_size, column_names, *this, settings.max_read_buffer_size));
}


BlockOutputStreamPtr StorageTinyLog::write(
    ASTPtr query, const Settings & settings)
{
    return std::make_shared<TinyLogBlockOutputStream>(*this);
}


void StorageTinyLog::drop()
{
    for (Files_t::iterator it = files.begin(); it != files.end(); ++it)
    {
        if (it->second.data_file.exists())
            it->second.data_file.remove();
    }
}

bool StorageTinyLog::checkData() const
{
    return file_checker.check();
}

}
