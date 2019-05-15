#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

#include <map>
#include <optional>

#include <Common/escapeForFileName.h>

#include <Common/Exception.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockInputStream.h>

#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnArray.h>

#include <Interpreters/Context.h>

#include <Storages/StorageStripeLog.h>
#include <Storages/StorageFactory.h>
#include <Poco/DirectoryIterator.h>


namespace DB
{

#define INDEX_BUFFER_SIZE 4096

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
    extern const int LOGICAL_ERROR;
}


class StripeLogBlockInputStream final : public IBlockInputStream
{
public:
    StripeLogBlockInputStream(StorageStripeLog & storage_, size_t max_read_buffer_size_,
        std::shared_ptr<const IndexForNativeFormat> & index_,
        IndexForNativeFormat::Blocks::const_iterator index_begin_,
        IndexForNativeFormat::Blocks::const_iterator index_end_)
        : storage(storage_), max_read_buffer_size(max_read_buffer_size_),
        index(index_), index_begin(index_begin_), index_end(index_end_)
    {
        if (index_begin != index_end)
        {
            for (const auto & column : index_begin->columns)
            {
                auto type = DataTypeFactory::instance().get(column.type);
                header.insert(ColumnWithTypeAndName{ type, column.name });
            }
        }
    }

    String getName() const override { return "StripeLog"; }

    Block getHeader() const override
    {
        return header;
    }

protected:
    Block readImpl() override
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

        return res;
    }

private:
    StorageStripeLog & storage;
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

            data_in.emplace(
                storage.full_path() + "data.bin", 0, 0,
                std::min(static_cast<Poco::File::FileSize>(max_read_buffer_size), Poco::File(storage.full_path() + "data.bin").getSize()));

            block_in.emplace(*data_in, 0, index_begin, index_end);
        }
    }
};


class StripeLogBlockOutputStream final : public IBlockOutputStream
{
public:
    explicit StripeLogBlockOutputStream(StorageStripeLog & storage_)
        : storage(storage_), lock(storage.rwlock),
        data_out_compressed(storage.full_path() + "data.bin", DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT),
        data_out(data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), storage.max_compress_block_size),
        index_out_compressed(storage.full_path() + "index.mrk", INDEX_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT),
        index_out(index_out_compressed),
        block_out(data_out, 0, storage.getSampleBlock(), false, &index_out, Poco::File(storage.full_path() + "data.bin").getSize())
    {
    }

    ~StripeLogBlockOutputStream() override
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

    void write(const Block & block) override
    {
        block_out.write(block);
    }

    void writeSuffix() override
    {
        if (done)
            return;

        block_out.writeSuffix();
        data_out.next();
        data_out_compressed.next();
        index_out.next();
        index_out_compressed.next();

        FileChecker::Files files{ data_out_compressed.getFileName(), index_out_compressed.getFileName() };
        storage.file_checker.update(files.begin(), files.end());

        done = true;
    }

private:
    StorageStripeLog & storage;
    std::unique_lock<std::shared_mutex> lock;

    WriteBufferFromFile data_out_compressed;
    CompressedWriteBuffer data_out;
    WriteBufferFromFile index_out_compressed;
    CompressedWriteBuffer index_out;
    NativeBlockOutputStream block_out;

    bool done = false;
};


StorageStripeLog::StorageStripeLog(
    const std::string & path_,
    const std::string & name_,
    const ColumnsDescription & columns_,
    bool attach,
    size_t max_compress_block_size_)
    : IStorage{columns_},
    path(path_), name(name_),
    max_compress_block_size(max_compress_block_size_),
    file_checker(path + escapeForFileName(name) + '/' + "sizes.json"),
    log(&Logger::get("StorageStripeLog"))
{
    if (path.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

    String full_path = path + escapeForFileName(name) + '/';
    if (!attach)
    {
        /// create files if they do not exist
        if (0 != mkdir(full_path.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) && errno != EEXIST)
            throwFromErrno("Cannot create directory " + full_path, ErrorCodes::CANNOT_CREATE_DIRECTORY);
    }
}


void StorageStripeLog::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    std::unique_lock<std::shared_mutex> lock(rwlock);

    /// Rename directory with data.
    Poco::File(path + escapeForFileName(name)).renameTo(new_path_to_db + escapeForFileName(new_table_name));

    path = new_path_to_db;
    name = new_table_name;
    file_checker.setPath(path + escapeForFileName(name) + "/" + "sizes.json");
}


BlockInputStreams StorageStripeLog::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    unsigned num_streams)
{
    std::shared_lock<std::shared_mutex> lock(rwlock);

    check(column_names);

    NameSet column_names_set(column_names.begin(), column_names.end());

    if (!Poco::File(full_path() + "index.mrk").exists())
        return { std::make_shared<NullBlockInputStream>(getSampleBlockForColumns(column_names)) };

    CompressedReadBufferFromFile index_in(full_path() + "index.mrk", 0, 0, INDEX_BUFFER_SIZE);
    std::shared_ptr<const IndexForNativeFormat> index{std::make_shared<IndexForNativeFormat>(index_in, column_names_set)};

    BlockInputStreams res;

    size_t size = index->blocks.size();
    if (num_streams > size)
        num_streams = size;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        IndexForNativeFormat::Blocks::const_iterator begin = index->blocks.begin();
        IndexForNativeFormat::Blocks::const_iterator end = index->blocks.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.emplace_back(std::make_shared<StripeLogBlockInputStream>(
            *this, context.getSettingsRef().max_read_buffer_size, index, begin, end));
    }

    /// We do not keep read lock directly at the time of reading, because we read ranges of data that do not change.

    return res;
}


BlockOutputStreamPtr StorageStripeLog::write(
    const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<StripeLogBlockOutputStream>(*this);
}


bool StorageStripeLog::checkData() const
{
    std::shared_lock<std::shared_mutex> lock(rwlock);
    return file_checker.check();
}

void StorageStripeLog::truncate(const ASTPtr &, const Context &)
{
    if (name.empty())
        throw Exception("Logical error: table name is empty", ErrorCodes::LOGICAL_ERROR);

    std::shared_lock<std::shared_mutex> lock(rwlock);

    auto file = Poco::File(path + escapeForFileName(name));
    file.remove(true);
    file.createDirectories();

    file_checker = FileChecker{path + escapeForFileName(name) + '/' + "sizes.json"};
}


void registerStorageStripeLog(StorageFactory & factory)
{
    factory.registerStorage("StripeLog", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageStripeLog::create(
            args.data_path, args.table_name, args.columns,
            args.attach, args.context.getSettings().max_compress_block_size);
    });
}

}
