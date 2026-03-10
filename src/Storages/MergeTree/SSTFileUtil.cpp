#include <Storages/MergeTree/SSTFileUtil.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/file_system.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/sst_file_writer.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

namespace
{
class ReadBufferBasedSequentialFile : public rocksdb::FSSequentialFile
{
public:
    ReadBufferBasedSequentialFile(SeekableReadBuffer & read_buffer_, uint64_t base_offset_, uint64_t region_size_)
        : read_buffer(read_buffer_)
        , base_offset(base_offset_)
        , region_size(region_size_)
        , current_offset(0)
    {
        read_buffer.seek(base_offset, SEEK_SET);
    }

    rocksdb::IOStatus Read(
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) override
    {
        size_t remaining = (current_offset < region_size) ? (region_size - current_offset) : 0;
        size_t to_read = std::min(n, remaining);
        auto read = read_buffer.read(scratch, to_read);
        current_offset += read;
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Skip(uint64_t n) override
    {
        size_t remaining = (current_offset < region_size) ? (region_size - current_offset) : 0;
        size_t to_skip = std::min(static_cast<size_t>(n), remaining);
        read_buffer.ignore(to_skip);
        current_offset += to_skip;
        return rocksdb::IOStatus::OK();
    }

private:
    SeekableReadBuffer & read_buffer;
    uint64_t base_offset;
    uint64_t region_size;
    uint64_t current_offset;
};

class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
{
public:
    ReadBufferBasedRandomAccessFile(SeekableReadBuffer & read_buffer_, uint64_t base_offset_, uint64_t region_size_)
        : read_buffer(read_buffer_)
        , base_offset(base_offset_)
        , region_size(region_size_)
    {
    }

    rocksdb::IOStatus Read(
        uint64_t offset,
        size_t n,
        const rocksdb::IOOptions &,
        rocksdb::Slice * result,
        char * scratch,
        rocksdb::IODebugContext *) const override
    {
        size_t remaining = (offset < region_size) ? (region_size - offset) : 0;
        size_t to_read = std::min(n, remaining);

        auto & mutable_buffer = const_cast<SeekableReadBuffer &>(read_buffer);
        mutable_buffer.seek(base_offset + offset, SEEK_SET);
        auto read = mutable_buffer.read(scratch, to_read);
        *result = rocksdb::Slice(scratch, read);
        return rocksdb::IOStatus::OK();
    }

private:
    SeekableReadBuffer & read_buffer;
    uint64_t base_offset;
    uint64_t region_size;
};

class WriteBufferWritableFile : public rocksdb::FSWritableFile
{
public:
    explicit WriteBufferWritableFile(WriteBuffer & write_buffer_)
        : write_buffer(write_buffer_)
        , file_size(0)
    {
    }

    rocksdb::IOStatus Append(
        const rocksdb::Slice & data,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        try
        {
            write_buffer.write(data.data(), data.size());
            file_size += data.size();
            return rocksdb::IOStatus::OK();
        }
        catch (...)
        {
            auto error_msg = getCurrentExceptionMessage(true);
            return rocksdb::IOStatus::IOError("Failed to write data: " + error_msg);
        }
    }

    rocksdb::IOStatus Close(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Flush(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Sync(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    uint64_t GetFileSize(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return file_size;
    }

private:
    WriteBuffer & write_buffer;
    uint64_t file_size;
};

std::string formatToString(const char * format, va_list ap)
{
    va_list ap_copy;
    va_copy(ap_copy, ap);

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

    int size = vsnprintf(nullptr, 0, format, ap_copy);
    va_end(ap_copy);

    if (size < 0)
        return "";

    std::string result(size + 1, '\0');
    if (vsnprintf(result.data(), size + 1, format, ap) < 0)
        return "";

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

    return result;
}


class CHLoggerWrapper : public rocksdb::Logger
{
public:
    CHLoggerWrapper() : logger(getLogger("DiskBasedUniqueIndexEnv")) {}

    rocksdb::Status Close() override
    {
        logger.reset();
        return rocksdb::Status();
    }

    void Logv(
        const rocksdb::InfoLogLevel log_level,
        const char * format,
        va_list ap) override
    {
        auto msg = formatToString(format, ap);
        switch (log_level)
        {
            case rocksdb::InfoLogLevel::DEBUG_LEVEL:
                LOG_DEBUG(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::INFO_LEVEL:
                LOG_INFO(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::WARN_LEVEL:
                LOG_WARNING(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::ERROR_LEVEL:
                LOG_ERROR(logger, "{}", msg);
                break;
            case rocksdb::InfoLogLevel::FATAL_LEVEL:
                LOG_FATAL(logger, "{}", msg);
                break;
            default:
                LOG_INFO(logger, "{}", msg);
                break;
        }
    }
private:
    LoggerPtr logger;
};


class ReadBufferFileSystem : public rocksdb::FileSystem
{
public:
    ReadBufferFileSystem(SeekableReadBuffer * read_buffer_, uint64_t base_offset_, uint64_t region_size_)
        : read_buffer(read_buffer_)
        , base_offset(base_offset_)
        , region_size(region_size_)
    {
    }

    const char* Name() const override { return "ReadBufferFileSystem"; }

    rocksdb::IOStatus NewSequentialFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSSequentialFile> * r,
        rocksdb::IODebugContext *) override
    {
        *r = std::make_unique<ReadBufferBasedSequentialFile>(*read_buffer, base_offset, region_size);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus NewRandomAccessFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> * r,
        rocksdb::IODebugContext *) override
    {
        *r = std::make_unique<ReadBufferBasedRandomAccessFile>(*read_buffer, base_offset, region_size);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus NewLogger(
        const std::string&,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*) override
    {
        *result = std::make_shared<CHLoggerWrapper>();
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus FileExists(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus GetFileSize(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t * file_size,
        rocksdb::IODebugContext *) override
    {
        if (file_size)
            *file_size = region_size;
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods:
    rocksdb::IOStatus NewWritableFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSWritableFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus NewDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        std::unique_ptr<rocksdb::FSDirectory> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetChildren(
        const std::string &,
        const rocksdb::IOOptions &,
        std::vector<std::string> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDirIfMissing(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileModificationTime(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetAbsolutePath(
        const std::string &,
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus RenameFile(
        const std::string &,
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus LockFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::FileLock **,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus UnlockFile(
        rocksdb::FileLock *,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetTestDirectory(
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus IsDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        bool *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
private:
    SeekableReadBuffer * read_buffer = nullptr;
    uint64_t base_offset = 0;
    uint64_t region_size = 0;
};

class WriteBufferFileSystem : public rocksdb::FileSystem
{
public:
    explicit WriteBufferFileSystem(WriteBuffer * write_buffer_)
        : write_buffer(write_buffer_)
    {
    }

    const char* Name() const override { return "WriteBufferFileSystem"; }
    rocksdb::IOStatus NewWritableFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSWritableFile> * r,
        rocksdb::IODebugContext *) override
    {
        if (!write_buffer)
            return rocksdb::IOStatus::InvalidArgument("WriteBuffer not set");

        *r = std::make_unique<WriteBufferWritableFile>(*write_buffer);
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus NewLogger(
        const std::string&,
        const rocksdb::IOOptions&,
        std::shared_ptr<rocksdb::Logger>* result,
        rocksdb::IODebugContext*) override
    {
        *result = std::make_shared<CHLoggerWrapper>();
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods:
    rocksdb::IOStatus NewSequentialFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSSequentialFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus NewRandomAccessFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus FileExists(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileSize(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus NewDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        std::unique_ptr<rocksdb::FSDirectory> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetChildren(
        const std::string &,
        const rocksdb::IOOptions &,
        std::vector<std::string> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus CreateDirIfMissing(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus DeleteDir(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileModificationTime(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetAbsolutePath(
        const std::string &,
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus RenameFile(
        const std::string &,
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus LockFile(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::FileLock **,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus UnlockFile(
        rocksdb::FileLock *,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetTestDirectory(
        const rocksdb::IOOptions &,
        std::string *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus IsDirectory(
        const std::string &,
        const rocksdb::IOOptions &,
        bool *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
private:
    WriteBuffer* write_buffer = nullptr;
};

}

SSTFileWriteStream::SSTFileWriteStream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    size_t buf_size,
    const WriteSettings & query_write_settings)
    : escaped_column_name(escaped_column_name_)
    , plain_file(data_part_storage->writeFile(escaped_column_name_ + SST_DATA_FILE_EXTENSION, buf_size, query_write_settings))
    , hashing(std::make_unique<HashingWriteBuffer>(*plain_file))
    , sst_writer(std::make_unique<SSTFileWriter>(&getWriteBuffer()))
{
}

SSTFileWriteStream::~SSTFileWriteStream() = default;

void SSTFileWriteStream::preFinalize()
{
    hashing->finalize();
    plain_file->preFinalize();
}

void SSTFileWriteStream::finalize()
{
    preFinalize();
    plain_file->finalize();
}

void SSTFileWriteStream::cancel() noexcept
{
    hashing->cancel();
    plain_file->cancel();
}

void SSTFileWriteStream::sync() const
{
    plain_file->sync();
}

SSTFileWriter & SSTFileWriteStream::getSSTWriter()
{
    if (!sst_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTFileWriter is not initialized in SSTFileWriteStream");
    return *sst_writer;
}

void SSTFileWriteStream::fillSSTChecksums(MergeTreeDataPartChecksums & checksums)
{
    if (sst_writer)
        sst_writer->finish();

    preFinalize();
    addToChecksums(checksums);
}

void SSTFileWriteStream::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + SST_DATA_FILE_EXTENSION].is_compressed = false;
    checksums.files[name + SST_DATA_FILE_EXTENSION].file_size = hashing->count();
    checksums.files[name + SST_DATA_FILE_EXTENSION].file_hash = hashing->getHash();
}

std::unique_ptr<rocksdb::Env> createWriteBufferFileSystemEnv(WriteBuffer * write_buffer)
{
    return rocksdb::NewCompositeEnv(std::make_shared<WriteBufferFileSystem>(write_buffer));
}

std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(SeekableReadBuffer * read_buffer, uint64_t base_offset, uint64_t region_size)
{
    return rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(read_buffer, base_offset, region_size));
}

SSTFileReader::SSTFileReader(SeekableReadBuffer * read_buffer, uint64_t base_offset, uint64_t region_size)
{
    sst_env = createReadBufferFileSystemEnv(read_buffer, base_offset, region_size);

    rocksdb::Options options;
    options.env = sst_env.get();

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    auto local_reader = std::make_unique<rocksdb::SstFileReader>(options);
    auto status = local_reader->Open("");
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to open SST reader from ReadBuffer: {}", status.ToString());

    status = local_reader->VerifyChecksum();
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to verify SST checksum: {}", status.ToString());

    index_reader = std::move(local_reader);

    // Load min max key
    rocksdb::ReadOptions read_opts;
    read_opts.fill_cache = false;
    auto iter = newIterator(read_opts);
    iter->SeekToFirst();
    if (iter->Valid())
    {
        auto min_key = iter->key().ToString();
        iter->SeekToLast();
        auto max_key = iter->key().ToString();
        key_range = std::make_pair(std::move(min_key), std::move(max_key));
    }
}

std::unique_ptr<rocksdb::Iterator> SSTFileReader::newIterator(const rocksdb::ReadOptions & options) const
{
    if (!index_reader)
        return std::unique_ptr<rocksdb::Iterator>(rocksdb::NewEmptyIterator());
    std::unique_ptr<rocksdb::Iterator> res;
    res.reset(index_reader->NewIterator(options));
    return res;
}

bool SSTFileReader::mayContainKey(std::string_view key) const
{
    return key >= key_range.first && key <= key_range.second;
}

void SSTFileReader::verifyChecksums() const
{
    auto status = index_reader->VerifyChecksum(rocksdb::ReadOptions{});
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to verify checksums of SST: {}", status.ToString());
}

SSTFileReader::IndexPropertiesPtr SSTFileReader::getProperties() const
{
    return index_reader->GetTableProperties();
}

bool SSTFileReader::isEmpty() const
{
    return key_range.first.empty() && key_range.second.empty();
}

SSTFileWriter::SSTFileWriter(WriteBuffer * write_buffer)
{
    sst_env = createWriteBufferFileSystemEnv(write_buffer);
    rocksdb::Options options;
    options.env = sst_env.get();

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(12));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    writer = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions(), options);
    auto status = writer->Open("");
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to open SST writer: {}", status.ToString());
}

void SSTFileWriter::put(const rocksdb::Slice & key, const rocksdb::Slice & value)
{
    auto status = writer->Put(key, value);
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to put key-value to SST: {}", status.ToString());
    has_entries = true;
}

void SSTFileWriter::finish()
{
    if (finished)
        return;
    finished = true;

    if (!writer)
        return;

    /// Skip empty SST files (RocksDB returns error for empty finish)
    if (!has_entries)
        return;

    auto status = writer->Finish();
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to finish SST file: {}", status.ToString());
}

uint64_t SSTFileWriter::fileSize() const
{
    if (!writer)
        return 0;
    return writer->FileSize();
}

}
