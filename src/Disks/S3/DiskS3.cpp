#include "DiskS3.h"

#include "Disks/DiskFactory.h"

#include <random>
#include <optional>
#include <utility>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>
#include <Common/ThreadPool.h>
#include <common/logger_useful.h>

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
    extern const int INCORRECT_DISK_INDEX;
    extern const int BAD_ARGUMENTS;
    extern const int PATH_ACCESS_DENIED;
    extern const int CANNOT_DELETE_DIRECTORY;
    extern const int LOGICAL_ERROR;
}


/// Helper class to collect keys into chunks of maximum size (to prepare batch requests to AWS API)
class DiskS3::AwsS3KeyKeeper : public std::list<Aws::Vector<Aws::S3::Model::ObjectIdentifier>>
{
public:
    void addKey(const String & key);

private:
    /// limit for one DeleteObject request
    /// see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    const static size_t chunk_limit = 1000;
};

void DiskS3::AwsS3KeyKeeper::addKey(const String & key)
{
    if (empty() || back().size() >= chunk_limit)
    { /// add one more chunk
        push_back(value_type());
        back().reserve(chunk_limit);
    }

    Aws::S3::Model::ObjectIdentifier obj;
    obj.SetKey(key);
    back().push_back(obj);
}

String getRandomName()
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    String res(32, ' '); /// The number of bits of entropy should be not less than 128.
    for (auto & c : res)
        c = distribution(thread_local_rng);
    return res;
}

template <typename Result, typename Error>
void throwIfError(Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(std::to_string(static_cast<int>(err.GetErrorType())) + ": " + err.GetMessage(), ErrorCodes::S3_ERROR);
    }
}

/**
 * S3 metadata file layout:
 * Number of S3 objects, Total size of all S3 objects.
 * Each S3 object represents path where object located in S3 and size of object.
 */
struct DiskS3::Metadata
{
    /// Metadata file version.
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    using PathAndSize = std::pair<String, size_t>;

    /// S3 root path.
    const String & s3_root_path;

    /// Disk path.
    const String & disk_path;
    /// Relative path to metadata file on local FS.
    String metadata_file_path;
    /// Total size of all S3 objects.
    size_t total_size;
    /// S3 objects paths and their sizes.
    std::vector<PathAndSize> s3_objects;
    /// Number of references (hardlinks) to this metadata file.
    UInt32 ref_count;
    /// Flag indicates that file is read only.
    bool read_only = false;

    /// Load metadata by path or create empty if `create` flag is set.
    explicit Metadata(const String & s3_root_path_, const String & disk_path_, const String & metadata_file_path_, bool create = false)
        : s3_root_path(s3_root_path_), disk_path(disk_path_), metadata_file_path(metadata_file_path_), total_size(0), s3_objects(0), ref_count(0)
    {
        if (create)
            return;

        try
        {
            ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */

            UInt32 version;
            readIntText(version, buf);

            if (version < VERSION_ABSOLUTE_PATHS || version > VERSION_READ_ONLY_FLAG)
                throw Exception(
                    "Unknown metadata file version. Path: " + disk_path + metadata_file_path
                    + " Version: " + std::to_string(version) + ", Maximum expected version: " + std::to_string(VERSION_READ_ONLY_FLAG),
                    ErrorCodes::UNKNOWN_FORMAT);

            assertChar('\n', buf);

            UInt32 s3_objects_count;
            readIntText(s3_objects_count, buf);
            assertChar('\t', buf);
            readIntText(total_size, buf);
            assertChar('\n', buf);
            s3_objects.resize(s3_objects_count);
            for (UInt32 i = 0; i < s3_objects_count; ++i)
            {
                String s3_object_path;
                size_t s3_object_size;
                readIntText(s3_object_size, buf);
                assertChar('\t', buf);
                readEscapedString(s3_object_path, buf);
                if (version == VERSION_ABSOLUTE_PATHS)
                {
                    if (!boost::algorithm::starts_with(s3_object_path, s3_root_path))
                        throw Exception(
                            "Path in metadata does not correspond S3 root path. Path: " + s3_object_path
                            + ", root path: " + s3_root_path + ", disk path: " + disk_path_,
                            ErrorCodes::UNKNOWN_FORMAT);
                    s3_object_path = s3_object_path.substr(s3_root_path.size());
                }
                assertChar('\n', buf);
                s3_objects[i] = {s3_object_path, s3_object_size};
            }

            readIntText(ref_count, buf);
            assertChar('\n', buf);

            if (version >= VERSION_READ_ONLY_FLAG)
            {
                readBoolText(read_only, buf);
                assertChar('\n', buf);
            }
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
                throw;

            throw Exception("Failed to read metadata file", e, ErrorCodes::UNKNOWN_FORMAT);
        }
    }

    void addObject(const String & path, size_t size)
    {
        total_size += size;
        s3_objects.emplace_back(path, size);
    }

    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false)
    {
        WriteBufferFromFile buf(disk_path + metadata_file_path, 1024);

        writeIntText(VERSION_RELATIVE_PATHS, buf);
        writeChar('\n', buf);

        writeIntText(s3_objects.size(), buf);
        writeChar('\t', buf);
        writeIntText(total_size, buf);
        writeChar('\n', buf);
        for (const auto & [s3_object_path, s3_object_size] : s3_objects)
        {
            writeIntText(s3_object_size, buf);
            writeChar('\t', buf);
            writeEscapedString(s3_object_path, buf);
            writeChar('\n', buf);
        }

        writeIntText(ref_count, buf);
        writeChar('\n', buf);

        writeBoolText(read_only, buf);
        writeChar('\n', buf);

        buf.finalize();
        if (sync)
            buf.sync();
    }
};

DiskS3::Metadata DiskS3::readMeta(const String & path) const
{
    return Metadata(s3_root_path, metadata_path, path);
}

DiskS3::Metadata DiskS3::createMeta(const String & path) const
{
    return Metadata(s3_root_path, metadata_path, path, true);
}

/// Reads data from S3 using stored paths in metadata.
class ReadIndirectBufferFromS3 final : public ReadBufferFromFileBase
{
public:
    explicit ReadIndirectBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_, DiskS3::Metadata metadata_, size_t max_single_read_retries_, size_t buf_size_)
        : client_ptr(std::move(client_ptr_))
        , bucket(bucket_)
        , metadata(std::move(metadata_))
        , max_single_read_retries(max_single_read_retries_)
        , buf_size(buf_size_)
    {
    }

    off_t seek(off_t offset_, int whence) override
    {
        if (whence == SEEK_CUR)
        {
            /// If position within current working buffer - shift pos.
            if (!working_buffer.empty() && size_t(getPosition() + offset_) < absolute_position)
            {
                pos += offset_;
                return getPosition();
            }
            else
            {
                absolute_position += offset_;
            }
        }
        else if (whence == SEEK_SET)
        {
            /// If position within current working buffer - shift pos.
            if (!working_buffer.empty() && size_t(offset_) >= absolute_position - working_buffer.size()
                && size_t(offset_) < absolute_position)
            {
                pos = working_buffer.end() - (absolute_position - offset_);
                return getPosition();
            }
            else
            {
                absolute_position = offset_;
            }
        }
        else
            throw Exception("Only SEEK_SET or SEEK_CUR modes are allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        current_buf = initialize();
        pos = working_buffer.end();

        return absolute_position;
    }

    off_t getPosition() override { return absolute_position - available(); }

    std::string getFileName() const override { return metadata.metadata_file_path; }

private:
    std::unique_ptr<ReadBufferFromS3> initialize()
    {
        size_t offset = absolute_position;
        for (size_t i = 0; i < metadata.s3_objects.size(); ++i)
        {
            current_buf_idx = i;
            const auto & [path, size] = metadata.s3_objects[i];
            if (size > offset)
            {
                auto buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, metadata.s3_root_path + path, max_single_read_retries, buf_size);
                buf->seek(offset, SEEK_SET);
                return buf;
            }
            offset -= size;
        }
        return nullptr;
    }

    bool nextImpl() override
    {
        /// Find first available buffer that fits to given offset.
        if (!current_buf)
            current_buf = initialize();

        /// If current buffer has remaining data - use it.
        if (current_buf)
        {
            bool result = nextAndShiftPosition();
            if (result)
                return true;
        }

        /// If there is no available buffers - nothing to read.
        if (current_buf_idx + 1 >= metadata.s3_objects.size())
            return false;

        ++current_buf_idx;
        const auto & path = metadata.s3_objects[current_buf_idx].first;

        current_buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, metadata.s3_root_path + path, max_single_read_retries, buf_size);

        return nextAndShiftPosition();
    }

    bool nextAndShiftPosition()
    {
        /// Transfer current position and working_buffer to actual ReadBuffer
        swap(*current_buf);
        /// Position and working_buffer will be updated in next() call
        auto result = current_buf->next();
        /// and assigned to current buffer.
        swap(*current_buf);

        /// absolute position is shifted by a data size that was read in next() call above.
        if (result)
            absolute_position += working_buffer.size();

        return result;
    }

    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    const String & bucket;
    DiskS3::Metadata metadata;
    UInt64 max_single_read_retries;
    size_t buf_size;

    size_t absolute_position = 0;
    size_t current_buf_idx = 0;
    std::unique_ptr<ReadBufferFromS3> current_buf;
};

/// Stores data in S3 and adds the object key (S3 path) and object size to metadata file on local FS.
class WriteIndirectBufferFromS3 final : public WriteBufferFromFileBase
{
public:
    WriteIndirectBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
        const String & bucket_,
        DiskS3::Metadata metadata_,
        const String & s3_path_,
        std::optional<DiskS3::ObjectMetadata> object_metadata_,
        size_t min_upload_part_size,
        size_t max_single_part_upload_size,
        size_t buf_size_)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , impl(WriteBufferFromS3(client_ptr_, bucket_, metadata_.s3_root_path + s3_path_, min_upload_part_size, max_single_part_upload_size,std::move(object_metadata_), buf_size_))
        , metadata(std::move(metadata_))
        , s3_path(s3_path_)
    {
    }

    ~WriteIndirectBufferFromS3() override
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void finalize() override
    {
        if (finalized)
            return;

        next();
        impl.finalize();

        metadata.addObject(s3_path, count());
        metadata.save();

        finalized = true;
    }

    void sync() override
    {
        if (finalized)
            metadata.save(true);
    }

    std::string getFileName() const override { return metadata.metadata_file_path; }

private:
    void nextImpl() override
    {
        /// Transfer current working buffer to WriteBufferFromS3.
        impl.swap(*this);

        /// Write actual data to S3.
        impl.next();

        /// Return back working buffer.
        impl.swap(*this);
    }

    WriteBufferFromS3 impl;
    bool finalized = false;
    DiskS3::Metadata metadata;
    String s3_path;
};


class DiskS3DirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskS3DirectoryIterator(const String & full_path, const String & folder_path_) : iter(full_path), folder_path(folder_path_) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != Poco::DirectoryIterator(); }

    String path() const override
    {
        if (iter->isDirectory())
            return folder_path + iter.name() + '/';
        else
            return folder_path + iter.name();
    }

    String name() const override { return iter.name(); }

private:
    Poco::DirectoryIterator iter;
    String folder_path;
};


using DiskS3Ptr = std::shared_ptr<DiskS3>;

class DiskS3Reservation final : public IReservation
{
public:
    DiskS3Reservation(const DiskS3Ptr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
        {
            throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
        }
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(disk->reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskS3Reservation() override
    {
        try
        {
            std::lock_guard lock(disk->reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservations size for disk '{}'.", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
                LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservation count for disk '{}'.", disk->getName());
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskS3Ptr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};

/// Runs tasks asynchronously using thread pool.
class AsyncExecutor : public Executor
{
public:
    explicit AsyncExecutor(int thread_pool_size) : pool(ThreadPool(thread_pool_size)) { }

    std::future<void> execute(std::function<void()> task) override
    {
        auto promise = std::make_shared<std::promise<void>>();

        pool.scheduleOrThrowOnError(
            [promise, task]()
            {
                try
                {
                    task();
                    promise->set_value();
                }
                catch (...)
                {
                    tryLogCurrentException(&Poco::Logger::get("DiskS3"), "Failed to run async task");

                    try
                    {
                        promise->set_exception(std::current_exception());
                    } catch (...) { }
                }
            });

        return promise->get_future();
    }

private:
    ThreadPool pool;
};


DiskS3::DiskS3(
    String name_,
    std::shared_ptr<Aws::S3::S3Client> client_,
    std::shared_ptr<S3::ProxyConfiguration> proxy_configuration_,
    String bucket_,
    String s3_root_path_,
    String metadata_path_,
    UInt64 max_single_read_retries_,
    size_t min_upload_part_size_,
    size_t max_single_part_upload_size_,
    size_t min_bytes_for_seek_,
    bool send_metadata_,
    int thread_pool_size_,
    int list_object_keys_size_)
    : IDisk(std::make_unique<AsyncExecutor>(thread_pool_size_))
    , name(std::move(name_))
    , client(std::move(client_))
    , proxy_configuration(std::move(proxy_configuration_))
    , bucket(std::move(bucket_))
    , s3_root_path(std::move(s3_root_path_))
    , metadata_path(std::move(metadata_path_))
    , max_single_read_retries(max_single_read_retries_)
    , min_upload_part_size(min_upload_part_size_)
    , max_single_part_upload_size(max_single_part_upload_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , send_metadata(send_metadata_)
    , revision_counter(0)
    , list_object_keys_size(list_object_keys_size_)
{
}

ReservationPtr DiskS3::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskS3Reservation>(std::static_pointer_cast<DiskS3>(shared_from_this()), bytes);
}

bool DiskS3::exists(const String & path) const
{
    return Poco::File(metadata_path + path).exists();
}

bool DiskS3::isFile(const String & path) const
{
    return Poco::File(metadata_path + path).isFile();
}

bool DiskS3::isDirectory(const String & path) const
{
    return Poco::File(metadata_path + path).isDirectory();
}

size_t DiskS3::getFileSize(const String & path) const
{
    auto metadata = readMeta(path);
    return metadata.total_size;
}

void DiskS3::createDirectory(const String & path)
{
    Poco::File(metadata_path + path).createDirectory();
}

void DiskS3::createDirectories(const String & path)
{
    Poco::File(metadata_path + path).createDirectories();
}

DiskDirectoryIteratorPtr DiskS3::iterateDirectory(const String & path)
{
    return std::make_unique<DiskS3DirectoryIterator>(metadata_path + path, path);
}

void DiskS3::clearDirectory(const String & path)
{
    for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        if (isFile(it->path()))
            removeFile(it->path());
}

void DiskS3::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

    if (send_metadata)
    {
        auto revision = ++revision_counter;
        const DiskS3::ObjectMetadata object_metadata {
            {"from_path", from_path},
            {"to_path", to_path}
        };
        createFileOperationObject("rename", revision, object_metadata);
    }

    Poco::File(metadata_path + from_path).renameTo(metadata_path + to_path);
}

void DiskS3::replaceFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
    {
        const String tmp_path = to_path + ".old";
        moveFile(to_path, tmp_path);
        moveFile(from_path, to_path);
        removeFile(tmp_path);
    }
    else
        moveFile(from_path, to_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskS3::readFile(const String & path, size_t buf_size, size_t, size_t, size_t) const
{
    auto metadata = readMeta(path);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Read from file by path: {}. Existing S3 objects: {}",
        backQuote(metadata_path + path), metadata.s3_objects.size());

    auto reader = std::make_unique<ReadIndirectBufferFromS3>(client, bucket, metadata, max_single_read_retries, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), min_bytes_for_seek);
}

std::unique_ptr<WriteBufferFromFileBase> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    bool exist = exists(path);
    if (exist && readMeta(path).read_only)
        throw Exception("File is read-only: " + path, ErrorCodes::PATH_ACCESS_DENIED);

    /// Path to store new S3 object.
    auto s3_path = getRandomName();

    std::optional<ObjectMetadata> object_metadata;
    if (send_metadata)
    {
        auto revision = ++revision_counter;
        object_metadata = {
            {"path", path}
        };
        s3_path = "r" + revisionToString(revision) + "-file-" + s3_path;
    }

    if (!exist || mode == WriteMode::Rewrite)
    {
        /// If metadata file exists - remove and create new.
        if (exist)
            removeFile(path);

        auto metadata = createMeta(path);
        /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Write to file by path: {}. New S3 path: {}", backQuote(metadata_path + path), s3_root_path + s3_path);

        return std::make_unique<WriteIndirectBufferFromS3>(
            client, bucket, metadata, s3_path, object_metadata, min_upload_part_size, max_single_part_upload_size, buf_size);
    }
    else
    {
        auto metadata = readMeta(path);

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Append to file by path: {}. New S3 path: {}. Existing S3 objects: {}.",
            backQuote(metadata_path + path), s3_root_path + s3_path, metadata.s3_objects.size());

        return std::make_unique<WriteIndirectBufferFromS3>(
            client, bucket, metadata, s3_path, object_metadata, min_upload_part_size, max_single_part_upload_size, buf_size);
    }
}

void DiskS3::removeMeta(const String & path, AwsS3KeyKeeper & keys)
{
    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Remove file by path: {}", backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);

    if (!file.isFile())
        throw Exception(ErrorCodes::CANNOT_DELETE_DIRECTORY, "Path '{}' is a directory", path);

    try
    {
        auto metadata = readMeta(path);

        /// If there is no references - delete content from S3.
        if (metadata.ref_count == 0)
        {
            file.remove();

            for (const auto & [s3_object_path, _] : metadata.s3_objects)
                keys.addKey(s3_root_path + s3_object_path);
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata.ref_count;
            metadata.save();
            file.remove();
        }
    }
    catch (const Exception & e)
    {
        /// If it's impossible to read meta - just remove it from FS.
        if (e.code() == ErrorCodes::UNKNOWN_FORMAT)
        {
            LOG_WARNING(
                &Poco::Logger::get("DiskS3"),
                "Metadata file {} can't be read by reason: {}. Removing it forcibly.",
                backQuote(path),
                e.nested() ? e.nested()->message() : e.message());

            file.remove();
        }
        else
            throw;
    }
}

void DiskS3::removeMetaRecursive(const String & path, AwsS3KeyKeeper & keys)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        removeMeta(path, keys);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeMetaRecursive(it->path(), keys);
        file.remove();
    }
}

void DiskS3::removeAws(const AwsS3KeyKeeper & keys)
{
    if (!keys.empty())
    {
        for (const auto & chunk : keys)
        {
            Aws::S3::Model::Delete delkeys;
            delkeys.SetObjects(chunk);

            /// TODO: Make operation idempotent. Do not throw exception if key is already deleted.
            Aws::S3::Model::DeleteObjectsRequest request;
            request.SetBucket(bucket);
            request.SetDelete(delkeys);
            auto outcome = client->DeleteObjects(request);
            throwIfError(outcome);
        }
    }
}

void DiskS3::removeFile(const String & path)
{
    AwsS3KeyKeeper keys;
    removeMeta(path, keys);
    removeAws(keys);
}

void DiskS3::removeFileIfExists(const String & path)
{
    AwsS3KeyKeeper keys;
    if (Poco::File(metadata_path + path).exists())
    {
        removeMeta(path, keys);
        removeAws(keys);
    }
}

void DiskS3::removeDirectory(const String & path)
{
    Poco::File(metadata_path + path).remove();
}

void DiskS3::removeRecursive(const String & path)
{
    AwsS3KeyKeeper keys;
    removeMetaRecursive(path, keys);
    removeAws(keys);
}

bool DiskS3::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Reserving 0 bytes on s3 disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

void DiskS3::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}

void DiskS3::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    Poco::File(metadata_path + path).setLastModified(timestamp);
}

Poco::Timestamp DiskS3::getLastModified(const String & path)
{
    return Poco::File(metadata_path + path).getLastModified();
}

void DiskS3::createHardLink(const String & src_path, const String & dst_path)
{
    /// We don't need to record hardlinks created to shadow folder.
    if (send_metadata && !dst_path.starts_with("shadow/"))
    {
        auto revision = ++revision_counter;
        const ObjectMetadata object_metadata {
            {"src_path", src_path},
            {"dst_path", dst_path}
        };
        createFileOperationObject("hardlink", revision, object_metadata);
    }

    /// Increment number of references.
    auto src = readMeta(src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
}

void DiskS3::createFile(const String & path)
{
    /// Create empty metadata file.
    auto metadata = createMeta(path);
    metadata.save();
}

void DiskS3::setReadOnly(const String & path)
{
    /// We should store read only flag inside metadata file (instead of using FS flag),
    /// because we modify metadata file when create hard-links from it.
    auto metadata = readMeta(path);
    metadata.read_only = true;
    metadata.save();
}

void DiskS3::shutdown()
{
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    client->DisableRequestProcessing();
}

void DiskS3::createFileOperationObject(const String & operation_name, UInt64 revision, const DiskS3::ObjectMetadata & metadata)
{
    const String key = "operations/r" + revisionToString(revision) + "-" + operation_name;
    WriteBufferFromS3 buffer(client, bucket, s3_root_path + key, min_upload_part_size, max_single_part_upload_size, metadata);
    buffer.write('0');
    buffer.finalize();
}

void DiskS3::startup()
{
    /// Need to be enabled if it was disabled during shutdown() call.
    client->EnableRequestProcessing();

    if (!send_metadata)
        return;

    LOG_INFO(&Poco::Logger::get("DiskS3"), "Starting up disk {}", name);

    /// Find last revision.
    UInt64 l = 0, r = LATEST_REVISION;
    while (l < r)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Check revision in bounds {}-{}", l, r);

        auto revision = l + (r - l + 1) / 2;
        auto revision_str = revisionToString(revision);

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Check object with revision {}", revision);

        /// Check file or operation with such revision exists.
        if (checkObjectExists(s3_root_path + "r" + revision_str)
            || checkObjectExists(s3_root_path + "operations/r" + revision_str))
            l = revision;
        else
            r = revision - 1;
    }
    revision_counter = l;
    LOG_INFO(&Poco::Logger::get("DiskS3"), "Found last revision number {} for disk {}", revision_counter, name);
}

bool DiskS3::checkObjectExists(const String & prefix)
{
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    request.SetPrefix(prefix);
    request.SetMaxKeys(1);

    auto outcome = client->ListObjectsV2(request);
    throwIfError(outcome);

    return !outcome.GetResult().GetContents().empty();
}

Aws::S3::Model::HeadObjectResult DiskS3::headObject(const String & source_bucket, const String & key)
{
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(source_bucket);
    request.SetKey(key);

    auto outcome = client->HeadObject(request);
    throwIfError(outcome);

    return outcome.GetResultWithOwnership();
}

void DiskS3::listObjects(const String & source_bucket, const String & source_path, std::function<bool(const Aws::S3::Model::ListObjectsV2Result &)> callback)
{
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(source_bucket);
    request.SetPrefix(source_path);
    request.SetMaxKeys(list_object_keys_size);

    Aws::S3::Model::ListObjectsV2Outcome outcome;
    do
    {
        outcome = client->ListObjectsV2(request);
        throwIfError(outcome);

        bool should_continue = callback(outcome.GetResult());

        if (!should_continue)
            break;

        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (outcome.GetResult().GetIsTruncated());
}

void DiskS3::copyObject(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key)
{
    Aws::S3::Model::CopyObjectRequest request;
    request.SetCopySource(src_bucket + "/" + src_key);
    request.SetBucket(dst_bucket);
    request.SetKey(dst_key);

    auto outcome = client->CopyObject(request);
    throwIfError(outcome);
}

struct DiskS3::RestoreInformation
{
    UInt64 revision = LATEST_REVISION;
    String source_bucket;
    String source_path;
};

void DiskS3::readRestoreInformation(DiskS3::RestoreInformation & restore_information)
{
    ReadBufferFromFile buffer(metadata_path + restore_file_name, 512);
    buffer.next();

    /// Empty file - just restore all metadata.
    if (!buffer.hasPendingData())
        return;

    try
    {
        readIntText(restore_information.revision, buffer);
        assertChar('\n', buffer);

        if (!buffer.hasPendingData())
            return;

        readText(restore_information.source_bucket, buffer);
        assertChar('\n', buffer);

        if (!buffer.hasPendingData())
            return;

        readText(restore_information.source_path, buffer);
        assertChar('\n', buffer);

        if (buffer.hasPendingData())
            throw Exception("Extra information at the end of restore file", ErrorCodes::UNKNOWN_FORMAT);
    }
    catch (const Exception & e)
    {
        throw Exception("Failed to read restore information", e, ErrorCodes::UNKNOWN_FORMAT);
    }
}

void DiskS3::restore()
{
    if (!exists(restore_file_name))
        return;

    try
    {
        RestoreInformation information;
        information.source_bucket = bucket;
        information.source_path = s3_root_path;

        readRestoreInformation(information);
        if (information.revision == 0)
            information.revision = LATEST_REVISION;
        if (!information.source_path.ends_with('/'))
            information.source_path += '/';

        if (information.source_bucket == bucket)
        {
            /// In this case we need to additionally cleanup S3 from objects with later revision.
            /// Will be simply just restore to different path.
            if (information.source_path == s3_root_path && information.revision != LATEST_REVISION)
                throw Exception("Restoring to the same bucket and path is allowed if revision is latest (0)", ErrorCodes::BAD_ARGUMENTS);

            /// This case complicates S3 cleanup in case of unsuccessful restore.
            if (information.source_path != s3_root_path && s3_root_path.starts_with(information.source_path))
                throw Exception("Restoring to the same bucket is allowed only if source path is not a sub-path of configured path in S3 disk", ErrorCodes::BAD_ARGUMENTS);
        }

        ///TODO: Cleanup FS and bucket if previous restore was failed.

        LOG_INFO(&Poco::Logger::get("DiskS3"), "Starting to restore disk {}. Revision: {}, Source bucket: {}, Source path: {}",
                 name, information.revision, information.source_bucket, information.source_path);

        restoreFiles(information.source_bucket, information.source_path, information.revision);
        restoreFileOperations(information.source_bucket, information.source_path, information.revision);

        Poco::File restore_file(metadata_path + restore_file_name);
        restore_file.remove();

        LOG_INFO(&Poco::Logger::get("DiskS3"), "Restore disk {} finished", name);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(&Poco::Logger::get("DiskS3"), "Failed to restore disk. Code: {}, e.displayText() = {}, Stack trace:\n\n{}", e.code(), e.displayText(), e.getStackTraceString());

        throw;
    }
}

void DiskS3::restoreFiles(const String & source_bucket, const String & source_path, UInt64 target_revision)
{
    LOG_INFO(&Poco::Logger::get("DiskS3"), "Starting restore files for disk {}", name);

    std::vector<std::future<void>> results;
    listObjects(source_bucket, source_path, [this, &source_bucket, &source_path, &target_revision, &results](auto list_result)
    {
        std::vector<String> keys;
        for (const auto & row : list_result.GetContents())
        {
            const String & key = row.GetKey();

            /// Skip file operations objects. They will be processed separately.
            if (key.find("/operations/") != String::npos)
                continue;

            const auto [revision, _] = extractRevisionAndOperationFromKey(key);
            /// Filter early if it's possible to get revision from key.
            if (revision > target_revision)
                continue;

            keys.push_back(key);
        }

        if (!keys.empty())
        {
            auto result = getExecutor().execute([this, &source_bucket, &source_path, keys]()
            {
                processRestoreFiles(source_bucket, source_path, keys);
            });

            results.push_back(std::move(result));
        }

        return true;
    });

    for (auto & result : results)
        result.wait();
    for (auto & result : results)
        result.get();

    LOG_INFO(&Poco::Logger::get("DiskS3"), "Files are restored for disk {}", name);
}

void DiskS3::processRestoreFiles(const String & source_bucket, const String & source_path, Strings keys)
{
    for (const auto & key : keys)
    {
        auto head_result = headObject(source_bucket, key);
        auto object_metadata = head_result.GetMetadata();

        /// Restore file if object has 'path' in metadata.
        auto path_entry = object_metadata.find("path");
        if (path_entry == object_metadata.end())
            throw Exception("Failed to restore key " + key + " because it doesn't have 'path' in metadata", ErrorCodes::S3_ERROR);

        const auto & path = path_entry->second;

        createDirectories(directoryPath(path));
        auto metadata = createMeta(path);
        auto relative_key = shrinkKey(source_path, key);

        /// Copy object if we restore to different bucket / path.
        if (bucket != source_bucket || s3_root_path != source_path)
            copyObject(source_bucket, key, bucket, s3_root_path + relative_key);

        metadata.addObject(relative_key, head_result.GetContentLength());
        metadata.save();

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Restored file {}", path);
    }
}

void DiskS3::restoreFileOperations(const String & source_bucket, const String & source_path, UInt64 target_revision)
{
    LOG_INFO(&Poco::Logger::get("DiskS3"), "Starting restore file operations for disk {}", name);

    /// Enable recording file operations if we restore to different bucket / path.
    send_metadata = bucket != source_bucket || s3_root_path != source_path;

    listObjects(source_bucket, source_path + "operations/", [this, &source_bucket, &target_revision](auto list_result)
    {
        const String rename = "rename";
        const String hardlink = "hardlink";

        for (const auto & row : list_result.GetContents())
        {
            const String & key = row.GetKey();

            const auto [revision, operation] = extractRevisionAndOperationFromKey(key);
            if (revision == UNKNOWN_REVISION)
            {
                LOG_WARNING(&Poco::Logger::get("DiskS3"), "Skip key {} with unknown revision", key);
                continue;
            }

            /// S3 ensures that keys will be listed in ascending UTF-8 bytes order (revision order).
            /// We can stop processing if revision of the object is already more than required.
            if (revision > target_revision)
                return false;

            /// Keep original revision if restore to different bucket / path.
            if (send_metadata)
                revision_counter = revision - 1;

            auto object_metadata = headObject(source_bucket, key).GetMetadata();
            if (operation == rename)
            {
                auto from_path = object_metadata["from_path"];
                auto to_path = object_metadata["to_path"];
                if (exists(from_path))
                {
                    moveFile(from_path, to_path);
                    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Revision {}. Restored rename {} -> {}", revision, from_path, to_path);
                }
            }
            else if (operation == hardlink)
            {
                auto src_path = object_metadata["src_path"];
                auto dst_path = object_metadata["dst_path"];
                if (exists(src_path))
                {
                    createDirectories(directoryPath(dst_path));
                    createHardLink(src_path, dst_path);
                    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Revision {}. Restored hardlink {} -> {}", revision, src_path, dst_path);
                }
            }
        }

        return true;
    });

    send_metadata = true;

    LOG_INFO(&Poco::Logger::get("DiskS3"), "File operations restored for disk {}", name);
}

std::tuple<UInt64, String> DiskS3::extractRevisionAndOperationFromKey(const String & key)
{
    UInt64 revision = UNKNOWN_REVISION;
    String operation;

    re2::RE2::FullMatch(key, key_regexp, &revision, &operation);

    return {revision, operation};
}

String DiskS3::shrinkKey(const String & path, const String & key)
{
    if (!key.starts_with(path))
        throw Exception("The key " + key + " prefix mismatch with given " + path, ErrorCodes::LOGICAL_ERROR);

    return key.substr(path.length());
}

String DiskS3::revisionToString(UInt64 revision)
{
    static constexpr size_t max_digits = 19; /// UInt64 max digits in decimal representation.

    /// Align revision number with leading zeroes to have strict lexicographical order of them.
    auto revision_str = std::to_string(revision);
    auto digits_to_align = max_digits - revision_str.length();
    for (size_t i = 0; i < digits_to_align; ++i)
        revision_str = "0" + revision_str;

    return revision_str;
}

void DiskS3::onFreeze(const String & path)
{
    createDirectories(path);
    WriteBufferFromFile revision_file_buf(metadata_path + path + "revision.txt", 32);
    writeIntText(revision_counter.load(), revision_file_buf);
    revision_file_buf.finalize();
}

}
