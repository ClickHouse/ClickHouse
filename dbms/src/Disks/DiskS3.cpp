#include "DiskS3.h"

#if USE_AWS_S3
#    include "DiskFactory.h"

#    include <random>
#    include <utility>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/ReadHelpers.h>
#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/WriteBufferFromS3.h>
#    include <IO/WriteHelpers.h>
#    include <Poco/File.h>
#    include <Common/checkStackSize.h>
#    include <Common/quoteString.h>
#    include <Common/thread_local_rng.h>

#    include <aws/s3/model/CopyObjectRequest.h>
#    include <aws/s3/model/DeleteObjectRequest.h>
#    include <aws/s3/model/GetObjectRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int PATH_ACCESS_DENIED;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
}

namespace
{
    template <typename Result, typename Error>
    void throwIfError(Aws::Utils::Outcome<Result, Error> && response)
    {
        if (!response.IsSuccess())
        {
            const auto & err = response.GetError();
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
    }

    /**
     * S3 metadata file layout:
     * Number of S3 objects, Total size of all S3 objects.
     * Each S3 object represents path where object located in S3 and size of object.
     */
    struct Metadata
    {
        // Metadata file version.
        const UInt32 VERSION = 1;

        using PathAndSize = std::pair<String, size_t>;

        // Path to metadata file on local FS.
        String metadata_file_path;
        // S3 object references count.
        UInt32 s3_objects_count;
        // Total size of all S3 objects.
        size_t total_size;
        // S3 objects paths and their sizes.
        std::vector<PathAndSize> s3_objects;

        explicit Metadata(const Poco::File & file) : Metadata(file.path(), false) {}

        // Load metadata by path or create empty if `create` flag is set.
        explicit Metadata(const String & file_path, bool create = false)
            : metadata_file_path(file_path), s3_objects_count(0), total_size(0), s3_objects(0)
        {
            if (create)
                return;

            ReadBufferFromFile buf(file_path, 1024); /* reasonable buffer size for small file */

            UInt32 version;
            readIntText(version, buf);

            if (version != VERSION)
                throw Exception(
                    "Unknown metadata file version. Path: " + file_path + ", Version: " + std::to_string(version)
                        + ", Expected version: " + std::to_string(VERSION),
                    ErrorCodes::UNKNOWN_FORMAT);

            assertChar('\n', buf);

            readIntText(s3_objects_count, buf);
            assertChar('\t', buf);
            readIntText(total_size, buf);
            assertChar('\n', buf);
            s3_objects.resize(s3_objects_count);
            for (UInt32 i = 0; i < s3_objects_count; ++i)
            {
                String path;
                size_t size;
                readIntText(size, buf);
                assertChar('\t', buf);
                readEscapedString(path, buf);
                assertChar('\n', buf);
                s3_objects[i] = std::make_pair(path, size);
            }
        }

        void addObject(const String & path, size_t size)
        {
            ++s3_objects_count;
            total_size += size;
            s3_objects.emplace_back(path, size);
        }

        void save()
        {
            WriteBufferFromFile buf(metadata_file_path, 1024);

            writeIntText(VERSION, buf);
            writeChar('\n', buf);

            writeIntText(s3_objects_count, buf);
            writeChar('\t', buf);
            writeIntText(total_size, buf);
            writeChar('\n', buf);
            for (UInt32 i = 0; i < s3_objects_count; ++i)
            {
                auto path_and_size = s3_objects[i];
                writeIntText(path_and_size.second, buf);
                writeChar('\t', buf);
                writeEscapedString(path_and_size.first, buf);
                writeChar('\n', buf);
            }
            buf.finalize();
        }
    };

    // Reads data from S3.
    // It supports reading from multiple S3 paths that resides in Metadata.
    class ReadIndirectBufferFromS3 : public ReadBufferFromFileBase
    {
    public:
        ReadIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_, Metadata metadata_, size_t buf_size_)
            : ReadBufferFromFileBase()
            , client_ptr(std::move(client_ptr_))
            , bucket(bucket_)
            , metadata(std::move(metadata_))
            , buf_size(buf_size_)
            , absolute_position(0)
            , initialized(false)
            , current_buf_idx(0)
            , current_buf(nullptr)
        {
        }

        off_t seek(off_t offset_, int whence) override
        {
            if (whence != SEEK_SET)
                throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

            if (offset_ < 0 || metadata.total_size <= static_cast<UInt64>(offset_))
                throw Exception(
                    "Seek position is out of bounds. "
                    "Offset: "
                        + std::to_string(offset_) + ", Max: " + std::to_string(metadata.total_size),
                    ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

            absolute_position = offset_;

            /// TODO: Do not re-initialize buffer if current position within working buffer.
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
            for (UInt32 i = 0; i < metadata.s3_objects_count; ++i)
            {
                current_buf_idx = i;
                auto path = metadata.s3_objects[i].first;
                auto size = metadata.s3_objects[i].second;
                if (size > offset)
                {
                    auto buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, path, buf_size);
                    buf->seek(offset, SEEK_SET);
                    return buf;
                }
                offset -= size;
            }
            initialized = true;
            return nullptr;
        }

        bool nextImpl() override
        {
            // Find first available buffer that fits to given offset.
            if (!initialized)
            {
                current_buf = initialize();
            }

            // If current buffer has remaining data - use it.
            if (current_buf && current_buf->next())
            {
                working_buffer = current_buf->buffer();
                absolute_position += working_buffer.size();
                return true;
            }

            // If there is no available buffers - nothing to read.
            if (current_buf_idx + 1 >= metadata.s3_objects_count)
                return false;

            ++current_buf_idx;
            auto path = metadata.s3_objects[current_buf_idx].first;
            current_buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, path, buf_size);
            current_buf->next();
            working_buffer = current_buf->buffer();
            absolute_position += working_buffer.size();

            return true;
        }

    private:
        std::shared_ptr<Aws::S3::S3Client> client_ptr;
        const String & bucket;
        Metadata metadata;
        size_t buf_size;

        size_t absolute_position = 0;
        bool initialized;
        UInt32 current_buf_idx;
        std::unique_ptr<ReadBufferFromS3> current_buf;
    };

    /// Stores data in S3 and adds the object key (S3 path) and object size to metadata file on local FS.
    class WriteIndirectBufferFromS3 : public WriteBufferFromS3
    {
    public:
        WriteIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
            const String & bucket_,
            Metadata metadata_,
            const String & s3_path_,
            size_t min_upload_part_size,
            size_t buf_size_)
            : WriteBufferFromS3(client_ptr_, bucket_, s3_path_, min_upload_part_size, buf_size_)
            , metadata(std::move(metadata_))
            , s3_path(s3_path_)
        {
        }

        void finalize() override
        {
            WriteBufferFromS3::finalize();
            metadata.addObject(s3_path, total_size);
            metadata.save();
            finalized = true;
        }

        ~WriteIndirectBufferFromS3() override
        {
            if (finalized)
                return;

            try
            {
                finalize();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

    private:
        bool finalized = false;
        Metadata metadata;
        String s3_path;
    };
}


class DiskS3DirectoryIterator : public IDiskDirectoryIterator
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

private:
    Poco::DirectoryIterator iter;
    String folder_path;
};


using DiskS3Ptr = std::shared_ptr<DiskS3>;

class DiskS3Reservation : public IReservation
{
public:
    DiskS3Reservation(const DiskS3Ptr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk() const override { return disk; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(disk->reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskS3Reservation() override;

private:
    DiskS3Ptr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


DiskS3::DiskS3(
    String name_,
    std::shared_ptr<Aws::S3::S3Client> client_,
    String bucket_,
    String s3_root_path_,
    String metadata_path_,
    size_t min_upload_part_size_)
    : name(std::move(name_))
    , client(std::move(client_))
    , bucket(std::move(bucket_))
    , s3_root_path(std::move(s3_root_path_))
    , metadata_path(std::move(metadata_path_))
    , min_upload_part_size(min_upload_part_size_)
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
    Metadata metadata(metadata_path + path);
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
            remove(it->path());
}

void DiskS3::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);
    Poco::File(metadata_path + from_path).renameTo(metadata_path + to_path);
}

void DiskS3::replaceFile(const String & from_path, const String & to_path)
{
    Poco::File from_file(metadata_path + from_path);
    Poco::File to_file(metadata_path + to_path);
    if (to_file.exists())
    {
        Poco::File tmp_file(metadata_path + to_path + ".old");
        to_file.renameTo(tmp_file.path());
        from_file.renameTo(metadata_path + to_path);
        remove(to_path + ".old");
    }
    else
        from_file.renameTo(to_file.path());
}

void DiskS3::copyFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        remove(to_path);

    Metadata from(metadata_path + from_path);
    Metadata to(metadata_path + to_path, true);

    for (UInt32 i = 0; i < from.s3_objects_count; ++i)
    {
        auto path = from.s3_objects[i].first;
        auto size = from.s3_objects[i].second;
        auto new_path = s3_root_path + getRandomName();
        Aws::S3::Model::CopyObjectRequest req;
        req.SetBucket(bucket);
        req.SetCopySource(path);
        req.SetKey(new_path);
        throwIfError(client->CopyObject(req));

        to.addObject(new_path, size);
    }

    to.save();
}

std::unique_ptr<ReadBufferFromFileBase> DiskS3::readFile(const String & path, size_t buf_size) const
{
    Metadata metadata(metadata_path + path);

    LOG_DEBUG(
        &Logger::get("DiskS3"),
        "Read from file by path: " << backQuote(metadata_path + path) << " Existing S3 objects: " << metadata.s3_objects_count);

    return std::make_unique<ReadIndirectBufferFromS3>(client, bucket, metadata, buf_size);
}

std::unique_ptr<WriteBuffer> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    bool exist = exists(path);
    // Reference to store new S3 object.
    auto s3_path = s3_root_path + getRandomName();
    if (!exist || mode == WriteMode::Rewrite)
    {
        // If metadata file exists - remove and create new.
        if (exist)
            remove(path);

        Metadata metadata(metadata_path + path, true);
        // Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(&Logger::get("DiskS3"), "Write to file by path: " << backQuote(metadata_path + path) << " New S3 path: " << s3_path);

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_path, min_upload_part_size, buf_size);
    }
    else
    {
        Metadata metadata(metadata_path + path);

        LOG_DEBUG(
            &Logger::get("DiskS3"),
            "Append to file by path: " << backQuote(metadata_path + path) << " New S3 path: " << s3_path
                                       << " Existing S3 objects: " << metadata.s3_objects_count);

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_path, min_upload_part_size, buf_size);
    }
}

void DiskS3::remove(const String & path)
{
    LOG_DEBUG(&Logger::get("DiskS3"), "Remove file by path: " << backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Metadata metadata(file);
        for (UInt32 i = 0; i < metadata.s3_objects_count; ++i)
        {
            auto s3_path = metadata.s3_objects[i].first;

            // TODO: Make operation idempotent. Do not throw exception if key is already deleted.
            Aws::S3::Model::DeleteObjectRequest request;
            request.SetBucket(bucket);
            request.SetKey(s3_path);
            throwIfError(client->DeleteObject(request));
        }
    }
    file.remove();
}

void DiskS3::removeRecursive(const String & path)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        remove(metadata_path + path);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeRecursive(it->path());
        file.remove();
    }
}

String DiskS3::getRandomName() const
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    String res(32, ' '); /// The number of bits of entropy should be not less than 128.
    for (auto & c : res)
        c = distribution(thread_local_rng);
    return res;
}

bool DiskS3::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(&Logger::get("DiskS3"), "Reserving 0 bytes on s3 disk " << backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(
            &Logger::get("DiskS3"),
            "Reserving " << formatReadableSizeWithBinarySuffix(bytes) << " on disk " << backQuote(name) << ", having unreserved "
                         << formatReadableSizeWithBinarySuffix(unreserved_space) << ".");
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}


DiskS3Reservation::~DiskS3Reservation()
{
    try
    {
        std::lock_guard lock(disk->reservation_mutex);
        if (disk->reserved_bytes < size)
        {
            disk->reserved_bytes = 0;
            LOG_ERROR(&Logger::get("DiskLocal"), "Unbalanced reservations size for disk '" + disk->getName() + "'.");
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(&Logger::get("DiskLocal"), "Unbalanced reservation count for disk '" + disk->getName() + "'.");
        else
            --disk->reservation_count;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

inline void checkWriteAccess(std::shared_ptr<DiskS3> & disk)
{
    auto file = disk->writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    file->write("test", 4);
}

inline void checkReadAccess(const String & disk_name, std::shared_ptr<DiskS3> & disk)
{
    auto file = disk->readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
    String buf(4, '0');
    file->readStrict(buf.data(), 4);
    if (buf != "test")
        throw Exception("No read access to S3 bucket in disk " + disk_name, ErrorCodes::PATH_ACCESS_DENIED);
}

inline void checkRemoveAccess(std::shared_ptr<DiskS3> & disk)
{
    disk->remove("test_acl");
}

void registerDiskS3(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      const Context & context) -> DiskPtr {
        Poco::File disk{context.getPath() + "disks/" + name};
        disk.createDirectories();

        S3::URI uri(Poco::URI(config.getString(config_prefix + ".endpoint")));
        auto client = S3::ClientFactory::instance().create(
            uri.endpoint,
            config.getString(config_prefix + ".access_key_id", ""),
            config.getString(config_prefix + ".secret_access_key", ""));

        if (uri.key.back() != '/')
            throw Exception("S3 path must ends with '/', but '" + uri.key + "' doesn't.", ErrorCodes::LOGICAL_ERROR);

        String metadata_path = context.getPath() + "disks/" + name + "/";

        auto s3disk
            = std::make_shared<DiskS3>(name, client, uri.bucket, uri.key, metadata_path, context.getSettingsRef().s3_min_upload_part_size);

        /// This code is used only to check access to the corresponding disk.
        checkWriteAccess(s3disk);
        checkReadAccess(name, s3disk);
        checkRemoveAccess(s3disk);

        return s3disk;
    };
    factory.registerDiskType("s3", creator);
}

}

#endif
