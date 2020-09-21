#include "DiskS3.h"

#include "Disks/DiskFactory.h"

#include <random>
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

#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>

#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
    extern const int INCORRECT_DISK_INDEX;
}

namespace
{
    String getRandomName()
    {
        std::uniform_int_distribution<int> distribution('a', 'z');
        String res(32, ' '); /// The number of bits of entropy should be not less than 128.
        for (auto & c : res)
            c = distribution(thread_local_rng);
        return res;
    }

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
        /// Metadata file version.
        static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
        static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;

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

        /// Load metadata by path or create empty if `create` flag is set.
        explicit Metadata(const String & s3_root_path_, const String & disk_path_, const String & metadata_file_path_, bool create = false)
            : s3_root_path(s3_root_path_), disk_path(disk_path_), metadata_file_path(metadata_file_path_), total_size(0), s3_objects(0), ref_count(0)
        {
            if (create)
                return;

            ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */

            UInt32 version;
            readIntText(version, buf);

            if (version != VERSION_RELATIVE_PATHS && version != VERSION_ABSOLUTE_PATHS)
                throw Exception(
                    "Unknown metadata file version. Path: " + disk_path + metadata_file_path
                        + " Version: " + std::to_string(version) + ", Maximum expected version: " + std::to_string(VERSION_RELATIVE_PATHS),
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

            buf.finalize();
            if (sync)
                buf.sync();
        }
    };

    /// Reads data from S3 using stored paths in metadata.
    class ReadIndirectBufferFromS3 final : public ReadBufferFromFileBase
    {
    public:
        ReadIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> client_ptr_, const String & bucket_, Metadata metadata_, size_t buf_size_)
            : client_ptr(std::move(client_ptr_)), bucket(bucket_), metadata(std::move(metadata_)), buf_size(buf_size_)
        {
        }

        off_t seek(off_t offset_, int whence) override
        {
            if (whence == SEEK_CUR)
            {
                /// If position within current working buffer - shift pos.
                if (working_buffer.size() && size_t(getPosition() + offset_) < absolute_position)
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
                if (working_buffer.size() && size_t(offset_) >= absolute_position - working_buffer.size()
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
                    auto buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, metadata.s3_root_path + path, buf_size);
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
            if (current_buf && current_buf->next())
            {
                working_buffer = current_buf->buffer();
                absolute_position += working_buffer.size();
                return true;
            }

            /// If there is no available buffers - nothing to read.
            if (current_buf_idx + 1 >= metadata.s3_objects.size())
                return false;

            ++current_buf_idx;
            const auto & path = metadata.s3_objects[current_buf_idx].first;
            current_buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, metadata.s3_root_path + path, buf_size);
            current_buf->next();
            working_buffer = current_buf->buffer();
            absolute_position += working_buffer.size();

            return true;
        }

        std::shared_ptr<Aws::S3::S3Client> client_ptr;
        const String & bucket;
        Metadata metadata;
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
            Metadata metadata_,
            const String & s3_path_,
            bool is_multipart,
            size_t min_upload_part_size,
            size_t buf_size_)
            : WriteBufferFromFileBase(buf_size_, nullptr, 0)
            , impl(WriteBufferFromS3(client_ptr_, bucket_, metadata_.s3_root_path + s3_path_, min_upload_part_size, is_multipart, buf_size_))
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
        Metadata metadata;
        String s3_path;
    };
}


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

    ~DiskS3Reservation() override;

private:
    DiskS3Ptr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


DiskS3::DiskS3(
    String name_,
    std::shared_ptr<Aws::S3::S3Client> client_,
    std::shared_ptr<S3::ProxyConfiguration> proxy_configuration_,
    String bucket_,
    String s3_root_path_,
    String metadata_path_,
    size_t min_upload_part_size_,
    size_t min_multi_part_upload_size_,
    size_t min_bytes_for_seek_)
    : name(std::move(name_))
    , client(std::move(client_))
    , proxy_configuration(std::move(proxy_configuration_))
    , bucket(std::move(bucket_))
    , s3_root_path(std::move(s3_root_path_))
    , metadata_path(std::move(metadata_path_))
    , min_upload_part_size(min_upload_part_size_)
    , min_multi_part_upload_size(min_multi_part_upload_size_)
    , min_bytes_for_seek(min_bytes_for_seek_)
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
    Metadata metadata(s3_root_path, metadata_path, path);
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

    Metadata from(s3_root_path, metadata_path, from_path);
    Metadata to(s3_root_path, metadata_path, to_path, true);

    for (const auto & [path, size] : from.s3_objects)
    {
        auto new_path = getRandomName();
        Aws::S3::Model::CopyObjectRequest req;
        req.SetCopySource(bucket + "/" + s3_root_path + path);
        req.SetBucket(bucket);
        req.SetKey(s3_root_path + new_path);
        throwIfError(client->CopyObject(req));

        to.addObject(new_path, size);
    }

    to.save();
}

std::unique_ptr<ReadBufferFromFileBase> DiskS3::readFile(const String & path, size_t buf_size, size_t, size_t, size_t) const
{
    Metadata metadata(s3_root_path, metadata_path, path);

    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Read from file by path: {}. Existing S3 objects: {}",
        backQuote(metadata_path + path), metadata.s3_objects.size());

    auto reader = std::make_unique<ReadIndirectBufferFromS3>(client, bucket, metadata, buf_size);
    return std::make_unique<SeekAvoidingReadBuffer>(std::move(reader), min_bytes_for_seek);
}

std::unique_ptr<WriteBufferFromFileBase> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t)
{
    bool exist = exists(path);
    /// Path to store new S3 object.
    auto s3_path = getRandomName();
    bool is_multipart = estimated_size >= min_multi_part_upload_size;
    if (!exist || mode == WriteMode::Rewrite)
    {
        /// If metadata file exists - remove and create new.
        if (exist)
            remove(path);

        Metadata metadata(s3_root_path, metadata_path, path, true);
        /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Write to file by path: {} New S3 path: {}", backQuote(metadata_path + path), s3_root_path + s3_path);

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_path, is_multipart, min_upload_part_size, buf_size);
    }
    else
    {
        Metadata metadata(s3_root_path, metadata_path, path);

        LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Append to file by path: {}. New S3 path: {}. Existing S3 objects: {}.",
            backQuote(metadata_path + path), s3_root_path + s3_path, metadata.s3_objects.size());

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_path, is_multipart, min_upload_part_size, buf_size);
    }
}

void DiskS3::remove(const String & path)
{
    LOG_DEBUG(&Poco::Logger::get("DiskS3"), "Remove file by path: {}", backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Metadata metadata(s3_root_path, metadata_path, path);

        /// If there is no references - delete content from S3.
        if (metadata.ref_count == 0)
        {
            file.remove();
            for (const auto & [s3_object_path, _] : metadata.s3_objects)
            {
                /// TODO: Make operation idempotent. Do not throw exception if key is already deleted.
                Aws::S3::Model::DeleteObjectRequest request;
                request.SetBucket(bucket);
                request.SetKey(s3_root_path + s3_object_path);
                throwIfError(client->DeleteObject(request));
            }
        }
        else /// In other case decrement number of references, save metadata and delete file.
        {
            --metadata.ref_count;
            metadata.save();
            file.remove();
        }
    }
    else
        file.remove();
}

void DiskS3::removeRecursive(const String & path)
{
    checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        remove(path);
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeRecursive(it->path());
        file.remove();
    }
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
    /// Increment number of references.
    Metadata src(s3_root_path, metadata_path, src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
}

void DiskS3::createFile(const String & path)
{
    /// Create empty metadata file.
    Metadata metadata(s3_root_path, metadata_path, path, true);
    metadata.save();
}

void DiskS3::setReadOnly(const String & path)
{
    Poco::File(metadata_path + path).setReadOnly(true);
}

DiskS3Reservation::~DiskS3Reservation()
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

}
