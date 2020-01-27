#include "DiskS3.h"

#if USE_AWS_S3
#    include "DiskFactory.h"

#    include <random>
#    include <utility>
#    include <IO/S3Common.h>
#    include <IO/ReadBufferFromS3.h>
#    include <IO/WriteBufferFromS3.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/ReadHelpers.h>
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
     * Number of references to S3 objects, Total size of all S3 objects.
     * Each reference to S3 object and size.
     */
    struct Metadata
    {
    	// Path to metadata file on local FS.
        String local_path;
        // S3 object references count.
        UInt32 ref_count;
        // Total size of all S3 objects.
        size_t total_size;
        // References to S3 objects and their sizes.
        std::vector<std::pair<String, size_t>> references;

        explicit Metadata(const Poco::File & file) : Metadata(file.path(), false) { }

        // Load metadata by path or create empty if `create` flag is set.
        explicit Metadata(const String & path, bool create = false) :
            local_path(path), ref_count(0), total_size(0), references(0)
        {
            if (create)
	            return;

            char x; // To skip separators.
            ReadBufferFromFile buf(path, 1024); /* reasonable buffer size for small file */
            readIntText(ref_count, buf);
            readChar(x, buf);
            readIntText(total_size, buf);
            readChar(x, buf);
	        references = std::vector<std::pair<String, size_t>> (ref_count);
            for (UInt32 i = 0; i < ref_count; ++i)
            {
                String ref;
                size_t size;
                readIntText(size, buf);
                readChar(x, buf);
                readEscapedString(ref, buf);
                readChar(x, buf);
                references[i] = std::make_pair(ref, size);
            }
        }

        void addReference(const String & ref, size_t size)
        {
            ref_count++;
            total_size += size;
            references.emplace_back(ref, size);
        }

        void save() {
	        WriteBufferFromFile buf(local_path, 1024);
	        writeIntText(ref_count, buf);
	        writeChar('\t', buf);
	        writeIntText(total_size, buf);
	        writeChar('\n', buf);
            for (UInt32 i = 0; i < ref_count; ++i)
            {
                auto ref_and_size = references[i];
                writeIntText(ref_and_size.second, buf);
                writeChar('\t', buf);
                writeEscapedString(ref_and_size.first, buf);
                writeChar('\n', buf);
            }
            buf.finalize();
        }
    };

    // Reads data from S3.
    // It supports multiple S3 references and reads them one by one.
    class ReadIndirectBufferFromS3 : public BufferWithOwnMemory<SeekableReadBuffer>
    {
    public:
        ReadIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> client_ptr_,
            const String & bucket_,
            Metadata metadata_,
            size_t buf_size_
            ) : BufferWithOwnMemory(buf_size_)
            , client_ptr(std::move(client_ptr_))
            , bucket(bucket_)
            , metadata(std::move(metadata_))
            , buf_size(buf_size_)
            , offset(0)
            , initialized(false)
            , current_buf_idx(0)
            , current_buf(nullptr)
        {
        }

        off_t seek(off_t off, int) override {
            if (!initialized)
            {
                if (off < 0 || metadata.total_size <= static_cast<UInt64>(off))
                    throw Exception("Seek position is out of bounds. "
                     "Offset: " + std::to_string(off) + ", Max: " + std::to_string(metadata.total_size),
                     ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

                offset = off;
            }
            return offset;
        }

    private:
        std::unique_ptr<ReadBufferFromS3> initialize()
        {
            for (UInt32 i = 0; i < metadata.ref_count; ++i)
            {
                current_buf_idx = i;
                auto ref = metadata.references[i].first;
                auto size = metadata.references[i].second;
	            if (size > offset)
	            {
                    auto buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, ref, buf_size);
                    buf->seek(offset, SEEK_SET);
                    return buf;
                }
                offset -= size;
            }
            return nullptr;
        }

        bool nextImpl() override
        {
            // Find first available buffer according to offset.
            if (!initialized)
            {
                current_buf = initialize();

                initialized = true;
            }

            // If current buffer has remaining data - use it.
            if (current_buf && current_buf->next())
            {
                working_buffer = current_buf->buffer();
                return true;
            }

            // If there is no available buffers - nothing to read.
            if (current_buf_idx + 1 >= metadata.ref_count)
                return false;

            current_buf_idx++;
            auto ref = metadata.references[current_buf_idx].first;
            current_buf = std::make_unique<ReadBufferFromS3>(client_ptr, bucket, ref, buf_size);
            current_buf->next();
            working_buffer = current_buf->buffer();

            return true;
        }

    private:
        std::shared_ptr<Aws::S3::S3Client> client_ptr;
        const String & bucket;
        Metadata metadata;
        size_t buf_size;

        size_t offset;
        bool initialized;
        UInt32 current_buf_idx;
        std::unique_ptr<ReadBufferFromS3> current_buf;
    };

    /// Stores data in S3 and appends the object key (reference) to metadata file on local FS.
    class WriteIndirectBufferFromS3 : public WriteBufferFromS3
    {
    public:
        WriteIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
            const String & bucket_,
            Metadata metadata_,
            const String & s3_ref_,
            size_t buf_size_)
            : WriteBufferFromS3(client_ptr_, bucket_, s3_ref_, DEFAULT_BLOCK_SIZE, buf_size_)
            , metadata(std::move(metadata_))
            , s3_ref(s3_ref_)
        {
        }

        void finalize() override
        {
            WriteBufferFromS3::finalize();
            metadata.addReference(s3_ref, total_size);
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
        String s3_ref;
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


DiskS3::DiskS3(String name_, std::shared_ptr<Aws::S3::S3Client> client_, String bucket_, String s3_root_path_, String metadata_path_)
    : name(std::move(name_))
    , client(std::move(client_))
    , bucket(std::move(bucket_))
    , s3_root_path(std::move(s3_root_path_))
    , metadata_path(std::move(metadata_path_))
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

    for (UInt32 i = 0; i < from.ref_count; ++i)
    {
        auto ref = from.references[i].first;
        auto size = from.references[i].second;
        auto new_ref = s3_root_path + getRandomName();
        Aws::S3::Model::CopyObjectRequest req;
        req.SetBucket(bucket);
        req.SetCopySource(ref);
        req.SetKey(new_ref);
        throwIfError(client->CopyObject(req));

        to.addReference(new_ref, size);
    }

    to.save();
}

std::unique_ptr<SeekableReadBuffer> DiskS3::readFile(const String & path, size_t buf_size) const
{
    Metadata metadata(metadata_path + path);

    LOG_DEBUG(
        &Logger::get("DiskS3"),
        "Read from file by path: " << backQuote(metadata_path + path)
        << " Existing S3 references: " << metadata.ref_count);

    return std::make_unique<ReadIndirectBufferFromS3>(client, bucket, metadata, buf_size);
}

std::unique_ptr<WriteBuffer> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    bool exist = exists(path);
    // Reference to store new S3 object.
    auto s3_ref = s3_root_path + getRandomName();
    if (!exist || mode == WriteMode::Rewrite)
    {
        // If metadata file exists - remove and create new.
        if (exist)
            remove(path);

        Metadata metadata(metadata_path + path, true);
        // Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(
            &Logger::get("DiskS3"),
            "Write to file by path: " << backQuote(metadata_path + path) << " New S3 reference: " << s3_ref);

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_ref, buf_size);
    }
    else
    {
        Metadata metadata(metadata_path + path);

        LOG_DEBUG(
            &Logger::get("DiskS3"),
            "Append to file by path: " << backQuote(metadata_path + path) << " New S3 reference: " << s3_ref
                      << " Existing S3 references: " << metadata.ref_count);

        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata, s3_ref, buf_size);
    }
}

void DiskS3::remove(const String & path)
{
    LOG_DEBUG(&Logger::get("DiskS3"), "Remove file by path: " << backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Metadata metadata(file);
        for (UInt32 i = 0; i < metadata.ref_count; ++i)
        {
            auto ref = metadata.references[i].first;

            // TODO: Make operation idempotent. Do not throw exception if key is already deleted.
            Aws::S3::Model::DeleteObjectRequest request;
            request.SetBucket(bucket);
            request.SetKey(ref);
            throwIfError(client->DeleteObject(request));
        }
    }
    file.remove();
}

void DiskS3::removeRecursive(const String & path)
{
    checkStackSize();   /// This is needed to prevent stack overflow in case of cyclic symlinks.

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
    String res(32, ' ');   /// The number of bits of entropy should be not less than 128.
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

        auto s3disk = std::make_shared<DiskS3>(name, client, uri.bucket, uri.key, metadata_path);

        /// This code is used only to check access to the corresponding disk.

        {
            auto file = s3disk->writeFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
            file->write("test", 4);
        }
        {
            auto file = s3disk->readFile("test_acl", DBMS_DEFAULT_BUFFER_SIZE);
            String buf(4, '0');
            file->readStrict(buf.data(), 4);
            if (buf != "test")
                throw Exception("No read accecss to S3 bucket in disk " + name, ErrorCodes::PATH_ACCESS_DENIED);
        }
        {
            s3disk->remove("test_acl");
        }

        return s3disk;
    };
    factory.registerDiskType("s3", creator);
}

}

#endif
