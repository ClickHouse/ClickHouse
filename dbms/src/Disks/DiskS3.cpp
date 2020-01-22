#include "DiskS3.h"

#if USE_AWS_S3
#    include "DiskFactory.h"

#    include <random>
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

    String readKeyFromFile(const String & path)
    {
        String key;
        ReadBufferFromFile buf(path, 1024); /* reasonable buffer size for small file */
        readStringUntilEOF(key, buf);
        return key;
    }

    void writeKeyToFile(const String & key, const String & path)
    {
        WriteBufferFromFile buf(path, 1024);
        writeString(key, buf);
        buf.next();
    }

    /// Stores data in S3 and the object key in file in local filesystem.
    class WriteIndirectBufferFromS3 : public WriteBufferFromS3
    {
    public:
        WriteIndirectBufferFromS3(
            std::shared_ptr<Aws::S3::S3Client> & client_ptr_,
            const String & bucket_,
            const String & metadata_path_,
            const String & s3_path_,
            size_t buf_size_)
            : WriteBufferFromS3(client_ptr_, bucket_, s3_path_, DEFAULT_BLOCK_SIZE, buf_size_)
            , metadata_path(metadata_path_)
            , s3_path(s3_path_)
        {
        }

        void finalize() override
        {
            WriteBufferFromS3::finalize();
            writeKeyToFile(s3_path, metadata_path);
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
        const String metadata_path;
        const String s3_path;
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
    // TODO: Consider storing actual file size in meta file.
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(getS3Path(path));
    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess())
    {
        auto & err = outcome.GetError();
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
    else
    {
        return outcome.GetResult().GetContentLength();
    }
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
        throw Exception("File already exists " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);
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

    String s3_from_path = readKeyFromFile(metadata_path + from_path);
    String s3_to_path = s3_root_path + getRandomName();

    Aws::S3::Model::CopyObjectRequest req;
    req.SetBucket(bucket);
    req.SetCopySource(s3_from_path);
    req.SetKey(s3_to_path);
    throwIfError(client->CopyObject(req));
    writeKeyToFile(s3_to_path, metadata_path + to_path);
}

std::unique_ptr<SeekableReadBuffer> DiskS3::readFile(const String & path, size_t buf_size) const
{
    return std::make_unique<ReadBufferFromS3>(client, bucket, getS3Path(path), buf_size);
}

std::unique_ptr<WriteBuffer> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    // TODO: Optimize append mode. Consider storing several S3 references in one meta file.
    if (!exists(path) || mode == WriteMode::Rewrite)
    {
        String new_s3_path = s3_root_path + getRandomName();
        return std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata_path + path, new_s3_path, buf_size);
    }
    else
    {
        auto old_s3_path = getS3Path(path);
        ReadBufferFromS3 read_buffer(client, bucket, old_s3_path, buf_size);
        auto writeBuffer = std::make_unique<WriteIndirectBufferFromS3>(client, bucket, metadata_path + path, old_s3_path, buf_size);
        std::vector<char> buffer(buf_size);
        while (!read_buffer.eof())
            writeBuffer->write(buffer.data(), read_buffer.read(buffer.data(), buf_size));
        return writeBuffer;
    }
}

void DiskS3::remove(const String & path)
{
    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(getS3Path(path));
        throwIfError(client->DeleteObject(request));
    }
    file.remove();
}

void DiskS3::removeRecursive(const String & path)
{
    checkStackSize();   /// This is needed to prevent stack overflow in case of cyclic symlinks.

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(getS3Path(path));
        throwIfError(client->DeleteObject(request));
    }
    else
    {
        for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
            removeRecursive(it->path());
    }
    file.remove();
}

String DiskS3::getS3Path(const String & path) const
{
    if (!exists(path))
        throw Exception("File not found: " + path, ErrorCodes::FILE_DOESNT_EXIST);

    return readKeyFromFile(metadata_path + path);
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
