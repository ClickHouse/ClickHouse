#include "DiskS3.h"

#if USE_AWS_S3
#    include "DiskFactory.h"

#    include <IO/ReadBufferFromS3.h>
#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromS3.h>
#    include <Poco/File.h>
#    include <Poco/FileStream.h>
#    include <Common/quoteString.h>

#    include <random>
#    include <aws/s3/model/CopyObjectRequest.h>
#    include <aws/s3/model/DeleteObjectRequest.h>
#    include <aws/s3/model/GetObjectRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DELETE_DIRECTORY;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int PATH_ACCESS_DENIED;
    extern const int S3_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

namespace
{
    template <typename R, typename E>
    void throwIfError(Aws::Utils::Outcome<R, E> && response)
    {
        if (!response.IsSuccess())
        {
            auto & err = response.GetError();
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
    }

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
            Poco::FileOutputStream(metadata_path) << s3_path;
            finalized = true;
        }

        ~WriteIndirectBufferFromS3() override
        {
            if (!finalized)
            {
                WriteBufferFromS3::finalize();
                Poco::FileOutputStream(metadata_path) << s3_path;
            }
        }

    private:
        bool finalized = false;
        const String metadata_path;
        const String s3_path;
    };
}

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
        remove(to_path + ".old", false);
    }
    else
        from_file.renameTo(to_file.path());
}

void DiskS3::copyFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        remove(to_path, false);

    String s3_from_path;
    String s3_to_path = s3_root_path + getRandomName();
    Poco::FileInputStream(metadata_path + from_path) >> s3_from_path;

    Aws::S3::Model::CopyObjectRequest req;
    req.SetBucket(bucket);
    req.SetCopySource(s3_from_path);
    req.SetKey(s3_to_path);
    throwIfError(client->CopyObject(req));
    Poco::FileOutputStream(metadata_path + to_path) << s3_to_path;
}

std::unique_ptr<ReadBuffer> DiskS3::readFile(const String & path, size_t buf_size) const
{
    return std::make_unique<ReadBufferFromS3>(client, bucket, getS3Path(path), buf_size);
}

std::unique_ptr<WriteBuffer> DiskS3::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
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

void DiskS3::remove(const String & path, bool recursive)
{
    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(getS3Path(path));
        throwIfError(client->DeleteObject(request));

        Poco::File(metadata_path + path).remove(true);
    }
    else
    {
        auto it{iterateDirectory(path)};
        if (!recursive && it->isValid())
            throw Exception("Directory " + path + "is not empty", ErrorCodes::CANNOT_DELETE_DIRECTORY);

        for (; it->isValid(); it->next())
            remove(it->path(), true);
        file.remove(false);
    }
}

String DiskS3::getS3Path(const String & path) const
{
    if (!exists(path))
        throw Exception("File not found: " + path, ErrorCodes::FILE_DOESNT_EXIST);

    String s3_path;
    Poco::FileInputStream(metadata_path + path) >> s3_path;
    return s3_path;
}

String DiskS3::getRandomName() const
{
    std::mt19937 random{std::random_device{}()};
    std::uniform_int_distribution<int> distribution('a', 'z');
    String suffix(16, ' ');
    for (auto & c : suffix)
        c = distribution(random);
    return suffix;
}

bool DiskS3::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(IDisk::reservation_mutex);
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
        std::lock_guard lock(IDisk::reservation_mutex);
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
            String s3_path;
            Poco::FileInputStream(metadata_path + "test_acl") >> s3_path;
            Aws::S3::Model::DeleteObjectRequest request;
            request.SetBucket(uri.bucket);
            request.SetKey(s3_path);
            throwIfError(client->DeleteObject(request));
            Poco::File(metadata_path + "test_acl").remove();
        }

        return s3disk;
    };
    factory.registerDiskType("s3", creator);
}

}

#endif
