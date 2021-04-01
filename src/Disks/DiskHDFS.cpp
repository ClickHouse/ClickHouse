#include "DiskHDFS.h"

#include <Storages/HDFS/ReadBufferFromFile.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/HDFS/WriteBufferFromHDFS.h>

#include "DiskFactory.h"
#include <random>
#include <utility>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Interpreters/Context.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/quoteString.h>
#include <Common/filesystemHelpers.h>

#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int UNKNOWN_FORMAT;
    extern const int CANNOT_REMOVE_FILE;
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

    /*
    template <typename Result, typename Error>
    void throwIfError(Aws::Utils::Outcome<Result, Error> && response)
    {
        if (!response.IsSuccess())
        {
            const auto & err = response.GetError();
            throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
        }
    }
    */
    
    struct Metadata
    {
        /// Metadata file version.
        static constexpr UInt32 VERSION = 1;

        using PathAndSize = std::pair<String, size_t>;

        /// Disk path.
        const String & disk_path;
        /// Relative path to metadata file on local FS.
        String metadata_file_path;
        /// Total size of all HDFS objects.
        size_t total_size;
        /// HDFS objects paths and their sizes.
        std::vector<PathAndSize> hdfs_objects;
        /// Number of references (hardlinks) to this metadata file.
        UInt32 ref_count;

        /// Load metadata by path or create empty if `create` flag is set.
        explicit Metadata(const String & disk_path_, const String & metadata_file_path_, bool create = false)
            : disk_path(disk_path_), metadata_file_path(metadata_file_path_), total_size(0), hdfs_objects(0), ref_count(0)
        {
            if (create)
                return;

            ReadBufferFromFile buf(disk_path + metadata_file_path, 1024); /* reasonable buffer size for small file */
            
            UInt32 version;
            readIntText(version, buf);

            if (version != VERSION)
                throw Exception(
                    "Unknown metadata file version. Path: " + disk_path + metadata_file_path
                        + " Version: " + std::to_string(version) + ", Expected version: " + std::to_string(VERSION),
                    ErrorCodes::UNKNOWN_FORMAT);

            assertChar('\n', buf);

            UInt32 hdfs_objects_count;
            readIntText(hdfs_objects_count, buf);
            assertChar('\t', buf);
            readIntText(total_size, buf);
            assertChar('\n', buf);
            hdfs_objects.resize(hdfs_objects_count);
            for (UInt32 i = 0; i < hdfs_objects_count; ++i)
            {
                String hdfs_object_path;
                size_t hdfs_object_size;
                readIntText(hdfs_object_size, buf);
                assertChar('\t', buf);
                readEscapedString(hdfs_object_path, buf);
                assertChar('\n', buf);
                hdfs_objects[i] = {hdfs_object_path, hdfs_object_size};
            }

            readIntText(ref_count, buf);
            assertChar('\n', buf);
        }

        void addObject(const String & path, size_t size)
        {
            total_size += size;
            hdfs_objects.emplace_back(path, size);
        }

        /// Fsync metadata file if 'sync' flag is set.
        void save(bool sync = false)
        {
            WriteBufferFromFile buf(disk_path + metadata_file_path, 1024);

            writeIntText(VERSION, buf);
            writeChar('\n', buf);

            writeIntText(hdfs_objects.size(), buf);
            writeChar('\t', buf);
            writeIntText(total_size, buf);
            writeChar('\n', buf);
            for (const auto & [hdfs_object_path, hdfs_object_size] : hdfs_objects)
            {
                writeIntText(hdfs_object_size, buf);
                writeChar('\t', buf);
                writeEscapedString(hdfs_object_path, buf);
                writeChar('\n', buf);
            }

            writeIntText(ref_count, buf);
            writeChar('\n', buf);

            buf.finalize();
            if (sync)
                buf.sync();
        }
    };

    /// Reads data from HDFS using stored paths in metadata.
    class ReadIndirectBufferFromHDFS final : public ReadBufferFromFileBase
    {
    public:
        ReadIndirectBufferFromHDFS(
            const String& hdfs_name_, const String & bucket_, Metadata metadata_, size_t buf_size_)
            : hdfs_name(hdfs_name_), bucket(bucket_), metadata(std::move(metadata_)), buf_size(buf_size_)
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
        std::unique_ptr<ReadBufferFromHDFS> initialize()
        {
            size_t offset = absolute_position;
            
            for (size_t i = 0; i < metadata.hdfs_objects.size(); ++i)
            {
                current_buf_idx = i;
                const auto & [path, size] = metadata.hdfs_objects[i];
                
                if (size > offset)
                {
                    auto buf = std::make_unique<ReadBufferFromHDFS>(hdfs_name + path, buf_size);
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
            if (current_buf_idx + 1 >= metadata.hdfs_objects.size())
                return false;

            ++current_buf_idx;
            const auto & path = metadata.hdfs_objects[current_buf_idx].first;
            current_buf = std::make_unique<ReadBufferFromHDFS>(hdfs_name + path, buf_size);
            current_buf->next();
            working_buffer = current_buf->buffer();
            absolute_position += working_buffer.size();

            return true;
        }

        const String & hdfs_name;
        const String & bucket;
        Metadata metadata;
        size_t buf_size;

        size_t absolute_position = 0;
        size_t current_buf_idx = 0;
        std::unique_ptr<ReadBufferFromHDFS> current_buf;
    };

    /// Stores data in HDFS and adds the object key (HDFS path) and object size to metadata file on local FS.
    class WriteIndirectBufferFromHDFS final : public WriteBufferFromFileBase
    {
    public:
        WriteIndirectBufferFromHDFS(
            const String & hdfs_name_,
            const String & hdfs_path_,
            Metadata metadata_,
            size_t buf_size_)
            : WriteBufferFromFileBase(buf_size_, nullptr, 0)
            , impl(WriteBufferFromHDFS(hdfs_name_, buf_size_))
            , metadata(std::move(metadata_))
            , hdfs_path(hdfs_path_)
        {
        }

        ~WriteIndirectBufferFromHDFS() override
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

            metadata.addObject(hdfs_path, count());
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
            /// Transfer current working buffer to WriteBufferFromHDFS.
            impl.swap(*this);

            /// Write actual data to HDFS.
            impl.next();

            /// Return back working buffer.
            impl.swap(*this);
        }

        WriteBufferFromHDFS impl;
        bool finalized = false;
        Metadata metadata;
        String hdfs_path;
    };
}


class DiskHDFSDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    DiskHDFSDirectoryIterator(const String & full_path, const String & folder_path_) : iter(full_path), folder_path(folder_path_) {}

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


using DiskHDFSPtr = std::shared_ptr<DiskHDFS>;

class DiskHDFSReservation final : public IReservation
{
public:
    DiskHDFSReservation(const DiskHDFSPtr & disk_, UInt64 size_)
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

    ~DiskHDFSReservation() override;

private:
    DiskHDFSPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


DiskHDFS::DiskHDFS(
    String name_,
    String hdfs_name_,
    String metadata_path_)
    : name(std::move(name_))
    , hdfs_name(std::move(hdfs_name_))
    , metadata_path(std::move(metadata_path_))
    , builder(createHDFSBuilder(hdfs_name))
    , fs(createHDFSFS(builder.get()))
{
}

ReservationPtr DiskHDFS::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskHDFSReservation>(std::static_pointer_cast<DiskHDFS>(shared_from_this()), bytes);
}

bool DiskHDFS::exists(const String & path) const
{
    return Poco::File(metadata_path + path).exists();
}

bool DiskHDFS::isFile(const String & path) const
{
    return Poco::File(metadata_path + path).isFile();
}

bool DiskHDFS::isDirectory(const String & path) const
{
    return Poco::File(metadata_path + path).isDirectory();
}

size_t DiskHDFS::getFileSize(const String & path) const
{
    Metadata metadata(metadata_path, path);
    return metadata.total_size;
}

void DiskHDFS::createDirectory(const String & path)
{
    Poco::File(metadata_path + path).createDirectory();
}

void DiskHDFS::createDirectories(const String & path)
{
    Poco::File(metadata_path + path).createDirectories();
}

DiskDirectoryIteratorPtr DiskHDFS::iterateDirectory(const String & path)
{
    return std::make_unique<DiskHDFSDirectoryIterator>(metadata_path + path, path);
}

void DiskHDFS::clearDirectory(const String & path)
{
    for (auto it{iterateDirectory(path)}; it->isValid(); it->next())
        if (isFile(it->path()))
            remove(it->path());
}

void DiskHDFS::moveFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);
    Poco::File(metadata_path + from_path).renameTo(metadata_path + to_path);
}

void DiskHDFS::replaceFile(const String & from_path, const String & to_path)
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

void DiskHDFS::copyFile(const String & from_path, const String & to_path)
{
    if (exists(to_path))
        remove(to_path);

    Metadata from(metadata_path, from_path);
    Metadata to(metadata_path, to_path, true);

    for (const auto & [path, size] : from.hdfs_objects)
    {
        auto new_path = hdfs_name + getRandomName();
        /// TODO:: hdfs copy semantics
        /*
        Aws::HDFS::Model::CopyObjectRequest req;
        req.SetCopySource(bucket + "/" + path);
        req.SetBucket(bucket);
        req.SetKey(new_path);
        throwIfError(client->CopyObject(req));
        */
        throw Exception("is not implemented yet", 1);
        to.addObject(new_path, size);
    }

    to.save();
}

std::unique_ptr<ReadBufferFromFileBase> DiskHDFS::readFile(const String & path, size_t buf_size, size_t, size_t, size_t) const
{
    Metadata metadata(metadata_path, path);

    LOG_DEBUG(
        &Logger::get("DiskHDFS"),
        "Read from file by path: " << backQuote(metadata_path + path) << " Existing HDFS objects: " << metadata.hdfs_objects.size());
    
    return std::make_unique<ReadIndirectBufferFromHDFS>(hdfs_name, "", metadata, buf_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskHDFS::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t, size_t)
{
    bool exist = exists(path);
    /// Path to store new HDFS object.
    auto file_name = getRandomName();
    auto HDFS_path = hdfs_name + file_name;
    if (!exist || mode == WriteMode::Rewrite)
    {
        /// If metadata file exists - remove and new.
        if (exist)
            remove(path);
        Metadata metadata(metadata_path, path, true);
        /// Save empty metadata to disk to have ability to get file size while buffer is not finalized.
        metadata.save();

        LOG_DEBUG(&Logger::get("DiskHDFS"), "Write to file by path: " << backQuote(metadata_path + path) << " New HDFS path: " << HDFS_path);

        return std::make_unique<WriteIndirectBufferFromHDFS>(HDFS_path, file_name, metadata, buf_size);
    }
    else
    {
        Metadata metadata(metadata_path, path);

        LOG_DEBUG(
            &Logger::get("DiskHDFS"),
            "Append to file by path: " << backQuote(metadata_path + path) << " New HDFS path: " << HDFS_path
                                       << " Existing HDFS objects: " << metadata.hdfs_objects.size());

        return std::make_unique<WriteIndirectBufferFromHDFS>(HDFS_path, file_name, metadata, buf_size);
    }
}

void DiskHDFS::remove(const String & path)
{
    LOG_DEBUG(&Logger::get("DiskHDFS"), "Remove file by path: " << backQuote(metadata_path + path));

    Poco::File file(metadata_path + path);
    if (file.isFile())
    {
        Metadata metadata(metadata_path, path);

        /// If there is no references - delete content from HDFS.
        if (metadata.ref_count == 0)
        {
            file.remove();
            for (const auto & [hdfs_object_path, _] : metadata.hdfs_objects)
            {
                const size_t begin_of_path = hdfs_name.find('/', hdfs_name.find("//") + 2);
                const std::string hdfs_path = hdfs_name.substr(begin_of_path) + hdfs_object_path;
                int res = hdfsDelete(fs.get(), hdfs_path.c_str(), 0);
                if (res == -1)
                    throw Exception("fuck " + hdfs_path, 1);
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

void DiskHDFS::removeRecursive(const String & path)
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


bool DiskHDFS::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(&Logger::get("DiskHDFS"), "Reserving 0 bytes on HDFS disk " << backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(
            &Logger::get("DiskHDFS"),
            "Reserving " << formatReadableSizeWithBinarySuffix(bytes) << " on disk " << backQuote(name) << ", having unreserved "
                         << formatReadableSizeWithBinarySuffix(unreserved_space) << ".");
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

void DiskHDFS::listFiles(const String & path, std::vector<String> & file_names)
{
    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}

void DiskHDFS::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    Poco::File(metadata_path + path).setLastModified(timestamp);
}

Poco::Timestamp DiskHDFS::getLastModified(const String & path)
{
    return Poco::File(metadata_path + path).getLastModified();
}

void DiskHDFS::createHardLink(const String & src_path, const String & dst_path)
{
    /// Increment number of references.
    Metadata src(metadata_path, src_path);
    ++src.ref_count;
    src.save();

    /// Create FS hardlink to metadata file.
    DB::createHardLink(metadata_path + src_path, metadata_path + dst_path);
}

void DiskHDFS::createFile(const String & path)
{
    /// Create empty metadata file.
    Metadata metadata(metadata_path, path, true);
    metadata.save();
}

void DiskHDFS::setReadOnly(const String & path)
{
    Poco::File(metadata_path + path).setReadOnly(true);
}

DiskHDFSReservation::~DiskHDFSReservation()
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

void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      const Context & context) -> DiskPtr {
                const auto * disk_config = config.createView(config_prefix);

        Poco::File disk{context.getPath() + "disks/" + name};
        disk.createDirectories();

        DB::String uri{disk_config->getString("endpoint")};
        if (uri.back() != '/')
            throw Exception("HDFS path must ends with '/', but '" + uri + "' doesn't.", ErrorCodes::BAD_ARGUMENTS);
        
        String metadata_path = context.getPath() + "disks/" + name + "/";
        
        return std::make_shared<DiskHDFS>(
            name,
            uri,
            metadata_path
        );
    };
    factory.registerDiskType("hdfs", creator);
}

}
