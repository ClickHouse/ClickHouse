#include <IO/Archives/LibArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <IO/Archives/ArchiveUtils.h>

#include <mutex>

namespace DB
{

#if USE_LIBARCHIVE

namespace ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNSUPPORTED_METHOD;
}

class LibArchiveReader::Handle
{
public:
    explicit Handle(std::string path_to_archive_, bool lock_on_reading_)
        : path_to_archive(path_to_archive_), lock_on_reading(lock_on_reading_)
    {
        current_archive = open(path_to_archive);
    }

    Handle(const Handle &) = delete;
    Handle(Handle && other) noexcept
        : current_archive(other.current_archive)
        , current_entry(other.current_entry)
        , lock_on_reading(other.lock_on_reading)
    {
        other.current_archive = nullptr;
        other.current_entry = nullptr;
    }

    ~Handle()
    {
        close(current_archive);
    }

    bool locateFile(const std::string & filename)
    {
        return locateFile([&](const std::string & file) { return file == filename; });
    }

    bool locateFile(NameFilter filter)
    {
        resetFileInfo();
        int err = ARCHIVE_OK;
        while (true)
        {
            err = readNextHeader(current_archive, &current_entry);

            if (err == ARCHIVE_RETRY)
                continue;

            if (err != ARCHIVE_OK)
                break;

            if (filter(archive_entry_pathname(current_entry)))
                return true;
        }

        checkError(err);
        return false;
    }

    bool nextFile()
    {
        resetFileInfo();
        int err = ARCHIVE_OK;
        do
        {
            err = readNextHeader(current_archive, &current_entry);
        } while (err == ARCHIVE_RETRY);

        checkError(err);
        return err == ARCHIVE_OK;
    }

    std::vector<std::string> getAllFiles(NameFilter filter)
    {
        auto * archive = open(path_to_archive);
        SCOPE_EXIT(
            close(archive);
        );

        struct archive_entry * entry = nullptr;

        std::vector<std::string> files;
        int error = readNextHeader(archive, &entry);
        while (error == ARCHIVE_OK || error == ARCHIVE_RETRY)
        {
            chassert(entry != nullptr);
            std::string name = archive_entry_pathname(entry);
            if (!filter || filter(name))
                files.push_back(std::move(name));

            error = readNextHeader(archive, &entry);
        }

        checkError(error);
        return files;
    }

    const String & getFileName() const
    {
        chassert(current_entry);
        if (!file_name)
            file_name.emplace(archive_entry_pathname(current_entry));

        return *file_name;
    }

    const FileInfo & getFileInfo() const
    {
        chassert(current_entry);
        if (!file_info)
        {
            file_info.emplace();
            file_info->uncompressed_size = archive_entry_size(current_entry);
            file_info->compressed_size = archive_entry_size(current_entry);
            file_info->is_encrypted = false;
        }

        return *file_info;
    }

    struct archive * current_archive;
    struct archive_entry * current_entry = nullptr;
private:
    void checkError(int error) const
    {
        if (error == ARCHIVE_FATAL)
            throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Failed to read archive while fetching all files: {}", archive_error_string(current_archive));
    }

    void resetFileInfo()
    {
        file_name.reset();
        file_info.reset();
    }

    static struct archive * open(const String & path_to_archive)
    {
        auto * archive = archive_read_new();
        try
        {
            archive_read_support_filter_all(archive);
            archive_read_support_format_all(archive);
            if (archive_read_open_filename(archive, path_to_archive.c_str(), 10240) != ARCHIVE_OK)
                throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open archive {}: {}", quoteString(path_to_archive), archive_error_string(archive));
        }
        catch (...)
        {
            close(archive);
            throw;
        }

        return archive;
    }

    static void close(struct archive * archive)
    {
        if (archive)
        {
            archive_read_close(archive);
            archive_read_free(archive);
        }
    }

    int readNextHeader(struct archive * archive, struct archive_entry ** entry) const
    {
        std::unique_lock lock(Handle::read_lock, std::defer_lock);
        if (lock_on_reading)
            lock.lock();

        return archive_read_next_header(archive, entry);
    }

    const String path_to_archive;

    /// for some archive types when we are reading headers static variables are used
    /// which are not thread-safe
    const bool lock_on_reading;
    static inline std::mutex read_lock;

    mutable std::optional<String> file_name;
    mutable std::optional<FileInfo> file_info;
};

class LibArchiveReader::FileEnumeratorImpl : public FileEnumerator
{
public:
    explicit FileEnumeratorImpl(Handle handle_) : handle(std::move(handle_)) {}

    const String & getFileName() const override { return handle.getFileName(); }
    const FileInfo & getFileInfo() const override { return handle.getFileInfo(); }
    bool nextFile() override { return handle.nextFile(); }

    /// Releases owned handle to pass it to a read buffer.
    Handle releaseHandle() && { return std::move(handle); }
private:
    Handle handle;
};

class LibArchiveReader::ReadBufferFromLibArchive : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromLibArchive(Handle handle_, std::string path_to_archive_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , handle(std::move(handle_))
        , path_to_archive(std::move(path_to_archive_))
    {}

    off_t seek(off_t /* off */, int /* whence */) override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Seek is not supported when reading from archive");
    }

    off_t getPosition() override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "getPosition not supported when reading from archive");
    }

    String getFileName() const override { return handle.getFileName(); }

    size_t getFileSize() override { return handle.getFileInfo().uncompressed_size; }

    Handle releaseHandle() &&
    {
        return std::move(handle);
    }

private:
    bool nextImpl() override
    {
        auto bytes_read = archive_read_data(handle.current_archive, internal_buffer.begin(), static_cast<int>(internal_buffer.size()));

        if (bytes_read < 0)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to read file {} from {}: {}", handle.getFileName(), path_to_archive, archive_error_string(handle.current_archive));

        if (!bytes_read)
            return false;

        total_bytes_read += bytes;

        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        return true;
    }

    Handle handle;
    const String path_to_archive;
    size_t total_bytes_read = 0;
};

LibArchiveReader::LibArchiveReader(std::string archive_name_, bool lock_on_reading_, std::string path_to_archive_)
    : archive_name(std::move(archive_name_)), lock_on_reading(lock_on_reading_), path_to_archive(std::move(path_to_archive_))
{}

LibArchiveReader::~LibArchiveReader() = default;

const std::string & LibArchiveReader::getPath() const
{
    return path_to_archive;
}

bool LibArchiveReader::fileExists(const String & filename)
{
    Handle handle(path_to_archive, lock_on_reading);
    return handle.locateFile(filename);
}

LibArchiveReader::FileInfo LibArchiveReader::getFileInfo(const String & filename)
{
    Handle handle(path_to_archive, lock_on_reading);
    if (!handle.locateFile(filename))
        throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't unpack archive {}: file not found", path_to_archive);
    return handle.getFileInfo();
}

std::unique_ptr<LibArchiveReader::FileEnumerator> LibArchiveReader::firstFile()
{
    Handle handle(path_to_archive, lock_on_reading);
    if (!handle.nextFile())
        return nullptr;

    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader::readFile(const String & filename, bool throw_on_not_found)
{
    return readFile([&](const std::string & file) { return file == filename; }, throw_on_not_found);
}

std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader::readFile(NameFilter filter, bool throw_on_not_found)
{
    Handle handle(path_to_archive, lock_on_reading);
    if (!handle.locateFile(filter))
    {
        if (throw_on_not_found)
            throw Exception(
                ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't unpack archive {}: no file found satisfying the filter", path_to_archive);
        return nullptr;
    }
    return std::make_unique<ReadBufferFromLibArchive>(std::move(handle), path_to_archive);
}

std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader::readFile(std::unique_ptr<FileEnumerator> enumerator)
{
    if (!dynamic_cast<FileEnumeratorImpl *>(enumerator.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong enumerator passed to readFile()");
    auto enumerator_impl = std::unique_ptr<FileEnumeratorImpl>(static_cast<FileEnumeratorImpl *>(enumerator.release()));
    auto handle = std::move(*enumerator_impl).releaseHandle();
    return std::make_unique<ReadBufferFromLibArchive>(std::move(handle), path_to_archive);
}

std::unique_ptr<LibArchiveReader::FileEnumerator> LibArchiveReader::nextFile(std::unique_ptr<ReadBuffer> read_buffer)
{
    if (!dynamic_cast<ReadBufferFromLibArchive *>(read_buffer.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong ReadBuffer passed to nextFile()");
    auto read_buffer_from_libarchive = std::unique_ptr<ReadBufferFromLibArchive>(static_cast<ReadBufferFromLibArchive *>(read_buffer.release()));
    auto handle = std::move(*read_buffer_from_libarchive).releaseHandle();
    if (!handle.nextFile())
        return nullptr;
    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

std::vector<std::string> LibArchiveReader::getAllFiles()
{
    return getAllFiles({});
}

std::vector<std::string> LibArchiveReader::getAllFiles(NameFilter filter)
{
    Handle handle(path_to_archive, lock_on_reading);
    return handle.getAllFiles(filter);
}

void LibArchiveReader::setPassword(const String & /*password_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not set password to {} archive", archive_name);
}

#endif

}
