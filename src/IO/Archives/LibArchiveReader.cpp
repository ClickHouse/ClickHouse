#include <IO/Archives/LibArchiveReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/quoteString.h>

#include <IO/Archives/ArchiveUtils.h>


namespace DB
{

#if USE_LIBARCHIVE

namespace ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNSUPPORTED_METHOD;
}


template <typename ArchiveInfo>
class LibArchiveReader<ArchiveInfo>::Handle
{
public:
    explicit Handle(const String & path_to_archive_) : path_to_archive(path_to_archive_)
    {
        current_archive = open(path_to_archive);
        current_entry = archive_entry_new();
    }

    Handle(const Handle &) = delete;
    Handle(Handle && other) noexcept
        : current_archive(other.current_archive)
        , current_entry(other.current_entry)
    {
        other.current_archive = nullptr;
        other.current_entry = nullptr;
    }

    ~Handle()
    {
        if (current_archive)
        {
            archive_read_close(current_archive);
            archive_read_free(current_archive);
        }
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
            err = archive_read_next_header(current_archive, &current_entry);

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
            err = archive_read_next_header(current_archive, &current_entry);
        } while (err == ARCHIVE_RETRY);

        checkError(err);
        return err == ARCHIVE_OK;
    }

    static struct archive * open(const String & path_to_archive)
    {
        auto * archive = archive_read_new();
        archive_read_support_filter_all(archive);
        archive_read_support_format_all(archive);
        if (archive_read_open_filename(archive, path_to_archive.c_str(), 10240) != ARCHIVE_OK)
            throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open {} archive: {}", ArchiveInfo::name, quoteString(path_to_archive));

        return archive;
    }

    std::vector<std::string> getAllFiles(NameFilter filter)
    {
        auto * archive = open(path_to_archive);
        auto * entry = archive_entry_new();

        std::vector<std::string> files;
        int error = archive_read_next_header(archive, &entry);
        while (error == ARCHIVE_OK || error == ARCHIVE_RETRY)
        {
            std::string name = archive_entry_pathname(entry);
            if (!filter || filter(name))
                files.push_back(std::move(name));

            error = archive_read_next_header(archive, &entry);
        }

        archive_read_close(archive);
        archive_read_free(archive);

        checkError(error);
        return files;
    }

    void checkError(int error)
    {
        if (error == ARCHIVE_FATAL)
            throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Failed to read archive while fetching all files: {}", archive_error_string(current_archive));
    }

    void resetFileInfo()
    {
        file_name.reset();
        file_info.reset();
    }

    const String & getFileName() const
    {
        if (!file_name)
            file_name.emplace(archive_entry_pathname(current_entry));

        return *file_name;
    }

    const FileInfo & getFileInfo() const
    {
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
    struct archive_entry * current_entry;
private:
    const String path_to_archive;
    mutable std::optional<String> file_name;
    mutable std::optional<FileInfo> file_info;
};

template <typename ArchiveInfo>
class LibArchiveReader<ArchiveInfo>::FileEnumeratorImpl : public FileEnumerator
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

template <typename ArchiveInfo>
class LibArchiveReader<ArchiveInfo>::ReadBufferFromLibArchive : public ReadBufferFromFileBase
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

template <typename ArchiveInfo>
LibArchiveReader<ArchiveInfo>::LibArchiveReader(const String & path_to_archive_) : path_to_archive(path_to_archive_)
{}

template <typename ArchiveInfo>
LibArchiveReader<ArchiveInfo>::~LibArchiveReader() = default;

template <typename ArchiveInfo>
const std::string & LibArchiveReader<ArchiveInfo>::getPath() const
{
    return path_to_archive;
}

template <typename ArchiveInfo>
bool LibArchiveReader<ArchiveInfo>::fileExists(const String & filename)
{
    Handle handle(path_to_archive);
    return handle.locateFile(filename);
}

template <typename ArchiveInfo>
LibArchiveReader<ArchiveInfo>::FileInfo LibArchiveReader<ArchiveInfo>::getFileInfo(const String & filename)
{
    Handle handle(path_to_archive);
    handle.locateFile(filename);
    return handle.getFileInfo();
}

template <typename ArchiveInfo>
std::unique_ptr<typename LibArchiveReader<ArchiveInfo>::FileEnumerator> LibArchiveReader<ArchiveInfo>::firstFile()
{
    Handle handle(path_to_archive);
    if (!handle.nextFile())
        return nullptr;

    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

template <typename ArchiveInfo>
std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader<ArchiveInfo>::readFile(const String & filename)
{
    return readFile([&](const std::string & file) { return file == filename; });
}

template <typename ArchiveInfo>
std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader<ArchiveInfo>::readFile(NameFilter filter)
{
    Handle handle(path_to_archive);
    handle.locateFile(filter);
    return std::make_unique<ReadBufferFromLibArchive>(std::move(handle), path_to_archive);
}

template <typename ArchiveInfo>
std::unique_ptr<ReadBufferFromFileBase> LibArchiveReader<ArchiveInfo>::readFile(std::unique_ptr<FileEnumerator> enumerator)
{
    if (!dynamic_cast<FileEnumeratorImpl *>(enumerator.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong enumerator passed to readFile()");
    auto enumerator_impl = std::unique_ptr<FileEnumeratorImpl>(static_cast<FileEnumeratorImpl *>(enumerator.release()));
    auto handle = std::move(*enumerator_impl).releaseHandle();
    return std::make_unique<ReadBufferFromLibArchive>(std::move(handle), path_to_archive);
}

template <typename ArchiveInfo> std::unique_ptr<typename LibArchiveReader<ArchiveInfo>::FileEnumerator>
LibArchiveReader<ArchiveInfo>::nextFile(std::unique_ptr<ReadBuffer> read_buffer)
{
    if (!dynamic_cast<ReadBufferFromLibArchive *>(read_buffer.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong ReadBuffer passed to nextFile()");
    auto read_buffer_from_libarchive = std::unique_ptr<ReadBufferFromLibArchive>(static_cast<ReadBufferFromLibArchive *>(read_buffer.release()));
    auto handle = std::move(*read_buffer_from_libarchive).releaseHandle();
    if (!handle.nextFile())
        return nullptr;
    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

template <typename ArchiveInfo>
std::vector<std::string> LibArchiveReader<ArchiveInfo>::getAllFiles()
{
    return getAllFiles({});
}

template <typename ArchiveInfo>
std::vector<std::string> LibArchiveReader<ArchiveInfo>::getAllFiles(NameFilter filter)
{
    Handle handle(path_to_archive);
    return handle.getAllFiles(filter);
}

template <typename ArchiveInfo>
void LibArchiveReader<ArchiveInfo>::setPassword(const String & /*password_*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not set password to {} archive", ArchiveInfo::name);
}

template class LibArchiveReader<TarArchiveInfo>;
template class LibArchiveReader<SevenZipArchiveInfo>;

#endif

}
