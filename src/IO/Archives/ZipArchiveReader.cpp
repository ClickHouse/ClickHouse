#include <IO/Archives/ZipArchiveReader.h>

#if USE_MINIZIP
#include <IO/Archives/ZipArchiveWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <base/errnoToString.h>
#include <unzip.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

using RawHandle = unzFile;


namespace
{
    void checkCompressionMethodIsEnabled(int compression_method_)
    {
        ZipArchiveWriter::checkCompressionMethodIsEnabled(compression_method_);
    }

    void checkEncryptionIsEnabled()
    {
        ZipArchiveWriter::checkEncryptionIsEnabled();
    }
}


/// Holds a raw handle, calls acquireRawHandle() in the constructor and releaseRawHandle() in the destructor.
class ZipArchiveReader::HandleHolder
{
public:
    HandleHolder() = default;

    explicit HandleHolder(const std::shared_ptr<ZipArchiveReader> & reader_) : reader(reader_), raw_handle(reader->acquireRawHandle()) { }

    ~HandleHolder()
    {
        if (raw_handle)
        {
            try
            {
                closeFile();
            }
            catch (...)
            {
                tryLogCurrentException("ZipArchiveReader");
            }
            reader->releaseRawHandle(raw_handle);
        }
    }

    HandleHolder(HandleHolder && src) noexcept
    {
        *this = std::move(src);
    }

    HandleHolder & operator=(HandleHolder && src) noexcept
    {
        reader = std::exchange(src.reader, nullptr);
        raw_handle = std::exchange(src.raw_handle, nullptr);
        file_name = std::exchange(src.file_name, {});
        file_info = std::exchange(src.file_info, {});
        return *this;
    }

    RawHandle getRawHandle() const { return raw_handle; }
    std::shared_ptr<ZipArchiveReader> getReader() const { return reader; }

    bool locateFile(const String & file_name_)
    {
        resetFileInfo();
        bool case_sensitive = true;
        int err = unzLocateFile(raw_handle, file_name_.c_str(), reinterpret_cast<unzFileNameComparer>(static_cast<size_t>(case_sensitive)));
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return false;
        file_name = file_name_;
        return true;
    }

    bool locateFile(NameFilter filter)
    {
        int err = unzGoToFirstFile(raw_handle);
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return false;

        do
        {
            checkResult(err);
            resetFileInfo();
            retrieveFileInfo();
            if (filter(getFileName()))
                return true;

            err = unzGoToNextFile(raw_handle);
        } while (err != UNZ_END_OF_LIST_OF_FILE);

        return false;
    }

    bool tryLocateFile(const String & file_name_)
    {
        resetFileInfo();
        bool case_sensitive = true;
        int err = unzLocateFile(raw_handle, file_name_.c_str(), reinterpret_cast<unzFileNameComparer>(static_cast<size_t>(case_sensitive)));
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return false;
        checkResult(err);
        file_name = file_name_;
        return true;
    }

    bool firstFile()
    {
        resetFileInfo();
        int err = unzGoToFirstFile(raw_handle);
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return false;
        checkResult(err);
        return true;
    }

    bool nextFile()
    {
        resetFileInfo();
        int err = unzGoToNextFile(raw_handle);
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return false;
        checkResult(err);
        return true;
    }

    const String & getFileName() const
    {
        if (!file_name)
            retrieveFileInfo();
        return *file_name;
    }

    const FileInfoImpl & getFileInfo() const
    {
        if (!file_info)
            retrieveFileInfo();
        return *file_info;
    }

    std::vector<std::string> getAllFiles(NameFilter filter)
    {
        std::vector<std::string> files;
        resetFileInfo();
        int err = unzGoToFirstFile(raw_handle);
        if (err == UNZ_END_OF_LIST_OF_FILE)
            return files;

        do
        {
            checkResult(err);
            resetFileInfo();
            retrieveFileInfo();
            if (!filter || filter(getFileName()))
                files.push_back(*file_name);
            err = unzGoToNextFile(raw_handle);
        } while (err != UNZ_END_OF_LIST_OF_FILE);

        return files;
    }

    void closeFile()
    {
        int err = unzCloseCurrentFile(raw_handle);
        /// If err == UNZ_PARAMERROR the file is already closed.
        if (err != UNZ_PARAMERROR)
            checkResult(err);
    }

    void checkResult(int code) const { reader->checkResult(code); }
    [[noreturn]] void showError(const String & message) const { reader->showError(message); }

private:
    void retrieveFileInfo() const
    {
        if (file_name && file_info)
            return;
        unz_file_info64 finfo;
        int err = unzGetCurrentFileInfo64(raw_handle, &finfo, nullptr, 0, nullptr, 0, nullptr, 0);
        if (err == UNZ_PARAMERROR)
            showError("No current file");
        checkResult(err);
        if (!file_info)
        {
            file_info.emplace();
            file_info->uncompressed_size = finfo.uncompressed_size;
            file_info->compressed_size = finfo.compressed_size;
            file_info->compression_method = finfo.compression_method;
            file_info->is_encrypted = (finfo.flag & MZ_ZIP_FLAG_ENCRYPTED);
        }
        if (!file_name)
        {
            file_name.emplace();
            file_name->resize(finfo.size_filename);
            checkResult(unzGetCurrentFileInfo64(raw_handle, nullptr, file_name->data(), finfo.size_filename, nullptr, 0, nullptr, 0));
        }
    }

    void resetFileInfo()
    {
        file_info.reset();
        file_name.reset();
    }

    std::shared_ptr<ZipArchiveReader> reader;
    RawHandle raw_handle = nullptr;
    mutable std::optional<String> file_name;
    mutable std::optional<FileInfoImpl> file_info;
};


/// This class represents a ReadBuffer actually returned by readFile().
class ZipArchiveReader::ReadBufferFromZipArchive : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromZipArchive(HandleHolder && handle_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , handle(std::move(handle_))
    {
        const auto & file_info = handle.getFileInfo();
        checkCompressionMethodIsEnabled(file_info.compression_method);

        const char * password_cstr = nullptr;
        if (file_info.is_encrypted)
        {
            const auto & password_str = handle.getReader()->password;
            if (password_str.empty())
                showError("Password is required");
            password_cstr = password_str.c_str();
            checkEncryptionIsEnabled();
        }

        RawHandle raw_handle = handle.getRawHandle();
        int err = unzOpenCurrentFilePassword(raw_handle, password_cstr);
        if (err == MZ_PASSWORD_ERROR)
            showError("Wrong password");
        checkResult(err);
    }

    off_t seek(off_t off, int whence) override
    {
        off_t current_pos = getPosition();
        off_t new_pos;
        if (whence == SEEK_SET)
            new_pos = off;
        else if (whence == SEEK_CUR)
            new_pos = off + current_pos;
        else
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Only SEEK_SET and SEEK_CUR seek modes allowed.");

        if (new_pos == current_pos)
            return current_pos; /// The position is the same.

        if (new_pos < 0)
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bound");

        off_t working_buffer_start_pos = current_pos - offset();
        off_t working_buffer_end_pos = current_pos + available();

        if ((working_buffer_start_pos <= new_pos) && (new_pos <= working_buffer_end_pos))
        {
            /// The new position is still inside the buffer.
            position() += new_pos - current_pos;
            return new_pos;
        }

        RawHandle raw_handle = handle.getRawHandle();

        /// Check that the new position is now beyond the end of the file.
        const auto & file_info = handle.getFileInfo();
        if (new_pos > static_cast<off_t>(file_info.uncompressed_size))
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bound");

        /// unzSeek64() works only for non-compressed files.
        ///
        /// We used to have a fallback here, where we would:
        ///  * ignore() to "seek" forward,
        ///  * unzCloseCurrentFile(raw_handle) + unzOpenCurrentFile(raw_handle) to seek to the
        ///    beginning of the file.
        /// But the close+open didn't work: after closing+reopening once, the second
        /// unzCloseCurrentFile() was failing with MZ_CRC_ERROR in mz_zip_entry_read_close(). Maybe
        /// it's a bug in minizip where some state was inadvertently left over after close+reopen.
        /// Didn't investigate because re-reading the whole file should be avoided anyway.
        if (file_info.compression_method != MZ_COMPRESS_METHOD_STORE)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Seek in compressed archive is not supported.");

        checkResult(unzSeek64(raw_handle, off, whence));
        return unzTell64(raw_handle);
    }

    bool checkIfActuallySeekable() override
    {
        /// The library doesn't support seeking in compressed files.
        return handle.getFileInfo().compression_method == MZ_COMPRESS_METHOD_STORE;
    }

    off_t getPosition() override
    {
        RawHandle raw_handle = handle.getRawHandle();
        return unzTell64(raw_handle) - available();
    }

    String getFileName() const override { return handle.getFileName(); }

    std::optional<size_t> tryGetFileSize() override { return handle.getFileInfo().uncompressed_size; }

    /// Releases owned handle to pass it to an enumerator.
    HandleHolder releaseHandle() &&
    {
        handle.closeFile();
        return std::move(handle);
    }

private:
    bool nextImpl() override
    {
        RawHandle raw_handle = handle.getRawHandle();
        auto bytes_read = unzReadCurrentFile(raw_handle, internal_buffer.begin(), static_cast<int>(internal_buffer.size()));

        if (bytes_read < 0)
            checkResult(bytes_read);

        if (!bytes_read)
            return false;

        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        return true;
    }

    void checkResult(int code) const { handle.checkResult(code); }
    [[noreturn]] void showError(const String & message) const { handle.showError(message); }

    HandleHolder handle;
};


class ZipArchiveReader::FileEnumeratorImpl : public FileEnumerator
{
public:
    explicit FileEnumeratorImpl(HandleHolder && handle_) : handle(std::move(handle_)) {}

    const String & getFileName() const override { return handle.getFileName(); }
    const FileInfo & getFileInfo() const override { return handle.getFileInfo(); }
    bool nextFile() override { return handle.nextFile(); }

    /// Releases owned handle to pass it to a read buffer.
    HandleHolder releaseHandle() && { return std::move(handle); }

private:
    HandleHolder handle;
};


namespace
{
    /// Provides a set of functions allowing the minizip library to read its input
    /// from a SeekableReadBuffer instead of an ordinary file in the local filesystem.
    class StreamFromReadBuffer
    {
    public:
        static RawHandle open(std::unique_ptr<SeekableReadBuffer> archive_read_buffer, UInt64 archive_size)
        {
            StreamFromReadBuffer::Opaque opaque{std::move(archive_read_buffer), archive_size};

            zlib_filefunc64_def func_def;
            func_def.zopen64_file = &StreamFromReadBuffer::openFileFunc;
            func_def.zclose_file = &StreamFromReadBuffer::closeFileFunc;
            func_def.zread_file = &StreamFromReadBuffer::readFileFunc;
            func_def.zwrite_file = &StreamFromReadBuffer::writeFileFunc;
            func_def.zseek64_file = &StreamFromReadBuffer::seekFunc;
            func_def.ztell64_file = &StreamFromReadBuffer::tellFunc;
            func_def.zerror_file = &StreamFromReadBuffer::testErrorFunc;
            func_def.opaque = &opaque;

            return unzOpen2_64(/* path= */ nullptr,
                               &func_def);
        }

    private:
        std::unique_ptr<SeekableReadBuffer> read_buffer;
        UInt64 start_offset = 0;
        UInt64 total_size = 0;
        bool at_end = false;

        struct Opaque
        {
            std::unique_ptr<SeekableReadBuffer> read_buffer;
            UInt64 total_size = 0;
        };

        static void * openFileFunc(void * opaque, const void *, int)
        {
            auto & opq = *reinterpret_cast<Opaque *>(opaque);
            return new StreamFromReadBuffer(std::move(opq.read_buffer), opq.total_size);
        }

        StreamFromReadBuffer(std::unique_ptr<SeekableReadBuffer> read_buffer_, UInt64 total_size_)
            : read_buffer(std::move(read_buffer_)), start_offset(read_buffer->getPosition()), total_size(total_size_) {}

        static int closeFileFunc(void *, void * stream)
        {
            delete reinterpret_cast<StreamFromReadBuffer *>(stream);
            return ZIP_OK;
        }

        static StreamFromReadBuffer & get(void * ptr)
        {
            return *reinterpret_cast<StreamFromReadBuffer *>(ptr);
        }

        static int testErrorFunc(void *, void *)
        {
            return ZIP_OK;
        }

        static unsigned long readFileFunc(void *, void * stream, void * buf, unsigned long size) // NOLINT(google-runtime-int)
        {
            auto & strm = get(stream);
            if (strm.at_end)
                return 0;
            auto read_bytes = strm.read_buffer->read(reinterpret_cast<char *>(buf), size);
            return read_bytes;
        }

        static ZPOS64_T tellFunc(void *, void * stream)
        {
            auto & strm = get(stream);
            if (strm.at_end)
                return strm.total_size;
            auto pos = strm.read_buffer->getPosition() - strm.start_offset;
            return pos;
        }

        static long seekFunc(void *, void * stream, ZPOS64_T offset, int origin) // NOLINT(google-runtime-int)
        {
            auto & strm = get(stream);
            if (origin == SEEK_END)
            {
                /// Our implementations of SeekableReadBuffer don't support SEEK_END,
                /// but the minizip library needs it, so we have to simulate it here.
                strm.at_end = true;
                return ZIP_OK;
            }
            strm.at_end = false;
            if (origin == SEEK_SET)
                offset += strm.start_offset;
            strm.read_buffer->seek(offset, origin);
            return ZIP_OK;
        }

        static unsigned long writeFileFunc(void *, void *, const void *, unsigned long) // NOLINT(google-runtime-int)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamFromReadBuffer::writeFile must not be called");
        }
    };
}


ZipArchiveReader::ZipArchiveReader(const String & path_to_archive_)
    : path_to_archive(path_to_archive_)
{
    init();

}

ZipArchiveReader::ZipArchiveReader(
    const String & path_to_archive_, const ReadArchiveFunction & archive_read_function_, UInt64 archive_size_)
    : path_to_archive(path_to_archive_), archive_read_function(archive_read_function_), archive_size(archive_size_)
{
    init();
}

void ZipArchiveReader::init()
{
    /// Prepare the first handle in `free_handles` and check that the archive can be read.
    releaseRawHandle(acquireRawHandle());
}

ZipArchiveReader::~ZipArchiveReader()
{
    /// Close all `free_handles`.
    for (RawHandle free_handle : free_handles)
    {
        try
        {
            checkResult(unzClose(free_handle));
        }
        catch (...)
        {
            tryLogCurrentException("ZipArchiveReader");
        }
    }
}

const std::string & ZipArchiveReader::getPath() const
{
    return path_to_archive;
}

bool ZipArchiveReader::fileExists(const String & filename)
{
    return acquireHandle().tryLocateFile(filename);
}

ZipArchiveReader::FileInfo ZipArchiveReader::getFileInfo(const String & filename)
{
    auto handle = acquireHandle();
    if (!handle.locateFile(filename))
        showError(fmt::format("File {} was not found in archive", quoteString(filename)));

    return handle.getFileInfo();
}

std::unique_ptr<ZipArchiveReader::FileEnumerator> ZipArchiveReader::firstFile()
{
    auto handle = acquireHandle();
    if (!handle.firstFile())
        return nullptr;
    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

std::unique_ptr<ReadBufferFromFileBase> ZipArchiveReader::readFile(const String & filename, bool throw_on_not_found)
{
    auto handle = acquireHandle();
    if (!handle.locateFile(filename))
    {
        if (throw_on_not_found)
            showError(fmt::format("File {} was not found in archive", quoteString(filename)));

        return nullptr;
    }

    return std::make_unique<ReadBufferFromZipArchive>(std::move(handle));
}

std::unique_ptr<ReadBufferFromFileBase> ZipArchiveReader::readFile(NameFilter filter, bool throw_on_not_found)
{
    auto handle = acquireHandle();
    if (!handle.locateFile(filter))
    {
        if (throw_on_not_found)
            showError(fmt::format("No file satisfying filter in archive"));

        return nullptr;
    }

    return std::make_unique<ReadBufferFromZipArchive>(std::move(handle));
}

std::unique_ptr<ReadBufferFromFileBase> ZipArchiveReader::readFile(std::unique_ptr<FileEnumerator> enumerator)
{
    if (!dynamic_cast<FileEnumeratorImpl *>(enumerator.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong enumerator passed to readFile()");
    auto enumerator_impl = std::unique_ptr<FileEnumeratorImpl>(static_cast<FileEnumeratorImpl *>(enumerator.release()));
    auto handle = std::move(*enumerator_impl).releaseHandle();
    return std::make_unique<ReadBufferFromZipArchive>(std::move(handle));
}

std::unique_ptr<ZipArchiveReader::FileEnumerator> ZipArchiveReader::nextFile(std::unique_ptr<ReadBuffer> read_buffer)
{
    if (!dynamic_cast<ReadBufferFromZipArchive *>(read_buffer.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong ReadBuffer passed to nextFile()");
    auto read_buffer_from_zip = std::unique_ptr<ReadBufferFromZipArchive>(static_cast<ReadBufferFromZipArchive *>(read_buffer.release()));
    auto handle = std::move(*read_buffer_from_zip).releaseHandle();
    if (!handle.nextFile())
        return nullptr;
    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

std::unique_ptr<ZipArchiveReader::FileEnumerator> ZipArchiveReader::currentFile(std::unique_ptr<ReadBuffer> read_buffer)
{
    if (!dynamic_cast<ReadBufferFromZipArchive *>(read_buffer.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong ReadBuffer passed to nextFile()");
    auto read_buffer_from_zip = std::unique_ptr<ReadBufferFromZipArchive>(static_cast<ReadBufferFromZipArchive *>(read_buffer.release()));
    auto handle = std::move(*read_buffer_from_zip).releaseHandle();
    return std::make_unique<FileEnumeratorImpl>(std::move(handle));
}

std::vector<std::string> ZipArchiveReader::getAllFiles()
{
    return getAllFiles({});
}

std::vector<std::string> ZipArchiveReader::getAllFiles(NameFilter filter)
{
    auto handle = acquireHandle();
    return handle.getAllFiles(filter);
}

void ZipArchiveReader::setPassword(const String & password_)
{
    std::lock_guard lock{mutex};
    password = password_;
}

ZipArchiveReader::HandleHolder ZipArchiveReader::acquireHandle()
{
    return HandleHolder{std::static_pointer_cast<ZipArchiveReader>(shared_from_this())};
}

ZipArchiveReader::RawHandle ZipArchiveReader::acquireRawHandle()
{
    std::lock_guard lock{mutex};

    if (!free_handles.empty())
    {
        RawHandle free_handle = free_handles.back();
        free_handles.pop_back();
        return free_handle;
    }

    RawHandle new_handle = nullptr;
    if (archive_read_function)
        new_handle = StreamFromReadBuffer::open(archive_read_function(), archive_size);
    else
        new_handle = unzOpen64(path_to_archive.c_str());

    if (!new_handle)
        throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open zip archive {}", quoteString(path_to_archive));

    return new_handle;
}

void ZipArchiveReader::releaseRawHandle(RawHandle handle_)
{
    if (!handle_)
        return;

    std::lock_guard lock{mutex};
    free_handles.push_back(handle_);
}

void ZipArchiveReader::checkResult(int code) const
{
    if (code >= UNZ_OK)
        return;

    String message = "Code = ";
    switch (code)
    {
        case UNZ_OK: return;
        case UNZ_ERRNO: message += "ERRNO, errno = " + errnoToString(); break;
        case UNZ_PARAMERROR: message += "PARAMERROR"; break;
        case UNZ_BADZIPFILE: message += "BADZIPFILE"; break;
        case UNZ_INTERNALERROR: message += "INTERNALERROR"; break;
        case UNZ_CRCERROR: message += "CRCERROR"; break;
        case UNZ_BADPASSWORD: message += "BADPASSWORD"; break;
        default: message += std::to_string(code); break;
    }
    showError(message);
}

void ZipArchiveReader::showError(const String & message) const
{
    throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't unpack zip archive {}: {}", quoteString(path_to_archive), message);
}

}

#endif
