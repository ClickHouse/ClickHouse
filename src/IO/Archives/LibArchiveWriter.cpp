#include <IO/Archives/LibArchiveWriter.h>

#include <filesystem>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>

#include <mutex>

#if USE_LIBARCHIVE

// this implemation follows the ZipArchiveWriter implemation as closely as possible.

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_PACK_ARCHIVE;
extern const int NOT_IMPLEMENTED;
extern const int LIMIT_EXCEEDED;
}

namespace
{
void checkResultCodeImpl(int code, const String & filename)
{
    if (code == ARCHIVE_OK)
        return;
    throw Exception(
        ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't pack archive: LibArchive Code = {}, filename={}", code, quoteString(filename));
}
}

/// This is a thin wrapper for libarchive to be able to write the archive to a WriteBuffer.
///
/// C++ exceptions must not propagate through the libarchive C library (undefined behavior).
/// The callback catches exceptions and stores them for later re-throwing in C++ code.
class LibArchiveWriter::StreamInfo
{
public:
    explicit StreamInfo(std::unique_ptr<WriteBuffer> archive_write_buffer_) : archive_write_buffer(std::move(archive_write_buffer_)) { }

    static ssize_t memory_write(struct archive *, void * client_data, const void * buff, size_t length)
    {
        auto * stream_info = reinterpret_cast<StreamInfo *>(client_data);
        try
        {
            stream_info->archive_write_buffer->write(reinterpret_cast<const char *>(buff), length);
            return length;
        }
        catch (...)
        {
            if (!stream_info->stored_exception)
                stream_info->stored_exception = std::current_exception();
            return -1;
        }
    }

    void rethrowIfNeeded()
    {
        if (stored_exception)
        {
            auto ex = stored_exception;
            stored_exception = nullptr;
            std::rethrow_exception(ex);
        }
    }

    std::unique_ptr<WriteBuffer> archive_write_buffer;
    std::exception_ptr stored_exception;
};

class LibArchiveWriter::WriteBufferFromLibArchive : public WriteBufferFromFileBase
{
public:
    WriteBufferFromLibArchive(
        std::shared_ptr<LibArchiveWriter> archive_writer_,
        const String & filename_,
        const size_t & size_,
        const size_t buf_size_,
        bool use_adaptive_buffer_size_,
        size_t adaptive_buffer_max_size_)
        : WriteBufferFromFileBase(buf_size_, nullptr, 0)
        , use_adaptive_buffer_size(use_adaptive_buffer_size_)
        , adaptive_max_buffer_size(adaptive_buffer_max_size_)
        , archive_writer(archive_writer_)
        , filename(filename_)
        , size(size_)
    {
        startWritingFile();
        archive = archive_writer_->getArchive();
        entry = nullptr;
    }

    ~WriteBufferFromLibArchive() override
    {
        try
        {
            closeFile(/* throw_if_error= */ false);
            endWritingFile();
        }
        catch (...)
        {
            tryLogCurrentException("WriteBufferFromTarArchive");
        }
    }

    void finalizeImpl() override
    {
        if (use_adaptive_buffer_size)
        {
            if (offset())
                writeDataChunk();
        }
        else
            next();
        closeFile(/* throw_if_error=*/true);
        endWritingFile();
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }

private:
    void nextImpl() override
    {
        if (use_adaptive_buffer_size)
        {
            if (!available())
            {
                if (memory.size() == adaptive_max_buffer_size)
                    throw Exception(
                        ErrorCodes::LIMIT_EXCEEDED,
                        "Adaptive buffer size limit of {} bytes is exceeded for the file '{}'. "
                        "Consider using ZIP format or increase archive_adaptive_buffer_max_size_bytes",
                        adaptive_max_buffer_size,
                        filename);

                /// Prevents overwriting the beginning of the chunk.
                nextimpl_working_buffer_offset = offset();
                resize(std::min(memory.size() * 2, adaptive_max_buffer_size));
            }
            return;
        }

        if (offset())
            writeDataChunk();
    }

    void writeEntry()
    {
        expected_size = getSize();
        entry = archive_entry_new();
        archive_entry_set_pathname(entry, filename.c_str());
        archive_entry_set_size(entry, expected_size);
        archive_entry_set_filetype(entry, static_cast<__LA_MODE_T>(0100000));
        archive_entry_set_perm(entry, 0644);
        int code = archive_write_header(archive, entry);
        if (auto writer = archive_writer.lock())
            writer->rethrowStoredException();
        checkResult(code);
    }

    void writeDataChunk()
    {
        if (entry == nullptr)
            writeEntry();
        ssize_t to_write = offset();
        ssize_t written = archive_write_data(archive, working_buffer.begin(), offset());
        if (auto writer = archive_writer.lock())
            writer->rethrowStoredException();
        if (written != to_write)
        {
            throw Exception(
                ErrorCodes::CANNOT_PACK_ARCHIVE,
                "Couldn't pack tar archive: Failed to write all bytes, {} of {}, filename={}",
                written,
                to_write,
                quoteString(filename));
        }
    }

    size_t getSize() const
    {
        if (size)
            return size;
        return offset();
    }

    void closeFile(bool throw_if_error)
    {
        if (entry)
        {
            archive_entry_free(entry);
            entry = nullptr;
        }
        /// Bytes counter is incorrect for adaptive buffer because of adjusted nextimpl_working_buffer_offset.
        if (throw_if_error and (!use_adaptive_buffer_size and bytes != expected_size))
        {
            throw Exception(
                ErrorCodes::CANNOT_PACK_ARCHIVE,
                "Couldn't pack tar archive: Wrote {} of expected {} , filename={}",
                bytes,
                expected_size,
                quoteString(filename));
        }
    }

    void endWritingFile()
    {
        if (auto archive_writer_ptr = archive_writer.lock())
            archive_writer_ptr->endWritingFile();
    }

    void startWritingFile()
    {
        if (auto archive_writer_ptr = archive_writer.lock())
            archive_writer_ptr->startWritingFile();
    }

    void checkResult(int code) { checkResultCodeImpl(code, filename); }

    const bool use_adaptive_buffer_size;
    const size_t adaptive_max_buffer_size;

    std::weak_ptr<LibArchiveWriter> archive_writer;
    const String filename;
    Entry entry = nullptr;
    Archive archive = nullptr;
    size_t size = 0;
    size_t expected_size = 0;
};

LibArchiveWriter::LibArchiveWriter(
    const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_, size_t buf_size_, size_t adaptive_buffer_max_size_)
    : path_to_archive(path_to_archive_)
    , buf_size(buf_size_)
    , adaptive_buffer_max_size(std::max(adaptive_buffer_max_size_, buf_size_))
{
    if (archive_write_buffer_)
        stream_info = std::make_unique<StreamInfo>(std::move(archive_write_buffer_));
}

void LibArchiveWriter::createArchive()
{
    std::lock_guard lock{mutex};
    archive = archive_write_new();
    setFormatAndSettings();
    if (stream_info)
    {
        //This allows use to write directly to a writebuffer rather than an intermediate buffer in libarchive.
        //This has to be set otherwise zstd breaks due to extra bytes being written at the end of the archive.
        archive_write_set_bytes_per_block(archive, 0);
        archive_write_open2(archive, stream_info.get(), nullptr, &StreamInfo::memory_write, nullptr, nullptr);
    }
    else
        archive_write_open_filename(archive, path_to_archive.c_str());
}

LibArchiveWriter::~LibArchiveWriter()
{
    chassert((finalized || std::uncaught_exceptions() || std::current_exception()) && "LibArchiveWriter is not finalized in destructor.");
    if (archive)
        archive_write_free(archive);
}

std::unique_ptr<WriteBufferFromFileBase> LibArchiveWriter::writeFile(const String & filename, size_t size)
{
    return std::make_unique<WriteBufferFromLibArchive>(
        std::static_pointer_cast<LibArchiveWriter>(shared_from_this()),
        filename,
        size,
        buf_size,
        /*use_adaptive_buffer_size*/ false,
        adaptive_buffer_max_size);
}

std::unique_ptr<WriteBufferFromFileBase> LibArchiveWriter::writeFile(const String & filename)
{
    /// Size is not known in advance. If it exceeds the buffer, archive_entry_set_size() cannot be used
    /// so an adaptive buffer is used instead of writing in chunks.
    return std::make_unique<WriteBufferFromLibArchive>(
        std::static_pointer_cast<LibArchiveWriter>(shared_from_this()),
        filename,
        /*size*/ 0,
        buf_size,
        /*use_adaptive_buffer_size*/ true,
        adaptive_buffer_max_size);
}

bool LibArchiveWriter::isWritingFile() const
{
    std::lock_guard lock{mutex};
    return is_writing_file;
}

void LibArchiveWriter::endWritingFile()
{
    std::lock_guard lock{mutex};
    is_writing_file = false;
}

void LibArchiveWriter::startWritingFile()
{
    std::lock_guard lock{mutex};
    if (std::exchange(is_writing_file, true))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write two files to a tar archive in parallel");
}

void LibArchiveWriter::finalize()
{
    std::lock_guard lock{mutex};
    if (finalized)
        return;
    if (archive)
        archive_write_close(archive);
    rethrowStoredExceptionLocked();
    if (stream_info)
    {
        stream_info->archive_write_buffer->finalize();
        stream_info.reset();
    }
    finalized = true;
}

void LibArchiveWriter::cancel() noexcept
{
    std::lock_guard lock{mutex};
    if (finalized)
        return;
    if (archive)
        archive_write_close(archive);
    if (stream_info)
    {
        stream_info->archive_write_buffer->cancel();
        stream_info.reset();
    }
    finalized = true;
}

void LibArchiveWriter::setPassword(const String & password_)
{
    if (password_.empty())
        return;
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Setting a password is not currently supported for libarchive");
}

LibArchiveWriter::Archive LibArchiveWriter::getArchive()
{
    std::lock_guard lock{mutex};
    return archive;
}

void LibArchiveWriter::rethrowStoredException()
{
    std::lock_guard lock{mutex};
    rethrowStoredExceptionLocked();
}

void LibArchiveWriter::rethrowStoredExceptionLocked()
{
    if (stream_info)
        stream_info->rethrowIfNeeded();
}
}
#endif
