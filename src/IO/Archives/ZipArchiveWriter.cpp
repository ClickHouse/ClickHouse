#include <IO/Archives/ZipArchiveWriter.h>

#if USE_MINIZIP
#include <IO/WriteBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <zip.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PACK_ARCHIVE;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
}

using RawHandle = zipFile;


/// Holds a raw handle, calls acquireRawHandle() in the constructor and releaseRawHandle() in the destructor.
class ZipArchiveWriter::HandleHolder
{
public:
    HandleHolder() = default;

    explicit HandleHolder(const std::shared_ptr<ZipArchiveWriter> & writer_) : writer(writer_), raw_handle(writer->acquireRawHandle()) { }

    ~HandleHolder()
    {
        if (raw_handle)
        {
            try
            {
                int err = zipCloseFileInZip(raw_handle);
                /// If err == ZIP_PARAMERROR the file is already closed.
                if (err != ZIP_PARAMERROR)
                    checkResult(err);
            }
            catch (...)
            {
                tryLogCurrentException("ZipArchiveWriter");
            }
            writer->releaseRawHandle(raw_handle);
        }
    }

    HandleHolder(HandleHolder && src) noexcept
    {
        *this = std::move(src);
    }

    HandleHolder & operator=(HandleHolder && src) noexcept
    {
        writer = std::exchange(src.writer, nullptr);
        raw_handle = std::exchange(src.raw_handle, nullptr);
        return *this;
    }

    RawHandle getRawHandle() const { return raw_handle; }
    std::shared_ptr<ZipArchiveWriter> getWriter() const { return writer; }

    void checkResult(int code) const { writer->checkResult(code); }

private:
    std::shared_ptr<ZipArchiveWriter> writer;
    RawHandle raw_handle = nullptr;
};


/// This class represents a WriteBuffer actually returned by writeFile().
class ZipArchiveWriter::WriteBufferFromZipArchive : public WriteBufferFromFileBase
{
public:
    WriteBufferFromZipArchive(HandleHolder && handle_, const String & filename_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , handle(std::move(handle_))
        , filename(filename_)
    {
        auto compress_method = handle.getWriter()->compression_method;
        auto compress_level = handle.getWriter()->compression_level;
        checkCompressionMethodIsEnabled(static_cast<CompressionMethod>(compress_method));

        const char * password_cstr = nullptr;
        const String & password_str = handle.getWriter()->password;
        if (!password_str.empty())
        {
            checkEncryptionIsEnabled();
            password_cstr = password_str.c_str();
        }

        RawHandle raw_handle = handle.getRawHandle();

        checkResult(zipOpenNewFileInZip3_64(
            raw_handle,
            filename_.c_str(),
            /* zipfi= */ nullptr,
            /* extrafield_local= */ nullptr,
            /* size_extrafield_local= */ 0,
            /* extrafield_global= */ nullptr,
            /* size_extrafield_global= */ 0,
            /* comment= */ nullptr,
            compress_method,
            compress_level,
            /* raw= */ false,
            /* windowBits= */ 0,
            /* memLevel= */ 0,
            /* strategy= */ 0,
            password_cstr,
            /* crc_for_crypting= */ 0,
            /* zip64= */ true));
    }

    ~WriteBufferFromZipArchive() override
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException("ZipArchiveWriter");
        }
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }

private:
    void nextImpl() override
    {
        if (!offset())
            return;
        RawHandle raw_handle = handle.getRawHandle();
        checkResult(zipWriteInFileInZip(raw_handle, working_buffer.begin(), offset()));
    }

    void checkResult(int code) const { handle.checkResult(code); }

    HandleHolder handle;
    String filename;
};


namespace
{
    /// Provides a set of functions allowing the minizip library to write its output
    /// to a WriteBuffer instead of an ordinary file in the local filesystem.
    class StreamFromWriteBuffer
    {
    public:
        static RawHandle open(std::unique_ptr<WriteBuffer> archive_write_buffer)
        {
            Opaque opaque{std::move(archive_write_buffer)};

            zlib_filefunc64_def func_def;
            func_def.zopen64_file = &StreamFromWriteBuffer::openFileFunc;
            func_def.zclose_file = &StreamFromWriteBuffer::closeFileFunc;
            func_def.zread_file = &StreamFromWriteBuffer::readFileFunc;
            func_def.zwrite_file = &StreamFromWriteBuffer::writeFileFunc;
            func_def.zseek64_file = &StreamFromWriteBuffer::seekFunc;
            func_def.ztell64_file = &StreamFromWriteBuffer::tellFunc;
            func_def.zerror_file = &StreamFromWriteBuffer::testErrorFunc;
            func_def.opaque = &opaque;

            return zipOpen2_64(
                /* path= */ nullptr,
                /* append= */ false,
                /* globalcomment= */ nullptr,
                &func_def);
        }

    private:
        std::unique_ptr<WriteBuffer> write_buffer;
        UInt64 start_offset = 0;

        struct Opaque
        {
            std::unique_ptr<WriteBuffer> write_buffer;
        };

        static void * openFileFunc(void * opaque, const void *, int)
        {
            Opaque & opq = *reinterpret_cast<Opaque *>(opaque);
            return new StreamFromWriteBuffer(std::move(opq.write_buffer));
        }

        explicit StreamFromWriteBuffer(std::unique_ptr<WriteBuffer> write_buffer_)
            : write_buffer(std::move(write_buffer_)), start_offset(write_buffer->count()) {}

        static int closeFileFunc(void *, void * stream)
        {
            delete reinterpret_cast<StreamFromWriteBuffer *>(stream);
            return ZIP_OK;
        }

        static StreamFromWriteBuffer & get(void * ptr)
        {
            return *reinterpret_cast<StreamFromWriteBuffer *>(ptr);
        }

        static unsigned long writeFileFunc(void *, void * stream, const void * buf, unsigned long size) // NOLINT(google-runtime-int)
        {
            auto & strm = get(stream);
            strm.write_buffer->write(reinterpret_cast<const char *>(buf), size);
            return size;
        }

        static int testErrorFunc(void *, void *)
        {
            return ZIP_OK;
        }

        static ZPOS64_T tellFunc(void *, void * stream)
        {
            auto & strm = get(stream);
            auto pos = strm.write_buffer->count() - strm.start_offset;
            return pos;
        }

        static long seekFunc(void *, void *, ZPOS64_T, int) // NOLINT(google-runtime-int)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamFromWriteBuffer::seek must not be called");
        }

        static unsigned long readFileFunc(void *, void *, void *, unsigned long) // NOLINT(google-runtime-int)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "StreamFromWriteBuffer::readFile must not be called");
        }
    };
}


ZipArchiveWriter::ZipArchiveWriter(const String & path_to_archive_)
    : ZipArchiveWriter(path_to_archive_, nullptr)
{
}

ZipArchiveWriter::ZipArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_)
    : path_to_archive(path_to_archive_)
{
    if (archive_write_buffer_)
        handle = StreamFromWriteBuffer::open(std::move(archive_write_buffer_));
    else
        handle = zipOpen64(path_to_archive.c_str(), /* append= */ false);
    if (!handle)
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't create zip archive {}", quoteString(path_to_archive));
}

ZipArchiveWriter::~ZipArchiveWriter()
{
    if (handle)
    {
        try
        {
            checkResult(zipClose(handle, /* global_comment= */ nullptr));
        }
        catch (...)
        {
            tryLogCurrentException("ZipArchiveWriter");
        }
    }
}

std::unique_ptr<WriteBufferFromFileBase> ZipArchiveWriter::writeFile(const String & filename)
{
    return std::make_unique<WriteBufferFromZipArchive>(acquireHandle(), filename);
}

bool ZipArchiveWriter::isWritingFile() const
{
    std::lock_guard lock{mutex};
    return !handle;
}

void ZipArchiveWriter::setCompression(int compression_method_, int compression_level_)
{
    std::lock_guard lock{mutex};
    compression_method = compression_method_;
    compression_level = compression_level_;
}

void ZipArchiveWriter::setPassword(const String & password_)
{
    std::lock_guard lock{mutex};
    password = password_;
}

ZipArchiveWriter::CompressionMethod ZipArchiveWriter::parseCompressionMethod(const String & str)
{
    if (str.empty())
        return CompressionMethod::kDeflate; /// Default compression method is DEFLATE.
    else if (boost::iequals(str, "store"))
        return CompressionMethod::kStore;
    else if (boost::iequals(str, "deflate"))
        return CompressionMethod::kDeflate;
    else if (boost::iequals(str, "bzip2"))
        return CompressionMethod::kBzip2;
    else if (boost::iequals(str, "lzma"))
        return CompressionMethod::kLzma;
    else if (boost::iequals(str, "zstd"))
        return CompressionMethod::kZstd;
    else if (boost::iequals(str, "xz"))
        return CompressionMethod::kXz;
    else
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a zip archive: {}", str);
}

/// Checks that a passed compression method can be used.
void ZipArchiveWriter::checkCompressionMethodIsEnabled(CompressionMethod method)
{
    switch (method)
    {
        case CompressionMethod::kStore: [[fallthrough]];
        case CompressionMethod::kDeflate:
        case CompressionMethod::kLzma:
        case CompressionMethod::kXz:
        case CompressionMethod::kZstd:
            return;

        case CompressionMethod::kBzip2:
        {
#if USE_BZIP2
            return;
#else
            throw Exception("BZIP2 compression method is disabled", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }
    }
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a zip archive: {}", static_cast<int>(method));
}

/// Checks that encryption is enabled.
void ZipArchiveWriter::checkEncryptionIsEnabled()
{
#if !USE_SSL
    throw Exception("Encryption in zip archive is disabled", ErrorCodes::SUPPORT_IS_DISABLED);
#endif
}

ZipArchiveWriter::HandleHolder ZipArchiveWriter::acquireHandle()
{
    return HandleHolder{std::static_pointer_cast<ZipArchiveWriter>(shared_from_this())};
}

RawHandle ZipArchiveWriter::acquireRawHandle()
{
    std::lock_guard lock{mutex};
    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot have more than one write buffer while writing a zip archive");
    return std::exchange(handle, nullptr);
}

void ZipArchiveWriter::releaseRawHandle(RawHandle raw_handle_)
{
    std::lock_guard lock{mutex};
    handle = raw_handle_;
}

void ZipArchiveWriter::checkResult(int code) const
{
    if (code >= ZIP_OK)
        return;

    String message = "Code= ";
    switch (code)
    {
        case ZIP_ERRNO: message += "ERRNO, errno= " + String{strerror(errno)}; break;
        case ZIP_PARAMERROR: message += "PARAMERROR"; break;
        case ZIP_BADZIPFILE: message += "BADZIPFILE"; break;
        case ZIP_INTERNALERROR: message += "INTERNALERROR"; break;
        default: message += std::to_string(code); break;
    }
    showError(message);
}

void ZipArchiveWriter::showError(const String & message) const
{
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't pack zip archive {}: {}", quoteString(path_to_archive), message);
}

}

#endif
