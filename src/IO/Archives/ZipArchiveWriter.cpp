#include <IO/Archives/ZipArchiveWriter.h>

#if USE_MINIZIP
#include <IO/WriteBufferFromFileBase.h>
#include <Common/quoteString.h>
#include <base/errnoToString.h>
#include <zip.h>
#include <boost/algorithm/string/predicate.hpp>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PACK_ARCHIVE;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    void checkResultCodeImpl(int code, const String & file_name)
    {
        if (code >= ZIP_OK)
            return;

        String message = "Code = ";
        switch (code)
        {
            case ZIP_ERRNO: message += "ERRNO, errno = " + errnoToString(); break;
            case ZIP_PARAMERROR: message += "PARAMERROR"; break;
            case ZIP_BADZIPFILE: message += "BADZIPFILE"; break;
            case ZIP_INTERNALERROR: message += "INTERNALERROR"; break;
            default: message += std::to_string(code); break;
        }
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't pack zip archive: {}, filename={}", message, quoteString(file_name));
    }
}


/// This class represents a WriteBuffer actually returned by writeFile().
class ZipArchiveWriter::WriteBufferFromZipArchive : public WriteBufferFromFileBase
{
public:
    WriteBufferFromZipArchive(std::shared_ptr<ZipArchiveWriter> archive_writer_, const String & filename_)
        : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , filename(filename_)
    {
        zip_handle = archive_writer_->startWritingFile();
        archive_writer = archive_writer_;

        auto compress_method = archive_writer_->getCompressionMethod();
        auto compress_level = archive_writer_->getCompressionLevel();
        checkCompressionMethodIsEnabled(compress_method);

        const char * password_cstr = nullptr;
        String current_password = archive_writer_->getPassword();
        if (!current_password.empty())
        {
            checkEncryptionIsEnabled();
            password_cstr = current_password.c_str();
        }

        int code = zipOpenNewFileInZip3_64(
            zip_handle,
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
            /* zip64= */ true);
        checkResultCode(code);
    }

    ~WriteBufferFromZipArchive() override
    {
        try
        {
            closeFile(/* throw_if_error= */ false);
            endWritingFile();
        }
        catch (...)
        {
            tryLogCurrentException("WriteBufferFromZipArchive");
        }
    }

    void finalizeImpl() override
    {
        next();
        closeFile(/* throw_if_error= */ true);
        endWritingFile();
    }

    void sync() override { next(); }
    std::string getFileName() const override { return filename; }

private:
    void nextImpl() override
    {
        if (!offset())
            return;
        chassert(zip_handle);
        int code = zipWriteInFileInZip(zip_handle, working_buffer.begin(), static_cast<uint32_t>(offset()));
        checkResultCode(code);
    }

    void closeFile(bool throw_if_error)
    {
        if (zip_handle)
        {
            int code = zipCloseFileInZip(zip_handle);
            zip_handle = nullptr;
            if (throw_if_error)
                checkResultCode(code);
        }
    }

    void endWritingFile()
    {
        if (auto archive_writer_ptr = archive_writer.lock())
        {
            archive_writer_ptr->endWritingFile();
            archive_writer.reset();
        }
    }

    void checkResultCode(int code) const { checkResultCodeImpl(code, filename); }

    std::weak_ptr<ZipArchiveWriter> archive_writer;
    const String filename;
    ZipHandle zip_handle;
};


/// Provides a set of functions allowing the minizip library to write its output
/// to a WriteBuffer instead of an ordinary file in the local filesystem.
class ZipArchiveWriter::StreamInfo
{
public:
    explicit StreamInfo(std::unique_ptr<WriteBuffer> write_buffer_)
        : write_buffer(std::move(write_buffer_)), start_offset(write_buffer->count())
    {
    }

    ~StreamInfo() = default;

    ZipHandle makeZipHandle()
    {
        zlib_filefunc64_def func_def;
        func_def.zopen64_file = &StreamInfo::openFileFunc;
        func_def.zclose_file = &StreamInfo::closeFileFunc;
        func_def.zread_file = &StreamInfo::readFileFunc;
        func_def.zwrite_file = &StreamInfo::writeFileFunc;
        func_def.zseek64_file = &StreamInfo::seekFunc;
        func_def.ztell64_file = &StreamInfo::tellFunc;
        func_def.zerror_file = &StreamInfo::testErrorFunc;
        func_def.opaque = this;

        return zipOpen2_64(
            /* path= */ nullptr,
            /* append= */ false,
            /* globalcomment= */ nullptr,
            &func_def);
    }

    WriteBuffer & getWriteBuffer() { return *write_buffer; }

private:
    /// We do nothing in openFileFunc() and in closeFileFunc() because we already have `write_buffer` (file is already opened).
    static void * openFileFunc(void * opaque, const void *, int) { return opaque; }
    static int closeFileFunc(void *, void *) { return ZIP_OK; }

    static unsigned long writeFileFunc(void * opaque, void *, const void * buf, unsigned long size) // NOLINT(google-runtime-int)
    {
        auto * stream_info = reinterpret_cast<StreamInfo *>(opaque);
        stream_info->write_buffer->write(reinterpret_cast<const char *>(buf), size);
        return size;
    }

    static int testErrorFunc(void *, void *) { return ZIP_OK; }

    static ZPOS64_T tellFunc(void * opaque, void *)
    {
        auto * stream_info = reinterpret_cast<StreamInfo *>(opaque);
        auto pos = stream_info->write_buffer->count() - stream_info->start_offset;
        return pos;
    }

    static long seekFunc(void *, void *, ZPOS64_T, int) // NOLINT(google-runtime-int)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StreamInfo::seek() is not implemented");
    }

    static unsigned long readFileFunc(void *, void *, void *, unsigned long) // NOLINT(google-runtime-int)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StreamInfo::readFile() is not implemented");
    }

    std::unique_ptr<WriteBuffer> write_buffer;
    UInt64 start_offset;
};


ZipArchiveWriter::ZipArchiveWriter(const String & path_to_archive_)
    : ZipArchiveWriter(path_to_archive_, nullptr)
{
}

ZipArchiveWriter::ZipArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_)
    : path_to_archive(path_to_archive_), compression_method(MZ_COMPRESS_METHOD_DEFLATE)
{
    if (archive_write_buffer_)
    {
        stream_info = std::make_unique<StreamInfo>(std::move(archive_write_buffer_));
        zip_handle = stream_info->makeZipHandle();
    }
    else
    {
        zip_handle = zipOpen64(path_to_archive.c_str(), /* append= */ false);
    }

    if (!zip_handle)
        throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Couldn't create zip archive {}", quoteString(path_to_archive));
}

ZipArchiveWriter::~ZipArchiveWriter()
{
    if (!finalized)
    {
        /// It is totally OK to destroy instance without finalization when an exception occurs.
        /// However it is suspicious to destroy instance without finalization at the green path.
        if (!std::uncaught_exceptions() && std::current_exception() == nullptr)
        {
            LoggerPtr log = getLogger("ZipArchiveWriter");
            LOG_ERROR(log,
                       "ZipArchiveWriter is not finalized when destructor is called. "
                       "The zip archive might not be written at all or might be truncated. "
                       "Stack trace: {}", StackTrace().toString());
            chassert(false && "ZipArchiveWriter is not finalized in destructor.");
        }
    }

    if (zip_handle)
    {
        try
        {
            zipCloseFileInZip(zip_handle);
            zipClose(zip_handle, /* global_comment= */ nullptr);
        }
        catch (...)
        {
            tryLogCurrentException("ZipArchiveWriter");
        }
    }
}

std::unique_ptr<WriteBufferFromFileBase> ZipArchiveWriter::writeFile(const String & filename)
{
    return std::make_unique<WriteBufferFromZipArchive>(std::static_pointer_cast<ZipArchiveWriter>(shared_from_this()), filename);
}

std::unique_ptr<WriteBufferFromFileBase> ZipArchiveWriter::writeFile(const String & filename, [[maybe_unused]] size_t size)
{
    return ZipArchiveWriter::writeFile(filename);
}

bool ZipArchiveWriter::isWritingFile() const
{
    std::lock_guard lock{mutex};
    return is_writing_file;
}

void ZipArchiveWriter::finalize()
{
    std::lock_guard lock{mutex};
    if (finalized)
        return;

    if (is_writing_file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ZipArchiveWriter::finalize() is called in the middle of writing a file into the zip archive. That's not allowed");

    if (zip_handle)
    {
        int code = zipClose(zip_handle, /* global_comment= */ nullptr);
        zip_handle = nullptr;
        checkResultCode(code);
    }

    if (stream_info)
    {
        stream_info->getWriteBuffer().finalize();
        stream_info.reset();
    }

    finalized = true;
}

void ZipArchiveWriter::setCompression(const String & compression_method_, int compression_level_)
{
    std::lock_guard lock{mutex};
    compression_method = compressionMethodToInt(compression_method_);
    compression_level = compression_level_;
}

int ZipArchiveWriter::getCompressionMethod() const
{
    std::lock_guard lock{mutex};
    return compression_method;
}

int ZipArchiveWriter::getCompressionLevel() const
{
    std::lock_guard lock{mutex};
    return compression_level;
}

void ZipArchiveWriter::setPassword(const String & password_)
{
    std::lock_guard lock{mutex};
    password = password_;
}

String ZipArchiveWriter::getPassword() const
{
    std::lock_guard lock{mutex};
    return password;
}

int ZipArchiveWriter::compressionMethodToInt(const String & compression_method_)
{
    if (compression_method_.empty())
        return MZ_COMPRESS_METHOD_DEFLATE; /// By default the compression method is "deflate".
    if (compression_method_ == kStore)
        return MZ_COMPRESS_METHOD_STORE;
    if (compression_method_ == kDeflate)
        return MZ_COMPRESS_METHOD_DEFLATE;
    if (compression_method_ == kBzip2)
        return MZ_COMPRESS_METHOD_BZIP2;
    if (compression_method_ == kLzma)
        return MZ_COMPRESS_METHOD_LZMA;
    if (compression_method_ == kZstd)
        return MZ_COMPRESS_METHOD_ZSTD;
    if (compression_method_ == kXz)
        return MZ_COMPRESS_METHOD_XZ;
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a zip archive: {}", compression_method_);
}

String ZipArchiveWriter::intToCompressionMethod(int compression_method_)
{
    switch (compression_method_) // NOLINT(bugprone-switch-missing-default-case)
    {
        case MZ_COMPRESS_METHOD_STORE:   return kStore;
        case MZ_COMPRESS_METHOD_DEFLATE: return kDeflate;
        case MZ_COMPRESS_METHOD_BZIP2:   return kBzip2;
        case MZ_COMPRESS_METHOD_LZMA:    return kLzma;
        case MZ_COMPRESS_METHOD_ZSTD:    return kZstd;
        case MZ_COMPRESS_METHOD_XZ:      return kXz;
    }
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a zip archive: {}", compression_method_);
}

/// Checks that a passed compression method can be used.
void ZipArchiveWriter::checkCompressionMethodIsEnabled(int compression_method_)
{
    switch (compression_method_) // NOLINT(bugprone-switch-missing-default-case)
    {
        case MZ_COMPRESS_METHOD_STORE: [[fallthrough]];
        case MZ_COMPRESS_METHOD_DEFLATE:
        case MZ_COMPRESS_METHOD_LZMA:
        case MZ_COMPRESS_METHOD_ZSTD:
        case MZ_COMPRESS_METHOD_XZ:
            return;

        case MZ_COMPRESS_METHOD_BZIP2:
        {
#if USE_BZIP2
            return;
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "bzip2 compression method is disabled");
#endif
        }
    }
    throw Exception(ErrorCodes::CANNOT_PACK_ARCHIVE, "Unknown compression method specified for a zip archive: {}", compression_method_);
}

/// Checks that encryption is enabled.
void ZipArchiveWriter::checkEncryptionIsEnabled()
{
#if !USE_SSL
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Encryption in zip archive is disabled");
#endif
}

ZipArchiveWriter::ZipHandle ZipArchiveWriter::startWritingFile()
{
    std::lock_guard lock{mutex};
    if (is_writing_file)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot write two files to a zip archive in parallel");
    is_writing_file = true;
    return zip_handle;
}

void ZipArchiveWriter::endWritingFile()
{
    std::lock_guard lock{mutex};
    is_writing_file = false;
}

void ZipArchiveWriter::checkResultCode(int code) const
{
    checkResultCodeImpl(code, path_to_archive);
}

}

#endif
