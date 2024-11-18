#pragma once

#include "config.h"

#if USE_MINIZIP
#include <IO/Archives/IArchiveWriter.h>
#include <base/defines.h>
#include <mutex>


namespace DB
{
class WriteBuffer;
class WriteBufferFromFileBase;

/// Implementation of IArchiveWriter for writing zip archives.
class ZipArchiveWriter : public IArchiveWriter
{
public:
    /// Constructs an archive that will be written as a file in the local filesystem.
    explicit ZipArchiveWriter(const String & path_to_archive_);

    /// Constructs an archive that will be written by using a specified `archive_write_buffer_`.
    ZipArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_);

    /// Call finalize() before destructing IArchiveWriter.
    ~ZipArchiveWriter() override;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename, size_t size) override;


    /// Returns true if there is an active instance of WriteBuffer returned by writeFile().
    /// This function should be used mostly for debugging purposes.
    bool isWritingFile() const override;

    /// Finalizes writing of the archive. This function must be always called at the end of writing.
    /// (Unless an error appeared and the archive is in fact no longer needed.)
    void finalize() override;

    /// Supported compression methods.
    static constexpr const char kStore[] = "store";
    static constexpr const char kDeflate[] = "deflate";
    static constexpr const char kBzip2[] = "bzip2";
    static constexpr const char kLzma[] = "lzma";
    static constexpr const char kZstd[] = "zstd";
    static constexpr const char kXz[] = "xz";

    /// Some compression levels.
    enum class CompressionLevels : int8_t
    {
        kDefault = kDefaultCompressionLevel,
        kFast = 2,
        kNormal = 6,
        kBest = 9,
    };

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    void setCompression(const String & compression_method_, int compression_level_) override;

    /// Sets password. Only contents of the files are encrypted,
    /// names of files are not encrypted.
    /// Changing the password will affect next file in the archive.
    void setPassword(const String & password_) override;

    /// Utility functions.
    static int compressionMethodToInt(const String & compression_method_);
    static String intToCompressionMethod(int compression_method_);
    static void checkCompressionMethodIsEnabled(int compression_method_);
    static void checkEncryptionIsEnabled();

private:
    class StreamInfo;
    using ZipHandle = void *;
    class WriteBufferFromZipArchive;

    int getCompressionMethod() const;
    int getCompressionLevel() const;
    String getPassword() const;

    ZipHandle startWritingFile();
    void endWritingFile();

    void checkResultCode(int code) const;

    const String path_to_archive;
    std::unique_ptr<StreamInfo> TSA_GUARDED_BY(mutex) stream_info;
    int compression_method TSA_GUARDED_BY(mutex); /// By default the compression method is "deflate".
    int compression_level TSA_GUARDED_BY(mutex) = kDefaultCompressionLevel;
    String password TSA_GUARDED_BY(mutex);
    ZipHandle zip_handle TSA_GUARDED_BY(mutex) = nullptr;
    bool is_writing_file TSA_GUARDED_BY(mutex) = false;
    bool finalized TSA_GUARDED_BY(mutex) = false;
    mutable std::mutex mutex;
};

}

#endif
