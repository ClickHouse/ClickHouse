#pragma once

#include <Common/config.h>

#if USE_MINIZIP
#include <IO/Archives/IArchiveWriter.h>
#include <base/shared_ptr_helper.h>
#include <mutex>


namespace DB
{
class WriteBuffer;
class WriteBufferFromFileBase;

/// Implementation of IArchiveWriter for writing zip archives.
class ZipArchiveWriter : public shared_ptr_helper<ZipArchiveWriter>, public IArchiveWriter
{
public:
    /// Destructors finalizes writing the archive.
    ~ZipArchiveWriter() override;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) override;

    /// Returns true if there is an active instance of WriteBuffer returned by writeFile().
    /// This function should be used mostly for debugging purposes.
    bool isWritingFile() const override;

    /// Supported compression methods.
    enum class CompressionMethod
    {
        /// See mz.h
        kStore = 0,
        kDeflate = 8,
        kBzip2 = 12,
        kLzma = 14,
        kZstd = 93,
        kXz = 95,
    };

    /// Some compression levels.
    enum class CompressionLevels
    {
        kDefault = kDefaultCompressionLevel,
        kFast = 2,
        kNormal = 6,
        kBest = 9,
    };

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    void setCompression(int compression_method_, int compression_level_) override;

    /// Sets password. Only contents of the files are encrypted,
    /// names of files are not encrypted.
    /// Changing the password will affect next file in the archive.
    void setPassword(const String & password_) override;

    /// Utility functions.
    static CompressionMethod parseCompressionMethod(const String & str);
    static void checkCompressionMethodIsEnabled(CompressionMethod method);
    static void checkEncryptionIsEnabled();

private:
    /// Constructs an archive that will be written as a file in the local filesystem.
    explicit ZipArchiveWriter(const String & path_to_archive_);

    /// Constructs an archive that will be written by using a specified `archive_write_buffer_`.
    ZipArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_);

    friend struct shared_ptr_helper<ZipArchiveWriter>;
    class WriteBufferFromZipArchive;
    class HandleHolder;
    using RawHandle = void *;

    HandleHolder acquireHandle();
    RawHandle acquireRawHandle();
    void releaseRawHandle(RawHandle raw_handle_);

    void checkResult(int code) const;
    [[noreturn]] void showError(const String & message) const;

    const String path_to_archive;
    int compression_method = static_cast<int>(CompressionMethod::kDeflate);
    int compression_level = kDefaultCompressionLevel;
    String password;
    RawHandle handle = nullptr;
    mutable std::mutex mutex;
};

}

#endif
