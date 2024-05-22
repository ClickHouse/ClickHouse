#pragma once

#include "config.h"

#if USE_LIBARCHIVE
#    include <IO/Archives/ArchiveUtils.h>
#    include <IO/Archives/IArchiveWriter.h>
#    include <IO/WriteBufferFromFileBase.h>
#    include <base/defines.h>


namespace DB
{
class WriteBufferFromFileBase;

/// Interface for writing an archive.
class LibArchiveWriter : public IArchiveWriter
{
public:
    /// Constructs an archive that will be written as a file in the local filesystem.
    explicit LibArchiveWriter(const String & path_to_archive_, std::unique_ptr<WriteBuffer> archive_write_buffer_);

    /// Call finalize() before destructing IArchiveWriter.
    ~LibArchiveWriter() override;

    /// Starts writing a file to the archive. The function returns a write buffer,
    /// any data written to that buffer will be compressed and then put to the archive.
    /// You can keep only one such buffer at a time, a buffer returned by previous call
    /// of the function `writeFile()` should be destroyed before next call of `writeFile()`.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename) override;
    /// LibArchive needs to know the size of the file being written. If the file size is not
    /// passed in the the archive writer tries to infer the size by looking at the available
    /// data in the buffer, if next is called before all data is written to the buffer
    /// an exception is thrown.
    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & filename, size_t size) override;

    /// Returns true if there is an active instance of WriteBuffer returned by writeFile().
    /// This function should be used mostly for debugging purposes.
    bool isWritingFile() const override;

    /// Finalizes writing of the archive. This function must be always called at the end of writing.
    /// (Unless an error appeared and the archive is in fact no longer needed.)
    void finalize() override;

    /// Sets compression method and level.
    /// Changing them will affect next file in the archive.
    //void setCompression(const String & compression_method_, int compression_level_) override;

    /// Sets password. If the password is not empty it will enable encryption in the archive.
    void setPassword(const String & password) override;

protected:
    using Archive = struct archive *;
    using Entry = struct archive_entry *;

    /// derived classes must call createArchive. CreateArchive calls setFormatAndSettings.
    void createArchive();
    virtual void setFormatAndSettings() = 0;

    Archive archive = nullptr;
    String path_to_archive;

private:
    class WriteBufferFromLibArchive;
    class StreamInfo;

    Archive getArchive();
    void startWritingFile();
    void endWritingFile();

    std::unique_ptr<StreamInfo> stream_info TSA_GUARDED_BY(mutex);
    bool is_writing_file TSA_GUARDED_BY(mutex) = false;
    bool finalized TSA_GUARDED_BY(mutex) = false;
    mutable std::mutex mutex;
};
}
#endif
