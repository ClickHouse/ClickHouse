#include "config.h"

#if USE_HDFS

#include <Storages/ObjectStorage/HDFS/WriteBufferFromHDFS.h>
#include <Storages/ObjectStorage/HDFS/HDFSCommon.h>
#include <Storages/ObjectStorage/HDFS/HDFSErrorWrapper.h>
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Stopwatch.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>
#include <Common/ErrnoException.h>
#include <Common/logger_useful.h>
#include <Interpreters/BlobStorageLog.h>
#include <hdfs/hdfs.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSYNC;
}

struct WriteBufferFromHDFS::WriteBufferFromHDFSImpl : public HDFSErrorWrapper
{
    std::string hdfs_uri;
    std::string hdfs_file_path;
    hdfsFile fout;
    HDFSFSPtr fs;
    WriteSettings write_settings;
    bool created_file;

    WriteBufferFromHDFSImpl(
            const std::string & hdfs_uri_,
            const std::string & hdfs_file_path_,
            const Poco::Util::AbstractConfiguration & config_,
            int replication_,
            const WriteSettings & write_settings_,
            int flags)
        : HDFSErrorWrapper(hdfs_uri_, config_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , write_settings(write_settings_)
        , created_file(!(flags & O_APPEND))
    {
        fs = createHDFSFS(builder.get());

        /// O_WRONLY meaning create or overwrite i.e., implies O_TRUNCAT here
        fout = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), flags, 0, static_cast<int16_t>(replication_), 0);

        if (fout == nullptr)
        {
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open HDFS file: {} ({}) error: {}",
                hdfs_file_path, hdfs_uri, std::string(hdfsGetLastError()));
        }
    }

    ~WriteBufferFromHDFSImpl()
    {
        if (fout != nullptr)
            hdfsCloseFile(fs.get(), fout);
    }

    /// `hdfsWrite` and `hdfsFlush` may keep incomplete chunks and packets buffered in the client, and the data becomes
    /// complete only when the file is closed. So the file is closed at finalize, with the error propagated,
    /// and not at destruction, where the error would be lost and the data would not be complete yet for a reader.
    void close()
    {
        if (fout == nullptr)
            return;

        /// Not through `wrapErr`: the handle is freed by `hdfsCloseFile` even if it fails, so it cannot be retried.
        int result = hdfsCloseFile(fs.get(), fout);
        fout = nullptr;
        if (result != 0)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to close HDFS file: {}, hdfs_uri: {}, {}",
                hdfs_file_path, hdfs_uri, std::string(hdfsGetLastError()));
    }

    void removeFileOnCancel() noexcept
    {
        if (fout != nullptr)
        {
            hdfsCloseFile(fs.get(), fout);
            fout = nullptr;
        }

        /// Unlike blob storages, which create the object only at finalize, HDFS creates
        /// the file already at open, so a canceled write has to remove it explicitly.
        /// Appends do not create the file, and removing it would lose the previous content.
        if (!created_file)
            return;

        if (hdfsDelete(fs.get(), hdfs_file_path.c_str(), 0) == -1)
        {
            LOG_WARNING(getLogger("WriteBufferFromHDFS"),
                "Cannot remove the file of a canceled write: {} ({}) error: {}",
                hdfs_file_path, hdfs_uri, std::string(hdfsGetLastError()));
        }
    }

    int write(const char * start, size_t size)
    {
        ResourceGuard rlock(ResourceGuard::Metrics::getIOWrite(), write_settings.io_scheduling.write_resource_link, size);
        int bytes_written = wrapErr<tSize>(hdfsWrite, fs.get(), fout, start, safe_cast<int>(size));
        rlock.unlock(std::max(0, bytes_written));

        if (bytes_written < 0)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to write HDFS file: {}, hdfs_uri: {}, {}", hdfs_file_path, hdfs_uri, std::string(hdfsGetLastError()));

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->throttle(bytes_written);

        return bytes_written;
    }

    void sync() const
    {
        int result = wrapErr<int>(hdfsSync, fs.get(), fout);
        if (result < 0)
            throw ErrnoException(ErrorCodes::CANNOT_FSYNC, "Cannot HDFS sync {}, hdfs_url: {}, {}", hdfs_file_path, hdfs_uri, std::string(hdfsGetLastError()));
    }
};

WriteBufferFromHDFS::WriteBufferFromHDFS(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        int replication_,
        const WriteSettings & write_settings_,
        size_t buf_size_,
        int flags_,
        BlobStorageLogWriterPtr blob_log_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::make_unique<WriteBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, replication_, write_settings_, flags_))
    , hdfs_uri(hdfs_uri_)
    , filename(hdfs_file_path_)
    , blob_log(std::move(blob_log_))
{
}


void WriteBufferFromHDFS::nextImpl()
{
    if (!offset())
        return;

    Stopwatch stopwatch;

    size_t bytes_written = 0;

    while (bytes_written != offset())
        bytes_written += impl->write(working_buffer.begin() + bytes_written, offset() - bytes_written);

    total_bytes_written += bytes_written;
    total_time_microseconds += stopwatch.elapsedMicroseconds();
}


void WriteBufferFromHDFS::sync()
{
    Stopwatch stopwatch;
    impl->sync();
    total_time_microseconds += stopwatch.elapsedMicroseconds();
}

void WriteBufferFromHDFS::finalizeImpl()
{
    WriteBufferFromFileBase::finalizeImpl();

    Stopwatch stopwatch;
    impl->close();
    total_time_microseconds += stopwatch.elapsedMicroseconds();

    if (blob_log)
    {
        blob_log->addEvent(
            BlobStorageLogElement::EventType::Upload,
            /* bucket */ hdfs_uri,
            /* remote_path */ filename,
            /* local_path */ {},
            /* data_size */ total_bytes_written,
            /* elapsed_microseconds */ total_time_microseconds,
            /* error_code */ 0,
            /* error_message */ {});
    }
}

void WriteBufferFromHDFS::cancelImpl() noexcept
{
    WriteBufferFromFileBase::cancelImpl();
    impl->removeFileOnCancel();
}

WriteBufferFromHDFS::~WriteBufferFromHDFS()
{
    try
    {
        if (!canceled)
            finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
#endif
