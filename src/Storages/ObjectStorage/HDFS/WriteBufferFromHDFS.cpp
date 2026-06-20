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
        hdfsCloseFile(fs.get(), fout);
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
