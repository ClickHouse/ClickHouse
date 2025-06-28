#include "config.h"

#if USE_HDFS

#include "WriteBufferFromHDFS.h"
#include "HDFSCommon.h"
#include "HDFSErrorWrapper.h"
#include <Common/Scheduler/ResourceGuard.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>
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
        fout = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), flags, 0, replication_, 0);

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
            write_settings.remote_throttler->add(bytes_written);

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
        int flags_)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::make_unique<WriteBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, replication_, write_settings_, flags_))
    , filename(hdfs_file_path_)
{
}


void WriteBufferFromHDFS::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;

    while (bytes_written != offset())
        bytes_written += impl->write(working_buffer.begin() + bytes_written, offset() - bytes_written);
}


void WriteBufferFromHDFS::sync()
{
    impl->sync();
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
