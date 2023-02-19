#include "config.h"

#if USE_HDFS

#include <Storages/HDFS/WriteBufferFromHDFS.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <IO/ResourceGuard.h>
#include <Common/Throttler.h>
#include <Common/safe_cast.h>
#include <hdfs/hdfs.h>


namespace ProfileEvents
{
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSYNC;
}

struct WriteBufferFromHDFS::WriteBufferFromHDFSImpl
{
    std::string hdfs_uri;
    hdfsFile fout;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;
    WriteSettings write_settings;

    WriteBufferFromHDFSImpl(
            const std::string & hdfs_uri_,
            const Poco::Util::AbstractConfiguration & config_,
            int replication_,
            const WriteSettings & write_settings_,
            int flags)
        : hdfs_uri(hdfs_uri_)
        , builder(createHDFSBuilder(hdfs_uri, config_))
        , fs(createHDFSFS(builder.get()))
        , write_settings(write_settings_)
    {
        const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
        const String path = hdfs_uri.substr(begin_of_path);

        fout = hdfsOpenFile(fs.get(), path.c_str(), flags, 0, replication_, 0);     /// O_WRONLY meaning create or overwrite i.e., implies O_TRUNCAT here

        if (fout == nullptr)
        {
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open HDFS file: {} error: {}",
                path, std::string(hdfsGetLastError()));
        }
    }

    ~WriteBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs.get(), fout);
    }


    int write(const char * start, size_t size)
    {
        ResourceGuard rlock(write_settings.resource_link, size);
        int bytes_written;
        try
        {
            bytes_written = hdfsWrite(fs.get(), fout, start, safe_cast<int>(size));
        }
        catch (...)
        {
            write_settings.resource_link.accumulate(size); // We assume no resource was used in case of failure
            throw;
        }
        rlock.unlock();

        if (bytes_written < 0)
        {
            write_settings.resource_link.accumulate(size); // We assume no resource was used in case of failure
            throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to write HDFS file: {} {}", hdfs_uri, std::string(hdfsGetLastError()));
        }
        write_settings.resource_link.adjust(size, bytes_written);

        if (write_settings.remote_throttler)
            write_settings.remote_throttler->add(bytes_written, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);

        return bytes_written;
    }

    void sync() const
    {
        int result = hdfsSync(fs.get(), fout);
        if (result < 0)
            throwFromErrno("Cannot HDFS sync" + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::CANNOT_FSYNC);
    }
};

WriteBufferFromHDFS::WriteBufferFromHDFS(
        const std::string & hdfs_name_,
        const Poco::Util::AbstractConfiguration & config_,
        int replication_,
        const WriteSettings & write_settings_,
        size_t buf_size_,
        int flags_)
    : BufferWithOwnMemory<WriteBuffer>(buf_size_)
    , impl(std::make_unique<WriteBufferFromHDFSImpl>(hdfs_name_, config_, replication_, write_settings_, flags_))
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


void WriteBufferFromHDFS::finalizeImpl()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


WriteBufferFromHDFS::~WriteBufferFromHDFS()
{
    finalize();
}

}
#endif
