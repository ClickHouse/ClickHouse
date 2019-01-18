#include <IO/WriteBufferFromHDFS.h>

#if USE_HDFS
#include <Poco/URI.h>
#include <hdfs/hdfs.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NETWORK_ERROR;
extern const int CANNOT_FSYNC;
}


struct WriteBufferFromHDFS::WriteBufferFromHDFSImpl
{
    struct HDFSBuilderDeleter
    {
        void operator()(hdfsBuilder * builder)
        {
            hdfsFreeBuilder(builder);
        }
    };

    std::string hdfs_uri;
    std::unique_ptr<hdfsBuilder, HDFSBuilderDeleter> builder;
    hdfsFS fs;
    hdfsFile fout;

    WriteBufferFromHDFSImpl(const std::string & hdfs_name_)
        : hdfs_uri(hdfs_name_)
        , builder(hdfsNewBuilder())
    {
        Poco::URI uri(hdfs_name_);
        auto & host = uri.getHost();
        auto port = uri.getPort();
        auto & path = uri.getPath();
        if (host.empty() || port == 0 || path.empty())
        {
            throw Exception("Illegal HDFS URI: " + hdfs_uri, ErrorCodes::BAD_ARGUMENTS);
        }

        // set read/connect timeout, default value in libhdfs3 is about 1 hour, and too large
        /// TODO Allow to tune from query Settings.
        hdfsBuilderConfSetStr(builder.get(), "input.read.timeout", "60000"); // 1 min
        hdfsBuilderConfSetStr(builder.get(), "input.write.timeout", "60000"); // 1 min
        hdfsBuilderConfSetStr(builder.get(), "input.connect.timeout", "60000"); // 1 min

        hdfsBuilderSetNameNode(builder.get(), host.c_str());
        hdfsBuilderSetNameNodePort(builder.get(), port);
        fs = hdfsBuilderConnect(builder.get());

        if (fs == nullptr)
        {
            throw Exception("Unable to connect to HDFS: " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        }

        fout = hdfsOpenFile(fs, path.c_str(), O_WRONLY, 0, 0, 0);
    }

    ~WriteBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs, fout);
        hdfsDisconnect(fs);
    }


    int write(const char * start, size_t size)
    {
        int bytes_written = hdfsWrite(fs, fout, start, size);
        if (bytes_written < 0)
            throw Exception("Fail to write HDFS file: " + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        return bytes_written;
    }

    void sync()
    {
        int result = hdfsSync(fs, fout);
        if (result < 0)
            throwFromErrno("Cannot HDFS sync" + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::CANNOT_FSYNC);
    }
};

WriteBufferFromHDFS::WriteBufferFromHDFS(const std::string & hdfs_name_, size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , impl(std::make_unique<WriteBufferFromHDFSImpl>(hdfs_name_))
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
}

}
#endif
