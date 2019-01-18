#include <IO/ReadBufferFromHDFS.h>

#if USE_HDFS
#include <Poco/URI.h>
#include <hdfs/hdfs.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NETWORK_ERROR;
}

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl
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
    hdfsFile fin;

    ReadBufferFromHDFSImpl(const std::string & hdfs_name_)
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

        fin = hdfsOpenFile(fs, path.c_str(), O_RDONLY, 0, 0, 0);
    }

    ~ReadBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs, fin);
        hdfsDisconnect(fs);
    }

    int read(char * start, size_t size)
    {
        int bytes_read = hdfsRead(fs, fin, start, size);
        if (bytes_read < 0)
            throw Exception("Fail to read HDFS file: " + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        return bytes_read;
    }
};

ReadBufferFromHDFS::ReadBufferFromHDFS(const std::string & hdfs_name_, size_t buf_size)
    : BufferWithOwnMemory<ReadBuffer>(buf_size)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(hdfs_name_))
{
}


bool ReadBufferFromHDFS::nextImpl()
{
    int bytes_read = impl->read(internal_buffer.begin(), internal_buffer.size());

    if (bytes_read)
        working_buffer.resize(bytes_read);
    else
        return false;
    return true;
}

ReadBufferFromHDFS::~ReadBufferFromHDFS()
{
}

}

#endif
