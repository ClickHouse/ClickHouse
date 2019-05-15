#include <Common/config.h>

#if USE_HDFS

#include <IO/ReadBufferFromHDFS.h> // Y_IGNORE
#include <IO/HDFSCommon.h>
#include <Poco/URI.h>
#include <hdfs/hdfs.h> // Y_IGNORE


namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl
{
    Poco::URI hdfs_uri;
    hdfsFile fin;
    HDFSBuilderPtr builder;
    HDFSFSPtr fs;

    ReadBufferFromHDFSImpl(const std::string & hdfs_name_)
        : hdfs_uri(hdfs_name_)
        , builder(createHDFSBuilder(hdfs_uri))
        , fs(createHDFSFS(builder.get()))
    {

        auto & path = hdfs_uri.getPath();
        fin = hdfsOpenFile(fs.get(), path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception("Unable to open HDFS file: " + path + " error: " + std::string(hdfsGetLastError()),
                ErrorCodes::CANNOT_OPEN_FILE);
    }

    int read(char * start, size_t size)
    {
        int bytes_read = hdfsRead(fs.get(), fin, start, size);
        if (bytes_read < 0)
            throw Exception("Fail to read HDFS file: " + hdfs_uri.toString() + " " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        return bytes_read;
    }

    ~ReadBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs.get(), fin);
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
