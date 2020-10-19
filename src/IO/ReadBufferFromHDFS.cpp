#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <IO/HDFSCommon.h>
#include <hdfs/hdfs.h>
#include <mutex>


namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
}

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl
{
    /// HDFS create/open functions are not thread safe
    static std::mutex hdfs_init_mutex;

    std::string hdfs_uri;
    hdfsFile fin;
    HDFSBuilderPtr builder;
    HDFSFSPtr fs;

    explicit ReadBufferFromHDFSImpl(const std::string & hdfs_name_)
        : hdfs_uri(hdfs_name_)
    {
        std::lock_guard lock(hdfs_init_mutex);

        builder = createHDFSBuilder(hdfs_uri);
        fs = createHDFSFS(builder.get());
        const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
        const std::string path = hdfs_uri.substr(begin_of_path);
        fin = hdfsOpenFile(fs.get(), path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception("Unable to open HDFS file: " + path + " error: " + std::string(hdfsGetLastError()),
                ErrorCodes::CANNOT_OPEN_FILE);
    }

    int read(char * start, size_t size) const
    {
        int bytes_read = hdfsRead(fs.get(), fin, start, size);
        if (bytes_read < 0)
            throw Exception("Fail to read HDFS file: " + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        return bytes_read;
    }

    ~ReadBufferFromHDFSImpl()
    {
        std::lock_guard lock(hdfs_init_mutex);
        hdfsCloseFile(fs.get(), fin);
    }
};

std::mutex ReadBufferFromHDFS::ReadBufferFromHDFSImpl::hdfs_init_mutex;

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

}

#endif
