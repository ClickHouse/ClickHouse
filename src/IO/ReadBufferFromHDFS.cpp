#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <IO/HDFSCommon.h>
#include <hdfs/hdfs.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TELL_THROUGH_FILE;
}

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl
{
    std::string hdfs_uri;
    hdfsFile fin;
    HDFSBuilderPtr builder;
    HDFSFSPtr fs;
    off_t offset = 0;
    bool initialized = false;

    explicit ReadBufferFromHDFSImpl(const std::string & hdfs_name_)
        : hdfs_uri(hdfs_name_)
        , builder(createHDFSBuilder(hdfs_uri))
        , fs(createHDFSFS(builder.get()))
    {
        const size_t begin_of_path = hdfs_uri.find('/', hdfs_uri.find("//") + 2);
        const std::string path = hdfs_uri.substr(begin_of_path);
        fin = hdfsOpenFile(fs.get(), path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception("Unable to open HDFS file: " + path + " error: " + std::string(hdfsGetLastError()),
                ErrorCodes::CANNOT_OPEN_FILE);

    }

    void initialize()
    {
        if (offset)
        {
            int seek_status = hdfsSeek(fs.get(), fin, offset);
            if (seek_status != 0)
                throw Exception("Fail to seek HDFS file: " + hdfs_uri + " " + std::string(hdfsGetLastError()),
                    ErrorCodes::NETWORK_ERROR);
        }
    }

    int read(char * start, size_t size)
    {
        if (!initialized)
        {
            initialize();
            initialized = true;
        }

        int bytes_read = hdfsRead(fs.get(), fin, start, size);
        if (bytes_read < 0)
            throw Exception("Fail to read HDFS file: " + hdfs_uri + " " + std::string(hdfsGetLastError()),
                ErrorCodes::NETWORK_ERROR);
        return bytes_read;
    }

    int seek(off_t off, int whence)
    {
        if (initialized)
            throw Exception("Seek is allowed only before first read attempt from the buffer. ",
                ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
        
        if (whence != SEEK_SET)
            throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
    
        offset = off;
        return offset;
    }
    
    int tell() const
    {
        return offset;
    }

    ~ReadBufferFromHDFSImpl()
    {
        hdfsCloseFile(fs.get(), fin);
    }
};

ReadBufferFromHDFS::ReadBufferFromHDFS(const std::string & hdfs_name_, size_t buf_size)
    : BufferWithOwnMemory<SeekableReadBuffer>(buf_size)
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

off_t ReadBufferFromHDFS::seek(off_t off, int whence)
{
    return impl->seek(off, whence);
}

off_t ReadBufferFromHDFS::getPosition()
{
    return impl->tell() + count();
}

}

#endif
