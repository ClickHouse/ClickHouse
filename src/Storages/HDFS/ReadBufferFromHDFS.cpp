#include "ReadBufferFromHDFS.h"

#if USE_HDFS
#include <Storages/HDFS/HDFSCommon.h>
#include <hdfs/hdfs.h>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

struct ReadBufferFromHDFS::ReadBufferFromHDFSImpl : public BufferWithOwnMemory<SeekableReadBuffer>
{
    /// HDFS create/open functions are not thread safe
    static std::mutex hdfs_init_mutex;

    String hdfs_uri;
    String hdfs_file_path;

    hdfsFile fin;
    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;

    off_t offset = 0;
    bool initialized = false;

    explicit ReadBufferFromHDFSImpl(
        const std::string & hdfs_uri_,
        const std::string & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_)
        : BufferWithOwnMemory<SeekableReadBuffer>(buf_size_)
        , hdfs_uri(hdfs_uri_)
        , hdfs_file_path(hdfs_file_path_)
        , builder(createHDFSBuilder(hdfs_uri_, config_))
    {
        std::lock_guard lock(hdfs_init_mutex);

        fs = createHDFSFS(builder.get());
        fin = hdfsOpenFile(fs.get(), hdfs_file_path.c_str(), O_RDONLY, 0, 0, 0);

        if (fin == nullptr)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
                "Unable to open HDFS file: {}. Error: {}",
                hdfs_uri + hdfs_file_path, std::string(hdfsGetLastError()));
    }

    ~ReadBufferFromHDFSImpl() override
    {
        std::lock_guard lock(hdfs_init_mutex);
        hdfsCloseFile(fs.get(), fin);
    }

    void initialize() const
    {
        if (!offset)
            return;

        int seek_status = hdfsSeek(fs.get(), fin, offset);
        if (seek_status != 0)
            throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}, error: {}", hdfs_uri, std::string(hdfsGetLastError()));
    }

    bool nextImpl() override
    {
        if (!initialized)
        {
            initialize();
            initialized = true;
        }

        int bytes_read = hdfsRead(fs.get(), fin, internal_buffer.begin(), internal_buffer.size());
        if (bytes_read < 0)
            throw Exception(ErrorCodes::NETWORK_ERROR,
                "Fail to read from HDFS: {}, file path: {}. Error: {}",
                hdfs_uri, hdfs_file_path, std::string(hdfsGetLastError()));

        working_buffer.resize(bytes_read);
        offset += bytes_read;

        return bytes_read;
    }

    off_t seek(off_t offset_, int whence) override
    {
        if (initialized)
            throw Exception("Seek is allowed only before first read attempt from the buffer.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        if (whence != SEEK_SET)
            throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

        if (offset_ < 0)
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", std::to_string(offset_));

        offset = offset_;

        return offset;
    }

    off_t getPosition() override
    {
        return offset;
    }
};


std::mutex ReadBufferFromHDFS::ReadBufferFromHDFSImpl::hdfs_init_mutex;

ReadBufferFromHDFS::ReadBufferFromHDFS(
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t buf_size_)
    : SeekableReadBuffer(nullptr, 0)
    , impl(std::make_unique<ReadBufferFromHDFSImpl>(hdfs_uri_, hdfs_file_path_, config_, buf_size_))
{
}


bool ReadBufferFromHDFS::nextImpl()
{
    auto result = impl->next();

    if (result)
    {
        working_buffer = internal_buffer = impl->buffer();
        pos = working_buffer.begin();
    }
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
    return impl->getPosition() - available();
}

}

#endif
