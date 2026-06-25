#include <Disks/IO/WriteBufferFromTemporaryFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <fcntl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

WriteBufferFromTemporaryFile::WriteBufferFromTemporaryFile(TemporaryFileOnDiskHolder && tmp_file_)
    : WriteBufferFromFile(tmp_file_->getAbsolutePath(), DBMS_DEFAULT_BUFFER_SIZE, O_RDWR | O_TRUNC | O_CREAT, /* throttler= */ {}, 0600)
    , tmp_file(std::move(tmp_file_))
{
}

class ReadBufferFromTemporaryWriteBuffer : public ReadBufferFromFile
{
public:
    static std::unique_ptr<ReadBufferFromTemporaryWriteBuffer> createFrom(WriteBufferFromTemporaryFile * origin)
    {
        int fd = origin->getFD();
        std::string file_name = origin->getFileName();

        off_t res = lseek(fd, 0, SEEK_SET);
        if (-1 == res)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, file_name, "Cannot reread temporary file {}", file_name);

        return std::make_unique<ReadBufferFromTemporaryWriteBuffer>(fd, file_name, std::move(origin->tmp_file));
    }

    ReadBufferFromTemporaryWriteBuffer(int fd_, const std::string & file_name_, TemporaryFileOnDiskHolder && tmp_file_)
        : ReadBufferFromFile(fd_, file_name_), tmp_file(std::move(tmp_file_))
    {}

    TemporaryFileOnDiskHolder tmp_file;
};


std::unique_ptr<ReadBuffer> WriteBufferFromTemporaryFile::getReadBufferImpl()
{
    /// ignore buffer, write all data to file and reread it
    finalize();

    auto res = ReadBufferFromTemporaryWriteBuffer::createFrom(this);

    /// invalidate FD to avoid close() in destructor
    setFD(-1);
    file_name = {};

    return res;
}


WriteBufferFromTemporaryFile::~WriteBufferFromTemporaryFile() = default;


}
