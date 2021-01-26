#include <IO/WriteBuffer.h>
#include <IO/IReadableWriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/filesystemHelpers.h>


namespace DB
{

/// Rereadable WriteBuffer, could be used as disk buffer
/// Creates unique temporary in directory (and directory itself)
class WriteBufferFromTemporaryFile : public WriteBufferFromFile, public IReadableWriteBuffer
{
public:
    using Ptr = std::shared_ptr<WriteBufferFromTemporaryFile>;

    static Ptr create(const std::string & tmp_dir);

    ~WriteBufferFromTemporaryFile() override;

protected:

    WriteBufferFromTemporaryFile(std::unique_ptr<TemporaryFile> && tmp_file);

    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

protected:

    std::unique_ptr<TemporaryFile> tmp_file;

    friend class ReadBufferFromTemporaryWriteBuffer;
};

}
