#pragma once

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

class ReadBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromRemoteFS(const RemoteMetadata & metadata_);

    off_t seek(off_t offset, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return metadata.metadata_file_path; }

    bool readNext() { return nextImpl(); }

protected:
    virtual SeekableReadBufferPtr createReadBuffer(const String & path) const = 0;
    RemoteMetadata metadata;

private:
    bool nextImpl() override;

    SeekableReadBufferPtr initialize();

    bool readImpl();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;
    size_t absolute_position = 0;
};

using ReadBufferFromRemoteFSImpl = std::shared_ptr<ReadBufferFromRemoteFS>;

}
