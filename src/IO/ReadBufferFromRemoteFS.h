#pragma once

#include <Disks/IDiskRemote.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

class ReadBufferFromRemoteFS : public ReadBufferFromFileBase
{
friend class ThreadPoolRemoteFSReader;

public:
    explicit ReadBufferFromRemoteFS(const RemoteMetadata & metadata_);

    off_t seek(off_t offset, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return metadata.metadata_file_path; }

    void reset(bool reset_inner_buf = false);

    bool check = false;
protected:
    size_t fetch(size_t offset);

    virtual SeekableReadBufferPtr createReadBuffer(const String & path) const = 0;

    RemoteMetadata metadata;

private:
    bool nextImpl() override;

    SeekableReadBufferPtr initialize();

    bool read();

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t absolute_position = 0;
};

}
