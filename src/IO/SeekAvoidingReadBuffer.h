#pragma once

#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

class SeekAvoidingReadBuffer : public ReadBufferFromFileBase
{
    std::unique_ptr<ReadBufferFromFileBase> nested;
    UInt64 min_bytes_for_seek;

public:
    SeekAvoidingReadBuffer(std::unique_ptr<ReadBufferFromFileBase> nested_, UInt64 min_bytes_for_seek_);

    std::string getFileName() const override;

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;
};

}
