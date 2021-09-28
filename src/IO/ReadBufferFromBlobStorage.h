#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AZURE_BLOB_STORAGE

#include <IO/SeekableReadBuffer.h>

namespace DB
{

class ReadBufferFromBlobStorage : public SeekableReadBuffer
{
public:
    explicit ReadBufferFromBlobStorage() :
        SeekableReadBuffer(nullptr, 0)
    {}

    off_t seek(off_t, int) override { return 0; }
    off_t getPosition() override { return 0; }
};

}

#endif
