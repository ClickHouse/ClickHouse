#pragma once

#include <IO/SeekableReadBuffer.h>

namespace DB
{

/// Just a stub - reads nothing from nowhere.
class EmptyReadBuffer : public SeekableReadBuffer
{
public:
    EmptyReadBuffer() : SeekableReadBuffer(nullptr, 0) {}

    off_t seek(off_t, int) override
    {
        return 0;
    }

    off_t getPosition() override
    {
        return 0;
    }

private:
    bool nextImpl() override { return false; }
};

}
