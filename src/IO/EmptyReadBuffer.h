#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

/// Just a stub - reads nothing from nowhere.
class EmptyReadBuffer : public ReadBuffer
{
public:
    EmptyReadBuffer() : ReadBuffer(nullptr, 0) {}

private:
    bool nextImpl() override { return false; }
};

}
