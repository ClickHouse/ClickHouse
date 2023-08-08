#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

/// Base class for fast counting the number of rows in data in some format
class IRowCounter
{
public:
    explicit IRowCounter(ReadBuffer & in_) : in(in_) {}
    virtual size_t count() = 0;
    virtual ~IRowCounter() = default;

protected:
    ReadBuffer & in;
};

}
