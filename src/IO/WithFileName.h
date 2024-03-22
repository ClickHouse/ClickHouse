#pragma once
#include <base/types.h>

namespace DB
{

class ReadBuffer;

class WithFileName
{
public:
    virtual String getFileName() const = 0;
    virtual ~WithFileName() = default;
};

String getFileNameFromReadBuffer(const ReadBuffer & in);

}
