#pragma once

#include <IO/BufferBase.h>

namespace DB
{

class SwapHelper
{
public:
    SwapHelper(BufferBase & b1_, BufferBase & b2_);
    ~SwapHelper();

private:
    BufferBase & b1;
    BufferBase & b2;
};

}
