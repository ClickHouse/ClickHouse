#pragma once
#include <IO/BufferBase.h>

namespace DB
{
    class SwapHelper
    {
    public:
        SwapHelper(BufferBase & b1_, BufferBase & b2_) : b1(b1_), b2(b2_) { b1.swap(b2); }
        ~SwapHelper() { b1.swap(b2); }

    private:
        BufferBase & b1;
        BufferBase & b2;
    };
}
