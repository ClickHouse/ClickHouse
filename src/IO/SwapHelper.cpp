#include <IO/SwapHelper.h>

namespace DB
{

SwapHelper::SwapHelper(BufferBase & b1_, BufferBase & b2_)
    : b1(b1_), b2(b2_)
{
    b1.swap(b2);
}

SwapHelper::~SwapHelper()
{
    b1.swap(b2);
}

}
