#pragma once

#include <base/types.h>

namespace DB
{

class Histogram
{


private:
    Float64 lower;
    Float64 upper;
    size_t count;
    size_t distinct;
};

}
