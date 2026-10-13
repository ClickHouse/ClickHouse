#pragma once

#include <cstddef>
#include <source_location>

namespace DB::Runtime::Wyrd
{

class SpinWait
{
public:
    void spin(std::source_location location = std::source_location::current());

private:
    size_t rounds = 0;
};

}
