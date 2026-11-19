#pragma once

#include <Common/Wyrd/Namespace.h>

#include <cstddef>
#include <source_location>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

class SpinWait
{
public:
    void spin(std::source_location location = std::source_location::current());

private:
    size_t rounds = 0;
};

}
