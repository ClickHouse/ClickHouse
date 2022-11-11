#pragma once

#if defined(OS_LINUX)

#include <cstddef>
#include <cstdint>


namespace DB
{

struct EventFD
{
    EventFD();
    ~EventFD();

    uint64_t read() const;
    bool write(uint64_t increase = 1) const;

    int fd = -1;
};

}

#elif

namespace DB
{

struct EventFD
{
};

}

#endif
