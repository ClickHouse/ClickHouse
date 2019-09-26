#include <Core/DateTime64.h>

#include <cassert>

namespace DB {

//static constexpr UInt32 NANOS_PER_SECOND = 1000 * 1000 * 1000;

//DateTime64::Components DateTime64::split() const
//{
//    auto datetime = static_cast<time_t>(t / NANOS_PER_SECOND);
//    auto nanos = static_cast<UInt32>(t % NANOS_PER_SECOND);
//    return Components { datetime, nanos };
//}

//DateTime64::DateTime64(DateTime64::Components c)
//    : t {c.datetime * NANOS_PER_SECOND + c.nanos}
//{
//    assert(c.nanos < NANOS_PER_SECOND);
//}

}
