#pragma once
#include <cstdint>


namespace DB
{

/** Adds offset to the time returned by all time-related functions.
  * The library is similar to "libfaketime" but much more simple and incomplete.
  *
  * Usage: FAKE_TIME_OFFSET=-1000000000 ./clickhouse-server
  *
  * NOTE: libfaketime cannot be used for the following reasons:
  * - LD_PRELOAD does not work for binaries with fs capabilities.
  */
class FakeTime
{
public:
    static FakeTime & instance()
    {
        static FakeTime res;
        return res;
    }

    static bool isEffective();

private:
    FakeTime();
};

}

