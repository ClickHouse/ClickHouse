#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>

#include <time.h>
#include <errno.h>

namespace DB
{

void SleepForNanoseconds(UInt64 nanoseconds);

void SleepForMicroseconds(UInt64 microseconds);

void SleepForMilliseconds(UInt64 milliseconds);

void SleepForSeconds(UInt64 seconds);

}
