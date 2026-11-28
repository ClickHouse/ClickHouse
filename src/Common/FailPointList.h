#pragma once

#include <Common/FailPoint.h>

#include <span>


namespace DB::FailPointList
{

struct Entry
{
    const char * name;
    FailPointInjection::FailPointType type;
};

/// Every fail point declared in FailPoint.cpp, in the order of declaration.
std::span<const Entry> entries();

}
