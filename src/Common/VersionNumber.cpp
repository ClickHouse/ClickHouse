#include <Common/VersionNumber.h>
#include <cstdlib>
#include <iostream>

namespace DB
{

VersionNumber::VersionNumber(std::string version_string)
{
    if (version_string.empty())
        return;

    char * start = &version_string.front();
    char * end = start;
    const char * eos = &version_string.back() + 1;

    do
    {
        Int64 value = strtol(start, &end, 10);
        components.push_back(value);
        start = end + 1;
    }
    while (start < eos && (end < eos && *end == '.'));
}

std::string VersionNumber::toString() const
{
    std::string str;
    for (Int64 v : components)
    {
        if (!str.empty())
            str += '.';
        str += std::to_string(v);
    }
    return str;
}

int VersionNumber::compare(const VersionNumber & rhs) const
{
    size_t min = std::min(components.size(), rhs.components.size());
    for (size_t i = 0; i < min; ++i)
    {
        if (int d = components[i] - rhs.components[i])
            return d;
    }

    if (components.size() > min)
    {
        if (components[min] != 0)
            return components[min];
        else
            return 1;
    }
    else if (rhs.components.size() > min)
    {
        if (rhs.components[min] != 0)
            return -rhs.components[min];
        else
            return -1;
    }

    return 0;
}

}
