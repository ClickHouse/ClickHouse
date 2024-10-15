#include <Common/VersionNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

VersionNumber::VersionNumber(std::string version_string)
{
    if (version_string.empty())
        return;

    ReadBufferFromString rb(version_string);
    while (!rb.eof())
    {
        Int64 value;
        if (!tryReadIntText(value, rb))
            break;
        components.push_back(value);
        if (!checkChar('.', rb))
            break;
    }
}

std::string VersionNumber::toString() const
{
    return fmt::format("{}", fmt::join(components, "."));
}

int VersionNumber::compare(const VersionNumber & rhs) const
{
    size_t min = std::min(components.size(), rhs.components.size());
    for (size_t i = 0; i < min; ++i)
    {
        if (auto d = components[i] - rhs.components[i])
            return d > 0 ? 1 : -1;
    }

    if (components.size() > min)
    {
        return components[min] >= 0 ? 1 : -1;
    }
    if (rhs.components.size() > min)
    {
        return -rhs.components[min] > 0 ? 1 : -1;
    }

    return 0;
}

}
