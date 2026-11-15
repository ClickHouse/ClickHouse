#include <Common/VersionNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <fmt/format.h>

namespace DB
{

VersionNumber::VersionNumber(std::string version_string)
{
    if (version_string.empty())
        return;

    ReadBufferFromString rb(version_string);
    Int64 * components[] = {&version_major, &version_minor, &version_patch};
    for (auto * component : components)
    {
        if (rb.eof())
            break;
        if (!tryReadIntText(*component, rb))
            break;
        if (!checkChar('.', rb))
            break;
    }
}

std::string VersionNumber::toString() const
{
    return fmt::format("{}.{}.{}", version_major, version_minor, version_patch);
}

int VersionNumber::compare(const VersionNumber & rhs) const
{
    if (version_major != rhs.version_major)
        return version_major > rhs.version_major ? 1 : -1;
    if (version_minor != rhs.version_minor)
        return version_minor > rhs.version_minor ? 1 : -1;
    if (version_patch != rhs.version_patch)
        return version_patch > rhs.version_patch ? 1 : -1;
    return 0;
}

}
