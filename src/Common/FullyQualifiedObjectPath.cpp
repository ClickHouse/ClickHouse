#include <Common/FullyQualifiedObjectPath.h>

namespace DB
{

std::optional<FullyQualifiedObjectPath> trySplitFullyQualifiedObjectPath(std::string_view path)
{
    static constexpr std::string_view scheme_delimiter = "://";

    const size_t scheme_end = path.find(scheme_delimiter);
    if (scheme_end == 0 || scheme_end == std::string_view::npos)
        return {};

    const std::string_view scheme = path.substr(0, scheme_end);
    if (scheme.contains('/'))
        return {};

    const std::string_view rest = path.substr(scheme_end + scheme_delimiter.size());
    const size_t namespace_end = rest.find('/');
    if (namespace_end == 0 || namespace_end == std::string_view::npos || namespace_end + 1 == rest.size())
        return {};

    return FullyQualifiedObjectPath{scheme, rest.substr(0, namespace_end), rest.substr(namespace_end + 1)};
}

}
