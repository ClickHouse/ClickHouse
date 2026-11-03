#pragma once

#include <optional>
#include <string_view>

namespace DB
{

/// `<scheme>://<namespace>/<key>`
struct FullyQualifiedObjectPath
{
    std::string_view scheme;
    std::string_view object_namespace;
    std::string_view key;
};

std::optional<FullyQualifiedObjectPath> trySplitFullyQualifiedObjectPath(std::string_view path);

}
