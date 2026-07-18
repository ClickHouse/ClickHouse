#pragma once

#include <string>
#include <utility>
#include <vector>

namespace DB
{

/// Representation of a plain_rewritable `prefix.path` object.
///
/// Implicit form (existing, used when the directory has no hard links):
///     /hello/world/
///
/// Explicit form (used when files may reference blobs outside the directory prefix):
///     /hello/world/
///     files: 2
///     upyachka.bin    aaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin
///     hello.json      gfkoqxvyhaasroiodbeurnftnwieiihy/hello.json
///
/// Blob paths are relative to the object-storage common key prefix.
struct PlainRewritablePrefixPath
{
    std::string logical_path;
    bool explicit_files = false;
    /// (logical file name, relative blob object key)
    std::vector<std::pair<std::string, std::string>> files;
};

std::string serializePlainRewritablePrefixPath(const PlainRewritablePrefixPath & prefix_path);
PlainRewritablePrefixPath parsePlainRewritablePrefixPath(std::string_view content);

}
