#pragma once

namespace DB
{
enum class RemoteDiskFeature
{
    None,
    Zerocopy,
    VFS
};
}
