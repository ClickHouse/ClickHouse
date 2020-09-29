#pragma once

#include <common/types.h>

namespace DB
{

/// Create a hard link `destination_path` pointing to `source_path`.
/// If the destination already exists, check that it has the same inode (and throw if they are different).
void createHardLink(const String & source_path, const String & destination_path);

}
