#pragma once

#include <base/types.h>


namespace DB
{

/// Returns true if a specified path has one of the registered file extensions for an archive.
bool hasRegisteredArchiveFileExtension(const String & path);

}
