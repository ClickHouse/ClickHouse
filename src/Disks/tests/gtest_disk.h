#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

/// Creates a `DiskLocal` over a fresh directory under the system temporary directory. `name` only
/// labels that directory, it is never a path relative to the current one: a unit test must not touch
/// whatever happens to sit next to the binary, and `destroyDisk` removes the directory recursively.
DB::DiskPtr createDisk(const std::string & name = "disk");

/// Removes the directory `createDisk` created for this disk and resets the pointer.
void destroyDisk(DB::DiskPtr & disk);
