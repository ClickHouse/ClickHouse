#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

DB::DiskPtr createDisk(const std::string & path = "tmp/");

void destroyDisk(DB::DiskPtr & disk);
