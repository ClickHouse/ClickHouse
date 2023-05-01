#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

DB::DiskPtr createDisk();

void destroyDisk(DB::DiskPtr & disk);
