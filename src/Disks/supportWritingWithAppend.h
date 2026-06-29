#pragma once

#include <Disks/IDisk.h>
namespace DB
{
bool supportWritingWithAppend(const DiskPtr & disk);
}
