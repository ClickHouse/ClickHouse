#pragma once

#include <Disks/IDisk.h>

namespace DB
{

std::shared_ptr<const IDisk> getUnderlyingDisk(const DiskPtr & disk);

}
