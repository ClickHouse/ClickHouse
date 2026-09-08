#pragma once

#include <memory>
#include <vector>

#include <base/types.h>

namespace DB
{

/// Keeps a disk defined inline with `disk(...)` in a table or database definition registered in
/// the disk selector.
///
/// Such a disk belongs to the tables and databases that define it: it is absent from the server
/// configuration, and `DiskFromAST::ensureDiskIsNotCustom` forbids referring to it by name, so
/// every user of the disk holds a registration for it. When the last registration is destroyed --
/// that is, when the last table or database using the disk has been dropped or detached -- the
/// disk is unregistered and shut down. Otherwise it would stay in `system.disks` and keep running
/// its background threads (blob cleanup, metadata refresh, ...) until the server stops.
class CustomDiskRegistration
{
public:
    explicit CustomDiskRegistration(String disk_name_) : disk_name(std::move(disk_name_)) { }

    ~CustomDiskRegistration();

private:
    const String disk_name;
};

using CustomDiskRegistrationPtr = std::shared_ptr<CustomDiskRegistration>;
/// A disk definition may nest other definitions (e.g. `disk(type = cache, disk = disk(...))`),
/// and each of them is registered on its own.
using CustomDiskRegistrations = std::vector<CustomDiskRegistrationPtr>;

}
