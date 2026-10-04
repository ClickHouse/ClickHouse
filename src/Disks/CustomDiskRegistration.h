#pragma once

#include <memory>
#include <vector>

#include <base/types.h>

namespace DB
{

class CustomDiskRegistration;
using CustomDiskRegistrationPtr = std::shared_ptr<CustomDiskRegistration>;
using CustomDiskRegistrations = std::vector<CustomDiskRegistrationPtr>;

/// Keeps a disk defined inline with `disk(...)` in a table or database definition registered in
/// the disk selector.
///
/// Such a disk belongs to the tables and databases that define it: it is absent from the server
/// configuration, and `DiskFromAST::ensureDiskIsNotCustom` forbids referring to it by name, so
/// every user of the disk holds a registration for it. When the last registration is destroyed --
/// that is, when the last table or database using the disk has been dropped or detached -- the
/// disk is unregistered and shut down. Otherwise it would stay in `system.disks` and keep running
/// its background threads (blob cleanup, metadata refresh, ...) until the server stops.
///
/// A definition may nest other definitions, e.g. `disk(type = cache, disk = disk(...))`, and the
/// wrapper disk keeps a reference to the disk it wraps. The registration of the wrapper therefore
/// owns the registrations of the nested disks: they are destroyed after the wrapper has been
/// released, so the disks are shut down from the outside in, and a nested disk is never shut down
/// while a wrapper still uses it.
class CustomDiskRegistration
{
public:
    CustomDiskRegistration(String disk_name_, CustomDiskRegistrations nested_)
        : disk_name(std::move(disk_name_)), nested(std::move(nested_))
    {
    }

    ~CustomDiskRegistration();

    const String & getDiskName() const { return disk_name; }

private:
    const String disk_name;
    /// Destroyed after the destructor body has released `disk_name`.
    const CustomDiskRegistrations nested;
};

}
