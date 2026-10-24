#pragma once

#include <memory>
#include <vector>

#include <base/types.h>

namespace DB
{

class CustomDiskRegistration;
using CustomDiskRegistrationPtr = std::shared_ptr<CustomDiskRegistration>;
using CustomDiskRegistrations = std::vector<CustomDiskRegistrationPtr>;

/// Keeps a disk defined inline with `disk(...)` registered while a table or database uses it; the
/// disk is unregistered and shut down when the last registration is destroyed.
/// A registration owns the registrations of the disks nested in its definition (e.g. the disk
/// wrapped by `disk(type = cache, disk = disk(...))`), so disks are released from the outside in.
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
