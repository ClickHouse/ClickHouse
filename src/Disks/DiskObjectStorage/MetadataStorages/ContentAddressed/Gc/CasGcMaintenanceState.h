#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcMaintenanceStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <optional>

namespace DB::Cas
{

enum class GcMaintenanceReadStatus : uint8_t { Absent, Valid, Corrupt };
struct GcMaintenanceReadResult
{
    GcMaintenanceReadStatus status;
    std::optional<GcMaintenanceState> state;
    std::optional<Etag> etag;
    String diagnostic;
};

GcMaintenanceReadResult readGcMaintenanceState(CasOperation & op, const Layout & layout);
/// `create`s the maintenance-state key on absence, `replace`s it when `expected` names the
/// incarnation last observed. `policy` is named by the caller because a write issued from inside an
/// already-failed step (a reset after a failed enumeration) must send at most one attempt rather than
/// spend the round's remaining time retrying a write nothing downstream is waiting on.
WriteResult casGcMaintenanceState(
    CasOperation & op, const Layout & layout, const std::optional<Etag> & expected,
    const GcMaintenanceState & next, const Retry & policy);

}
