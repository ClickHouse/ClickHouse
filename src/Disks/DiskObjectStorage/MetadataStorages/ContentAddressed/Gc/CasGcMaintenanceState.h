#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
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
    std::optional<Token> token;
    String diagnostic;
};
enum class GcMaintenanceCasOutcome : uint8_t { Committed, Conflict };
struct GcMaintenanceCasResult
{
    GcMaintenanceCasOutcome outcome = GcMaintenanceCasOutcome::Conflict;
    Token token;
};

GcMaintenanceReadResult readGcMaintenanceState(Backend & backend, const Layout & layout);
GcMaintenanceCasResult casGcMaintenanceState(
    Backend & backend, const Layout & layout, const std::optional<Token> & expected, const GcMaintenanceState & next);

}
