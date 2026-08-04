#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMaintenanceState.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::Cas
{

GcMaintenanceReadResult readGcMaintenanceState(Backend & backend, const Layout & layout)
{
    const auto got = backend.get(layout.gcMaintenanceStateKey());
    if (!got)
        return {.status = GcMaintenanceReadStatus::Absent, .state = std::nullopt, .token = std::nullopt, .diagnostic = {}};
    try
    {
        return {.status = GcMaintenanceReadStatus::Valid, .state = decodeGcMaintenanceState(got->bytes),
            .token = got->token, .diagnostic = {}};
    }
    catch (const DB::Exception & e)
    {
        if (e.code() != ErrorCodes::CORRUPTED_DATA)
            throw;
        return {.status = GcMaintenanceReadStatus::Corrupt, .state = std::nullopt,
            .token = got->token, .diagnostic = e.message()};
    }
}

GcMaintenanceCasResult casGcMaintenanceState(
    Backend & backend, const Layout & layout, const std::optional<Token> & expected, const GcMaintenanceState & next)
{
    const CasResult result = backend.casPut(layout.gcMaintenanceStateKey(), encodeGcMaintenanceState(next), expected);
    if (result.outcome == CasOutcome::Committed)
        return {.outcome = GcMaintenanceCasOutcome::Committed, .token = result.token};
    return {.outcome = GcMaintenanceCasOutcome::Conflict, .token = {}};
}

}
