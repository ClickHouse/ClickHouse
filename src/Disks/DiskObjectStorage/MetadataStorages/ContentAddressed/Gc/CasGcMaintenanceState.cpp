#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMaintenanceState.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::Cas
{

GcMaintenanceReadResult readGcMaintenanceState(CasOperation & op, const Layout & layout)
{
    const auto got = op.read(layout.gcMaintenanceStateKey(), Retry::standard());
    if (!got)
        return {.status = GcMaintenanceReadStatus::Absent, .state = std::nullopt, .etag = std::nullopt, .diagnostic = {}};
    try
    {
        return {.status = GcMaintenanceReadStatus::Valid, .state = decodeGcMaintenanceState(got->bytes),
            .etag = got->etag, .diagnostic = {}};
    }
    catch (const DB::Exception & e)
    {
        if (e.code() != ErrorCodes::CORRUPTED_DATA)
            throw;
        return {.status = GcMaintenanceReadStatus::Corrupt, .state = std::nullopt,
            .etag = got->etag, .diagnostic = e.message()};
    }
}

WriteResult casGcMaintenanceState(
    CasOperation & op, const Layout & layout, const std::optional<Etag> & expected,
    const GcMaintenanceState & next, const Retry & policy)
{
    const String key = layout.gcMaintenanceStateKey();
    const String bytes = encodeGcMaintenanceState(next);
    return expected ? op.replace(key, bytes, *expected, policy) : op.create(key, bytes, policy);
}

}
