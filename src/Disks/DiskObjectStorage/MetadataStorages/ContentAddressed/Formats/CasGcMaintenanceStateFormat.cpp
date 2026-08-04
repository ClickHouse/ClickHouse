#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcMaintenanceStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
}

namespace DB::Cas
{

String encodeGcMaintenanceState(const GcMaintenanceState & state)
{
    if (state.janitor_cursor.size() > kMaxGcMaintenanceCursorBytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "CAS gc maintenance state: cursor has {} bytes, limit {}",
            state.janitor_cursor.size(), kMaxGcMaintenanceCursorBytes);

    CasJsonWriter out;
    writeHeaderLine(out, FormatId::GcMaintenanceState);
    bool first = true;
    writeKey(out, "cur", first);
    writeStringValue(out, state.janitor_cursor);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

GcMaintenanceState decodeGcMaintenanceState(std::string_view data)
{
    const uint64_t object_cap = traitsFor(FormatId::GcMaintenanceState).object_cap;
    if (data.size() > object_cap)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS gc maintenance state: object has {} bytes, limit {}", data.size(), object_cap);
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::GcMaintenanceState);
    const String body = readLine(in, traitsFor(FormatId::GcMaintenanceState).line_cap, "cas_gc_maintenance_state");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader reader(body_in, KeyStrictness::Strict, "cas_gc_maintenance_state");

    GcMaintenanceState result;
    bool has_cursor = false;
    String key;
    while (reader.nextKey(key))
    {
        if (key == "cur")
        {
            result.janitor_cursor = reader.readString();
            has_cursor = true;
        }
        else
            reader.skipUnknown(key);
    }
    if (!has_cursor)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc maintenance state: missing cur");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc maintenance state: trailing bytes");
    if (result.janitor_cursor.size() > kMaxGcMaintenanceCursorBytes)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc maintenance state: cursor has {} bytes, limit {}",
            result.janitor_cursor.size(), kMaxGcMaintenanceCursorBytes);
    return result;
}

}
