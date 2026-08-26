#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

String encodeGcState(const GcState & state)
{
    if (state.gc_shards < 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "encodeGcState: gc_shards must be >= 1 -- refusing to persist an unreadable gc/state");
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::GcState);
    bool first = true;
    writeKey(out, "rnd", first); writeU64StringValue(out, state.round);
    writeKey(out, "gcs", first); writeIntText(state.gc_shards, out);
    writeKey(out, "sg", first);  writeU64StringValue(out, state.snap_generation);
    writeKey(out, "spt", first); writeU64StringValue(out, state.snap_pruned_through);
    writeKey(out, "sa", first);  writeU64StringValue(out, state.snap_attempt);
    writeKey(out, "msc", first); writeStringValue(out, state.manifest_sweep_cursor);
    writeKey(out, "lo", first);  writeHex128Value(out, state.lease.owner);
    writeKey(out, "ls", first);  writeU64StringValue(out, state.lease.seq);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

GcState decodeGcState(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::GcState);
    const String body = readLine(in, traitsFor(FormatId::GcState).line_cap, "gc/state");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "gc/state");

    GcState state;
    bool saw_gcs = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == "rnd") state.round = r.readU64String();
        else if (key == "gcs") { state.gc_shards = r.readU64Number(); saw_gcs = true; }
        else if (key == "sg") state.snap_generation = r.readU64String();
        else if (key == "spt") state.snap_pruned_through = r.readU64String();
        else if (key == "sa") state.snap_attempt = r.readU64String();
        else if (key == "msc") state.manifest_sweep_cursor = r.readString();
        else if (key == "lo") state.lease.owner = r.readHex128();
        else if (key == "ls") state.lease.seq = r.readU64String();
        else r.skipUnknown(key);
    }
    /// Fail closed on an absent gcs: the writer always emits it, so a missing key means a corrupt object.
    /// Do NOT silently keep the struct default (1) — that would hide corruption (no-fallback principle).
    if (!saw_gcs)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: missing gcs");
    if (state.gc_shards == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: gc_shards must be >= 1");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc/state: trailing bytes");
    return state;
}

String encodeGcHeartbeat(const GcHeartbeat & hb)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::GcHeartbeat);
    bool first = true;
    writeKey(out, "by", first);  writeHex128Value(out, hb.owner);
    writeKey(out, "seq", first); writeU64StringValue(out, hb.hb_seq);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

GcHeartbeat decodeGcHeartbeat(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::GcHeartbeat);
    const String body = readLine(in, traitsFor(FormatId::GcHeartbeat).line_cap, "gc heartbeat");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "gc heartbeat");

    GcHeartbeat hb;
    bool saw_by = false;
    bool saw_seq = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == "by")
        {
            hb.owner = r.readHex128();
            saw_by = true;
        }
        else if (key == "seq")
        {
            hb.hb_seq = r.readU64String();
            saw_seq = true;
        }
        else r.skipUnknown(key);
    }
    if (!saw_by || !saw_seq)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc heartbeat: missing identity field");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS gc heartbeat: trailing bytes");
    return hb;
}

}
