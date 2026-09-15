#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

namespace OwnerWire
{
    constexpr WireKey server_uuid{"server_uuid"};
    constexpr WireKey retired_at_ms{"retired_at_ms"};
}

namespace ServerEpochWire
{
    constexpr WireKey next_writer_epoch{"next_writer_epoch"};
}

namespace MountLeaseWire
{
    constexpr WireKey server_uuid{"server_uuid"};
    constexpr WireKey writer_epoch{"writer_epoch"};
    constexpr WireKey hostname{"hostname"};
    constexpr WireKey pid{"pid"};
    constexpr WireKey started_at_ms{"started_at_ms"};
    constexpr WireKey seq{"seq"};
    constexpr WireKey expires_at_ms{"expires_at_ms"};
    constexpr WireKey min_active_build_sequence{"min_active_build_sequence"};
    constexpr WireKey gc_fenced{"gc_fenced"};
    constexpr WireKey write_attempt_id{"write_attempt_id"};
}

namespace
{

/// Read exactly the one JSON body line allowed by a server-root control object. `readLine` rejects a
/// missing newline and a line over the format-specific cap; each decoder separately checks that no
/// bytes follow this line, so a concatenated object cannot be accepted accidentally.
String readBodyLine(ReadBuffer & in, FormatId id, std::string_view what)
{
    return readLine(in, traitsFor(id).line_cap, what);
}

}

String encodeOwner(const OwnerObject & o)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::Owner);
    bool first = true;
    writeHex128Field(out, OwnerWire::server_uuid, o.server_uuid, first);
    if (o.retired_at_ms)
        writeNumberField(out, OwnerWire::retired_at_ms, *o.retired_at_ms, first);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

OwnerObject decodeOwner(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::Owner);
    const String body = readBodyLine(in, FormatId::Owner, "owner");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "owner");

    OwnerObject o;
    bool saw = false;
    std::optional<uint64_t> rt;
    String key;
    while (r.nextKey(key))
    {
        if (key == OwnerWire::server_uuid)
        {
            o.server_uuid = r.readHex128();
            saw = true;
        }
        else if (key == OwnerWire::retired_at_ms)
            rt = r.readU64Number();
        else
            r.skipUnknown(key);
    }
    o.retired_at_ms = rt;
    if (!saw)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS owner: missing server_uuid");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS owner: trailing bytes");
    return o;
}

String encodeServerEpoch(const ServerEpoch & e)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::ServerEpoch);
    bool first = true;
    writeU64StringField(out, ServerEpochWire::next_writer_epoch, e.next_writer_epoch, first);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

ServerEpoch decodeServerEpoch(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::ServerEpoch);
    const String body = readBodyLine(in, FormatId::ServerEpoch, "server-epoch");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "server-epoch");

    ServerEpoch e;
    bool saw = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == ServerEpochWire::next_writer_epoch)
        {
            e.next_writer_epoch = r.readU64String();
            saw = true;
        }
        else
            r.skipUnknown(key);
    }
    if (!saw)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS server-epoch: missing next_writer_epoch");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS server-epoch: trailing bytes");
    return e;
}

String encodeMountLease(const MountLease & m)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::MountLease);
    bool first = true;
    writeHex128Field(out, MountLeaseWire::server_uuid, m.server_uuid, first);
    writeU64StringField(out, MountLeaseWire::writer_epoch, m.writer_epoch, first);
    writeStringField(out, MountLeaseWire::hostname, m.hostname, first);
    writeNumberField(out, MountLeaseWire::pid, m.pid, first);
    writeNumberField(out, MountLeaseWire::started_at_ms, m.started_at_ms, first);
    writeU64StringField(out, MountLeaseWire::seq, m.seq, first);
    writeNumberField(out, MountLeaseWire::expires_at_ms, m.expires_at_ms, first);
    writeU64StringField(out, MountLeaseWire::min_active_build_sequence, m.min_active_build_sequence, first);
    writeBoolField(out, MountLeaseWire::gc_fenced, m.gc_fenced, first);
    writeHex128Field(out, MountLeaseWire::write_attempt_id, m.write_attempt_id, first);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

MountLease decodeMountLease(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::MountLease);
    const String body = readBodyLine(in, FormatId::MountLease, "mount-lease");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "mount-lease");

    MountLease m;
    bool saw_su = false;
    bool saw_we = false;
    bool saw_write_attempt_id = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == MountLeaseWire::server_uuid)
        {
            m.server_uuid = r.readHex128();
            saw_su = true;
        }
        else if (key == MountLeaseWire::writer_epoch)
        {
            m.writer_epoch = r.readU64String();
            saw_we = true;
        }
        else if (key == MountLeaseWire::hostname)
            m.hostname = r.readString();
        else if (key == MountLeaseWire::pid)
            m.pid = r.readU64Number();
        else if (key == MountLeaseWire::started_at_ms)
            m.started_at_ms = r.readU64Number();
        else if (key == MountLeaseWire::seq)
            m.seq = r.readU64String();
        else if (key == MountLeaseWire::expires_at_ms)
            m.expires_at_ms = r.readU64Number();
        else if (key == MountLeaseWire::min_active_build_sequence)
            m.min_active_build_sequence = r.readU64String();
        else if (key == MountLeaseWire::gc_fenced)
            m.gc_fenced = r.readBool();
        else if (key == MountLeaseWire::write_attempt_id)
        {
            m.write_attempt_id = r.readHex128();
            saw_write_attempt_id = true;
        }
        else
            r.skipUnknown(key);
    }
    /// Named one by one rather than as a single condition: these are three separate identities, and a
    /// shared message cannot tell an operator which of them the object is missing -- nor let a test
    /// prove that each is actually required.
    if (!saw_su)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS mount-lease: missing server_uuid");
    if (!saw_we)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS mount-lease: missing writer_epoch");
    if (!saw_write_attempt_id || m.write_attempt_id == UInt128{})
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS mount-lease: missing or zero write_attempt_id");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS mount-lease: trailing bytes");
    return m;
}

}
