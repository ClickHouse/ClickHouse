#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>
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

namespace
{

std::string_view metaStateToWord(MetaState s)
{
    switch (s)
    {
        case MetaState::Clean:     return "clean";
        case MetaState::Condemned: return "condemned";
    }
    // The enum is persisted as a closed vocabulary. Do not silently invent a spelling for a value
    // added without a corresponding format decision: that would make the writer emit data older
    // readers cannot classify.
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: unknown MetaState {}", static_cast<int>(s));
}

MetaState metaStateFromWord(std::string_view w)
{
    if (w == "clean")     return MetaState::Clean;
    if (w == "condemned") return MetaState::Condemned;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: unknown state '{}'", w);
}

}

String encodeBlobMeta(const BlobMeta & meta)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::BlobMeta);
    // `version` is represented by the header line. The JSON body contains only fields that describe
    // the current marker and its accounting data.
    bool first = true;
    writeKey(out, "st", first);
    writeStringValue(out, metaStateToWord(meta.state));
    writeKey(out, "cr", first);
    writeU64StringValue(out, meta.condemn_round);
    writeKey(out, "sz", first);
    writeU64StringValue(out, meta.size);
    closeObject(out, first);
    writeChar('\n', out);
    return std::move(out).take();
}

BlobMeta decodeBlobMeta(std::string_view bytes)
{
    ReadBufferFromMemory in(bytes.data(), bytes.size());
    expectHeaderLine(in, FormatId::BlobMeta);
    const String body = readLine(in, traitsFor(FormatId::BlobMeta).line_cap, "blob meta");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "blob meta");

    // Start with the documented defaults. In particular, `version` stays at 1 because the header's
    // version is authoritative and is not copied into the body struct.
    BlobMeta m;
    bool saw_state = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == "st")
        {
            m.state = metaStateFromWord(r.readString());
            saw_state = true;
        }
        else if (key == "cr")
            m.condemn_round = r.readU64String();
        else if (key == "sz")
            m.size = r.readU64String();
        else
            r.skipUnknown(key);
    }
    if (!saw_state)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: missing st");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: trailing bytes");
    return m;
}

}
