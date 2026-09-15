#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
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

namespace BlobMetaWire
{
    constexpr WireKey state{"state"};
    constexpr WireKey condemn_round{"condemn_round"};
    constexpr WireKey size{"size"};
}

constexpr EnumWireTable<MetaState, 2> kMetaStateWords{{{
    {MetaState::Clean, "clean"},
    {MetaState::Condemned, "condemned"},
}}};

static_assert(casEnumTableCoversEnum<kMetaStateWords, MetaState>());

}

std::string_view metaStateToWireWord(MetaState state)
{
    return kMetaStateWords.toWord(state, "CAS blob meta");
}

MetaState metaStateFromWireWord(std::string_view w)
{
    return kMetaStateWords.fromWord(w, "CAS blob meta");
}

String encodeBlobMeta(const BlobMeta & meta)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::BlobMeta);
    // `version` is represented by the header line. The JSON body contains only fields that describe
    // the current marker and its accounting data.
    bool first = true;
    writeWordField(out, BlobMetaWire::state, metaStateToWireWord(meta.state), first);
    writeU64StringField(out, BlobMetaWire::condemn_round, meta.condemn_round, first);
    writeU64StringField(out, BlobMetaWire::size, meta.size, first);
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
        if (key == BlobMetaWire::state)
        {
            m.state = metaStateFromWireWord(r.readString());
            saw_state = true;
        }
        else if (key == BlobMetaWire::condemn_round)
            m.condemn_round = r.readU64String();
        else if (key == BlobMetaWire::size)
            m.size = r.readU64String();
        else
            r.skipUnknown(key);
    }
    if (!saw_state)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: missing state");
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob meta: trailing bytes");
    return m;
}

}
