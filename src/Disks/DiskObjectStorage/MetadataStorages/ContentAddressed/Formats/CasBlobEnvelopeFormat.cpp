#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasEnvelopeLimits.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <algorithm>
#include <limits>

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

namespace
{

constexpr std::string_view kBlobType = "cas_blob";

namespace EnvelopeWire
{
    constexpr WireKey type{"type"};
    constexpr WireKey version{"v"};
    constexpr WireKey tag{"tag"};
    constexpr WireKey build{"build"};
    constexpr WireKey time_ms{"time_ms"};
    constexpr WireKey creator{"creator"};
    constexpr WireKey op{"op"};
    constexpr WireKey chver{"chver"};
    constexpr WireKey ref{"ref"};
    /// Not a field this build understands: written only to exercise the reader's `!`-key policy.
    constexpr WireKey unknown_critical{"!x"};
}

constexpr EnumWireTable<ProvenanceOp, 6> kProvenanceOpWords{{{
    {ProvenanceOp::Other, "other"},
    {ProvenanceOp::Insert, "insert"},
    {ProvenanceOp::Merge, "merge"},
    {ProvenanceOp::Mutation, "mutation"},
    {ProvenanceOp::Attach, "attach"},
    {ProvenanceOp::Repack, "repack"},
}}};

static_assert(casEnumTableCoversEnum<kProvenanceOpWords, ProvenanceOp>());

/// Byte cost of one JSON key as written by `CasJsonWriter::key`: the leading `{`/`,` separator (1)
/// plus the opening quote (1), the key text, and the closing quote and colon (2).
constexpr size_t keyCost(WireKey key)
{
    return 4 + key.text.size();
}

/// A quoted `hex128Value` is always exactly this wide -- 2 quote bytes plus 2 hex digits per
/// `UInt128` byte -- because `writeHexUIntLowercase` zero-pads; there is no smaller or larger case.
constexpr size_t kQuotedHex128Len = 2 + sizeof(UInt128) * 2;

/// Maximum decimal digits an unquoted `writeIntText` value can produce for each integer width the
/// envelope persists, taken from the type itself rather than re-typed as a literal.
constexpr size_t kMaxU64DecimalLen = std::numeric_limits<uint64_t>::digits10 + 1;
constexpr size_t kMaxU32DecimalLen = std::numeric_limits<uint32_t>::digits10 + 1;

/// The longest persisted `op` word, found by walking the table rather than hardcoding one -- the
/// worst case must track `kProvenanceOpWords` even if a future entry outgrows "mutation".
constexpr size_t maxProvenanceOpWordLen()
{
    size_t max_len = 0;
    for (const auto & entry : kProvenanceOpWords.entries)
        max_len = std::max(max_len, entry.word.size());
    return max_len;
}

/// Mandatory (always-written whenever `provenance` is set) non-`ref` fields at type maxima, in the
/// exact field order `encodeEnvelopeHeader` writes them. `CasPoolMetaFormat.cpp` records why 240 was
/// chosen as the floor above this bound.
constexpr size_t kMandatoryNonRefWorstCase =
      keyCost(EnvelopeWire::type) + 2 + kBlobType.size()
    + keyCost(EnvelopeWire::version) + kMaxU32DecimalLen
    + keyCost(EnvelopeWire::tag) + kQuotedHex128Len
    + keyCost(EnvelopeWire::build) + kQuotedHex128Len
    + keyCost(EnvelopeWire::time_ms) + kMaxU64DecimalLen
    + keyCost(EnvelopeWire::creator) + kQuotedHex128Len
    + keyCost(EnvelopeWire::op) + 2 + maxProvenanceOpWordLen()
    + keyCost(EnvelopeWire::chver) + kMaxU32DecimalLen;

/// The encoder always frames `ref`, even when empty: the key (`,"ref":`), the empty quotes, the
/// closing `}`, and the trailing '\n' reserved at byte `blob_header_len - 1`.
constexpr size_t kRefFramingAndTerminator = keyCost(EnvelopeWire::ref) + 2 + 1 + 1;

/// The worst-case byte count `encodeEnvelopeHeader` can ever produce before the diagnostic `ref`
/// gets any budget at all. Proven, not merely documented: the static_assert below fails the BUILD if
/// a future key or type change ever closes the margin under `kMinBlobHeaderLen`.
constexpr size_t kMandatoryDescriptorWorstCase = kMandatoryNonRefWorstCase + kRefFramingAndTerminator;

static_assert(kMandatoryDescriptorWorstCase <= kMinBlobHeaderLen - 1,
    "the mandatory blob-envelope fields plus the empty-ref framing must fit under kMinBlobHeaderLen "
    "(the trailing '\\n' is already counted above, so the spare byte is the diagnostic ref's floor "
    "budget, not the newline); if a field grew, either shrink it back or "
    "raise kMinBlobHeaderLen (CasEnvelopeLimits.h) to match");

/// The escaped byte-length of one raw ref char under the frozen envelope alphabet (see writeEnvelopeRefField).
size_t escapedLen(char c)
{
    const unsigned char u = static_cast<unsigned char>(c);
    if (c == '"' || c == '\\')
        return 2;
    if (u < 0x20)
        return 6;   /// \uXXXX
    return 1;       /// everything else, INCLUDING '/', verbatim
}

void appendEscaped(String & out, char c)
{
    const unsigned char u = static_cast<unsigned char>(c);
    if (c == '"')  { out += "\\\""; return; }
    if (c == '\\') { out += "\\\\"; return; }
    if (u < 0x20)
    {
        static constexpr char hexd[] = "0123456789abcdef";
        out += "\\u00";
        out += hexd[(u >> 4) & 0xF];
        out += hexd[u & 0xF];
        return;
    }
    out += c;
}

/// The blob-envelope's OWN ref-string writer. DELIBERATELY NOT `writeStringValue` and MUST NOT be
/// "unified" with it: the 256-byte header budget arithmetic and the stored blob bytes depend on this
/// alphabet being codec-owned and FROZEN — only `"`, `\`, and control chars (< 0x20, as `\uXXXX`)
/// escape; `/` and every other byte pass verbatim. (`writeStringValue`/`FormatSettings::JSON` may
/// legitimately evolve for the control-plane formats; this codec must not inherit that.) Writes the
/// opening quote, the ref content escaped and truncated to at most `budget` escaped bytes (stopping at
/// the first char that would overflow — never splitting an escape), then the closing quote.
void writeEnvelopeRefField(String & json, size_t budget, std::string_view raw_ref)
{
    json += '"';
    size_t used = 0;
    for (char c : raw_ref)
    {
        const size_t need = escapedLen(c);
        if (used + need > budget)
            break;
        appendEscaped(json, c);
        used += need;
    }
    json += '"';
}

}

const size_t mandatory_descriptor_worst_case = kMandatoryDescriptorWorstCase;

std::string_view provenanceOpToWireWord(ProvenanceOp op)
{
    return kProvenanceOpWords.toWord(op, "CAS blob envelope");
}

ProvenanceOp provenanceOpFromWireWord(std::string_view w)
{
    return kProvenanceOpWords.fromWord(w, "CAS blob envelope");
}

String encodeEnvelopeHeader(EnvelopeHeader & header, uint32_t blob_header_len,
                            std::optional<uint32_t> version_override)
{
    if (header.kind != ObjectKind::Blob)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS blob envelope: unexpected ObjectKind {}", static_cast<int>(header.kind));

    /// Build every field EXCEPT `ref` into a buffer (small, bounded by blob_header_len). `ref` is the
    /// only truncated field, appended last so the truncation never disturbs another field.
    String json;
    {
        CasJsonWriter buf(256);
        bool first = true;
        writeStringField(buf, EnvelopeWire::type, kBlobType, first);
        writeNumberField(buf, EnvelopeWire::version, version_override.value_or(currentCompatibilityVersion()), first);
        writeHex128Field(buf, EnvelopeWire::tag, header.incarnation_tag, first);
        writeHex128Field(buf, EnvelopeWire::build, header.build_id, first);
        if (header.provenance)
        {
            writeNumberField(buf, EnvelopeWire::time_ms, header.provenance->created_at_ms, first);
            writeHex128Field(buf, EnvelopeWire::creator, header.provenance->creator_server_id, first);
            writeStringField(buf, EnvelopeWire::op, provenanceOpToWireWord(header.provenance->op), first);
            writeNumberField(buf, EnvelopeWire::chver, header.provenance->ch_version, first);
        }
        /// Test-only critical extension: an unknown `!`-key BEFORE `ref`.
        if (header.emit_unknown_critical_key)
            writeStringField(buf, EnvelopeWire::unknown_critical, "1", first);
        json = std::move(buf).take();   /// e.g. {"type":"cas_blob","v":1,...,"chver":26006001   (no ref, no closing brace)
    }

    /// Optional `ref`, truncated to the exact remaining budget. Layout after this block:
    ///   json + `,"ref":` + `"` + <escaped ref, <= budget bytes> + `"` + `}`   must be <= blob_header_len-1
    /// (byte blob_header_len-1 is reserved for '\n'; the pad zone fills the gap with spaces).
    if (header.intended_ref)
    {
        /// 4 = the `,"` before and `":` after the key text — the `,"ref":` framing minus the key itself.
        constexpr size_t ref_key_size = 4 + EnvelopeWire::ref.text.size();
        /// +3 = opening quote + closing quote + closing brace.
        const size_t fixed = json.size() + ref_key_size + 3;
        if (blob_header_len < 1 || fixed > static_cast<size_t>(blob_header_len) - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS blob envelope: non-ref fields ({} bytes) do not fit blob_header_len {} before the ref",
                fixed, blob_header_len);
        const size_t budget = (static_cast<size_t>(blob_header_len) - 1) - fixed;
        json += ",\"";
        json += EnvelopeWire::ref.text;
        json += "\":";
        writeEnvelopeRefField(json, budget, *header.intended_ref);
    }
    json += '}';

    if (json.size() > static_cast<size_t>(blob_header_len) - 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS blob envelope: header object {} bytes exceeds blob_header_len {} - 1",
            json.size(), blob_header_len);

    /// Space pad to byte blob_header_len-2, then '\n' at byte blob_header_len-1.
    String out = std::move(json);
    out.append((static_cast<size_t>(blob_header_len) - 1) - out.size(), ' ');
    out += '\n';

    header.header_len = blob_header_len;
    return out;
}

EnvelopeHeader decodeEnvelopeHeader(std::string_view head_bytes, uint64_t /*object_size*/, ObjectKind expected_kind)
{
    ReadBufferFromMemory in(head_bytes.data(), head_bytes.size());
    JsonObjectReader r(in, KeyStrictness::Tolerant, "blob envelope");

    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    bool saw_type = false;
    bool saw_v = false;
    bool have_prov = false;
    Provenance prov;
    String key;
    while (r.nextKey(key))
    {
        if (key == EnvelopeWire::type)
        {
            const String t = r.readString();
            if (t != kBlobType)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS blob envelope: object is a '{}', not a '{}'", t, kBlobType);
            saw_type = true;
        }
        else if (key == EnvelopeWire::version)
        {
            h.compatibility_version = r.readU32Number();
            checkCompatibility(h.compatibility_version, "blob envelope");
            saw_v = true;
        }
        else if (key == EnvelopeWire::tag)
            h.incarnation_tag = r.readHex128();
        else if (key == EnvelopeWire::build)
            h.build_id = r.readHex128();
        else if (key == EnvelopeWire::time_ms)
        {
            prov.created_at_ms = r.readU64Number();
            have_prov = true;
        }
        else if (key == EnvelopeWire::creator)
        {
            prov.creator_server_id = r.readHex128();
            have_prov = true;
        }
        else if (key == EnvelopeWire::op)
        {
            prov.op = provenanceOpFromWireWord(r.readString());
            have_prov = true;
        }
        else if (key == EnvelopeWire::chver)
        {
            prov.ch_version = static_cast<uint32_t>(r.readU64Number());
            have_prov = true;
        }
        else if (key == EnvelopeWire::ref)
            h.intended_ref = r.readString();
        else
            r.skipUnknown(key);   /// `!`-key -> UNKNOWN_FORMAT_VERSION; unknown plain key -> skipped (tolerant)
    }
    if (!saw_type)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob envelope: missing type");
    if (!saw_v)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob envelope: missing v");
    if (h.kind != expected_kind)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS blob envelope: kind {} does not match expected {}",
            static_cast<int>(h.kind), static_cast<int>(expected_kind));
    if (have_prov)
        h.provenance = prov;

    /// Pad-verify: JsonObjectReader consumed through the closing '}', so in.count() == json_len. Every
    /// byte up to the terminating '\n' must be an ASCII space (no smuggling); header_len is DERIVED from
    /// the '\n' position (blob_header_len is never passed to decode).
    while (true)
    {
        if (in.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS blob envelope: pad zone has no '\\n' terminator");
        const char c = *in.position();
        ++in.position();
        if (c == '\n')
        {
            h.header_len = static_cast<uint32_t>(in.count());
            break;
        }
        if (c != ' ')
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS blob envelope: non-space byte 0x{:02x} in the header pad zone", static_cast<unsigned>(static_cast<unsigned char>(c)));
    }

    return h;
}

}
