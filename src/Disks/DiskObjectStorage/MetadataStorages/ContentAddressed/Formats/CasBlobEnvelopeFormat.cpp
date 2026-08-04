#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

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

std::string_view opToWord(ProvenanceOp op)
{
    switch (op)
    {
        case ProvenanceOp::Other:    return "other";
        case ProvenanceOp::Insert:   return "insert";
        case ProvenanceOp::Merge:    return "merge";
        case ProvenanceOp::Mutation: return "mutation";
        case ProvenanceOp::Attach:   return "attach";
        case ProvenanceOp::Repack:   return "repack";
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob envelope: unknown ProvenanceOp {}", static_cast<int>(op));
}

ProvenanceOp opFromWord(std::string_view w)
{
    if (w == "other")    return ProvenanceOp::Other;
    if (w == "insert")   return ProvenanceOp::Insert;
    if (w == "merge")    return ProvenanceOp::Merge;
    if (w == "mutation") return ProvenanceOp::Mutation;
    if (w == "attach")   return ProvenanceOp::Attach;
    if (w == "repack")   return ProvenanceOp::Repack;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS blob envelope: unknown op '{}'", w);
}

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

String encodeEnvelopeHeader(EnvelopeHeader & header, uint32_t blob_header_len)
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
        writeKey(buf, "type", first); writeStringValue(buf, kBlobType);
        writeKey(buf, "v", first);    writeIntText(currentCompatibilityVersion(), buf);
        writeKey(buf, "tag", first);  writeHex128Value(buf, header.incarnation_tag);
        writeKey(buf, "bld", first);  writeHex128Value(buf, header.build_id);
        if (header.provenance)
        {
            writeKey(buf, "ts", first); writeIntText(header.provenance->created_at_ms, buf);
            writeKey(buf, "by", first); writeHex128Value(buf, header.provenance->creator_server_id);
            writeKey(buf, "op", first); writeStringValue(buf, opToWord(header.provenance->op));
            writeKey(buf, "ch", first); writeIntText(header.provenance->ch_version, buf);
        }
        /// Test-only critical extension: an unknown `!`-key BEFORE `ref`.
        if (header.emit_unknown_critical_key)
        {
            writeKey(buf, "!x", first); writeStringValue(buf, "1");
        }
        json = std::move(buf).take();   /// e.g. {"type":"cas_blob","v":3,...,"ch":26006001   (no ref, no closing brace)
    }

    /// Optional `ref`, truncated to the exact remaining budget. Layout after this block:
    ///   json + `,"ref":` + `"` + <escaped ref, <= budget bytes> + `"` + `}`   must be <= blob_header_len-1
    /// (byte blob_header_len-1 is reserved for '\n'; the pad zone fills the gap with spaces).
    if (header.intended_ref)
    {
        static constexpr std::string_view ref_key = ",\"ref\":";
        /// +3 = opening quote + closing quote + closing brace.
        const size_t fixed = json.size() + ref_key.size() + 3;
        if (blob_header_len < 1 || fixed > static_cast<size_t>(blob_header_len) - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS blob envelope: non-ref fields ({} bytes) do not fit blob_header_len {} before the ref",
                fixed, blob_header_len);
        const size_t budget = (static_cast<size_t>(blob_header_len) - 1) - fixed;
        json += ref_key;
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
        if (key == "type")
        {
            const String t = r.readString();
            if (t != kBlobType)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS blob envelope: object is a '{}', not a '{}'", t, kBlobType);
            saw_type = true;
        }
        else if (key == "v")
        {
            h.compatibility_version = r.readU32Number();
            checkCompatibility(h.compatibility_version, "blob envelope");
            saw_v = true;
        }
        else if (key == "tag")
            h.incarnation_tag = r.readHex128();
        else if (key == "bld")
            h.build_id = r.readHex128();
        else if (key == "ts")
        {
            prov.created_at_ms = r.readU64Number();
            have_prov = true;
        }
        else if (key == "by")
        {
            prov.creator_server_id = r.readHex128();
            have_prov = true;
        }
        else if (key == "op")
        {
            prov.op = opFromWord(r.readString());
            have_prov = true;
        }
        else if (key == "ch")
        {
            prov.ch_version = static_cast<uint32_t>(r.readU64Number());
            have_prov = true;
        }
        else if (key == "ref")
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
