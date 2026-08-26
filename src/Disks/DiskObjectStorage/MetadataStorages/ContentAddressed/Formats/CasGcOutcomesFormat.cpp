#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcOutcomesFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
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

std::string_view outcomeKindToWord(OutcomeKind o)
{
    switch (o)
    {
        case OutcomeKind::Deleted:  return "deleted";
        case OutcomeKind::Absent:   return "absent";
        case OutcomeKind::Replaced: return "replaced";
        case OutcomeKind::Spared:   return "spared";
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: unknown OutcomeKind {}", static_cast<int>(o));
}

OutcomeKind outcomeKindFromWord(std::string_view w)
{
    if (w == "deleted")  return OutcomeKind::Deleted;
    if (w == "absent")   return OutcomeKind::Absent;
    if (w == "replaced") return OutcomeKind::Replaced;
    if (w == "spared")   return OutcomeKind::Spared;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: unknown outcome '{}'", w);
}

}

String encodeOutcomeLog(const OutcomeLog & log)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::GcOutcomes);
    for (const OutcomeEntry & e : log.entries)
    {
        bool first = true;
        writeKey(out, "k", first);
        writeStringValue(out, objectKindToWord(e.kind));
        writeBlobRefFields(out, first, e.ref);   /// ha + h
        writeTokenFields(out, first, e.token);   /// tt + tv
        writeKey(out, "oc", first);
        writeStringValue(out, outcomeKindToWord(e.outcome));
        closeObject(out, first);
        writeChar('\n', out);
    }
    writeTrailerLine(out, log.entries.size());
    return std::move(out).take();
}

OutcomeLog decodeOutcomeLog(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::GcOutcomes);
    const uint64_t line_cap = traitsFor(FormatId::GcOutcomes).line_cap;

    OutcomeLog log;
    while (true)
    {
        const String line = readLine(in, line_cap, "outcome log");
        ReadBufferFromMemory line_in(line.data(), line.size());
        JsonObjectReader r(line_in, KeyStrictness::Tolerant, "outcome log");

        String key;
        /// The first key distinguishes a trailer ("n") from a record ("k").
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: empty line");
        if (key == "n")
        {
            const uint64_t n = r.readU64Number();
            while (r.nextKey(key))
                r.skipUnknown(key);
            if (!line_in.eof() || !in.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: bytes after trailer");
            if (n != log.entries.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS outcome log: trailer count {} != {} records", n, log.entries.size());
            return log;
        }

        OutcomeEntry e;
        String ha;
        String hhex;
        String tv;
        bool have_ha = false;
        bool have_h = false;
        bool have_tt = false;
        TokenType tt{};
        do
        {
            if (key == "k") e.kind = objectKindFromWord(r.readString(), "outcome log");
            else if (key == "ha") { ha = r.readString(); have_ha = true; }
            else if (key == "h") { hhex = r.readString(); have_h = true; }
            else if (key == "tt") { tt = tokenTypeFromWord(r.readString(), "outcome log"); have_tt = true; }
            else if (key == "tv") tv = r.readString();
            else if (key == "oc") e.outcome = outcomeKindFromWord(r.readString());
            else r.skipUnknown(key);
        } while (r.nextKey(key));

        if (!have_ha || !have_h || !have_tt)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: record missing ha/h/tt");
        const BlobHashAlgo algo = blobHashAlgoFromWord(ha, "outcome log");
        /// Validate the digest width before `fromHex`: a width mismatch must surface as the
        /// CORRUPTED_DATA required for malformed serialized input, not fromHex's BAD_ARGUMENTS.
        if (hhex.size() != blobHashLenFor(algo) * 2)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS outcome log: digest width {} does not match algo '{}'", hhex.size(), ha);
        e.ref = BlobRef{algo, codecFor(algo).fromHex(hhex)};
        e.token = Token{tv, tt};
        if (!line_in.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS outcome log: junk after record");
        log.entries.push_back(std::move(e));
    }
}

}
