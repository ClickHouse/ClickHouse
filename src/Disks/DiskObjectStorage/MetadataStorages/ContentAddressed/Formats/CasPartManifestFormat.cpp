#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <city.h>
#include <algorithm>
#include <optional>

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

std::string_view placementToWord(EntryPlacement p)
{
    switch (p)
    {
        case EntryPlacement::Inline: return "inline";
        case EntryPlacement::Blob:   return "blob";
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: unknown placement {}", static_cast<int>(p));
}

EntryPlacement placementFromWord(std::string_view w)
{
    if (w == "inline") return EntryPlacement::Inline;
    if (w == "blob")   return EntryPlacement::Blob;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: unknown placement '{}'", w);
}

/// One entry-record line: {"p","pm", then either the Blob's "ha"/"h"/"sz" or the Inline's "il"}.
void writeEntryRecord(CasJsonWriter & out, const ManifestEntry & e)
{
    bool first = true;
    writeKey(out, "p", first);
    writeStringValue(out, e.path);
    writeKey(out, "pm", first);
    writeStringValue(out, placementToWord(e.placement));
    if (e.placement == EntryPlacement::Blob)
    {
        writeBlobRefFields(out, first, e.ref);   /// ha + h
        writeKey(out, "sz", first);
        writeIntText(e.blob_size, out);
    }
    else
    {
        writeKey(out, "il", first);
        writeIntText(e.inline_bytes.size(), out);
    }
    closeObject(out, first);
    writeChar('\n', out);
}

/// The path is written into the banner through the SAME escaper the entry-record line uses. It has to be
/// the same one: the decoder rebuilds this banner from the path it read out of the record line and
/// compares byte-wise, so any spelling difference between the two writers is an object the writer cannot
/// read back. Concatenating the path raw here is what made a part-file path containing a newline
/// undecodable -- the LF split this line, and no reader could match it again.
String bannerFor(std::string_view path, uint64_t n)
{
    CasJsonWriter w(path.size() + 32);
    w.append("==> ");
    w.stringValue(path);
    w.append(" il=");
    w.u64Number(n);
    w.append(" <==");
    return std::move(w).take();
}

}

String encodePartManifest(const PartManifest & m)
{
    /// Canonical path order plus duplicate-path rejection makes the encoded record sequence
    /// deterministic and establishes the ordering required by the lookup helpers.
    std::vector<const ManifestEntry *> sorted;
    sorted.reserve(m.entries.size());
    for (const auto & e : m.entries)
        sorted.push_back(&e);
    std::sort(sorted.begin(), sorted.end(),
              [](const ManifestEntry * a, const ManifestEntry * b) { return a->path < b->path; });
    for (size_t i = 1; i < sorted.size(); ++i)
        if (sorted[i]->path == sorted[i - 1]->path)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: duplicate path '{}'", sorted[i]->path);

    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::PartManifest);

    /// descriptor meta line: ManifestRef (me/mb/mo, shared rendering with refsnaplog) + root
    /// namespace + payload digest.
    {
        bool first = true;
        writeManifestRefFields(out, first, "", m.ref);
        writeKey(out, "ns", first);
        writeStringValue(out, m.root_namespace_id.string());
        writeKey(out, "pd", first);
        writeHex128Value(out, m.payload_digest);
        closeObject(out, first);
        writeChar('\n', out);
    }

    for (const ManifestEntry * e : sorted)
        writeEntryRecord(out, *e);

    writeTrailerLine(out, sorted.size());

    /// payload zone: one banner + raw bytes + '\n' per Inline entry, in path order. Blob entries
    /// carry no payload-zone bytes (their bytes live in a separately addressed CAS blob).
    for (const ManifestEntry * e : sorted)
    {
        if (e->placement != EntryPlacement::Inline)
            continue;
        const String banner = bannerFor(e->path, e->inline_bytes.size());
        out.append(banner);
        writeChar('\n', out);
        out.append(e->inline_bytes);
        writeChar('\n', out);
    }

    return std::move(out).take();
}

PartManifest decodePartManifest(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::PartManifest);
    const uint64_t line_cap = traitsFor(FormatId::PartManifest).line_cap;

    PartManifest m;

    /// descriptor meta line
    {
        const String meta = readLine(in, line_cap, "cas_part_manifest");
        ReadBufferFromMemory mm(meta.data(), meta.size());
        JsonObjectReader r(mm, KeyStrictness::Tolerant, "cas_part_manifest");
        std::optional<uint64_t> me;
        std::optional<uint64_t> mb;
        std::optional<uint64_t> mo;
        std::optional<String> ns;
        std::optional<UInt128> pd;
        String key;
        while (r.nextKey(key))
        {
            if (key == "me") me = r.readU64String();
            else if (key == "mb") mb = r.readU64String();
            else if (key == "mo") mo = r.readU64Number();
            else if (key == "ns") ns = r.readString();
            else if (key == "pd") pd = r.readHex128();
            else r.skipUnknown(key);
        }
        if (!me || !mb || !mo)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: descriptor missing me/mb/mo");
        if (!ns)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: descriptor missing ns");
        if (!pd)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: descriptor missing pd");
        m.ref = manifestRefFromFields(*me, *mb, *mo, "PartManifest", "descriptor");
        m.root_namespace_id = RootNamespace(*ns);
        m.payload_digest = *pd;
        if (!mm.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: junk after descriptor line");
    }

    /// entry record lines, until the trailer. Inline entries remember their declared `il` length so
    /// the payload zone below can read exactly that many raw bytes back into `inline_bytes`.
    /// Index-aligned with `m.entries` (Blob entries push an unused 0 placeholder).
    std::vector<uint64_t> inline_lens;
    while (true)
    {
        const String line = readLine(in, line_cap, "cas_part_manifest");
        ReadBufferFromMemory l(line.data(), line.size());
        JsonObjectReader r(l, KeyStrictness::Tolerant, "cas_part_manifest");
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: empty line");

        if (key == "n")
        {
            const uint64_t declared_n = r.readU64Number();
            while (r.nextKey(key))
                r.skipUnknown(key);
            if (!l.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: junk after trailer");
            if (declared_n != m.entries.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "PartManifest: trailer count {} != {} records", declared_n, m.entries.size());
            break;
        }

        if (key != "p")
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: record must start with \"p\"");
        ManifestEntry e;
        e.path = r.readString();

        /// Manifest bytes arrive over the interserver relink channel: enforce the same path hygiene
        /// as CasLayout::checkNamespace so no future consumer can inherit a traversal. Relative,
        /// no empty/'.'/'..' segments. (Syntactic only — legal projection subdirs pass.)
        if (e.path.empty() || e.path.front() == '/')
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS part manifest: invalid entry path '{}'", e.path);
        for (std::string_view rest = e.path; !rest.empty();)
        {
            const size_t slash = rest.find('/');
            const std::string_view seg = rest.substr(0, slash);
            if (seg.empty() || seg == "." || seg == "..")
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS part manifest: invalid entry path '{}'", e.path);
            rest = (slash == std::string_view::npos) ? std::string_view{} : rest.substr(slash + 1);
        }

        std::optional<String> pm;
        std::optional<String> ha;
        std::optional<String> h;
        std::optional<uint64_t> sz;
        std::optional<uint64_t> il;
        while (r.nextKey(key))
        {
            if (key == "pm") pm = r.readString();
            else if (key == "ha") ha = r.readString();
            else if (key == "h") h = r.readString();
            else if (key == "sz") sz = r.readU64Number();
            else if (key == "il") il = r.readU64Number();
            else r.skipUnknown(key);
        }
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: junk after record");
        if (!pm)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: entry '{}' missing pm", e.path);
        e.placement = placementFromWord(*pm);

        if (e.placement == EntryPlacement::Blob)
        {
            if (!ha || !h || !sz)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: blob entry '{}' missing ha/h/sz", e.path);
            const BlobHashAlgo algo = blobHashAlgoFromWord(*ha, "PartManifest entry");
            /// Validate the digest width before calling `fromHex`. A width mismatch otherwise
            /// produces `BAD_ARGUMENTS` instead of the `CORRUPTED_DATA` required for malformed
            /// serialized input, allowing an invalid manifest to escape the decoder's fail-closed
            /// error contract.
            const uint64_t expected_hex_len = blobHashLenFor(algo) * 2;
            if (h->size() != expected_hex_len)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "PartManifest: entry '{}' digest hex width {} does not match algo width {}",
                    e.path, h->size(), expected_hex_len);
            e.ref = BlobRef{algo, codecFor(algo).fromHex(*h)};
            e.blob_size = *sz;
            inline_lens.push_back(0);   /// unused for Blob; keeps inline_lens index-aligned with entries
        }
        else
        {
            if (!il)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: inline entry '{}' missing il", e.path);
            inline_lens.push_back(*il);   /// bytes filled from the payload zone below
        }

        /// Canonical ascending-order and no-duplicate-path enforcement: compare only against the
        /// immediately preceding entry, requiring
        /// strict '<'. This is sufficient to catch a NON-adjacent duplicate too (e.g. forging entry
        /// c's path to equal entry a's path in an a<b<c stream): the forged c is compared against b,
        /// and since originally a<b, the forged c(=a) is NOT greater than b, so the strict check
        /// fires regardless of which earlier entry the duplicate collides with. This also keeps
        /// findEntry/entryRange's binary-search precondition (strictly ascending m.entries) sound.
        if (!m.entries.empty() && !(m.entries.back().path < e.path))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "PartManifest: entries not in canonical ascending order (prev '{}', got '{}')",
                m.entries.back().path, e.path);
        m.entries.push_back(std::move(e));
    }

    /// payload zone: for each Inline entry, in the same order it appeared above, a banner line then
    /// exactly `il` raw bytes then a terminating '\n'.
    for (size_t i = 0; i < m.entries.size(); ++i)
    {
        if (m.entries[i].placement != EntryPlacement::Inline)
            continue;
        const uint64_t n = inline_lens[i];
        const String banner_line = readLine(in, line_cap, "cas_part_manifest payload zone");
        const String expected = bannerFor(m.entries[i].path, n);
        if (banner_line != expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "PartManifest: payload-zone banner mismatch, expected '{}', got '{}'", expected, banner_line);
        m.entries[i].inline_bytes = readFixedBytes(in, n);
        const String terminator = readFixedBytes(in, 1);
        if (terminator[0] != '\n')
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "PartManifest: payload-zone chunk for '{}' missing terminating newline", m.entries[i].path);
    }

    if (!in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "PartManifest: trailing bytes after the payload zone");

    /// Recompute and verify `payload_digest` last, over the fully-decoded body. `computePayloadDigest`
    /// builds its own probe copy with payload_digest zeroed before hashing (see below), so calling it
    /// directly here on `m` (whose payload_digest is whatever was read off the wire) is safe and
    /// matches encode's own computation exactly.
    const UInt128 expected_digest = computePayloadDigest(m);
    if (expected_digest != m.payload_digest)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "PartManifest: payload_digest mismatch, expected {}, got {}",
            u128ToHex(expected_digest), u128ToHex(m.payload_digest));

    return m;
}

UInt128 computePayloadDigest(const PartManifest & m)
{
    /// Digest the canonical encoding with payload_digest zeroed, so the digest does not depend on
    /// itself (it would be circular otherwise) and is stable for identical bodies, changing whenever
    /// ref / namespace / entries change. Uses the CAS content-hash primitive (CityHash128) over the
    /// deterministic encodePartManifest bytes - the same primitive blob/tree hashing uses.
    PartManifest probe = m;
    probe.payload_digest = UInt128{};
    const String bytes = encodePartManifest(probe);
    const auto h = CityHash_v1_0_2::CityHash128(bytes.data(), bytes.size());
    return (static_cast<UInt128>(h.high64) << 64) | static_cast<UInt128>(h.low64);
}

bool refMatchesBody(const ManifestRef & journal_ref, const PartManifest & body)
{
    return journal_ref == body.ref;
}

bool manifestNamespaceMatches(const RootNamespace & owning, const PartManifest & body)
{
    return owning == body.root_namespace_id;
}

const ManifestEntry * findEntry(const std::vector<ManifestEntry> & entries, std::string_view path)
{
    const auto it = std::lower_bound(entries.begin(), entries.end(), path,
        [](const ManifestEntry & e, std::string_view p) { return std::string_view(e.path) < p; });
    if (it == entries.end() || std::string_view(it->path) != path)
        return nullptr;
    return &*it;
}

std::pair<const ManifestEntry *, const ManifestEntry *>
entryRange(const std::vector<ManifestEntry> & entries, std::string_view dir_prefix)
{
    if (dir_prefix.empty())
        return {entries.data(), entries.data() + entries.size()};
    /// Every path starting with `dir_prefix` compares >= `dir_prefix`, and prefixed paths form a
    /// contiguous run from the first such position.
    const auto first = std::lower_bound(entries.begin(), entries.end(), dir_prefix,
        [](const ManifestEntry & e, std::string_view p) { return std::string_view(e.path) < p; });
    auto last = first;
    while (last != entries.end() && std::string_view(last->path).starts_with(dir_prefix))
        ++last;
    return {entries.data() + (first - entries.begin()), entries.data() + (last - entries.begin())};
}

}
