#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
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

UInt128 toWideChecksum(CityHash_v1_0_2::uint128 h)
{
    /// Keep the high and low halves in the same order for the write-side helper and the streaming
    /// reader. The value is compared internally rather than exposed as a separately interpreted wire
    /// field, but both paths must use the same packing for a stored run to verify successfully.
    return (static_cast<UInt128>(h.high64) << 64) | static_cast<UInt128>(h.low64);
}

int hexNibble(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

BlobHashAlgo algoFromByte(uint8_t b, std::string_view what)
{
    switch (b)
    {
        case static_cast<uint8_t>(BlobHashAlgo::CityHash128): return BlobHashAlgo::CityHash128;
        case static_cast<uint8_t>(BlobHashAlgo::XXH3_128):    return BlobHashAlgo::XXH3_128;
        case static_cast<uint8_t>(BlobHashAlgo::Sha256):      return BlobHashAlgo::Sha256;
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: unknown algo byte {} in record key", what, b);
    }
}

/// `b` = the algo byte as two lowercase hex chars, then the digest hex at the algo's width. The algo
/// byte leads so that string-sorting `b` reproduces the binary (algo, digest) byte order.
String renderB(const BlobRef & ref)
{
    static constexpr char H[] = "0123456789abcdef";
    const uint8_t a = static_cast<uint8_t>(ref.algo);
    String b;
    b.push_back(H[(a >> 4) & 0xF]);
    b.push_back(H[a & 0xF]);
    b += codecFor(ref.algo).toHex(ref.digest);
    return b;
}

BlobRef parseB(std::string_view b)
{
    if (b.size() < 2)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: record key too short");
    const int hi = hexNibble(b[0]);
    const int lo = hexNibble(b[1]);
    if (hi < 0 || lo < 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: non-hex algo byte in record key");
    const BlobHashAlgo algo = algoFromByte(static_cast<uint8_t>((hi << 4) | lo), "cas_run");
    const std::string_view digest_hex = b.substr(2);
    if (digest_hex.size() != static_cast<size_t>(blobHashLenFor(algo)) * 2)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS cas_run: digest hex width {} does not match algo width {}", digest_hex.size(), blobHashLenFor(algo) * 2);
    BlobRef ref;
    ref.algo = algo;
    ref.digest = codecFor(algo).fromHex(String(digest_hex));
    return ref;
}

std::string_view markerToWord(char m)
{
    switch (m)
    {
        case kEdgeActive: return "edge";
        case kZeroMarker: return "zero";
        case kCondemned:  return "condemned";
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: unknown row marker 0x{:02x}", static_cast<uint8_t>(m));
    }
}

char markerFromWord(std::string_view w)
{
    if (w == "edge")      return kEdgeActive;
    if (w == "zero")      return kZeroMarker;
    if (w == "condemned") return kCondemned;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: unknown row marker '{}'", w);
}

}

void writeRunHeaderLine(WriteBuffer & out, std::string_view kind)
{
    const FormatTraits & t = traitsFor(FormatId::RunFile);
    CasJsonWriter line(64);
    bool first = true;
    writeKey(line, "type", first);
    writeStringValue(line, t.type);
    writeKey(line, "v", first);
    writeIntText(currentCompatibilityVersion(), line);
    writeKey(line, "kind", first);
    writeStringValue(line, kind);
    closeObject(line, first);
    writeChar('\n', line);
    const std::string_view line_view = line.view();
    out.write(line_view.data(), line_view.size());
}

void expectRunHeaderLine(ReadBuffer & in, std::string_view expected_kind)
{
    const FormatTraits & t = traitsFor(FormatId::RunFile);
    const String line = readLine(in, t.line_cap, t.type);
    ReadBufferFromMemory buf(line.data(), line.size());
    JsonObjectReader r(buf, KeyStrictness::Tolerant, t.type);

    String key;
    if (!r.nextKey(key) || key != "type")
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: header line must start with \"type\"");
    const String type = r.readString();
    if (type != t.type)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: object is a '{}', not a '{}'", type, t.type);

    if (!r.nextKey(key) || key != "v")
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: header line must carry \"v\" second");
    const uint32_t v = r.readU32Number();
    checkCompatibility(v, t.type);

    if (!r.nextKey(key) || key != "kind")
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: header line must carry \"kind\" third");
    const String kind = r.readString();
    if (kind != expected_kind)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: unknown run kind '{}'", kind);

    while (r.nextKey(key))
        r.skipUnknown(key);
    if (!buf.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: junk after the header object");
}

SourceEdgeRunWriter::SourceEdgeRunWriter(WriteBuffer & out_)
    : out(out_)
{
    writeRunHeaderLine(out, kSourceEdgeKindWord);
}

void SourceEdgeRunWriter::append(const SourceEdgeRecord & rec)
{
    if (finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS cas_run: append after finish");
    /// Non-decreasing (ref, source_id) is a HARD writer contract (deterministic run + streaming merge).
    /// A regression is a programming bug at the producer, not corrupt on-disk data => LOGICAL_ERROR.
    if (have_prev)
    {
        const bool regressed = (rec.ref < prev_ref)
            || (rec.ref == prev_ref && rec.source_id < prev_source_id);
        if (regressed)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS cas_run: records appended out of (ref, source_id) order");
    }
    have_prev = true;
    prev_ref = rec.ref;
    prev_source_id = rec.source_id;

    scratch.clear();
    bool first = true;
    writeKey(scratch, "b", first);
    writeStringValue(scratch, renderB(rec.ref));
    writeKey(scratch, "s", first);
    writeHex128Value(scratch, rec.source_id);
    writeKey(scratch, "m", first);
    writeStringValue(scratch, markerToWord(rec.marker));
    if (rec.marker == kCondemned)
    {
        writeKey(scratch, "pend", first);
        writeBoolValue(scratch, rec.delete_pending);
        writeTokenFields(scratch, first, rec.token);   /// tt + tv
        writeKey(scratch, "sz", first);
        writeIntText(rec.size, scratch);
        writeKey(scratch, "cr", first);
        writeU64StringValue(scratch, rec.condemn_round);
        writeKey(scratch, "mc", first);
        writeBoolValue(scratch, rec.marker_confirmed);
    }
    closeObject(scratch, first);
    writeChar('\n', scratch);
    {
        const std::string_view scratch_view = scratch.view();
        out.write(scratch_view.data(), scratch_view.size());
    }
    ++count;
}

void SourceEdgeRunWriter::finish()
{
    if (finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CAS cas_run: finish called twice");
    scratch.clear();
    writeTrailerLine(scratch, count);
    const std::string_view scratch_view = scratch.view();
    out.write(scratch_view.data(), scratch_view.size());
    finished = true;
}

UInt128 sourceEdgeRunChecksum(std::string_view stored_bytes)
{
    /// Use the same chained `CityHash128` and default block size as the reader. A one-shot hash would
    /// diverge from the streaming hash for sufficiently large input, so the producer and later fold
    /// must both process exactly the stored bytes through `HashingReadBuffer`.
    ReadBufferFromMemory mem(stored_bytes.data(), stored_bytes.size());
    HashingReadBuffer hashing(mem);
    hashing.ignoreAll();   /// drain the whole object through the hash
    return toWideChecksum(hashing.getHash());
}

SourceEdgeRunReader::SourceEdgeRunReader(ReadBuffer & in_)
    : hashing(in_)
{
    /// Typed open: gate type/v/kind (and hash the header bytes) before any record is interpreted.
    expectRunHeaderLine(hashing, kSourceEdgeKindWord);
}

bool SourceEdgeRunReader::next(SourceEdgeRecord & rec)
{
    if (done)
        return false;

    const String line = readLine(hashing, traitsFor(FormatId::RunFile).line_cap, "cas_run");
    ReadBufferFromMemory line_in(line.data(), line.size());
    JsonObjectReader r(line_in, KeyStrictness::Strict, "cas_run");

    String key;
    if (!r.nextKey(key))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: empty line");

    if (key == "n")
    {
        const uint64_t n = r.readU64Number();
        if (r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: trailer has extra keys");
        if (!line_in.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: junk after trailer object");
        /// The trailer must be the last line of the object; hashing must be at EOF (this also drains and
        /// hashes the final bytes so accumulatedChecksum covers the whole object).
        if (!hashing.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: bytes after trailer");
        if (n != seen)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS cas_run: trailer count {} != {} records (line truncation?)", n, seen);
        done = true;
        return false;
    }

    SourceEdgeRecord out;
    String b;
    String tv;
    bool have_b = false;
    bool have_s = false;
    bool have_m = false;
    bool have_pend = false;
    bool have_tt = false;
    bool have_tv = false;
    bool have_sz = false;
    bool have_cr = false;
    bool have_mc = false;
    TokenType tt{};
    do
    {
        if (key == "b") { b = r.readString(); have_b = true; }
        else if (key == "s") { out.source_id = r.readHex128(); have_s = true; }
        else if (key == "m") { out.marker = markerFromWord(r.readString()); have_m = true; }
        else if (key == "pend") { out.delete_pending = r.readBool(); have_pend = true; }
        else if (key == "tt") { tt = tokenTypeFromWord(r.readString(), "cas_run"); have_tt = true; }
        else if (key == "tv") { tv = r.readString(); have_tv = true; }
        else if (key == "sz") { out.size = r.readU64Number(); have_sz = true; }
        else if (key == "cr") { out.condemn_round = r.readU64String(); have_cr = true; }
        else if (key == "mc") { out.marker_confirmed = r.readBool(); have_mc = true; }
        else r.skipUnknown(key);   /// Strict => any unknown key is CORRUPTED_DATA
    } while (r.nextKey(key));

    if (!have_b || !have_s || !have_m)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: record missing b/s/m");
    out.ref = parseB(b);
    if (out.marker == kCondemned)
    {
        if (!have_pend || !have_tt || !have_tv || !have_sz || !have_cr || !have_mc)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: condemned record missing pend/tt/tv/sz/cr/mc");
        out.token = Token{tv, tt};
    }
    else if (have_pend || have_tt || have_tv || have_sz || have_cr || have_mc)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: non-condemned record carries condemned fields");

    if (!line_in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_run: junk after record object");

    rec = std::move(out);
    ++seen;
    return true;
}

UInt128 SourceEdgeRunReader::accumulatedChecksum()
{
    return toWideChecksum(hashing.getHash());
}

void SourceEdgeRunReader::verifyAgainst(const UInt128 & expected)
{
    const UInt128 got = accumulatedChecksum();
    if (got != expected)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS cas_run: whole-file seal-checksum mismatch (the run bytes do not match the fold seal's "
            "RunRef.checksum); refusing to act on this run");
}

}
