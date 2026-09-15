#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <IO/ReadBuffer.h>
#include <base/hex.h>
#include <base/itoa.h>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// Shared container mechanics for versioned content-addressed text objects: a header line
/// {"type":"cas_<x>","v":N}, a body, an optional trailer line, and, for formats whose registry
/// policy requires it, one zstd frame around the whole object. This is the only code that knows
/// that container shape; per-object codecs add only key mappings and object-specific invariants.
///
/// Writers produce canonical text without whitespace outside JSON strings, and readers reject such
/// whitespace as `CORRUPTED_DATA`. Values that may span the full u64 range are decimal strings;
/// hashes are 32-character lowercase hexadecimal strings. The JSON writer settings are pinned in
/// the implementation so global `FormatSettings` changes cannot alter CAS bytes: slash-containing
/// ref paths and deterministic artifacts must retain the same representation and golden files.

/// Bulk-append writer for canonical CAS JSON text. Replaces WriteBuffer in every CAS encode
/// path: appends are inline stores into an owned String (no per-call finalized/canceled
/// lifecycle, no per-byte writes, no heap allocations per record). Two usage modes:
/// whole-object assembly (bounded formats; `take` at the end) and line-scratch (RecordStream:
/// assemble one line, bulk-write it to the surrounding WriteBuffer, `clear` — memory stays
/// bounded by the largest line). The JSON escaping semantics of `stringValue` are statically
/// fixed to the CAS canon (forward slashes NOT escaped); process-wide FormatSettings cannot
/// influence CAS bytes.
class CasJsonWriter
{
public:
    explicit CasJsonWriter(size_t reserve_hint = 256)
    {
        buf.reserve(reserve_hint);
    }

    void append(std::string_view s)
    {
        buf.append(s.data(), s.size());
    }

    void appendChar(char c)
    {
        buf.push_back(c);
    }

    /// '{' on the first call, ',' after, then "name": . `name` must be plain ASCII (written raw).
    void key(std::string_view name, bool & first)
    {
        appendChar(first ? '{' : ',');
        first = false;
        appendChar('"');
        append(name);
        append("\":");
    }

    /// Quoted JSON string with full escaping (bulk-run scan). Defined in CasTextFormat.cpp.
    void stringValue(std::string_view s);

    /// A value from a wire VOCABULARY -- an enum table's word or a record tag. Those are drawn from
    /// `[a-z0-9_]` by construction, so this writes the bytes as they are instead of running them
    /// through the escaper's byte scan and state machine. Output is identical to `stringValue` for
    /// every input the contract admits; a caller that passes an arbitrary string is the bug this
    /// asserts against, and `writeWordField` is the only intended way in.
    void wordValue(std::string_view word);

    /// JSON array of canonical word strings, emitted without intermediate storage.
    void wordArray(std::span<const std::string_view> words);

    void u64Number(uint64_t v)
    {
        char digits[24];
        char * end = itoa(v, digits);
        buf.append(digits, static_cast<size_t>(end - digits));
    }

    void u64StringValue(uint64_t v)
    {
        appendChar('"');
        u64Number(v);
        appendChar('"');
    }

    void hex128Value(const UInt128 & v)
    {
        char hex[32];
        writeHexUIntLowercase(v, hex);
        appendChar('"');
        buf.append(hex, sizeof(hex));
        appendChar('"');
    }

    void boolValue(bool v)
    {
        append(v ? std::string_view{"true"} : std::string_view{"false"});
    }

    void closeObject(bool & first)
    {
        if (first)
            appendChar('{');
        first = false;
        appendChar('}');
    }

    void newline()
    {
        appendChar('\n');
    }

    size_t size() const
    {
        return buf.size();
    }

    std::string_view view() const
    {
        return buf;
    }

    void clear()
    {
        buf.clear();
    }

    String take() &&
    {
        return std::move(buf);
    }

private:
    String buf;
};

/// The write-side JSON primitives used by the format codecs. `CasJsonWriter` is the only CAS
/// text writer; every codec assembles its object in one before handing bytes to the underlying
/// `WriteBuffer`.
inline void writeKey(CasJsonWriter & out, std::string_view key, bool & first) { out.key(key, first); }
inline void writeStringValue(CasJsonWriter & out, std::string_view s) { out.stringValue(s); }
inline void writeHex128Value(CasJsonWriter & out, const UInt128 & v) { out.hex128Value(v); }
inline void writeU64StringValue(CasJsonWriter & out, uint64_t v) { out.u64StringValue(v); }
inline void writeBoolValue(CasJsonWriter & out, bool v) { out.boolValue(v); }
inline void closeObject(CasJsonWriter & out, bool & first) { out.closeObject(first); }
/// Argument order mirrors the IO helpers so migrated codecs keep their call shapes.
inline void writeChar(char c, CasJsonWriter & out) { out.appendChar(c); }
inline void writeIntText(uint64_t v, CasJsonWriter & out) { out.u64Number(v); }
void writeHeaderLine(CasJsonWriter & out, FormatId id);
void writeTrailerLine(CasJsonWriter & out, uint64_t n);

/// A wire-key carrier for migrated writer call sites. Its explicit constructor makes a codec pass a
/// named constant, while an inline `WireKey{"..."}` is deliberately loud. `WireKey` borrows its
/// `string_view`; the referenced text must outlive the key, as with string literals and static constants.
struct WireKey
{
    std::string_view text;

    explicit constexpr WireKey(std::string_view text_) : text(text_) {}

    friend constexpr bool operator==(std::string_view s, const WireKey & k) { return s == k.text; }
};

inline void writeKey(CasJsonWriter & out, WireKey key, bool & first)
{
    writeKey(out, key.text, first);
}

/// For a value that comes from a wire vocabulary (an enum table's word, a record tag). It is NOT
/// interchangeable with `writeStringField`: this one promises its value needs no JSON escaping and
/// skips the escaper accordingly, which is why an open string must never be routed through it.
inline void writeWordField(CasJsonWriter & out, WireKey key, std::string_view word, bool & first)
{
    writeKey(out, key, first);
    out.wordValue(word);
}

inline void writeWordArrayField(CasJsonWriter & out, WireKey key, std::span<const std::string_view> words, bool & first)
{
    writeKey(out, key, first);
    out.wordArray(words);
}

inline void writeStringField(CasJsonWriter & out, WireKey key, std::string_view value, bool & first)
{
    writeKey(out, key, first);
    writeStringValue(out, value);
}

inline void writeU64StringField(CasJsonWriter & out, WireKey key, uint64_t value, bool & first)
{
    writeKey(out, key, first);
    writeU64StringValue(out, value);
}

inline void writeNumberField(CasJsonWriter & out, WireKey key, uint64_t value, bool & first)
{
    writeKey(out, key, first);
    out.u64Number(value);
}

inline void writeHex128Field(CasJsonWriter & out, WireKey key, const UInt128 & value, bool & first)
{
    writeKey(out, key, first);
    writeHex128Value(out, value);
}

inline void writeBoolField(CasJsonWriter & out, WireKey key, bool value, bool & first)
{
    writeKey(out, key, first);
    writeBoolValue(out, value);
}

/// True iff `c` is one lowercase hexadecimal digit. Persisted CAS digests deliberately reject
/// uppercase spellings so each digest has one canonical textual representation.
constexpr bool isLowercaseHexChar(char c)
{
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

/// Pull cursor over one canonical JSON object.
///
/// The reader borrows the input buffer and records the object name for exception messages. It
/// enforces unique keys and translates the several low-level parser exceptions into the CAS
/// `CORRUPTED_DATA` contract. Unknown keys follow the supplied evolution policy: ordinary keys
/// may be skipped in tolerant objects, while `!`-prefixed keys always fail with
/// `UNKNOWN_FORMAT_VERSION`.

class JsonObjectReader
{
public:
    /// Consumes the opening `{`; throws `CORRUPTED_DATA` when the object does not start there.
    JsonObjectReader(ReadBuffer & in_, KeyStrictness strictness_, std::string_view what_);

    /// An unbound reader, for a decoder that wants one reader outside its row loop and re-points it
    /// per row. `reset` must be called before any read; nothing else is valid on it.
    JsonObjectReader() = default;

    /// Re-point an existing reader at another object, as the constructor would, but WITHOUT
    /// releasing the buffers it has already grown. A stream decoder reads one object per row, and a
    /// reader built fresh each time re-allocates its seen-key store and its value scratch on every
    /// row; measured on the `cas_run` decode path, allocation accounting is about a fifth of all
    /// instructions executed inside the decoder. Reusing one reader amortises that away. The
    /// object-level state -- the key set and the position in the object -- is reset in full, so a
    /// reused reader accepts and rejects exactly what a fresh one would.
    void reset(ReadBuffer & in_, KeyStrictness strictness_, std::string_view what_);
    /// Advances to the next key; false when the closing '}' was consumed. The caller must
    /// consume the value (one read* / skipUnknown) before the next call. Duplicate keys are
    /// rejected with `CORRUPTED_DATA`.
    bool nextKey(String & key);
    /// Reads the value for the key returned by `nextKey` as a JSON string.
    String readString();
    /// Reads the value for the key returned by `nextKey` as an array of JSON strings.
    std::vector<String> readStringArray();
    /// Reads a quoted 32-character lowercase hexadecimal string as a `UInt128`.
    UInt128 readHex128();
    /// Reads a quoted decimal u64 string and rejects empty, trailing, or non-decimal text.
    uint64_t readU64String();
    /// Reads an unquoted JSON number into a u64; low-level parse failures become `CORRUPTED_DATA`.
    uint64_t readU64Number();
    /// Reads an unquoted JSON number into a u32; rejects a value that would silently narrow.
    uint32_t readU32Number();
    /// Reads the bare JSON literals `true` and `false`.
    bool readBool();
    /// Applies the evolution rule for an unrecognized key: `!`-prefixed keys produce
    /// `UNKNOWN_FORMAT_VERSION`; strict objects produce `CORRUPTED_DATA`; tolerant objects skip
    /// the value.
    void skipUnknown(const String & key);

private:
    /// Runs one parser operation under the CAS error taxonomy while preserving version exceptions.
    template <typename F>
    auto guarded(F && f);

    /// Reads one JSON string into `scratch` and returns a view of it, so a value that is parsed and
    /// discarded -- a hex digest, a decimal counter -- costs no allocation once the scratch has
    /// grown. The view is valid until the next read on this reader.
    std::string_view readStringIntoScratch();

    ReadBuffer * in = nullptr;
    KeyStrictness strictness = KeyStrictness::Strict;
    String what;
    std::vector<String> seen_keys;
    String scratch;
    bool first = true;
    bool done = false;
};

/// Header, trailer, and raw-line access for the common text container.

/// Header metadata returned after parsing a self-describing CAS object header line.
struct TextHeader
{
    String type;
    uint32_t v = 0;
};

/// Reads and gates line 1 against `id`'s registered type; wrong type -> CORRUPTED_DATA; v above
/// what this build understands -> UNKNOWN_FORMAT_VERSION.
TextHeader expectHeaderLine(ReadBuffer & in, FormatId id);
/// Best-effort "is this a CAS object, and which one" for fsck/dispatch: swallows every failure and
/// returns nullopt. Never the load-bearing gate — that is expectHeaderLine.
std::optional<TextHeader> sniffHeaderLine(std::string_view bytes);
/// Reads one line (excluding the '\n' terminator); CORRUPTED_DATA on missing terminator or a line
/// longer than `line_cap`.
/// Read one terminator-delimited line into `line`, replacing its contents and KEEPING its capacity,
/// so a caller streaming many rows can reuse one scratch and stop allocating after the longest line.
void readLineInto(ReadBuffer & in, String & line, uint64_t line_cap, std::string_view what);

/// Allocating form, for callers that read a single line.
String readLine(ReadBuffer & in, uint64_t line_cap, std::string_view what);

/// Position of the next byte `stringValue` treats specially (control byte, '"', '\\', or the
/// 0xE2 lead byte of the U+2028/U+2029 lookahead), or `end`. Scalar bulk-run scan: the win is on
/// the short hot strings, so SIMD is deferred to a benchmark-gated contingency.
const char * findNextSpecialJsonByte(const char * pos, const char * end);

/// Zstd detection and per-format compression policy.

/// True iff `bytes` starts with the zstd frame prefix 28 B5 2F FD.
bool looksZstd(std::string_view bytes);
/// Compression per the per-type policy: `Always` -> one zstd frame (any size, checksum on);
/// everything else -> identity (returns `text` unchanged).
String sealObject(FormatId id, String text);
/// Inverse of sealObject. A compressed body is only legal when `id`'s policy is `Always`
/// (declared content size checked against the cap before allocation); a raw body is accepted
/// (repair path — e.g. an operator-restored uncompressed copy) subject to the SAME `object_cap` --
/// skipping compression must never also skip the size cap.
String openObject(FormatId id, std::string_view stored);

}
