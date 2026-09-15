#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasEnvelopeLimits.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

#include <magic_enum.hpp>

#include <limits>
#include <stdexcept>

using namespace DB::Cas;

namespace DB::ErrorCodes { extern const int CORRUPTED_DATA; extern const int UNKNOWN_FORMAT_VERSION; }

namespace
{
EnvelopeHeader sampleHeader(const String & ref)
{
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = hexToU128("0102030405060708090a0b0c0d0e0f10");
    h.build_id = hexToU128("1112131415161718191a1b1c1d1e1f20");
    h.provenance = Provenance{1752537600123ULL, hexToU128("2122232425262728292a2b2c2d2e2f30"), 26006001u, ProvenanceOp::Merge};
    h.intended_ref = ref;
    return h;
}
constexpr uint32_t L = 256;

/// The `op` word with the most bytes on the wire, found by walking the enum through the REAL
/// public encoder-facing lookup (never by hardcoding "mutation") so a future longer word is
/// automatically picked up by the boundary tests below.
ProvenanceOp longestProvenanceOp()
{
    ProvenanceOp best = ProvenanceOp::Other;
    size_t best_len = 0;
    for (const auto op : magic_enum::enum_values<ProvenanceOp>())
    {
        const size_t len = provenanceOpToWireWord(op).size();
        if (len > best_len)
        {
            best_len = len;
            best = op;
        }
    }
    return best;
}

/// A header whose numeric provenance fields sit at their type maxima (`created_at_ms` at the
/// `uint64_t` max, `ch_version` at the `uint32_t` max, `op` at its longest wire word), so the
/// non-`ref` JSON this produces is the largest `encodeEnvelopeHeader` can emit for real field
/// values. `v` is not settable this way -- `encodeEnvelopeHeader` always stamps
/// `currentCompatibilityVersion()` -- so this is the worst case reachable through the real encoder
/// today, not the type-level bound `kMandatoryDescriptorWorstCase` proves for a hypothetical future
/// `v` at its own `uint32_t` maximum.
EnvelopeHeader maxReachableHeader(const String & ref)
{
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = hexToU128("0102030405060708090a0b0c0d0e0f10");
    h.build_id = hexToU128("1112131415161718191a1b1c1d1e1f20");
    h.provenance = Provenance{
        std::numeric_limits<uint64_t>::max(),
        hexToU128("2122232425262728292a2b2c2d2e2f30"),
        std::numeric_limits<uint32_t>::max(),
        longestProvenanceOp()};
    h.intended_ref = ref;
    return h;
}

/// The envelope has a fixed physical length. At generation 9 there is no unsupported one-digit
/// version, so replacing `9` with `10` must consume one byte from the space pad rather than silently
/// turning the 256-byte fixture into a different wire shape.
String blobEnvelopeWithFutureVersion(std::string_view text)
{
    const String v_now = fmt::format("\"v\":{}", currentCompatibilityVersion());
    const String v_next = fmt::format("\"v\":{}", currentCompatibilityVersion() + 1);
    String future(text);
    const size_t version_pos = future.find(v_now);
    if (version_pos == String::npos || v_next.size() < v_now.size())
        throw std::logic_error("blob-envelope future-version fixture cannot locate the current version");

    future.replace(version_pos, v_now.size(), v_next);
    const size_t growth = v_next.size() - v_now.size();
    const size_t newline_pos = future.find('\n');
    if (newline_pos == String::npos || newline_pos < growth
        || future.substr(newline_pos - growth, growth) != String(growth, ' '))
        throw std::logic_error("blob-envelope future-version fixture has insufficient padding");
    future.erase(newline_pos - growth, growth);
    return future;
}
}

TEST(CASBlobEnvelopeFormat, FixedLengthAndPadZone)
{
    EnvelopeHeader h = sampleHeader("t-abc/all_1_2_0");
    const String head = encodeEnvelopeHeader(h, L);
    ASSERT_EQ(head.size(), L);                       /// exactly blob_header_len
    EXPECT_EQ(head[L - 1], '\n');                     /// terminator at byte 255
    const String json = fmt::format(R"({{"type":"cas_blob","v":{},)", currentCompatibilityVersion()) +
                        "\"tag\":\"0102030405060708090a0b0c0d0e0f10\","
                        "\"build\":\"1112131415161718191a1b1c1d1e1f20\",\"time_ms\":1752537600123,"
                        "\"creator\":\"2122232425262728292a2b2c2d2e2f30\",\"op\":\"merge\",\"chver\":26006001,"
                        "\"ref\":\"t-abc/all_1_2_0\"}";
    ASSERT_LT(json.size(), L);
    EXPECT_EQ(head.substr(0, json.size()), json);                        /// '/' UNescaped (local escaper)
    EXPECT_EQ(head.substr(json.size(), (L - 1) - json.size()), String((L - 1) - json.size(), ' ')); /// pad = spaces
    /// round-trip
    const EnvelopeHeader back = decodeEnvelopeHeader(head, head.size(), ObjectKind::Blob);
    EXPECT_EQ(back.incarnation_tag, h.incarnation_tag);
    EXPECT_EQ(back.build_id, h.build_id);
    ASSERT_TRUE(back.provenance.has_value());
    EXPECT_EQ(back.provenance->created_at_ms, 1752537600123ULL);
    EXPECT_EQ(back.provenance->ch_version, 26006001u);
    EXPECT_EQ(back.provenance->op, ProvenanceOp::Merge);
    ASSERT_TRUE(back.intended_ref.has_value());
    EXPECT_EQ(*back.intended_ref, "t-abc/all_1_2_0");
    EXPECT_EQ(back.header_len, L);
    EXPECT_EQ(payloadOffset(back), L);
}

TEST(CASBlobEnvelopeFormat, RefTruncatedToExactBudget)
{
    /// A 200-char ref cannot fit; it is truncated so the header is EXACTLY 256 bytes and the pad holds.
    EnvelopeHeader h = sampleHeader(String(200, 'a'));
    const String head = encodeEnvelopeHeader(h, L);
    ASSERT_EQ(head.size(), L);
    EXPECT_EQ(head[L - 1], '\n');
    const EnvelopeHeader back = decodeEnvelopeHeader(head, head.size(), ObjectKind::Blob);
    ASSERT_TRUE(back.intended_ref.has_value());
    /// Budget is deterministic. Compute json_len for the SAME header with an empty ref; each extra 'a'
    /// is one escaped byte, so the truncated 'a' count is exactly (L-1) - json_len(empty ref).
    EnvelopeHeader probe = sampleHeader("");
    const String empty_ref_head = encodeEnvelopeHeader(probe, L);
    const size_t json_len_empty = empty_ref_head.find_last_not_of(' ', (L - 1) - 1) + 1;
    const size_t budget = (L - 1) - json_len_empty;
    EXPECT_EQ(back.intended_ref->size(), budget) << "ref truncated to the exact byte budget";
    for (char c : *back.intended_ref)
        EXPECT_EQ(c, 'a');
}

TEST(CASBlobEnvelopeFormat, MandatoryWorstCaseBoundary)
{
    /// `kMandatoryDescriptorWorstCase` (239, proven at compile time against the 240 floor) assumes
    /// `v` at its OWN type maximum (10 digits), because `currentCompatibilityVersion()` could grow
    /// with a future generation. Nothing can make a running build emit that many digits today --
    /// `encodeEnvelopeHeader` always stamps the CURRENT `currentCompatibilityVersion()`, one digit at
    /// this generation -- so the worst case reachable through the real encoder right now is 9 bytes
    /// smaller: a 10-byte `ref` budget at the floor, not 1. That 9-byte gap is exactly
    /// `kMaxU32DecimalLen - digit count of the current compatibility version`, so a generation that
    /// reaches two digits narrows it and this expectation must be re-derived then -- the literal below
    /// is deliberate, since deriving it from the version width here would restate the formula the
    /// compile-time bound already owns and prove nothing about the encoder.
    EnvelopeHeader h_floor = maxReachableHeader("");
    const String head_floor = encodeEnvelopeHeader(h_floor, static_cast<uint32_t>(kMinBlobHeaderLen));
    ASSERT_EQ(head_floor.size(), kMinBlobHeaderLen);
    EXPECT_EQ(head_floor[kMinBlobHeaderLen - 1], '\n');
    EXPECT_EQ(payloadOffset(decodeEnvelopeHeader(head_floor, head_floor.size(), ObjectKind::Blob)), kMinBlobHeaderLen);
    const size_t json_len_floor = head_floor.find_last_not_of(' ', kMinBlobHeaderLen - 2) + 1;
    const size_t budget_floor = (kMinBlobHeaderLen - 1) - json_len_floor;
    EXPECT_EQ(budget_floor, 10u) << "ref budget reachable through the real encoder at the floor";

    /// The default 256-byte header is exactly 16 bytes above the floor, so the SAME max-reachable
    /// content leaves exactly 16 more bytes of `ref` budget.
    EnvelopeHeader h_default = maxReachableHeader("");
    const String head_default = encodeEnvelopeHeader(h_default, L);
    ASSERT_EQ(head_default.size(), L);
    EXPECT_EQ(head_default[L - 1], '\n');
    EXPECT_EQ(payloadOffset(decodeEnvelopeHeader(head_default, head_default.size(), ObjectKind::Blob)), L);
    const size_t json_len_default = head_default.find_last_not_of(' ', L - 2) + 1;
    const size_t budget_default = (L - 1) - json_len_default;
    EXPECT_EQ(budget_default, budget_floor + (L - kMinBlobHeaderLen))
        << "ref budget reachable through the real encoder at the default header length";
}

/// The half a `static_assert` cannot do. The compile-time bound proves the FORMULA fits under the
/// floor; it cannot notice a formula that understates the encoder — shrink any component and the
/// assert only grows happier. So this reconstructs the same number from bytes the real encoder
/// produced, and the only quantity it borrows is the version field's type width:
///
///   what the encoder wrote at max-width values, with an empty ref
///   + the digits the version field did NOT use at this generation
///   == the mandatory worst case
///
/// Every other field in the fixture is already at its type maximum, so nothing else is missing from
/// the measured side. An understated key cost, or a shrunken `kMaxU32DecimalLen`, moves the formula
/// without moving the encoder and lands here.
TEST(CASBlobEnvelopeFormat, WorstCaseFormulaMatchesTheEncoder)
{
    /// Drive the encoder at the WIDEST version the budget reserves room for, rather than encoding at
    /// today's one-digit version and adding the missing digits by arithmetic. Doing the arithmetic
    /// here would re-derive the very formula this test exists to check, and would never send the
    /// ten-digit boundary through the encoder's own number formatting.
    EnvelopeHeader h = maxReachableHeader("");
    const String head = encodeEnvelopeHeader(h, static_cast<uint32_t>(kMinBlobHeaderLen),
                                             std::numeric_limits<uint32_t>::max());

    /// The mandatory shape is everything up to and including the closing brace, plus the newline the
    /// encoder reserves at the last byte; the padding between them is the ref budget this measures.
    const size_t json_len = head.find_last_not_of(' ', kMinBlobHeaderLen - 2) + 1;
    const size_t mandatory_at_max_version = json_len + 1;   /// + the reserved '\n'

    EXPECT_EQ(mandatory_at_max_version, mandatory_descriptor_worst_case)
        << "the formula and the encoder disagree about the mandatory descriptor at the widest "
           "version: encoder wrote " << mandatory_at_max_version << " bytes, formula says "
        << mandatory_descriptor_worst_case;

    /// And the whole point of the budget: even at that width one byte remains spare under the floor.
    EXPECT_LE(mandatory_descriptor_worst_case, kMinBlobHeaderLen - 1);
}

/// The version really is rendered at its full width by the encoder above, not merely accounted for.
/// Without this, an encoder that silently clamped or dropped the override would still satisfy the
/// equality it feeds.
TEST(CASBlobEnvelopeFormat, MaxWidthVersionIsActuallyRendered)
{
    EnvelopeHeader h = maxReachableHeader("");
    const String head = encodeEnvelopeHeader(h, static_cast<uint32_t>(kMinBlobHeaderLen),
                                             std::numeric_limits<uint32_t>::max());
    EXPECT_NE(head.find("\"v\":4294967295"), String::npos)
        << "the max-width version was not rendered; the boundary above proves nothing. Header: "
        << head;
}

TEST(CASBlobEnvelopeFormat, CriticalKeyDescriptorStillFitsAtDefaultLength)
{
    /// The test-only `!x` critical key is written BEFORE `ref`; even at max-reachable field values
    /// the descriptor still fits the default 256-byte header and fails closed as
    /// UNKNOWN_FORMAT_VERSION, never CORRUPTED_DATA or a LOGICAL_ERROR from encode itself.
    EnvelopeHeader h = maxReachableHeader("r");
    h.emit_unknown_critical_key = true;
    const String head = encodeEnvelopeHeader(h, L);
    ASSERT_EQ(head.size(), L);
    cas_battery_detail::expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION,
        [&] { decodeEnvelopeHeader(head, head.size(), ObjectKind::Blob); },
        "critical-key blob envelope at max-reachable field values");
}

/// Closed-set pin: the six `ProvenanceOp` words, walked through `magic_enum::enum_values`, which is what proves the
/// renderer and the parser consult the SAME table: a table entry missing altogether is already a
/// build error at the coverage assert, but two delegates drifting onto different tables is not.
TEST(CASBlobEnvelopeFormat, ClosedSetPinsProvenanceOpWords)
{
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Other), "other");
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Insert), "insert");
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Merge), "merge");
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Mutation), "mutation");
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Attach), "attach");
    EXPECT_EQ(provenanceOpToWireWord(ProvenanceOp::Repack), "repack");
    for (const auto op : magic_enum::enum_values<ProvenanceOp>())
        EXPECT_EQ(provenanceOpFromWireWord(provenanceOpToWireWord(op)), op);
}

TEST(CASBlobEnvelopeFormat, UnknownOpWordFailsClosed)
{
    /// `op` is written as a plain (non-critical) key, so an unrecognized word is a decode-time
    /// vocabulary violation, not a missing-extension one: CORRUPTED_DATA, not UNKNOWN_FORMAT_VERSION.
    EnvelopeHeader h = sampleHeader("r");
    String head = encodeEnvelopeHeader(h, L);
    const size_t op_at = head.find("\"op\":\"merge\"");
    ASSERT_NE(op_at, String::npos);
    head.replace(op_at, String("\"op\":\"merge\"").size(), "\"op\":\"bogus\"");
    cas_battery_detail::expectCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeEnvelopeHeader(head, head.size(), ObjectKind::Blob); }, "unknown op word");
}

TEST(CASBlobEnvelopeFormat, PadZoneSmugglingFailsClosed)
{
    EnvelopeHeader h = sampleHeader("r");
    const String head = encodeEnvelopeHeader(h, L);
    const size_t json_len = head.find_last_not_of(' ', (L - 1) - 1) + 1; /// first pad byte index = json_len
    ASSERT_LT(json_len, L - 1);
    /// A non-space byte smuggled into the pad zone -> CORRUPTED_DATA.
    String smuggled = head;
    smuggled[json_len + 1] = 'x';
    EXPECT_THROW(decodeEnvelopeHeader(smuggled, smuggled.size(), ObjectKind::Blob), DB::Exception);
    /// Byte 255 not '\n' -> CORRUPTED_DATA.
    String no_nl = head;
    no_nl[L - 1] = ' ';
    EXPECT_THROW(decodeEnvelopeHeader(no_nl, no_nl.size(), ObjectKind::Blob), DB::Exception);
}

TEST(CASBlobEnvelopeFormat, GatesAndCriticalKey)
{
    /// wrong type -> CORRUPTED_DATA; future v -> UNKNOWN_FORMAT_VERSION.
    EnvelopeHeader h = sampleHeader("r");
    const String head = encodeEnvelopeHeader(h, L);
    String wrong_type = head;
    wrong_type.replace(wrong_type.find("cas_blob"), 8, "cas_xxxx");
    EXPECT_THROW(decodeEnvelopeHeader(wrong_type, wrong_type.size(), ObjectKind::Blob), DB::Exception);
    const String current_version = fmt::format("\"v\":{}", currentCompatibilityVersion());
    String future = blobEnvelopeWithFutureVersion(head);
    cas_battery_detail::expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION,
        [&] { decodeEnvelopeHeader(future, future.size(), ObjectKind::Blob); }, "future blob-envelope version");

    String out_of_range = head;
    const size_t out_of_range_version_at = out_of_range.find(current_version);
    ASSERT_NE(out_of_range_version_at, String::npos);
    out_of_range.replace(out_of_range_version_at, current_version.size(), "\"v\":4294967299");
    try
    {
        decodeEnvelopeHeader(out_of_range, out_of_range.size(), ObjectKind::Blob);
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
    /// an unknown `!`-critical key fails closed.
    EnvelopeHeader hc = sampleHeader("r");
    hc.emit_unknown_critical_key = true;
    const String crit = encodeEnvelopeHeader(hc, L);
    EXPECT_THROW(decodeEnvelopeHeader(crit, crit.size(), ObjectKind::Blob), DB::Exception);
}

TEST(CASBlobEnvelopeFormat, RefEscaperAlphabetPinned)
{
    /// Pins the LOCAL escaper's alphabet (§ref-escaper): " and \ escape, control chars -> \uXXXX,
    /// '/' passes VERBATIM. Goes RED if anyone "unifies" this with writeStringValue/FormatSettings —
    /// the 256-byte budget arithmetic depends on this alphabet being codec-owned and frozen.
    EnvelopeHeader h = sampleHeader(String("a/b\"c\\d") + '\x01' + "e");
    const String head = encodeEnvelopeHeader(h, L);
    const String expected_ref_json = R"("a/b\"c\\d\u0001e")";
    EXPECT_NE(head.find("\"ref\":" + expected_ref_json), String::npos)
        << "escaper alphabet drifted: '/' must be verbatim, quote/backslash escaped, control -> \\uXXXX";
}

CAS_BATTERY_COVERS(Blob);

TEST(CASFormatBattery, BlobEnvelope)
{
    /// The golden is CONSTRUCTED from the hand-pinned json literal (same one FixedLengthAndPadZone
    /// asserts) + the derived pad — NOT self-computed via encodeEnvelopeHeader, which would compare
    /// the encoder to itself and pin nothing.
    const String json = fmt::format(R"({{"type":"cas_blob","v":{},)", currentCompatibilityVersion()) +
                        "\"tag\":\"0102030405060708090a0b0c0d0e0f10\","
                        "\"build\":\"1112131415161718191a1b1c1d1e1f20\",\"time_ms\":1752537600123,"
                        "\"creator\":\"2122232425262728292a2b2c2d2e2f30\",\"op\":\"merge\",\"chver\":26006001,"
                        "\"ref\":\"t-abc/all_1_2_0\"}";
    const String golden = json + String((L - 1) - json.size(), ' ') + '\n';
    runFormatBattery(FormatBatteryCase{
        .id = FormatId::Blob,
        .encode = [&] { EnvelopeHeader e = sampleHeader("t-abc/all_1_2_0"); return sealObject(FormatId::Blob, encodeEnvelopeHeader(e, L)); },
        .decode = [](std::string_view s) { decodeEnvelopeHeader(String(openObject(FormatId::Blob, s)), s.size(), ObjectKind::Blob); },
        .golden = golden,
        .make_future_version = blobEnvelopeWithFutureVersion});
}
