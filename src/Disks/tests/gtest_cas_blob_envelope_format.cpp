#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

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
                        "\"bld\":\"1112131415161718191a1b1c1d1e1f20\",\"ts\":1752537600123,"
                        "\"by\":\"2122232425262728292a2b2c2d2e2f30\",\"op\":\"merge\",\"ch\":26006001,"
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

TEST(CASFormatBattery, BlobEnvelope)
{
    /// The golden is CONSTRUCTED from the hand-pinned json literal (same one FixedLengthAndPadZone
    /// asserts) + the derived pad — NOT self-computed via encodeEnvelopeHeader, which would compare
    /// the encoder to itself and pin nothing.
    const String json = fmt::format(R"({{"type":"cas_blob","v":{},)", currentCompatibilityVersion()) +
                        "\"tag\":\"0102030405060708090a0b0c0d0e0f10\","
                        "\"bld\":\"1112131415161718191a1b1c1d1e1f20\",\"ts\":1752537600123,"
                        "\"by\":\"2122232425262728292a2b2c2d2e2f30\",\"op\":\"merge\",\"ch\":26006001,"
                        "\"ref\":\"t-abc/all_1_2_0\"}";
    const String golden = json + String((L - 1) - json.size(), ' ') + '\n';
    runFormatBattery(FormatBatteryCase{
        .id = FormatId::Blob,
        .encode = [&] { EnvelopeHeader e = sampleHeader("t-abc/all_1_2_0"); return sealObject(FormatId::Blob, encodeEnvelopeHeader(e, L)); },
        .decode = [](std::string_view s) { decodeEnvelopeHeader(String(openObject(FormatId::Blob, s)), s.size(), ObjectKind::Blob); },
        .golden = golden,
        .make_future_version = blobEnvelopeWithFutureVersion});
}
