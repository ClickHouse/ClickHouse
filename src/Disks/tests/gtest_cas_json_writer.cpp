#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatSettings.h>
#include <random>

using namespace DB;
using namespace DB::Cas;

TEST(CASJsonWriter, KeyValueSequenceMatchesCanonicalShape)
{
    CasJsonWriter w;
    bool first = true;
    w.key("we", first);
    w.u64StringValue(7);
    w.key("mo", first);
    w.u64Number(3);
    w.key("ok", first);
    w.boolValue(true);
    w.key("o", "me", first);
    w.u64StringValue(1);
    w.closeObject(first);
    w.newline();
    EXPECT_EQ(std::move(w).take(), "{\"we\":\"7\",\"mo\":3,\"ok\":true,\"ome\":\"1\"}\n");
}

TEST(CASJsonWriter, EmptyObjectAndClear)
{
    CasJsonWriter w;
    bool first = true;
    w.closeObject(first);
    EXPECT_EQ(w.view(), "{}");
    w.clear();
    EXPECT_EQ(w.size(), 0u);
}

TEST(CASJsonWriter, Hex128MatchesU128ToHex)
{
    const UInt128 v = (UInt128(0x0123456789abcdefULL) << 64) | UInt128(0xfedcba9876543210ULL);
    CasJsonWriter w;
    w.hex128Value(v);
    EXPECT_EQ(std::move(w).take(), "\"" + u128ToHex(v) + "\"");
}

TEST(CASJsonWriter, U64Extremes)
{
    CasJsonWriter w;
    w.u64Number(0);
    w.appendChar(' ');
    w.u64Number(UINT64_MAX);
    EXPECT_EQ(std::move(w).take(), "0 18446744073709551615");
}

namespace
{
String referenceJson(std::string_view s)
{
    DB::FormatSettings settings;
    settings.json.escape_forward_slashes = false;   /// the pinned CAS canon
    DB::WriteBufferFromOwnString out;
    DB::writeJSONString(s, out, settings);
    out.finalize();
    return out.str();
}

String writerJson(std::string_view s)
{
    DB::Cas::CasJsonWriter w;
    w.stringValue(s);
    return std::move(w).take();
}
}

TEST(CASJsonWriterEscaping, TargetedCorpusMatchesWriteJSONString)
{
    const std::vector<String> corpus = {
        "",
        "plain_safe_ref_name_20260101_0_1_1_1",
        "roots/pin",                                    /// '/' must stay UNESCAPED
        "quote\"inside", "back\\slash", "both\\\"x",
        String("\b\f\n\r\t"),
        String(1, '\0'), String("a") + '\0' + "b",
        String("\x01\x02\x03\x1e\x1f"),
        "\xE2\x80\xA8", "\xE2\x80\xA9",                 /// U+2028 / U+2029 ->   /
        "x\xE2\x80\xA8" "y", // NOLINT(bugprone-suspicious-missing-comma): deliberate adjacent-literal concatenation, testing a U+2028 sequence split across two source literals
        "\xE2",                                          /// truncated lead byte at end
        "\xE2\x80",                                      /// truncated pair at end
        "\xE2\x21\x21",                                  /// 0xE2 + non-continuation bytes
        "\xE2\x80\x21",
        "\xE2\xE2\x80\xA8",                              /// lead byte immediately before a real sequence
        "\xC3\xA9\xF0\x9F\x98\x80",                      /// ordinary multi-byte UTF-8 passes through
        "\xff\xfe invalid utf8 \x80",
        String(1000, 'a'),                               /// long safe run (vector path)
        String(1000, '"'),                               /// special-dense
    };
    for (const String & s : corpus)
        EXPECT_EQ(writerJson(s), referenceJson(s)) << "input bytes: " << s.size();
}

TEST(CASJsonWriterEscaping, FuzzMatchesWriteJSONString)
{
    std::mt19937 rng(20260720); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    for (int iter = 0; iter < 5000; ++iter)
    {
        const size_t len = rng() % 200;
        String s(len, '\0');
        const int mode = iter % 3;
        for (auto & c : s)
        {
            if (mode == 0)
                c = static_cast<char>(rng() % 256);                     /// full byte range
            else if (mode == 1)
                c = static_cast<char>('a' + rng() % 26);                /// safe-only
            else
            {
                static constexpr char specials[] = {'"', '\\', '\n', '\x01', '\xE2', '\x80', '\xA8', 'z'};
                c = specials[rng() % (sizeof(specials))];               /// special-dense
            }
        }
        ASSERT_EQ(writerJson(s), referenceJson(s)) << "iter " << iter;
    }
}

/// ---- CasJsonWriter overloads of the shared vocabulary (Task 4) ----
///
/// The production WriteBuffer vocabulary was retired in Task 9 (CasJsonWriter is now the only CAS
/// text writer). `reference_vocab` below is a verbatim copy of the retired implementation, kept
/// test-local so these differential tests keep an independent oracle instead of comparing
/// CasJsonWriter against itself.
namespace reference_vocab
{
namespace
{
/// Verbatim copy of the retired WriteBuffer-based CAS vocabulary (CasTextFormat.cpp pre-CasJsonWriter),
/// kept as the differential reference. jsonWriteSettings is inlined: escape_forward_slashes=false.
const DB::FormatSettings & settings()
{
    static const DB::FormatSettings s = []
    {
        DB::FormatSettings fs;
        fs.json.escape_forward_slashes = false;
        return fs;
    }();
    return s;
}

void writeKey(DB::WriteBuffer & out, std::string_view key, bool & first)
{
    DB::writeChar(first ? '{' : ',', out);
    first = false;
    DB::writeChar('"', out);
    out.write(key.data(), key.size());
    DB::writeChar('"', out);
    DB::writeChar(':', out);
}

void writeStringValue(DB::WriteBuffer & out, std::string_view s) { DB::writeJSONString(s, out, settings()); }

void writeHex128Value(DB::WriteBuffer & out, const UInt128 & v)
{
    DB::writeChar('"', out);
    const String hex = DB::Cas::u128ToHex(v);
    out.write(hex.data(), hex.size());
    DB::writeChar('"', out);
}

void writeU64StringValue(DB::WriteBuffer & out, uint64_t v)
{
    DB::writeChar('"', out);
    DB::writeIntText(v, out);
    DB::writeChar('"', out);
}

void writeBoolValue(DB::WriteBuffer & out, bool v) { writeCString(v ? "true" : "false", out); }

void closeObject(DB::WriteBuffer & out, bool & first)
{
    if (first)
        DB::writeChar('{', out);
    first = false;
    DB::writeChar('}', out);
}
}
}

TEST(CASJsonWriterVocab, MatchesReferenceVocabulary)
{
    using namespace DB::Cas;
    const UInt128 h = (UInt128(0xdeadbeefULL) << 64) | UInt128(42);

    DB::WriteBufferFromOwnString ref;
    CasJsonWriter w;
    bool rf = true;
    bool wf = true;

    reference_vocab::writeKey(ref, "a", rf);           writeKey(w, "a", wf);
    reference_vocab::writeStringValue(ref, "x/\"y");   writeStringValue(w, "x/\"y");
    reference_vocab::writeKey(ref, "h", rf);           writeKey(w, "h", wf);
    reference_vocab::writeHex128Value(ref, h);         writeHex128Value(w, h);
    reference_vocab::writeKey(ref, "u", rf);           writeKey(w, "u", wf);
    reference_vocab::writeU64StringValue(ref, UINT64_MAX); writeU64StringValue(w, UINT64_MAX);
    reference_vocab::writeKey(ref, "b", rf);           writeKey(w, "b", wf);
    reference_vocab::writeBoolValue(ref, false);       writeBoolValue(w, false);
    reference_vocab::writeKey(ref, "n", rf);           writeKey(w, "n", wf);
    DB::writeIntText(uint64_t(12345), ref); writeIntText(uint64_t(12345), w);
    reference_vocab::closeObject(ref, rf);             closeObject(w, wf);
    DB::writeChar('\n', ref);         writeChar('\n', w);
    ref.finalize();
    EXPECT_EQ(std::move(w).take(), ref.str());
}
