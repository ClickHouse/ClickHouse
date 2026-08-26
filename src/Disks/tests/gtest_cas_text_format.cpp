#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <fmt/format.h>
#include <algorithm>
#include <set>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{
/// Run `f` and require a DB::Exception with exactly `code`.
template <typename F>
void expectCode(int code, F && f)
{
    try
    {
        f();
        FAIL() << "expected exception code " << code;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), code);
    }
}
}

/// ---- Task 2: FormatId entries for refsnaplog / blob meta / heartbeat ----

TEST(CASFormatIds, NewIdsExistWithFrozenValues)
{
    EXPECT_EQ(static_cast<uint16_t>(FormatId::RefLog), 19);
    EXPECT_EQ(static_cast<uint16_t>(FormatId::RefSnapshot), 20);
    EXPECT_EQ(static_cast<uint16_t>(FormatId::BlobMeta), 21);
    EXPECT_EQ(static_cast<uint16_t>(FormatId::GcHeartbeat), 22);
    /// Every id, old and new, has a change-point ladder (BASELINE until a real bump).
    for (auto id : {FormatId::RefLog, FormatId::RefSnapshot, FormatId::BlobMeta, FormatId::GcHeartbeat})
        EXPECT_FALSE(changePoints(id).empty());
}

/// ---- Task 3: per-format traits registry ----

TEST(CASFormatTraits, CompleteUniqueAndGated)
{
    /// Completeness: every FormatId except the reserved Roster has traits.
    const FormatId all[] = {FormatId::Blob, FormatId::GcState, FormatId::PoolMeta,
                            FormatId::GcOutcomes, FormatId::PartManifest, FormatId::RunFile,
                            FormatId::FoldSeal, FormatId::Owner, FormatId::ServerEpoch, FormatId::MountLease,
                            FormatId::RefLog, FormatId::RefSnapshot, FormatId::BlobMeta, FormatId::GcHeartbeat,
                            FormatId::RefCkpt, FormatId::RefCatalog, FormatId::GcMaintenanceState};
    std::set<std::string_view> types;
    for (FormatId id : all)
    {
        const FormatTraits & t = traitsFor(id);
        EXPECT_EQ(t.id, id);
        EXPECT_TRUE(t.type.starts_with("cas_")) << t.type;
        EXPECT_TRUE(types.insert(t.type).second) << "duplicate type " << t.type;
        EXPECT_EQ(traitsForType(t.type), &t);
    }
    EXPECT_EQ(traitsForType("cas_nope"), nullptr);
#ifndef DEBUG_OR_SANITIZER_BUILD
    /// traitsFor(Roster) throws LOGICAL_ERROR (a reserved/unreachable FormatId), which aborts the
    /// whole process in debug/sanitizer builds instead of behaving like a catchable exception --
    /// CASFormatTraitsDeathTest below proves the abort positively in those builds instead.
    EXPECT_THROW(traitsFor(FormatId::Roster), DB::Exception);
#endif
    /// Deterministic formats are pinned raw + strict; spot-check the two.
    EXPECT_EQ(traitsFor(FormatId::RunFile).compression, CompressionPolicy::PinnedRaw);
    EXPECT_EQ(traitsFor(FormatId::RunFile).strictness, KeyStrictness::Strict);
    EXPECT_EQ(traitsFor(FormatId::FoldSeal).compression, CompressionPolicy::PinnedRaw);
    EXPECT_EQ(traitsFor(FormatId::FoldSeal).strictness, KeyStrictness::Strict);
    /// .zst key suffix is exactly the Always set (can-grow-large types).
    EXPECT_EQ(storedSuffix(FormatId::RefSnapshot), ".zst");
    EXPECT_EQ(storedSuffix(FormatId::RefLog), ".zst");
    EXPECT_EQ(storedSuffix(FormatId::PartManifest), ".zst");
    EXPECT_EQ(storedSuffix(FormatId::GcOutcomes), ".zst");
    EXPECT_EQ(storedSuffix(FormatId::PoolMeta), "");
    EXPECT_EQ(storedSuffix(FormatId::FoldSeal), "");
    EXPECT_EQ(storedSuffix(FormatId::RunFile), "");
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
/// Debug/sanitizer-build counterpart to CompleteUniqueAndGated's Roster check: LOGICAL_ERROR aborts
/// the process here instead of throwing a catchable exception, so the check must be a death test
/// (same pattern as CASBlobDigestDeathTest in gtest_cas_blob_digest.cpp).
TEST(CASFormatTraitsDeathTest, TraitsForRosterAborts)
{
    EXPECT_DEATH({ (void)traitsFor(FormatId::Roster); }, "");
}
#endif

/// ---- Task 4: JSON micro-vocabulary + JsonObjectReader ----

TEST(CASJsonVocab, WriteAndReadBack)
{
    CasJsonWriter out;
    bool first = true;
    writeKey(out, "tag", first);
    writeHex128Value(out, hexToU128("000102030405060708090a0b0c0d0e0f"));
    writeKey(out, "seq", first);
    writeU64StringValue(out, 18446744073709551615ULL);
    writeKey(out, "n", first);
    writeIntText(7, out);
    writeKey(out, "ref", first);
    writeStringValue(out, "t-1/all_1_2_0\n\"quoted\"");
    closeObject(out, first);
    const String rendered = std::move(out).take();
    EXPECT_EQ(rendered.substr(0, 45), R"({"tag":"000102030405060708090a0b0c0d0e0f","se)");

    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Strict, "test");
    String key;
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "tag");
    EXPECT_EQ(r.readHex128(), hexToU128("000102030405060708090a0b0c0d0e0f"));
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "seq");
    EXPECT_EQ(r.readU64String(), 18446744073709551615ULL);
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "n");
    EXPECT_EQ(r.readU64Number(), 7u);
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "ref");
    EXPECT_EQ(r.readString(), "t-1/all_1_2_0\n\"quoted\"");
    EXPECT_FALSE(r.nextKey(key));
}

TEST(CASJsonVocab, FailClosedRules)
{
    auto reader = [](std::string_view text, KeyStrictness s, auto && consume)
    {
        DB::ReadBufferFromMemory in(text.data(), text.size());
        JsonObjectReader r(in, s, "test");
        consume(r);
    };
    /// duplicate key
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"a":1,"a":2})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        while (r.nextKey(k)) r.readU64Number();
    }); });
    /// unknown key: Tolerant skips (nested value), Strict rejects
    reader(R"({"zz":{"deep":[1,2]},"n":5})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
        ASSERT_TRUE(r.nextKey(k)); EXPECT_EQ(r.readU64Number(), 5u);
        EXPECT_FALSE(r.nextKey(k));
    });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"zz":1})", KeyStrictness::Strict, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
    }); });
    /// critical key fails closed regardless of strictness
    expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { reader(R"({"!x":1})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
    }); });
    /// whitespace is not canonical
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({ "a":1})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k);
    }); });
    /// bad hex width / junk in u64 string
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"h":"0102"})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k); r.readHex128();
    }); });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"s":"12x"})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k); r.readU64String();
    }); });
}

/// ---- Task 5: header line, trailer line, readLine ----

TEST(CASTextHeader, WriteExpectSniffGate)
{
    CasJsonWriter out;
    writeHeaderLine(out, FormatId::PoolMeta);
    const String rendered = std::move(out).take();
    EXPECT_EQ(rendered, fmt::format("{{\"type\":\"cas_pool_meta\",\"v\":{}}}\n", currentCompatibilityVersion()));

    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    const TextHeader h = expectHeaderLine(in, FormatId::PoolMeta);
    EXPECT_EQ(h.type, "cas_pool_meta");
    EXPECT_EQ(h.v, currentCompatibilityVersion());
    EXPECT_TRUE(in.eof());

    const auto sniffed = sniffHeaderLine(rendered);
    ASSERT_TRUE(sniffed.has_value());
    EXPECT_EQ(sniffed->type, "cas_pool_meta");
    EXPECT_FALSE(sniffHeaderLine("PAR1 not a cas object").has_value());

    /// wrong type -> CORRUPTED_DATA; future v -> UNKNOWN_FORMAT_VERSION
    /// `v:3` is deliberate and must NOT follow a future `G_BUILD` bump: any version <= G_BUILD passes
    /// the header gate, which is the point — the BODY is what has to fail here.
    const String wrong = "{\"type\":\"cas_owner\",\"v\":3}\n";
    DB::ReadBufferFromMemory in2(wrong.data(), wrong.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { expectHeaderLine(in2, FormatId::PoolMeta); });
    const String future = fmt::format("{{\"type\":\"cas_pool_meta\",\"v\":{}}}\n", currentCompatibilityVersion() + 1);
    DB::ReadBufferFromMemory in3(future.data(), future.size());
    expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { expectHeaderLine(in3, FormatId::PoolMeta); });

    const String out_of_range = "{\"type\":\"cas_pool_meta\",\"v\":4294967299}\n";
    DB::ReadBufferFromMemory in4(out_of_range.data(), out_of_range.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { expectHeaderLine(in4, FormatId::PoolMeta); });
}

TEST(CASTextLines, ReadLineAndTrailer)
{
    CasJsonWriter out;
    writeTrailerLine(out, 42);
    EXPECT_EQ(std::move(out).take(), "{\"n\":42}\n");

    const String two = "abc\ndef\n";
    DB::ReadBufferFromMemory in(two.data(), two.size());
    EXPECT_EQ(readLine(in, 16, "test"), "abc");
    EXPECT_EQ(readLine(in, 16, "test"), "def");
    /// missing terminator and over-cap both fail closed
    const String noterm = "abc";
    DB::ReadBufferFromMemory in2(noterm.data(), noterm.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { readLine(in2, 16, "test"); });
    DB::ReadBufferFromMemory in3(two.data(), two.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { readLine(in3, 2, "test"); });
}

/// ---- Task 6: the zstd arm ----

TEST(CASZstdArm, SealOpenPolicyAndCaps)
{
    /// Always types compress regardless of size (no threshold — the .zst key must be
    /// constructible without knowing the body); a raw body is still readable (repair path).
    /// `v:3` here is NOT the "any version <= G_BUILD passes" case the other negative bodies rely on:
    /// `cas_ref_snap`'s own `changePoints` floor is generation 4, so a generation-3 ref snapshot is not
    /// readable by this build in principle. It passes the header gate only because nothing consults
    /// `changePoints` at decode time yet -- the gate is `v > G_BUILD` alone. Once a per-class floor is
    /// wired in, this literal must move to `G_BUILD`; the test's subject is the truncated BODY, not the
    /// version.
    const String small = "{\"type\":\"cas_ref_snap\",\"v\":3}\n{}\n";
    const String sealed_small = sealObject(FormatId::RefSnapshot, small);
    ASSERT_TRUE(looksZstd(sealed_small));
    EXPECT_EQ(openObject(FormatId::RefSnapshot, sealed_small), small);
    EXPECT_EQ(openObject(FormatId::RefSnapshot, small), small);

    String big = "{\"type\":\"cas_ref_snap\",\"v\":3}\n{\"pad\":\"";
    big += String(8192, 'a');
    big += "\"}\n";
    const String sealed = sealObject(FormatId::RefSnapshot, big);
    ASSERT_TRUE(looksZstd(sealed));
    EXPECT_LT(sealed.size(), big.size());
    EXPECT_EQ(openObject(FormatId::RefSnapshot, sealed), big);

    /// Never and PinnedRaw formats never compress on write and reject compressed input on read.
    EXPECT_EQ(sealObject(FormatId::FoldSeal, big), big);
    EXPECT_EQ(sealObject(FormatId::PoolMeta, big), big);
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { openObject(FormatId::FoldSeal, sealed); });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { openObject(FormatId::PoolMeta, sealed); });

    /// Declared content size over the cap fails BEFORE the output allocation: 65 MiB of text
    /// against RefSnapshot's 64 MiB cap (compresses to ~nothing, so the test is cheap on disk
    /// bytes; the 65 MiB source string is the only big allocation).
    const String over(65 * 1024 * 1024, 'b');
    const String sealed_over = sealObject(FormatId::RefSnapshot, over);
    ASSERT_TRUE(looksZstd(sealed_over));
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { openObject(FormatId::RefSnapshot, sealed_over); });

    /// A flipped byte inside the frame is caught by zstd (frame checksum is on).
    String corrupted = sealed;
    corrupted[corrupted.size() / 2] ^= 0x01;
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { openObject(FormatId::RefSnapshot, corrupted); });
}

TEST(CASTextValueEscaping, ForwardSlashPinnedUnescaped)
{
    /// Goes RED if the global escape_forward_slashes default ever leaks back into CAS string values.
    /// CAS values are dense with '/' (ref-paths, fold-seal keys); their bytes must be CAS-owned so
    /// cas_fold_seal byte-determinism and every golden text file are independent of the global default.
    CasJsonWriter out;
    writeStringValue(out, "ns/shard/all_1_2_0");
    EXPECT_EQ(std::move(out).take(), "\"ns/shard/all_1_2_0\"");
}
