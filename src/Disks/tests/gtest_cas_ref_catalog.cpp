#include "cas_format_test_battery.h"
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
/// Explicit rather than relying on a transitive path: `DEBUG_OR_SANITIZER_BUILD` (used below to gate
/// the `*DeathTest` split) must resolve in THIS translation unit.
#include <base/defines.h>
#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>
#include <fmt/format.h>
#include <algorithm>
#include <limits>
#include <sstream>
#include <type_traits>
#include <utility>

using namespace DB::Cas;

namespace ProfileEvents
{
    extern const Event CASGCUnmatchedAdoptedParentLives;
    extern const Event CASGCStuckRemovals;
}

namespace DB::Cas::tests
{

/// This friend-only compile pin is derived from the actual private production member pointers. It
/// fails if a raw round carrier becomes separately pairable with `fold`.
class GcRoundPlanSignatureAccess
{
public:
    using FoldSignature = decltype(&Gc::fold);
    using ExpectedFoldSignature = Gc::FoldResult (Gc::*)(
        GcState &, Token &, RoundReport &, uint64_t, const RefPlan &, UniversePolicy, GcRoundWorkBudget &);
    using BuilderSignature = decltype(&buildRefWalkPlan);
    using ExpectedBuilderSignature = RefPlan (*)(RoundInput &&);

    static_assert(std::is_same_v<FoldSignature, ExpectedFoldSignature>);
    static_assert(std::is_same_v<BuilderSignature, ExpectedBuilderSignature>);
};

}

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int LIMIT_EXCEEDED;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Hand-builds one raw "ent" line, bypassing `encodeRefCatalog` entirely -- used by the decode-side
/// rejection tests, which must exercise bytes the encoder itself would refuse to produce.
String rawEntLine(const String & ns, const String & state, const String & inc_hex,
                   std::optional<std::tuple<String, uint64_t, uint64_t>> creator = std::nullopt)
{
    if (!creator)
        return fmt::format(R"({{"k":"ent","ns":"{}","st":"{}","inc":"{}"}})", ns, state, inc_hex);
    const auto & [srid, we, fg] = *creator;
    return fmt::format(R"({{"k":"ent","ns":"{}","st":"{}","inc":"{}","csr":"{}","cwe":"{}","cfg":"{}"}})",
                        ns, state, inc_hex, srid, we, fg);
}

/// Wraps `ent_lines` in the header/trailer a real `cas_ref_catalog` object carries. `v:1` always
/// passes the header gate (any version <= the build's `G_BUILD` does), matching the convention
/// `gtest_cas_fold_seal_format.cpp`'s `RejectsOutOfRangeNsCleanupState` uses for the same reason.
String rawCatalog(const std::vector<String> & ent_lines)
{
    String out = R"({"type":"cas_ref_catalog","v":1})" "\n";
    for (const String & l : ent_lines)
        out += l + "\n";
    out += fmt::format("{{\"n\":{}}}\n", ent_lines.size());
    return out;
}

String withRemovalStartedRound(String line, uint64_t round)
{
    const size_t close = line.rfind('}');
    EXPECT_NE(close, String::npos);
    line.insert(close, fmt::format(R"(,"rsr":"{}")", round));
    return line;
}

CatalogEntry liveEntry(const String & ns, uint64_t inc)
{
    return CatalogEntry{.ns = RootNamespace{ns}, .state = NsState::Live, .incarnation = UInt128(inc)};
}

CatalogEntry entryInState(const String & ns, NsState state, uint64_t inc)
{
    CatalogEntry entry{.ns = RootNamespace{ns}, .state = state, .incarnation = UInt128(inc)};
    if (state == NsState::Creating)
        entry.creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};
    if (state == NsState::Removing)
        entry.removal_started_round = 1;
    return entry;
}

class EraseWinnerBackend final : public DB::Cas::tests::CountingBackend
{
public:
    using CountingBackend::casPut;
    using CountingBackend::get;

    void replaceOnNextCatalogCas(const String & key, std::optional<CatalogEntry> replacement_)
    {
        catalog_key = key;
        replacement = std::move(replacement_);
        armed = true;
    }

    bool fenceMoved() const { return fence_moved; }

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        if (armed && key == catalog_key)
        {
            armed = false;
            const auto current = CountingBackend::get(key);
            if (!current)
                throw std::runtime_error("test fixture lost mandatory catalog");
            RefCatalog winner_catalog;
            if (replacement)
                winner_catalog.entries.push_back(*replacement);
            const CasResult winner = CountingBackend::casPut(
                key, encodeRefCatalog(winner_catalog), current->token, meta);
            if (winner.outcome != CasOutcome::Committed)
                throw std::runtime_error("test fixture winner failed to replace catalog");
            fence_moved = true;
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }

private:
    String catalog_key;
    std::optional<CatalogEntry> replacement;
    bool armed = false;
    bool fence_moved = false;
};

class CasPutThrowsOnceBackend final : public DB::Cas::tests::CountingBackend
{
public:
    using CountingBackend::casPut;

    void armCasPutThrow(const String & key)
    {
        throw_key = key;
        armed = true;
    }

    CasResult casPut(
        const String & key, const String & bytes, const std::optional<Token> & expected,
        const ObjectMeta & meta) override
    {
        if (armed && key == throw_key)
        {
            armed = false;
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR,
                "injected casPut failure during completed-removal erase");
        }
        return CountingBackend::casPut(key, bytes, expected, meta);
    }

private:
    String throw_key;
    bool armed = false;
};

class ScopedCasGcLogCapture
{
public:
    ScopedCasGcLogCapture()
        : logger(getLogger("CasGc"))
        , channel(new Poco::StreamChannel(stream))
        , old_channel(logger->getChannel(), /*shared=*/true)
        , old_level(logger->getLevel())
    {
        logger->setChannel(channel.get());
        logger->setLevel("warning");
    }

    ~ScopedCasGcLogCapture()
    {
        logger->setChannel(old_channel);
        logger->setLevel(old_level);
    }

    String captured() const { return stream.str(); }

private:
    LoggerPtr logger;
    std::ostringstream stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::AutoPtr<Poco::StreamChannel> channel;
    /// A real reference (shared=true), so the parked previous channel cannot die while ours is installed.
    Poco::AutoPtr<Poco::Channel> old_channel;
    int old_level;
};

}

/// ---------- format-battery registration ----------

TEST(CASFormatBattery, RefCatalog)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = 5, .fence_generation = 2}});
    c.entries.push_back(liveEntry("b", 2));
    runFormatBattery({FormatId::RefCatalog,
        [&] { return sealObject(FormatId::RefCatalog, encodeRefCatalog(c)); },
        [](std::string_view s) { decodeRefCatalog(std::string(openObject(FormatId::RefCatalog, s))); },
        currentFormatHeader("cas_ref_catalog") +
        "{\"k\":\"ent\",\"ns\":\"a\",\"st\":\"creating\",\"inc\":\"00000000000000000000000000000001\","
        "\"csr\":\"srv1\",\"cwe\":\"5\",\"cfg\":\"2\"}\n"
        "{\"k\":\"ent\",\"ns\":\"b\",\"st\":\"live\",\"inc\":\"00000000000000000000000000000002\"}\n"
        "{\"n\":2}\n"});
}

/// ---------- codec round-trip ----------

TEST(CASRefCatalogFormat, RoundTripsAllThreeStates)
{
    RefCatalog in;
    in.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = 5, .fence_generation = 2}});
    in.entries.push_back(liveEntry("b", 2));
    in.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"c"},
        .state = NsState::Removing,
        .incarnation = UInt128(3),
        .removal_started_round = 11});

    const RefCatalog out = decodeRefCatalog(encodeRefCatalog(in));
    EXPECT_EQ(out, in);
    EXPECT_EQ(out.entries[0].state, NsState::Creating);
    EXPECT_EQ(out.entries[1].state, NsState::Live);
    EXPECT_EQ(out.entries[2].state, NsState::Removing);
}

/// Mutation caught: making removal age caller-local or optional would let an adopted `Removing` row
/// lose the immutable round from which stuck-removal diagnostics measure.
TEST(CASRefCatalogFormat, RemovalStartedRoundIsRequiredExactlyForRemoving)
{
    CatalogEntry removing{
        .ns = RootNamespace{"removing"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 19};
    const RefCatalog catalog{.entries = {removing}};
    const String encoded = encodeRefCatalog(catalog);
    EXPECT_NE(encoded.find("\"rsr\":\"19\""), String::npos);
    EXPECT_EQ(decodeRefCatalog(encoded), catalog);

    const String inc = "00000000000000000000000000000009";
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefCatalog(rawCatalog({rawEntLine("missing", "removing", inc)})); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)decodeRefCatalog(rawCatalog({withRemovalStartedRound(rawEntLine("forbidden", "live", inc), 21)}));
    });
}

TEST(CASRefCatalogFormat, EmptyCatalogRoundTrips)
{
    EXPECT_EQ(decodeRefCatalog(encodeRefCatalog(RefCatalog{})), RefCatalog{});
}

/// Mutation caught: replacing the reverse index with `emplace`-and-ignore would make the first row
/// win. Every lifecycle state participates, both duplicate ids are unresolvable, and an unrelated
/// unique row remains usable by point resolution.
TEST(CASRefCatalogLifeIndex, DuplicatePhysicalIdsAreAmbiguousWithoutPoisoningUniquePointResolution)
{
    RefCatalog catalog;
    catalog.entries = {
        entryInState("a-creating", NsState::Creating, 7),
        entryInState("b-live", NsState::Live, 7),
        entryInState("c-removing", NsState::Removing, 8),
        entryInState("d-live", NsState::Live, 8),
        entryInState("e-unique", NsState::Live, 9),
    };

    const CatalogLifeIndex index(catalog);
    EXPECT_TRUE(index.isAmbiguous(UInt128{7}));
    EXPECT_TRUE(index.isAmbiguous(UInt128{8}));
    EXPECT_THROW(index.resolve(UInt128{7}), DB::Exception);
    EXPECT_THROW(index.resolve(UInt128{8}), DB::Exception);
    const auto unique = index.resolve(UInt128{9});
    ASSERT_TRUE(unique);
    EXPECT_EQ(unique->ns.string(), "e-unique");
}

/// Catalog mutation is destructive authority: any ambiguous current id stops the mutation before a
/// candidate can be written. An unrelated unique point lookup remains available from the same cut.
TEST(CASRefCatalogLifeIndex, AmbiguityStopsCatalogMutationButNotUnrelatedPointLookup)
{
    InMemoryBackend backend;
    const Layout layout("p");
    RefCatalog catalog;
    catalog.entries = {
        entryInState("a", NsState::Live, 7),
        entryInState("b", NsState::Removing, 7),
        entryInState("c", NsState::Live, 9),
    };
    ASSERT_EQ(backend.putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(catalog)).outcome, PutOutcome::Done);
    const auto before = backend.get(layout.refCatalogKey());
    ASSERT_TRUE(before);

    EXPECT_THROW(CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & current) { return current; }), DB::Exception);
    const auto after = backend.get(layout.refCatalogKey());
    ASSERT_TRUE(after);
    EXPECT_EQ(after->token, before->token);
    EXPECT_EQ(after->bytes, before->bytes);

    const auto unique = CasRefCatalog::lifeIfCataloged(backend, layout, RootNamespace{"c"});
    ASSERT_TRUE(unique);
    EXPECT_EQ(unique->incarnation, UInt128{9});
}

TEST(CASRefCatalogFormat, NamespaceAtExactByteBoundRoundTrips)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes, 'a'), 1));
    const RefCatalog out = decodeRefCatalog(encodeRefCatalog(c));
    EXPECT_EQ(out, c);
}

/// ---------- strict rejections: encode side (LOGICAL_ERROR -- our own state, not yet durable) ----------

/// Every `expectThrowsCode(LOGICAL_ERROR, ...)` in this block aborts the process in debug/sanitizer
/// builds instead of behaving like a catchable exception (`Common/Exception.cpp`'s
/// `handle_error_code`), so each test is split: the throw-and-catch form below runs only on a plain
/// release build, and its `...DeathTest` counterpart (grouped after this block) proves the abort
/// positively on debug/sanitizer builds instead.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(CASRefCatalogFormat, EncodeRejectsDuplicateNamespace)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 1));
    c.entries.push_back(liveEntry("a", 2));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsNonCanonicalOrder)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("b", 1));
    c.entries.push_back(liveEntry("a", 2));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsCreatorPresentOnLive)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsCreatorAbsentOnCreating)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1)});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsZeroIncarnation)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 0));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsNameOverByteBound)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes + 1, 'a'), 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsEmptyNamespace)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("", 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

/// Mutation caught: making removal age caller-local or optional would let an adopted `Removing` row
/// lose the immutable round from which stuck-removal diagnostics measure.
TEST(CASRefCatalogFormat, EncodeRejectsLiveWithRemovalStartedRound)
{
    CatalogEntry live_with_round = liveEntry("live", 8);
    live_with_round.removal_started_round = 20;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { (void)encodeRefCatalog(RefCatalog{.entries = {live_with_round}}); });
}

#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsDuplicateNamespaceAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 1));
    c.entries.push_back(liveEntry("a", 2));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "not canonically ordered");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsNonCanonicalOrderAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("b", 1));
    c.entries.push_back(liveEntry("a", 2));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "not canonically ordered");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsCreatorPresentOnLiveAborts)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "carries a creator fence");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsCreatorAbsentOnCreatingAborts)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1)});
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "lacks a creator fence");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsZeroIncarnationAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 0));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "zero incarnation");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsNameOverByteBoundAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes + 1, 'a'), 1));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "admission bound");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsEmptyNamespaceAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("", 1));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "namespace must not be empty");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsLiveWithRemovalStartedRoundAborts)
{
    CatalogEntry live_with_round = liveEntry("live", 8);
    live_with_round.removal_started_round = 20;
    EXPECT_DEATH(
        { (void)encodeRefCatalog(RefCatalog{.entries = {live_with_round}}); }, "removal_started_round");
}

#endif

/// A namespace + creator server_root_id that both max out at their respective byte bounds (512 +
/// 255), escaped worst-case, land one "ent" line over the 4 KiB line cap (~4.7 KiB) -- reachable
/// because neither this codec nor `validateServerRootId` restricts the charset, only the length.
/// The refusal must be `LIMIT_EXCEEDED` (a capacity refusal), not `LOGICAL_ERROR` (a bug report) --
/// `encodeFoldSeal`'s own `checkLineBytes` raises `LIMIT_EXCEEDED` for the identical shape of gate.
TEST(CASRefCatalogFormat, EncodeLineOverCapRaisesLimitExceeded)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{
        .ns = RootNamespace{String(kMaxNamespaceBytes, '\x01')},
        .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = String(255, '\x01'), .writer_epoch = 1, .fence_generation = 1}});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED, [&] { encodeRefCatalog(c); });
}

/// ---------- strict rejections: decode side (CORRUPTED_DATA -- bytes may have come from anywhere) ----------

TEST(CASRefCatalogFormat, DecodeRejectsDuplicateNamespace)
{
    const String bad = rawCatalog({rawEntLine("a", "live", u128ToHex(UInt128(1))),
                                    rawEntLine("a", "live", u128ToHex(UInt128(2)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsNonCanonicalOrder)
{
    const String bad = rawCatalog({rawEntLine("b", "live", u128ToHex(UInt128(1))),
                                    rawEntLine("a", "live", u128ToHex(UInt128(2)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsCreatorPresentOnLive)
{
    const String bad = rawCatalog({rawEntLine("a", "live", u128ToHex(UInt128(1)),
                                               std::make_tuple(String("srv"), uint64_t(1), uint64_t(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsCreatorAbsentOnCreating)
{
    const String bad = rawCatalog({rawEntLine("a", "creating", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsZeroIncarnation)
{
    const String bad = rawCatalog({rawEntLine("a", "live", u128ToHex(UInt128(0)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsNameOverByteBound)
{
    const String too_long_ns(kMaxNamespaceBytes + 1, 'a');
    const String bad = rawCatalog({rawEntLine(too_long_ns, "live", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsUnknownState)
{
    const String bad = rawCatalog({rawEntLine("a", "bogus", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsEmptyNamespace)
{
    const String bad = rawCatalog({rawEntLine("", "live", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsMissingNamespaceKey)
{
    /// No "ns" key at all -- must be refused exactly like an explicit empty one, not read as "".
    const String bad = rawCatalog({R"({"k":"ent","st":"live","inc":")" + u128ToHex(UInt128(1)) + "\"}"});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

/// `nsStateToWord`'s only reachable input is either a live `NsState` or one `nsStateFromWord` already
/// validated on decode, so an unrecognized value is a bug in THIS process -- `LOGICAL_ERROR`, matching
/// this file's own stated taxonomy for the encode-side helper it (indirectly, via `creatorPairingOk`'s
/// error message) serves. Aborts under debug/sanitizer builds -- split like the block above;
/// `CASRefCatalogFormatDeathTest.NsStateToWordRaisesLogicalErrorOnImpossibleValueAborts` covers it there.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalogFormat, NsStateToWordRaisesLogicalErrorOnImpossibleValue)
{
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { nsStateToWord(static_cast<NsState>(99)); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogFormatDeathTest, NsStateToWordRaisesLogicalErrorOnImpossibleValueAborts)
{
    EXPECT_DEATH({ (void)nsStateToWord(static_cast<NsState>(99)); }, "unknown ns state"); // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange): the whole point of this test is an impossible enum value
}
#endif

/// ---------- registry row / raw-storage tripwire ----------

/// The registry row is part of the contract, mirroring `gtest_cas_ref_ckpt.cpp`'s
/// `RegistryRowIsControlStrictWithTightCaps`: Control/Strict decides how the decoder treats unknown
/// keys, and the caps are the first thing that fires if a foreign object ever lands at the key.
TEST(CASRefCatalogFormat, RegistryRowIsControlStrictWithRawStorage)
{
    const FormatTraits & traits = traitsFor(FormatId::RefCatalog);
    EXPECT_EQ(traits.type, "cas_ref_catalog");
    EXPECT_EQ(traits.family, TextFamily::Control);
    EXPECT_EQ(traits.strictness, KeyStrictness::Strict);
    EXPECT_EQ(traits.object_cap, 256u * 1024u * 1024u);
    EXPECT_EQ(traits.line_cap, 4u * 1024u);
    EXPECT_EQ(traitsForType("cas_ref_catalog"), &traits);
    /// Raw, so the key has no suffix: `Pool/CasRefCatalog.cpp` hands bytes to/from the backend
    /// directly, bypassing `sealObject`/`openObject` because both are the identity under
    /// `CompressionPolicy::Never`. This line is the TRIPWIRE for that shortcut -- a policy flip to
    /// `Always` would silently write uncompressed bodies under a `.zst` key, which this assertion
    /// catches first (see `CasRefCatalogFormat.h`'s comment on `encodeRefCatalog`).
    EXPECT_EQ(storedSuffix(FormatId::RefCatalog), "");
    EXPECT_EQ(traits.compression, CompressionPolicy::Never);
}

/// ---------- capacity admission: per-predicate boundary tests [codex r2/r3 finding 9] ----------

TEST(CASRefCatalogAdmission, Predicate1AcceptsEqualityRefusesCapPlusOne)
{
    const uint64_t cap = traitsFor(FormatId::RefCatalog).object_cap;
    const RootNamespace ns{"admitted"};
    EXPECT_NO_THROW(checkCatalogObjectBytes(cap, ns));
    try
    {
        checkCatalogObjectBytes(cap + 1, ns);
        FAIL() << "expected LIMIT_EXCEEDED";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LIMIT_EXCEEDED);
        EXPECT_NE(e.message().find("predicate 1"), String::npos) << e.message();
        EXPECT_NE(e.message().find(ns.string()), String::npos) << e.message();
    }
}

TEST(CASRefCatalogAdmission, Predicate2AcceptsEqualityRefusesOneEntryOver)
{
    const Layout layout("p");
    constexpr uint64_t gc_shards = 1;
    /// The exact boundary is expressed in ENTRIES (predicate (2) is a sum over admitted entries), so
    /// the boundary count is derived from the real registry constants rather than assumed.
    const uint64_t cap = foldSealCaps().object_cap;
    const uint64_t fixed = foldSealFixedBytes();
    const uint64_t reservation = worstCaseEntryFoldReservationBytes();
    ASSERT_GT(reservation, 0u);
    const uint64_t nonentry = widestBlobTargetRunReservationBytes(layout, gc_shards)
        + widestCondemnedSummaryReservationBytes(gc_shards);
    const uint64_t max_entries = (cap - fixed - nonentry) / reservation;

    const RootNamespace ns{"admitted"};
    EXPECT_NO_THROW(checkFoldSealReservation(max_entries, gc_shards, layout, ns));
    try
    {
        checkFoldSealReservation(max_entries + 1, gc_shards, layout, ns);
        FAIL() << "expected LIMIT_EXCEEDED";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LIMIT_EXCEEDED);
        EXPECT_NE(e.message().find("predicate 2"), String::npos) << e.message();
        EXPECT_NE(e.message().find(ns.string()), String::npos) << e.message();
    }
}

/// `entry_count * worstCaseEntryFoldReservationBytes()` must saturate, not wrap: choosing
/// `entry_count` as the SMALLEST value whose true (unbounded) product with `reservation` crosses
/// 2^64, an unsaturated `uint64_t` multiplication wraps to a remainder SMALLER than `reservation`
/// itself (a few KiB) -- which reads as trivially "fits" a 256 MiB cap even though the real
/// reservation this many entries demands is astronomically larger. A saturating multiply refuses it
/// regardless of the wraparound arithmetic underneath.
TEST(CASRefCatalogAdmission, Predicate2SaturatesEntryCountReservationInsteadOfWrapping)
{
    const Layout layout("p");
    const uint64_t reservation = worstCaseEntryFoldReservationBytes();
    const uint64_t entry_count = std::numeric_limits<uint64_t>::max() / reservation + 1;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED,
        [&] { checkFoldSealReservation(entry_count, 1, layout, RootNamespace{"huge"}); });
}

TEST(CASRefCatalogAdmission, CombinedAdmissionPropagatesCandidateEntryCount)
{
    /// `checkCatalogAdmission` runs predicate (1) then predicate (2) against the SAME candidate; for
    /// an ordinary small catalog both hold slack and it returns the exact bytes `encodeRefCatalog`
    /// would produce.
    RefCatalog candidate;
    candidate.entries.push_back(liveEntry("a", 1));
    candidate.entries.push_back(liveEntry("b", 2));
    const Layout layout("p");
    const String encoded = checkCatalogAdmission(candidate, 1, layout, RootNamespace{"b"});
    EXPECT_EQ(encoded, encodeRefCatalog(candidate));
}

TEST(CASRefCatalogAdmission, ReservationCoversActualWidestLegalRowsAcrossDecimalTransitions)
{
    const Layout layout("p/quoted-\"prefix");
    constexpr uint64_t gc_shards = 100;
    constexpr uint64_t max = std::numeric_limits<uint64_t>::max();

    for (const uint64_t entry_count : {9, 10, 99, 100})
    {
        CasFoldSeal seal;
        seal.generation = max;
        seal.parent_generation = max;
        for (uint64_t i = 0; i < entry_count; ++i)
        {
            seal.ref_lives.emplace(std::numeric_limits<UInt128>::max() - i, RefLifeFoldState{
                .coverage = RefCoverage{
                    .classification = 4,
                    .last_folded_ref_id = RefTxnId{max, max},
                    .hold = RefHold{
                        .reason = HoldReason::UnconsumedSealCrossing,
                        .offending_position = RefTxnId{max, max},
                        .retry_count = std::numeric_limits<uint32_t>::max(),
                        .next_retry_round = max}},
                .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{max, max}}});
        }
        for (uint64_t shard = 0; shard < gc_shards; ++shard)
        {
            /// Predicate 2 charges exactly `gc_shards` widest `btr` rows. This fixture is the maximum
            /// legal cardinality, not an optimistic producer convention: authoritative fold-seal
            /// grammar permits at most one run per shard and requires its canonical key to use seq 0.
            seal.blob_target_runs.push_back(RunRef{
                .key = layout.blobTargetRunKey(max, max, shard, 0),
                .checksum = std::numeric_limits<UInt128>::max(),
                .shard = shard,
                .generation = max});
            seal.condemned_summary.emplace(shard, CondemnedSummary{
                .condemned_total = max,
                .pending_total = max,
                .oldest_nonpending_condemn_round = max});
        }

        ASSERT_EQ(seal.blob_target_runs.size(), gc_shards);
        EXPECT_NO_THROW(validateFoldSealForWrite(seal, layout, gc_shards));

        const uint64_t bound = foldSealFixedBytes()
            + entry_count * worstCaseEntryFoldReservationBytes()
            + gc_shards * widestBlobTargetRunReservationBytes(layout, gc_shards)
            + gc_shards * widestCondemnedSummaryReservationBytes(gc_shards);
        EXPECT_LE(encodeFoldSeal(seal).size(), bound) << "entry_count=" << entry_count;
    }
}

/// ---------- Constraint 13: removal is never refused, even at the admission boundary ----------

TEST(CASRefCatalogAdmission, RemovalNeverRefusedEvenAtCapacity)
{
    const Layout layout("p");
    constexpr uint64_t gc_shards = 1;
    const uint64_t cap = foldSealCaps().object_cap;
    const uint64_t fixed = foldSealFixedBytes();
    const uint64_t reservation = worstCaseEntryFoldReservationBytes();
    const uint64_t nonentry = widestBlobTargetRunReservationBytes(layout, gc_shards)
        + widestCondemnedSummaryReservationBytes(gc_shards);
    const uint64_t max_entries = (cap - fixed - nonentry) / reservation;

    /// Confirm the boundary is real: one entry beyond it is refused through admission.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED,
        [&] { checkFoldSealReservation(max_entries + 1, gc_shards, layout, RootNamespace{"z"}); });

    /// Build a catalog carrying exactly `max_entries` Live entries -- as full as admission ever
    /// permits -- directly (a fixture, not itself an admission call).
    RefCatalog full;
    full.entries.reserve(max_entries);
    for (uint64_t i = 0; i < max_entries; ++i)
        full.entries.push_back(liveEntry(fmt::format("ns{:012}", i), i + 1));

    InMemoryBackend backend;
    backend.putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(full));

    /// The removal transition (Live -> Removing) on one entry goes through the PLAIN update path
    /// (`casUpdate`, which runs no admission check at all) and succeeds even though the catalog is
    /// already at the point where ANY growth would be refused.
    const RefCatalog after = CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & cur)
    {
        RefCatalog next = cur;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });
    EXPECT_EQ(after.entries.size(), max_entries);
    EXPECT_EQ(after.entries[0].state, NsState::Removing);
}

/// ---------- Pool/CasRefCatalog: token-CAS read / create / update / conflict-retry ----------

TEST(CASRefCatalog, ReadAbsentFailsClosed)
{
    InMemoryBackend backend;
    Layout layout("p");
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)CasRefCatalog::read(backend, layout); });
}

TEST(CASRefCatalog, CasUpdateRefusesWhenAbsent)
{
    InMemoryBackend backend;
    Layout layout("p");

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & cur) { return cur; });
    });
    EXPECT_FALSE(backend.head(layout.refCatalogKey()).exists);
}

TEST(CASRefCatalog, CasUpdateAppliesOnTopOfExistingState)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));

    const RefCatalog updated = CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & cur)
    {
        RefCatalog next = cur;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });
    ASSERT_EQ(updated.entries.size(), 1u);
    EXPECT_EQ(updated.entries[0].ns.string(), "a");
    EXPECT_EQ(updated.entries[0].state, NsState::Removing);
}

/// `CasRefCatalog::casUpdate`'s identity-preserving refusal throws `LOGICAL_ERROR`, which aborts the
/// whole process in debug/sanitizer builds (`Common/Exception.cpp`'s `handle_error_code`) instead of
/// behaving like a catchable exception -- so the throw-and-catch form below runs only on a plain
/// release build, and `CASRefCatalogDeathTest.GenericCasUpdateCannotDeleteOrReplaceCatalogIdentityAborts`
/// proves the abort positively on debug/sanitizer builds instead.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, GenericCasUpdateCannotDeleteOrReplaceCatalogIdentity)
{
    const Layout layout("p");
    {
        InMemoryBackend backend;
        CasRefCatalog::initializeEmptyForNewPool(backend, layout);
        CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
        {
            (void)CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog &) { return RefCatalog{}; });
        });
    }

    {
        InMemoryBackend backend;
        CasRefCatalog::initializeEmptyForNewPool(backend, layout);
        CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
        {
            (void)CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & current)
            {
                RefCatalog next = current;
                next.entries[0] = liveEntry("b", 2);
                return next;
            });
        });
    }
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, GenericCasUpdateCannotDeleteOrReplaceCatalogIdentityAborts)
{
    const Layout layout("p");
    {
        InMemoryBackend backend;
        CasRefCatalog::initializeEmptyForNewPool(backend, layout);
        CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
        EXPECT_DEATH(
            { (void)CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog &) { return RefCatalog{}; }); },
            "cannot add or delete catalog entries");
    }

    {
        InMemoryBackend backend;
        CasRefCatalog::initializeEmptyForNewPool(backend, layout);
        CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
        EXPECT_DEATH(
            {
                (void)CasRefCatalog::casUpdate(backend, layout, [](const RefCatalog & current)
                {
                    RefCatalog next = current;
                    next.entries[0] = liveEntry("b", 2);
                    return next;
                });
            },
            "cannot replace catalog identity");
    }
}
#endif

TEST(CASRefCatalog, CasUpdateRetriesOnConflictAgainstFreshState)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));

    backend.failNextCasPut(layout.refCatalogKey());   /// one-shot artificial Conflict on the next write

    int mutate_calls = 0;
    const RefCatalog result = CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & cur)
    {
        ++mutate_calls;
        RefCatalog next = cur;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });

    EXPECT_EQ(mutate_calls, 2);   /// first attempt hit the injected conflict; the retry succeeded
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_EQ(result.entries[0].state, NsState::Removing);

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(backend, layout);
    EXPECT_EQ(snap.catalog, result);
}

TEST(CASRefCatalog, BeginRemovingRechecksFenceAfterCatalogCasConflict)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const CatalogEntry observed = liveEntry("a", 1);
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, observed);

    uint64_t current_fence_generation = 7;
    size_t fence_checks = 0;
    const auto outcome = CasRefCatalog::beginRemoving(
        backend, layout, observed, /*removal_started_round*/ 13, /*admitted_generation*/ 7,
        [&](uint64_t admitted_generation)
        {
            ++fence_checks;
            if (admitted_generation != current_fence_generation)
                throw std::runtime_error("stale catalog mutation fence");
            if (fence_checks == 1)
            {
                /// Move the caller fence after the first admission check and force that attempt's
                /// catalog CAS to conflict. The next attempt must check the fence again before writing.
                current_fence_generation = 8;
                backend.failNextCasPut(layout.refCatalogKey());
            }
        });

    EXPECT_EQ(outcome, CasRefCatalog::BeginRemovingOutcome::FencedOut);
    EXPECT_EQ(fence_checks, 2u);
    const CasRefCatalog::Snapshot after = CasRefCatalog::read(backend, layout);
    EXPECT_EQ(after.catalog.entries, std::vector<CatalogEntry>{observed});
}

/// A re-read that finds the catalog genuinely ABSENT after it was previously observed present is a
/// real concurrent delete, not a bootstrap -- `casUpdate` must refuse rather than silently create a
/// fresh catalog containing only this one mutation's entry (which would drop every other namespace).
/// Reproduced with a REAL delete (no fault injection needed): `mutate`'s first invocation deletes the
/// seeded object using the token `casUpdate`'s own initial read observed, so the loop's own `casPut`
/// against that now-stale token gets a genuine `Conflict`, and the follow-up re-read genuinely finds
/// the key absent.
/// Missing mandatory authority raises `CORRUPTED_DATA`; the split remains only because the debug
/// variant historically lived in the death-test suite.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, CasUpdateThrowsOnVanishMidRetryInsteadOfReplacingTheCatalog)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
    const CasRefCatalog::Snapshot seeded = CasRefCatalog::read(backend, layout);
    ASSERT_TRUE(seeded.token.has_value());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & cur)
        {
            backend.deleteExact(layout.refCatalogKey(), *seeded.token);
            RefCatalog next = cur;
            next.entries[0].state = NsState::Removing;
            next.entries[0].removal_started_round = 1;
            return next;
        });
    });

    /// Nothing was written by the failed attempt: the object is exactly as the delete left it
    /// (absent), never a fresh single-entry catalog.
    EXPECT_FALSE(backend.head(layout.refCatalogKey()).exists);
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, CasUpdateThrowsOnVanishMidRetryInsteadOfReplacingTheCatalogAborts)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
    const CasRefCatalog::Snapshot seeded = CasRefCatalog::read(backend, layout);
    ASSERT_TRUE(seeded.token.has_value());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
            CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & cur)
            {
                backend.deleteExact(layout.refCatalogKey(), *seeded.token);
                RefCatalog next = cur;
                next.entries[0].state = NsState::Removing;
                next.entries[0].removal_started_round = 1;
                return next;
            });
    });
}
#endif

/// The retry loop is bounded (the same live-lock brake `publishCkpt`/`allocateWriterEpoch` use on
/// their own contended token-CAS singletons) and ends in the typed retryable error, not an infinite
/// spin. `mutate` re-arms the one-shot conflict injection on every call, so every attempt fails.
TEST(CASRefCatalog, CasUpdateGivesUpAfterBoundedAttemptsWithRetryLaterError)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);

    int mutate_calls = 0;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        CasRefCatalog::casUpdate(backend, layout, [&](const RefCatalog & cur)
        {
            ++mutate_calls;
            backend.failNextCasPut(layout.refCatalogKey());
            RefCatalog next = cur;
            return next;
        });
    });
    EXPECT_GT(mutate_calls, 1);   /// genuinely retried, not a single-shot failure
}

TEST(CASRefCatalog, CasAdmitEntryAcceptsAnOrdinaryCreation)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);

    const RefCatalog created = CasRefCatalog::casAdmitEntry(backend, layout, 1,
        CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1),
            .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    ASSERT_EQ(created.entries.size(), 1u);
    EXPECT_EQ(created.entries[0].state, NsState::Creating);
}

TEST(CASRefCatalog, CasAdmitEntryInsertsAtCanonicalPosition)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);

    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("b", 1));
    const RefCatalog after = CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 2));
    ASSERT_EQ(after.entries.size(), 2u);
    EXPECT_EQ(after.entries[0].ns.string(), "a");   /// inserted BEFORE "b", not appended
    EXPECT_EQ(after.entries[1].ns.string(), "b");
}

/// Caught by `encodeRefCatalog`'s own canonical-order/no-duplicate grammar check, inside
/// `checkCatalogAdmission` -- no separate duplicate check needed here. That `LOGICAL_ERROR` aborts
/// under debug/sanitizer builds -- split like the blocks above.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, CasAdmitEntryRejectsADuplicateNamespace)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 2)); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, CasAdmitEntryRejectsADuplicateNamespaceAborts)
{
    InMemoryBackend backend;
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 1));
    EXPECT_DEATH({ CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("a", 2)); }, "not canonically ordered");
}
#endif

TEST(CASRefCatalog, CasAdmitEntryRefusesOverCapacity)
{
    InMemoryBackend backend;
    Layout layout("p");

    const uint64_t cap = foldSealCaps().object_cap;
    const uint64_t fixed = foldSealFixedBytes();
    const uint64_t reservation = worstCaseEntryFoldReservationBytes();
    const uint64_t nonentry = widestBlobTargetRunReservationBytes(layout, 1)
        + widestCondemnedSummaryReservationBytes(1);
    const uint64_t max_entries = (cap - fixed - nonentry) / reservation;

    /// Seed the catalog directly at the admission boundary (a fixture -- not itself an admission call).
    RefCatalog full;
    full.entries.reserve(max_entries);
    for (uint64_t i = 0; i < max_entries; ++i)
        full.entries.push_back(liveEntry(fmt::format("ns{:012}", i), i + 1));
    backend.putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(full));

    /// Admitting ONE more namespace is refused -- the additive predicate is checked BEFORE the write,
    /// so the backend object is untouched.
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED, [&]
    {
        CasRefCatalog::casAdmitEntry(backend, layout, 1, liveEntry("zzz", 999999999));
    });

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(backend, layout);
    EXPECT_EQ(snap.catalog.entries.size(), max_entries);
}

TEST(CASRefCatalogRemoval, DeleteCompletedRemovingRequiresExactAdoptedProofAndLeaderFence)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
    ASSERT_EQ(backend.putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}})).outcome,
        PutOutcome::Done);

    CasFoldSeal held_parent;
    held_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{
            .classification = 4,
            .last_folded_ref_id = RefTxnId{1, 2},
            .hold = RefHold{.offending_position = RefTxnId{1, 3}}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});
    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, removing, held_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::ProofRefused);

    CasFoldSeal mismatched_parent;
    mismatched_parent.ref_lives.emplace(UInt128{8}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});
    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, removing, mismatched_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::ProofRefused);

    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});

    CatalogEntry live = removing;
    live.state = NsState::Live;
    live.removal_started_round.reset();
    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, live, ready_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::ProofRefused);
    CatalogEntry creating = live;
    creating.state = NsState::Creating;
    creating.creator = CreatorFence{.server_root_id = "server", .writer_epoch = 3, .fence_generation = 4};
    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, creating, ready_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::ProofRefused);

    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, removing, ready_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Moved; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::FencedOut);
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 0);

    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, removing, ready_parent, 5, [](uint64_t generation)
        {
            EXPECT_EQ(generation, 5);
            return CasRefCatalog::LeaderFenceStatus::Held;
        }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::Deleted);
    EXPECT_TRUE(CasRefCatalog::read(backend, layout).catalog.entries.empty());
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 1);
    EXPECT_EQ(backend.listTotal(), 0);
    EXPECT_EQ(backend.deleteTotal(), 0);
}

TEST(CASRefCatalogRemoval, ExactDeletionRefusesChangedEntryAndAdmissionCannotCarryRemoval)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
#ifndef DEBUG_OR_SANITIZER_BUILD
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { (void)CasRefCatalog::casAdmitEntry(backend, layout, 1, removing); });
#endif

    const CatalogEntry current{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 14};
    ASSERT_EQ(backend.putIfAbsent("unrelated", "sentinel").outcome, PutOutcome::Done);
    ASSERT_EQ(backend.casPut(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {current}}),
        CasRefCatalog::read(backend, layout).token).outcome, CasOutcome::Committed);

    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});
    EXPECT_EQ(CasRefCatalog::deleteCompletedRemoving(
        backend, layout, removing, ready_parent, 5,
        [](uint64_t) { return CasRefCatalog::LeaderFenceStatus::Held; }),
        CasRefCatalog::CompletedRemovingDeleteOutcome::EntryChanged);
    EXPECT_EQ(CasRefCatalog::read(backend, layout).catalog.entries, std::vector<CatalogEntry>{current});
}

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogRemovalDeathTest, AdmissionCannotCarryRemovalAborts)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(backend, layout);
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};

    EXPECT_DEATH(
        { (void)CasRefCatalog::casAdmitEntry(backend, layout, 1, removing); },
        "cannot admit namespace.*directly as Removing");
}
#endif

/// Mutation caught: deriving the control outcome from the resolution snapshot would turn a stale
/// leader's `FencedOut` into `Deleted` or `EntryChanged`. Resolution may prove the old life dead and
/// carry its invalidation, but it cannot restore the caller's authority to continue the GC round.
TEST(CASRefCatalogRemoval, FenceLossRemainsControlOutcomeWhenWinnerRemovesOrReplacesLife)
{
    for (const bool replace : {false, true})
    {
        EraseWinnerBackend backend;
        const Layout layout(replace ? "replacement" : "absence");
        const CatalogEntry removing{
            .ns = RootNamespace{"a"},
            .state = NsState::Removing,
            .incarnation = UInt128{7},
            .removal_started_round = 13};
        ASSERT_EQ(backend.putIfAbsent(
            layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}})).outcome,
            PutOutcome::Done);

        CasFoldSeal ready_parent;
        ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
            .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
            .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});
        std::optional<CatalogEntry> replacement;
        if (replace)
            replacement = CatalogEntry{
                .ns = removing.ns,
                .state = NsState::Live,
                .incarnation = UInt128{8}};
        backend.replaceOnNextCatalogCas(layout.refCatalogKey(), replacement);

        const CasRefCatalog::CompletedRemovingDeleteResult result
            = CasRefCatalog::deleteCompletedRemoving(
                backend, layout, removing, ready_parent, 5, [&](uint64_t)
                {
                    if (backend.fenceMoved())
                        return CasRefCatalog::LeaderFenceStatus::Moved;
                    return CasRefCatalog::LeaderFenceStatus::Held;
                });

        EXPECT_EQ(result.outcome, CasRefCatalog::CompletedRemovingDeleteOutcome::FencedOut);
        ASSERT_TRUE(result.invalidated_life);
        EXPECT_EQ(*result.invalidated_life,
            NamespaceLifeId::fromCatalogEntry(removing.ns, removing.incarnation));
        const RefCatalog current = CasRefCatalog::read(backend, layout).catalog;
        if (replace)
            EXPECT_EQ(current.entries, std::vector<CatalogEntry>{*replacement});
        else
            EXPECT_TRUE(current.entries.empty());
    }
}

/// Mutation caught: treating every authority-check exception as a moved fence hides corruption and
/// backend/decode failures. Before any CAS, inability to evaluate authority must propagate unchanged.
TEST(CASRefCatalogRemoval, NonFenceAuthorityExceptionPropagatesBeforeEraseCas)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("pre-cas-authority-error");
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
    ASSERT_EQ(backend.putIfAbsent(
        layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}})).outcome,
        PutOutcome::Done);
    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)CasRefCatalog::deleteCompletedRemoving(
            backend, layout, removing, ready_parent, 5, [](uint64_t) -> CasRefCatalog::LeaderFenceStatus
            {
                throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                    "injected authority read failure before erase CAS");
            });
    });
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 0u);
}

/// The post-CAS authority check is distinct: the erase may already be durable and its mandatory
/// resolution complete, but inability to evaluate authority is still the original error, not
/// `FencedOut`.
TEST(CASRefCatalogRemoval, NonFenceAuthorityExceptionPropagatesAfterEraseResolution)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("post-cas-authority-error");
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
    ASSERT_EQ(backend.putIfAbsent(
        layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}})).outcome,
        PutOutcome::Done);
    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});

    size_t authority_checks = 0;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)CasRefCatalog::deleteCompletedRemoving(
            backend, layout, removing, ready_parent, 5, [&](uint64_t)
            {
                if (++authority_checks == 2)
                    throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
                        "injected authority read failure after erase resolution");
                return CasRefCatalog::LeaderFenceStatus::Held;
            });
    });
    EXPECT_EQ(authority_checks, 2u);
    EXPECT_TRUE(CasRefCatalog::read(backend, layout).catalog.entries.empty());
}

/// Mutation caught: swallowing a synchronous `casPut` exception raised during the erase attempt
/// itself (as opposed to the authority/fence check) and treating it as ordinary non-convergence
/// would hide a real backend fault behind ProofRefused/EntryChanged, and would skip the mandatory
/// resolution read that this branch's siblings above already prove runs before any conclusion.
TEST(CASRefCatalogRemoval, CasPutExceptionPropagatesAfterMandatoryResolution)
{
    CasPutThrowsOnceBackend backend;
    const Layout layout("cas-put-throw");
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
    ASSERT_EQ(backend.putIfAbsent(
        layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}})).outcome,
        PutOutcome::Done);
    CasFoldSeal ready_parent;
    ready_parent.ref_lives.emplace(UInt128{7}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 2}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 2}}});

    backend.armCasPutThrow(layout.refCatalogKey());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        (void)CasRefCatalog::deleteCompletedRemoving(
            backend, layout, removing, ready_parent, 5, [](uint64_t)
            {
                return CasRefCatalog::LeaderFenceStatus::Held;
            });
    });
    /// The mandatory resolution read ran before the rethrow: the exact old row is still present,
    /// unchanged by the failed attempt.
    const RefCatalog current = CasRefCatalog::read(backend, layout).catalog;
    EXPECT_EQ(current.entries, std::vector<CatalogEntry>{removing});
}

TEST(CASRefCatalogRemoval, CancelStalledCreatingRequiresExactRowAndTerminalCreatorFence)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const CatalogEntry creating{
        .ns = RootNamespace{"a"},
        .state = NsState::Creating,
        .incarnation = UInt128{7},
        .creator = CreatorFence{.server_root_id = "server", .writer_epoch = 3, .fence_generation = 4}};
    ASSERT_EQ(backend.putIfAbsent(layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {creating}})).outcome,
        PutOutcome::Done);

    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        backend, layout, creating, [](const CreatorFence &) { return false; }, 5, [](uint64_t) {}),
        CasRefCatalog::StalledCreatingCancelOutcome::CreatorFenceStillLive);
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 0);

    CatalogEntry stale = creating;
    stale.creator->writer_epoch = 2;
    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        backend, layout, stale, [](const CreatorFence &) { return true; }, 5, [](uint64_t) {}),
        CasRefCatalog::StalledCreatingCancelOutcome::EntryChanged);
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 0);

    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        backend, layout, creating, [](const CreatorFence &) { return true; }, 5, [](uint64_t) {}),
        CasRefCatalog::StalledCreatingCancelOutcome::Cancelled);
    EXPECT_TRUE(CasRefCatalog::read(backend, layout).catalog.entries.empty());
    EXPECT_EQ(backend.casPutCount(layout.refCatalogKey()), 1);
    EXPECT_EQ(backend.listTotal(), 0);
    EXPECT_EQ(backend.deleteTotal(), 0);
}

TEST(CASGCRefWalkPlan, CatalogIsSoleRowAdmissionAuthorityAcrossOrdinaryAndRebuildInputs)
{
    RefCatalog catalog;
    catalog.entries = {
        CatalogEntry{
            .ns = RootNamespace{"creating"},
            .state = NsState::Creating,
            .incarnation = UInt128{1},
            .creator = CreatorFence{.server_root_id = "server", .writer_epoch = 1, .fence_generation = 1}},
        liveEntry("live", 2),
        CatalogEntry{
            .ns = RootNamespace{"removing"},
            .state = NsState::Removing,
            .incarnation = UInt128{3},
            .removal_started_round = 8},
    };
    const CasRefCatalog::Snapshot cut{
        .catalog = catalog, .token = std::nullopt, .life_index = CatalogLifeIndex(catalog)};

    RefScanSummary ordinary_scan;
    ordinary_scan.parent_ref_lives.emplace(UInt128{1}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{1, 1}}});
    ordinary_scan.parent_ref_lives.emplace(UInt128{3}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{3, 3}},
        .cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{3, 3}}});
    ordinary_scan.parent_ref_lives.emplace(UInt128{4}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{4, 4}}});
    ordinary_scan.listed_lives = {UInt128{1}, UInt128{2}, UInt128{4}};
    ordinary_scan.holds.emplace(UInt128{1}, RefHold{.offending_position = RefTxnId{1, 2}});
    ordinary_scan.holds.emplace(UInt128{2}, RefHold{.offending_position = RefTxnId{2, 2}});
    ordinary_scan.checkpoint_observations.emplace(UInt128{1}, RefTxnId{1, 9});
    ordinary_scan.checkpoint_observations.emplace(UInt128{2}, RefTxnId{2, 9});
    ordinary_scan.max_log_by_life.emplace(UInt128{1}, RefTxnId{1, 10});
    ordinary_scan.max_log_by_life.emplace(UInt128{2}, RefTxnId{2, 10});

    RefScanSummary rebuild_scan;
    rebuild_scan.parent_ref_lives.emplace(UInt128{1}, ordinary_scan.parent_ref_lives.at(UInt128{1}));
    rebuild_scan.parent_ref_lives.emplace(UInt128{5}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{5, 5}}});
    rebuild_scan.listed_lives = {UInt128{1}, UInt128{3}, UInt128{5}};
    rebuild_scan.holds.emplace(UInt128{1}, RefHold{.offending_position = RefTxnId{1, 3}});
    rebuild_scan.holds.emplace(UInt128{3}, RefHold{.offending_position = RefTxnId{3, 4}});
    rebuild_scan.checkpoint_observations.emplace(UInt128{1}, RefTxnId{1, 11});
    rebuild_scan.checkpoint_observations.emplace(UInt128{3}, RefTxnId{3, 11});
    rebuild_scan.max_log_by_life.emplace(UInt128{1}, RefTxnId{1, 12});
    rebuild_scan.max_log_by_life.emplace(UInt128{3}, RefTxnId{3, 12});

    const RefPlan ordinary = tests::buildRefWalkPlanForTest(ordinary_scan, cut);
    const RefPlan rebuild = tests::buildRefWalkPlanForTest(rebuild_scan, cut);
    const auto ordinary_parent_states = ordinary.parentFoldStates();
    const auto rebuild_parent_states = rebuild.parentFoldStates();
    const auto ordinary_successor_states = ordinary.successorFoldStates();
    const auto rebuild_successor_states = rebuild.successorFoldStates();
    EXPECT_EQ(ordinary_parent_states.size(), 1u);
    EXPECT_TRUE(ordinary_parent_states.contains(UInt128{3}));
    EXPECT_FALSE(ordinary_parent_states.contains(UInt128{2}));
    EXPECT_TRUE(ordinary_successor_states.contains(UInt128{2}));
    EXPECT_TRUE(ordinary_successor_states.contains(UInt128{3}));
    EXPECT_TRUE(rebuild_parent_states.empty());
    EXPECT_TRUE(rebuild_successor_states.contains(UInt128{2}));
    EXPECT_TRUE(rebuild_successor_states.contains(UInt128{3}));
    const std::set<UInt128> expected{UInt128{2}, UInt128{3}};
    EXPECT_EQ(ordinary.lifeIds(), expected);
    EXPECT_EQ(rebuild.lifeIds(), expected);

    EXPECT_TRUE(ordinary.row(UInt128{2}).listed_hint);
    ASSERT_TRUE(ordinary.row(UInt128{2}).fold_state.coverage.hold);
    EXPECT_EQ(ordinary.row(UInt128{2}).checkpoint_observation, (RefTxnId{2, 9}));
    EXPECT_EQ(ordinary.row(UInt128{2}).tail_observation, (RefTxnId{2, 10}));
    const std::optional<RefCleanupEvidence> cleanup_evidence{
        RefCleanupEvidence{.remove_txn_id = RefTxnId{3, 3}}};
    EXPECT_EQ(ordinary.row(UInt128{3}).fold_state.cleanup_evidence, cleanup_evidence);
    EXPECT_EQ(ordinary.row(UInt128{3}).removal_started_round, 8u);
    EXPECT_FALSE(ordinary.contains(UInt128{1}));
    EXPECT_FALSE(ordinary.contains(UInt128{4}));

    EXPECT_TRUE(rebuild.row(UInt128{3}).listed_hint);
    ASSERT_TRUE(rebuild.row(UInt128{3}).fold_state.coverage.hold);
    EXPECT_EQ(rebuild.row(UInt128{3}).checkpoint_observation, (RefTxnId{3, 11}));
    EXPECT_EQ(rebuild.row(UInt128{3}).tail_observation, (RefTxnId{3, 12}));
    EXPECT_FALSE(rebuild.contains(UInt128{1}));
    EXPECT_FALSE(rebuild.contains(UInt128{5}));
}

TEST(CASGCStuckRemoval, ThresholdAndRestartUseOnlyDurableRounds)
{
    const Layout layout("p");
    RefWalkPlanRow row{
        .life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"removing"}, UInt128{7}),
        .fold_state = {},
        .removal_started_round = 10,
        .has_parent_fold_state = false,
        .listed_hint = false,
        .checkpoint_observation = std::nullopt,
        .tail_observation = std::nullopt};

    EXPECT_FALSE(stuckRemovalWarning(row, /*current_round=*/12, /*threshold_rounds=*/3, layout));
    const auto at_threshold = stuckRemovalWarning(row, /*current_round=*/13, /*threshold_rounds=*/3, layout);
    const auto next_round = stuckRemovalWarning(row, /*current_round=*/14, /*threshold_rounds=*/3, layout);
    ASSERT_TRUE(at_threshold);
    ASSERT_TRUE(next_round);
    EXPECT_NE(at_threshold->find("age_rounds=3"), String::npos);
    EXPECT_NE(next_round->find("age_rounds=4"), String::npos);

    /// A fresh process given the same durable catalog row and adopted round produces the same signal.
    EXPECT_EQ(stuckRemovalWarning(row, 13, 3, layout), at_threshold);

    row.fold_state.cleanup_evidence = RefCleanupEvidence{.remove_txn_id = RefTxnId{1, 1}};
    EXPECT_FALSE(stuckRemovalWarning(row, 100, 3, layout));
}

TEST(CASGCStuckRemoval, BoundaryAndAbsentVersusUnreadableMessagesAreExact)
{
    const Layout layout("p");
    RefWalkPlanRow row{
        .life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"removing"}, UInt128{7}),
        .fold_state = {},
        .removal_started_round = std::numeric_limits<uint64_t>::max(),
        .has_parent_fold_state = false,
        .listed_hint = false,
        .checkpoint_observation = std::nullopt,
        .tail_observation = std::nullopt};
    EXPECT_FALSE(stuckRemovalWarning(row, 0, 1, layout));

    row.removal_started_round = 1;
    const auto absent = stuckRemovalWarning(row, 2, 1, layout);
    ASSERT_TRUE(absent);
    EXPECT_NE(absent->find("terminal has not folded"), String::npos);
    EXPECT_EQ(absent->find("/_log/"), String::npos) << "an absent terminal has no exact id to name";

    row.fold_state.coverage.classification = 4;
    row.fold_state.coverage.hold = RefHold{
        .reason = HoldReason::BodyUndecodable,
        .offending_position = RefTxnId{5, 6}};
    const auto unreadable = stuckRemovalWarning(row, 2, 1, layout);
    ASSERT_TRUE(unreadable);
    EXPECT_NE(unreadable->find(layout.refLogKey(row.life, RefTxnId{5, 6})), String::npos);
    EXPECT_NE(unreadable->find("is unreadable"), String::npos);
    EXPECT_NE(unreadable->find("restore the exact object"), String::npos);
    EXPECT_NE(unreadable->find("recreate the pool"), String::npos);
    EXPECT_EQ(unreadable->find("REBUILD"), String::npos)
        << "the diagnostic must not promise a command that cannot recover this exact object";
}

TEST(CASGCStuckRemoval, DiagnosticDoesNotAppendOrMutateBackend)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const RefWalkPlanRow row{
        .life = NamespaceLifeId::fromCatalogEntry(RootNamespace{"removing"}, UInt128{7}),
        .fold_state = {},
        .removal_started_round = 1,
        .has_parent_fold_state = false,
        .listed_hint = false,
        .checkpoint_observation = std::nullopt,
        .tail_observation = std::nullopt};
    const uint64_t puts_before = backend.putTotal();
    const uint64_t cas_before = backend.casPutTotal();
    EXPECT_TRUE(stuckRemovalWarning(row, 11, 10, layout));
    EXPECT_EQ(backend.putTotal(), puts_before);
    EXPECT_EQ(backend.casPutTotal(), cas_before);
    EXPECT_EQ(backend.deleteTotal(), 0u);
}

TEST(CASGCStuckRemoval, AdoptedRoundWarnsEveryRestartWithoutAppending)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .gc_stuck_removal_rounds = 10});
    const Layout & layout = store->layout();
    const UInt128 gc_id{99};
    const UInt128 life_id{7};

    const CatalogEntry removing{
        .ns = RootNamespace{"removing"},
        .state = NsState::Removing,
        .incarnation = life_id,
        .removal_started_round = 1};
    const auto catalog = backend->get(layout.refCatalogKey());
    ASSERT_TRUE(catalog);
    ASSERT_EQ(backend->casPut(
        layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {removing}}), catalog->token).outcome,
        CasOutcome::Committed);

    CasFoldSeal seal;
    seal.generation = 1;
    seal.ref_lives.emplace(life_id, RefLifeFoldState{
        .coverage = RefCoverage{
            .classification = 4,
            .hold = RefHold{
                .reason = HoldReason::BodyUndecodable,
                .offending_position = RefTxnId{5, 6},
                .retry_count = 0,
                .next_retry_round = 12}}});
    seal.condemned_summary[0] = CondemnedSummary{};
    ASSERT_EQ(backend->putIfAbsent(layout.foldSealKey(1, 1), encodeFoldSeal(seal)).outcome, PutOutcome::Done);

    GcState state;
    state.lease = GcLease{.owner = gc_id, .seq = 1};
    state.round = 11;
    state.gc_shards = 1;
    state.snap_generation = 1;
    state.snap_attempt = 1;
    ASSERT_EQ(backend->putIfAbsent(layout.gcStateKey(), encodeGcState(state)).outcome, PutOutcome::Done);

    const uint64_t signals_before
        = ProfileEvents::global_counters[ProfileEvents::CASGCStuckRemovals].load();
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(removing.ns, life_id);
    const String unreadable_ref_log_key = layout.refLogKey(life, RefTxnId{5, 6});
    const uint64_t append_puts_before = backend->putCount(unreadable_ref_log_key);
    ScopedCasGcLogCapture log_capture;
    Gc first_process(store, gc_id);
    EXPECT_TRUE(first_process.runRegularRound().acquired_lease);
    Gc restarted_process(store, gc_id);
    EXPECT_TRUE(restarted_process.runRegularRound().acquired_lease);

    EXPECT_EQ(ProfileEvents::global_counters[ProfileEvents::CASGCStuckRemovals].load() - signals_before, 2u);
    EXPECT_EQ(backend->putCount(unreadable_ref_log_key), append_puts_before)
        << "the diagnostic cannot append the unreadable ref log";
    const String captured = log_capture.captured();
    EXPECT_EQ(std::count(captured.begin(), captured.end(), '\n'), 2u);
    EXPECT_NE(captured.find(unreadable_ref_log_key), String::npos);
    EXPECT_NE(captured.find("is unreadable"), String::npos);
}

TEST(CASGCStuckRemoval, ZeroThresholdIsRefusedAtGcConstruction)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p",
        .server_root_id = "test",
        .gc_stuck_removal_rounds = 0});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS,
        [&] { Gc gc(store, UInt128{1}); });
}

TEST(CASGCRefWalkPlan, UnmatchedAdoptedParentLifeIsObservedWithoutEnteringThePlan)
{
    const NamespaceLifePhysicalId current_life{2};
    const NamespaceLifePhysicalId unmatched_life =
        hexToU128("fedcba98765432100123456789abcdef");
    RefCatalog catalog{.entries = {liveEntry("live", 2)}};
    const CasRefCatalog::Snapshot cut{
        .catalog = catalog, .token = std::nullopt, .life_index = CatalogLifeIndex(catalog)};
    RefScanSummary scan;
    scan.parent_ref_lives.emplace(current_life, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{2, 3}}});
    scan.parent_ref_lives.emplace(unmatched_life, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{9, 9}}});

    const uint64_t events_before =
        ProfileEvents::global_counters[ProfileEvents::CASGCUnmatchedAdoptedParentLives].load();
    const RefPlan plan = tests::buildRefWalkPlanForTest(scan, cut);

    EXPECT_EQ(
        ProfileEvents::global_counters[ProfileEvents::CASGCUnmatchedAdoptedParentLives].load() - events_before,
        1u);
    EXPECT_EQ(plan.droppedParentRows(), 1u);
    EXPECT_EQ(plan.size(), 1u);
    EXPECT_TRUE(plan.contains(current_life));
    EXPECT_FALSE(plan.contains(unmatched_life));
    EXPECT_FALSE(plan.parentFoldStates().contains(unmatched_life));
    EXPECT_FALSE(plan.successorFoldStates().contains(unmatched_life));
}

TEST(CASGCRefPlan, RoundInputOwnsObservationsAndSuccessorStateCannotChangePlan)
{
    /// This catches a plan that borrows the post-LIST observations or lets its successor state alias a
    /// row. Replacing the owning `RoundInput`/`RefPlan` boundary with the former loose inputs, or
    /// returning plan storage for the successor, must make this fail.
    static_assert(!std::is_constructible_v<RoundInput, RefScanSummary, CasRefCatalog::Snapshot>);
    static_assert(!std::is_default_constructible_v<RoundInput>);
    static_assert(!std::is_default_constructible_v<RefPlan>);
    static_assert(!std::is_assignable_v<RefPlan &, RefPlan>);
    static_assert(!std::is_assignable_v<RoundInput &, RoundInput>);

    RefCatalog catalog;
    catalog.entries = {liveEntry("live", 2)};
    CasRefCatalog::Snapshot cut{
        .catalog = catalog, .token = std::nullopt, .life_index = CatalogLifeIndex(catalog)};

    RefScanSummary observations;
    observations.max_log_by_life.emplace(UInt128{2}, RefTxnId{2, 7});
    observations.parent_ref_lives.emplace(UInt128{2}, RefLifeFoldState{
        .coverage = RefCoverage{.classification = 2, .last_folded_ref_id = RefTxnId{2, 3}}});

    const RefPlan plan = tests::buildRefWalkPlanForTest(observations, cut);

    /// The caller may reuse and mutate the sources after its one post-LIST/catalog observation and
    /// plan construction. Those mutations cannot retarget the plan DEFER, fold, and publication use.
    observations.max_log_by_life.at(UInt128{2}) = RefTxnId{2, 99};
    observations.parent_ref_lives.at(UInt128{2}).coverage.last_folded_ref_id = RefTxnId{2, 88};
    cut.catalog.entries.clear();

    ASSERT_TRUE(plan.contains(UInt128{2}));
    EXPECT_EQ(plan.row(UInt128{2}).tail_observation, (RefTxnId{2, 7}));
    EXPECT_EQ(plan.row(UInt128{2}).fold_state.coverage.last_folded_ref_id, (RefTxnId{2, 3}));

    /// A fold/rebuild successor starts as a copy. It can earn a new cleanup state without changing the
    /// immutable input that DEFER, the fold, and publication all consume.
    auto successor_lives = plan.successorFoldStates();
    successor_lives.at(UInt128{2}).coverage.last_folded_ref_id = RefTxnId{2, 9};
    successor_lives.emplace(UInt128{9}, RefLifeFoldState{});
    EXPECT_EQ(plan.row(UInt128{2}).fold_state.coverage.last_folded_ref_id, (RefTxnId{2, 3}));
    EXPECT_FALSE(plan.contains(UInt128{9}));
}
