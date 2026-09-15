#include <gtest/gtest.h>

#include "config.h"
#include "cas_format_test_battery.h"

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/SipHash.h>
#include <base/hex.h>

#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

/// Stage A task 5 (INV-4): the `_ckpt` object.
///
/// `_ckpt` exists because prefix cleaning made the ref stream unreadable from a LIST alone, so it is
/// simultaneously the thing recovery point-reads to find its base AND the gate on what cleanup may
/// delete. Both roles are only safe while three properties hold, and this suite pins exactly those:
///
///   1. the codec is STRICT in both directions -- a body that only partly decoded would be a cleanup
///      decision taken from a partly-read object;
///   2. there is ONE merge, by semantic maximum per field, used by BOTH writers -- a writer that
///      wrote back the value it sampled earlier regresses the other writer's progress, which is TLC
///      counterexample `_sab_sealclobbersbase` and costs an acked transaction;
///   3. every CAS attempt re-checks the admitted fence generation AFTER its read and BEFORE its write,
///      so a writer whose mount incarnation moved advances nothing.
///
/// The suite name is prefixed `Cas` so it is covered by the `Cas*` unit-test gate filter.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int NETWORK_ERROR;
extern const int UNKNOWN_FORMAT_VERSION;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::expectThrowsCode;
using DB::Cas::tests::namespaceBirthOp;
using DB::Cas::tests::publishCommittedOps;

namespace
{

const RefTxnId ID_1_1{1, 1};
const RefTxnId ID_1_2{1, 2};
const RefTxnId ID_2_1{2, 1};

PoolPtr openPool(const BackendPtr & backend)
{
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// The same one-transaction publish the other ref suites drive, so a namespace reaches `Live` through
/// the REAL append lane (which is also what creates its `_ckpt`).
RefTxnId publishRef(const PoolPtr & store, const RootNamespace & ns, const String & ref, uint64_t ordinal)
{
    return store->appendRefOps(ns, MutationScope::ref(ref),
        [&ref, ordinal](const RefTableState & state)
        {
            std::vector<RefOp> ops;
            if (state.getLifecycle() != RefLifecycle::Live)
                ops.push_back(namespaceBirthOp());
            for (const RefOp & op : publishCommittedOps(ref, ManifestRef{1, ordinal, 1}))
                ops.push_back(op);
            return ops;
        },
        RootMutationOrigin::Writer, RootMutationKind::Publish);
}

/// Reads `life`'s `_ckpt` and returns its body, or a default-constructed one after failing the
/// current test when the object is absent. Every assertion below goes through this rather than
/// dereferencing the optional directly: a bare `->` on a disengaged optional ABORTS the whole test
/// binary, so one regression would take every later suite's result with it instead of failing a test.
RefCkpt readCkptOrFail(CasOperation & op, const Layout & layout, const NamespaceLifeId & life)
{
    const std::optional<CkptSample> sample = readCkpt(op, layout, life);
    if (!sample)
    {
        ADD_FAILURE() << "expected a _ckpt for namespace '" << life.ns.string() << "', found none";
        return RefCkpt{};
    }
    return sample->ckpt;
}

/// Stage B: the incarnation `store`'s production birth wiring minted for `ns`, learned back
/// from the catalog exactly as a real reader would (`NamespaceLifeId::fromCatalogEntry`) -- once a real
/// `Pool`/`CasRefLedger` has opened the table, its ref-layer objects are no longer keyed at the
/// Stage-A sentinel, so every test below that drives the REAL append lane must ask the catalog what
/// incarnation it minted rather than assume the sentinel. Fails the current test (rather than
/// dereferencing a disengaged optional) if the catalog carries no entry for `ns` -- e.g. called before
/// the namespace's first append.
NamespaceLifeId liveLifeOrFail(CasOperation & op, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    for (const CatalogEntry & entry : snap.catalog.entries)
        if (entry.ns.string() == ns.string())
            return NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation);
    ADD_FAILURE() << "expected a catalog entry for namespace '" << ns.string() << "', found none";
    return DB::Cas::tests::fixture::fixtureLife(ns);
}

/// Replaces the whole body of one key, minting a new incarnation -- how a test installs a deliberately
/// malformed or concurrently-advanced object.
void overwriteObject(CasOperation & op, const String & key, const String & bytes)
{
    const WriteResult result = op.readModifyWrite(key,
        [&bytes](const std::optional<Object> & current) -> std::optional<String>
        {
            EXPECT_TRUE(current.has_value()) << "overwriteObject expects the key to exist";
            return bytes;
        },
        Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(result));
}

/// Per-key counts of the WRITE primitive. `CountingBackend` counts reads, heads and lists per key but
/// only totals for writes, and its legacy per-verb counters never see a caller that speaks the
/// primitives -- which every writer below does.
class WriteCountingBackend : public CountingBackend
{
public:
    uint64_t writes(const String & key) const
    {
        std::lock_guard lock(write_count_mutex);
        const auto it = write_counts.find(key);
        return it == write_counts.end() ? 0 : it->second;
    }

    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        {
            std::lock_guard lock(write_count_mutex);
            ++write_counts[key];
        }
        return CountingBackend::write(key, bytes, expected_value, access);
    }

private:
    mutable std::mutex write_count_mutex;
    std::map<String, uint64_t> write_counts;
};

/// The two PRIMITIVES `publishCkpt` speaks, instrumented: the exact request sequence against one
/// watched key, and the two shapes an ambiguous response has -- the store applied the write and then
/// lost the answer, or it never applied it. Hooks are public members rather than constructor arguments
/// so they can be installed AFTER the backend exists (every interesting hook writes through that same
/// backend) and only once the test's setup writes are done.
class CkptProbeBackend : public WriteCountingBackend
{
public:
    enum class Fault : uint8_t
    {
        None,
        CommitThenThrow,
        ThrowWithoutCommit,
        AlwaysThrowWithoutCommit,
    };

    String watched_key;
    Fault fault = Fault::None;
    /// Written over this call's own committed attempt, so the resolve read finds a WINNER rather than
    /// the bytes the attempt sent.
    String dominating_bytes;
    bool fail_reads_after_the_first = false;
    std::function<void()> after_write;
    std::function<void()> after_read;
    std::vector<String> journal;
    /// How many reads `fail_reads_after_the_first` actually made throw, so a test can assert the fault
    /// really fired rather than infer it from the journal's shape alone.
    size_t read_fault_hits = 0;

    /// A test that must watch a namespace's `_ckpt` key cannot compute it before the pool exists --
    /// the real incarnation is minted only once the namespace's first open resolves it. So the watch
    /// is retargeted once the test has learned the real key, strictly before arming any hook.
    void watch(String key)
    {
        watched_key = std::move(key);
        watched_reads = 0;
        journal.clear();
        read_fault_hits = 0;
    }

    void arm(const String & key, Fault fault_)
    {
        watch(key);
        fault = fault_;
    }

    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        if (key != watched_key)
            return WriteCountingBackend::read(key, access);

        journal.push_back("READ");
        ++watched_reads;
        if (watched_reads >= 2 && fail_reads_after_the_first)
        {
            ++read_fault_hits;
            throw Poco::TimeoutException("CkptProbeBackend: read response lost");
        }
        auto result = WriteCountingBackend::read(key, access);
        if (after_read)
            after_read();
        return result;
    }

    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        if (key != watched_key)
            return WriteCountingBackend::write(key, bytes, expected_value, access);

        journal.push_back("WRITE");
        const Fault this_fault = fault;
        if (fault != Fault::AlwaysThrowWithoutCommit)
            fault = Fault::None;
        if (this_fault == Fault::None)
            return WriteCountingBackend::write(key, bytes, expected_value, access);

        if (this_fault == Fault::CommitThenThrow)
        {
            const auto committed = WriteCountingBackend::write(key, bytes, expected_value, access);
            /// The winner's replacement is not journalled: it is not an attempt of the call under test.
            if (committed.has_value() && !dominating_bytes.empty())
                EXPECT_TRUE(WriteCountingBackend::write(key, dominating_bytes,
                                                        std::optional<String>{*committed}, access).has_value());
        }
        if (after_write)
            after_write();
        throw Poco::TimeoutException("CkptProbeBackend: write response lost");
    }

private:
    size_t watched_reads = 0;
};

}

/// ---------------------------------------------------------------------------------------------
/// The codec
/// ---------------------------------------------------------------------------------------------

/// Every combination of the frontier and existing optionals survives a round trip. Both-absent is the shape a namespace
/// carries from creation until its first snapshot, so it is a real state and not a degenerate one.
TEST(CASRefCheckpoint, RoundTripsEveryFieldCombination)
{
    const std::vector<RefCkpt> cases = {
        RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt},
        RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt},
        RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_2_1, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = ID_2_1},
        RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_2_1, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = ID_2_1},
        RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_2_1, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = ID_2_1},
    };
    for (const RefCkpt & ckpt : cases)
        EXPECT_EQ(decodeRefCkpt(encodeRefCkpt(ckpt)), ckpt);
}

TEST(CASRefCheckpoint, CommittedThroughHasCanonicalExactWireEncoding)
{
    const RefCkpt ckpt{.life_epoch = std::optional<uint64_t>{7},
                       .committed_through = RefTxnId{9, 11},
                       .checkpoint_snapshot_id = RefTxnId{9, 10},
                       .last_epoch_seal = RefTxnId{8, 12}};
    const String expected = R"({"type":"cas_ref_ckpt","v":1}
{"life_epoch":"7","committed_epoch":"9","committed_seq":"11","snapshot_epoch":"9","snapshot_seq":"10","seal_epoch":"8","seal_seq":"12"}
)";

    EXPECT_EQ(encodeRefCkpt(ckpt), expected);
    EXPECT_EQ(decodeRefCkpt(expected), ckpt);
}

CAS_BATTERY_COVERS(RefCkpt);

TEST(CASFormatBattery, RefCkpt)
{
    RefCkpt ckpt{.life_epoch = std::optional<uint64_t>{7},
                 .committed_through = RefTxnId{9, 11},
                 .checkpoint_snapshot_id = RefTxnId{9, 10},
                 .last_epoch_seal = RefTxnId{8, 12}};
    runFormatBattery({FormatId::RefCkpt,
        [&] { return sealObject(FormatId::RefCkpt, encodeRefCkpt(ckpt)); },
        [](std::string_view s) { decodeRefCkpt(std::string(openObject(FormatId::RefCkpt, s))); },
        currentFormatHeader("cas_ref_ckpt") +
        "{\"life_epoch\":\"7\",\"committed_epoch\":\"9\",\"committed_seq\":\"11\",\"snapshot_epoch\":\"9\",\"snapshot_seq\":\"10\",\"seal_epoch\":\"8\",\"seal_seq\":\"12\"}\n"});
}

/// `last_epoch_seal` is chain evidence, not an arbitrary lower bound. It either names the frontier
/// itself when that frontier is the terminal seal, or closes the immediately preceding numeric epoch.
/// Accepting a gap or a later same-epoch frontier would manufacture a boundary that INV-2 never proved.
TEST(CASRefCheckpoint, CodecRejectsIncoherentCommittedFrontierAndSealEpochs)
{
    const RefCkpt valid{.life_epoch = 7, .committed_through = RefTxnId{8, 5},
                        .checkpoint_snapshot_id = RefTxnId{7, 4}, .last_epoch_seal = RefTxnId{7, 9}};
    EXPECT_NO_THROW(encodeRefCkpt(valid));

    const RefCkpt skipped_epoch{.life_epoch = 7, .committed_through = RefTxnId{10, 1},
                                 .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{7, 9}};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefCkpt(skipped_epoch); });

    const RefCkpt frontier_after_same_epoch_seal{.life_epoch = 7, .committed_through = RefTxnId{8, 5},
                                                 .checkpoint_snapshot_id = std::nullopt,
                                                 .last_epoch_seal = RefTxnId{8, 1}};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { encodeRefCkpt(frontier_after_same_epoch_seal); });

    const RefCkpt unsealed_non_genesis{.life_epoch = 7, .committed_through = RefTxnId{8, 1},
                                        .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodeRefCkpt(unsealed_non_genesis); });

    String malformed = encodeRefCkpt(valid);
    const size_t committed_epoch = malformed.find(R"("committed_epoch":"8")");
    ASSERT_NE(committed_epoch, String::npos);
    malformed.replace(committed_epoch, String{R"("committed_epoch":"8")"}.size(), R"("committed_epoch":"10")");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(malformed); });
}

/// STRICT means an unknown key is corruption, not something to skip. A `_ckpt` decides deletions, so a
/// reader that ignored a field it did not understand would be authorizing them from a body it only
/// partly read.
TEST(CASRefCheckpoint, RejectsAnUnknownKey)
{
    const String good = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_1, .checkpoint_snapshot_id = ID_1_1,
                                              .last_epoch_seal = std::nullopt});
    String with_unknown = good;
    with_unknown.replace(with_unknown.rfind('}'), 1, R"(,"zz":"1"})");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(with_unknown); });

    /// A `!`-prefixed key is a REQUIRED extension and reports the version, not corruption -- the
    /// distinction is what lets an operator tell "this build is too old" from "this object is broken".
    String with_critical = good;
    with_critical.replace(with_critical.rfind('}'), 1, R"(,"!zz":"1"})");
    expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { decodeRefCkpt(with_critical); });
}

/// Replacing the abbreviated key is a format cut, not an alias. Treating it as an optional partial
/// pair would make an old writer's checkpoint appear to have no committed frontier.
TEST(CASRefCheckpoint, RejectsOldCommittedEpochKeyRatherThanAliasingIt)
{
    /// The values are chosen so ALIASING would be harmless: the spliced `"cte":"9"` re-assigns the
    /// epoch the object already carries, leaving a valid checkpoint. A reader that honoured the old
    /// spelling would therefore DECODE, and this test fails; only the strict unknown-key rejection
    /// makes it throw. Values under which aliasing corrupts the object would let the invariant
    /// checker throw the same code and hide the alias.
    String with_old_key = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{9},
                                                .committed_through = RefTxnId{9, 1},
                                                .checkpoint_snapshot_id = RefTxnId{9, 1},
                                                .last_epoch_seal = std::nullopt});
    with_old_key.replace(with_old_key.rfind('}'), 1, R"(,"cte":"9"})");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(with_old_key); });
}

/// A duplicate key has no single meaning, so it can never be resolved by a reader's preference.
TEST(CASRefCheckpoint, RejectsADuplicateKey)
{
    const String good = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = std::nullopt,
                                              .last_epoch_seal = std::nullopt});
    String duplicated = good;
    duplicated.replace(duplicated.rfind('}'), 1, R"(,"life_epoch":"9"})");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(duplicated); });
}

/// Truncation in each of its shapes. Half an optional pair is the dangerous one: silently dropping it
/// would turn a truncated body into a well-formed `_ckpt` with NO checkpoint, which reads as
/// "recovery has no base" and would be trusted.
TEST(CASRefCheckpoint, RejectsTruncation)
{
    const String good = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2,
                                              .last_epoch_seal = std::nullopt});

    const String header_only = good.substr(0, good.find('\n') + 1);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(header_only); });

    /// The body line without its terminator: a read that stopped mid-object.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(good.substr(0, good.size() - 1)); });

    /// An EMPTY body is not truncation, it is the legitimate "nobody knows anything yet" object -- the
    /// shape a namespace carries between its creation and its first checkpoint. Asserted here, next to
    /// the truncation cases, because the two are one character apart on the wire.
    const String empty_body = good.substr(0, good.find('\n') + 1) + "{}\n";
    EXPECT_EQ(decodeRefCkpt(empty_body), RefCkpt{});

    const String half_pair = good.substr(0, good.find('\n') + 1) + R"({"life_epoch":"7","snapshot_epoch":"1"})" + "\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(half_pair); });

    const String other_half = good.substr(0, good.find('\n') + 1) + R"({"life_epoch":"7","seal_seq":"2"})" + "\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(other_half); });

    const String frontier_half = good.substr(0, good.find('\n') + 1) + R"({"life_epoch":"7","committed_epoch":"1"})" + "\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(frontier_half); });
}

TEST(CASRefCheckpoint, RejectsTrailingBytes)
{
    const String good = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = std::nullopt,
                                              .last_epoch_seal = std::nullopt});
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(good + "junk\n"); });
}

/// The field-validity rule runs in BOTH directions: a struct this build refuses to read can never be
/// written by it either, so a bug on the write side surfaces at the writer and not as an unreadable
/// object discovered by a future recovery.
TEST(CASRefCheckpoint, RejectsInvalidFieldsOnEncodeAndOnDecode)
{
    /// PRESENT means REAL: an absent field is legal, a present-but-impossible one is not. A zero
    /// `life_epoch` would give the field two meanings ("unknown" and "epoch zero") on an object that
    /// gates deletions.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [] { encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{0}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [] { encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = RefTxnId{1, 0}, .last_epoch_seal = std::nullopt}); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [] { encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{0, 1}}); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [] { encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = std::nullopt}); });

    const String header = encodeRefCkpt(RefCkpt{.life_epoch = std::optional<uint64_t>{7}, .checkpoint_snapshot_id = std::nullopt,
                                                .last_epoch_seal = std::nullopt});
    const String prefix = header.substr(0, header.find('\n') + 1);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCkpt(prefix + R"({"life_epoch":"0"})" + "\n"); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodeRefCkpt(prefix + R"({"life_epoch":"7","snapshot_epoch":"1","snapshot_seq":"0"})" + "\n"); });
}

/// The registry row is part of the contract: Control/Strict decides how the decoder treats unknown
/// keys, and the caps are the first thing that fires if a foreign object ever lands at the key.
TEST(CASRefCheckpoint, RegistryRowIsControlStrictWithTightCaps)
{
    const FormatTraits & traits = traitsFor(FormatId::RefCkpt);
    EXPECT_EQ(traits.type, "cas_ref_ckpt");
    EXPECT_EQ(traits.family, TextFamily::Control);
    EXPECT_EQ(traits.strictness, KeyStrictness::Strict);
    EXPECT_EQ(traits.object_cap, 64u * 1024u);
    EXPECT_EQ(traits.line_cap, 4u * 1024u);
    EXPECT_EQ(traitsForType("cas_ref_ckpt"), &traits);
    /// Raw, so the key has no suffix -- the Stage A shape is exactly `<ns>/_ckpt`. This line is also
    /// the TRIPWIRE for the codec's shortcut: `encodeRefCkpt`/`decodeRefCkpt` hand bytes to and from
    /// the backend directly, bypassing `sealObject`/`openObject` because both are the identity under
    /// `CompressionPolicy::Never`. Flip the policy to `Always` and that bypass would silently write
    /// uncompressed bodies under a `.zst` key -- which this assertion catches first.
    EXPECT_EQ(storedSuffix(FormatId::RefCkpt), "");
    EXPECT_EQ(traits.compression, CompressionPolicy::Never);
}

/// ---------------------------------------------------------------------------------------------
/// The key
/// ---------------------------------------------------------------------------------------------

TEST(CASRefCheckpoint, KeyIsTheLifeLeafAndParsesBack)
{
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_key"};
    const NamespaceLifeId ns_id = DB::Cas::tests::fixture::fixtureLife(ns);
    EXPECT_EQ(layout.refCkptKey(ns_id),
        "p/cas/ns/state/" + renderIncarnation(ns_id.incarnation) + "/_ckpt");
    EXPECT_EQ(layout.parseRefCkptKey(layout.refCkptKey(ns_id)), ns_id.incarnation);

    /// `_ckpt` has no kind directory, so the id-bearing parser must NOT claim it -- and the `_ckpt`
    /// parser must not claim the id-bearing keys either. Each key has exactly one classifier.
    EXPECT_FALSE(layout.parseRefObjectKey(layout.refCkptKey(ns_id)).has_value());
    EXPECT_FALSE(layout.parseRefCkptKey(layout.refLogKey(ns_id, ID_1_1)).has_value());
    EXPECT_FALSE(layout.parseRefCkptKey(layout.refSnapshotKey(ns_id, ID_1_1)).has_value());
    EXPECT_FALSE(layout.parseRefCkptKey(layout.refCkptKey(ns_id) + ".zst").has_value());
    EXPECT_FALSE(layout.parseRefCkptKey("p/cas/ns/state/_ckpt").has_value());
    EXPECT_FALSE(layout.parseRefCkptKey("q" + layout.refCkptKey(ns_id).substr(1)).has_value());
}

/// The hot stream grouping accepts logs and snapshots while ignoring a checkpoint from the separate
/// state tree. An unrecognized key inside the stream tree still aborts the round.
TEST(CASRefCheckpoint, GroupRefKeysScopesHotIntakeToTheStreamTree)
{
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_group"};
    const std::vector<String> keys = {
        layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), ID_1_1),
        layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), ID_1_1),
        layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)),
    };

    const auto grouped = groupRefKeys(layout, keys);
    ASSERT_EQ(grouped.size(), 1u);
    const RefTableListing & listing = grouped.at(DB::Cas::tests::fixture::fixtureLife(ns).incarnation);
    EXPECT_EQ(listing.logs, std::vector<RefTxnId>{ID_1_1});
    EXPECT_EQ(listing.snapshots, std::vector<RefTxnId>{ID_1_1});

    /// A genuinely unrecognizable key inside this life stream is still corruption.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { groupRefKeys(layout, {layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_bogus"}); });
}

/// ---------------------------------------------------------------------------------------------
/// The merge -- per field, both directions
/// ---------------------------------------------------------------------------------------------

/// The per-field table the ledger obligation from the TLA phase asks for: each field independently
/// newer on either side, plus both-absent and equal bodies. A merge that is not per-field would pass
/// some rows and fail others, which is the point of enumerating them.
TEST(CASRefCheckpoint, MergeTakesThePerFieldSemanticMaximum)
{
    const RefCkpt low{.life_epoch = std::optional<uint64_t>{3}, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = ID_1_1};
    const RefCkpt high_ckpt{.life_epoch = std::optional<uint64_t>{3}, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = ID_1_1};
    const RefCkpt high_seal{.life_epoch = std::optional<uint64_t>{3}, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = ID_2_1};
    const RefCkpt high_life{.life_epoch = std::optional<uint64_t>{9}, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = ID_1_1};

    /// Each field newer on the RIGHT, then the same case mirrored to the LEFT: the merge is symmetric,
    /// which is exactly why the two writers need no ordering between them.
    ///
    /// The `life_epoch` rows stay mirrored, and that is a deliberate statement rather than an oversight:
    /// a `life_epoch` that FALLS is refused, but the refusal lives in `publishCkpt`, which knows which
    /// side is durable, and NOT here. This function stays commutative, so both directions must keep
    /// yielding the maximum. See `CASRefCheckpointJoin` (`gtest_cas_ref_ckpt_join.cpp`) for the refusal itself
    /// and for why it cannot be expressed at this level.
    EXPECT_EQ(mergeCkpt(low, high_ckpt), high_ckpt);
    EXPECT_EQ(mergeCkpt(high_ckpt, low), high_ckpt);
    EXPECT_EQ(mergeCkpt(low, high_seal), high_seal);
    EXPECT_EQ(mergeCkpt(high_seal, low), high_seal);
    EXPECT_EQ(mergeCkpt(low, high_life), high_life);
    EXPECT_EQ(mergeCkpt(high_life, low), high_life);

    /// Fields advance INDEPENDENTLY: a merge of two bodies each newer in a different field keeps both.
    const RefCkpt both = mergeCkpt(high_ckpt, high_seal);
    EXPECT_EQ(both.checkpoint_snapshot_id, ID_1_2);
    EXPECT_EQ(both.last_epoch_seal, ID_2_1);

    /// An absent optional loses to a present one, whichever side it is on, and two absents stay absent.
    const RefCkpt none{.life_epoch = std::optional<uint64_t>{3}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(mergeCkpt(none, low), low);
    EXPECT_EQ(mergeCkpt(low, none), low);
    EXPECT_EQ(mergeCkpt(none, none), none);

    /// Identical bodies merge to themselves -- the property `publishCkpt` turns into "no write".
    EXPECT_EQ(mergeCkpt(low, low), low);

    /// A contribution that knows NOTHING about `life_epoch` (the snapshot publisher's shape) must not
    /// erase it. This is the case a plain assignment would get wrong.
    const RefCkpt publisher_only{.life_epoch = std::nullopt, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt};
    const RefCkpt advanced = mergeCkpt(low, publisher_only);
    EXPECT_EQ(advanced.life_epoch, 3u);
    EXPECT_EQ(advanced.checkpoint_snapshot_id, ID_1_2);
    EXPECT_EQ(advanced.last_epoch_seal, ID_1_1) << "the publisher knows nothing about the seal and must "
                                                   "not drag it backwards";
}

/// ---------------------------------------------------------------------------------------------
/// publishCkpt
/// ---------------------------------------------------------------------------------------------

TEST(CASRefCheckpoint, CreatesTheObjectWhenItIsAbsent)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_create"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const RefCkpt birth{.life_epoch = std::optional<uint64_t>{5}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};

    EXPECT_EQ(publishCkpt(op, layout, life, birth), CkptPublishOutcome::Published);
    const auto sample = readCkpt(op, layout, life);
    ASSERT_TRUE(sample.has_value());
    EXPECT_EQ(sample->ckpt, birth);
}

/// Any writer may CREATE the object, and none of them may complete it. A publisher knows only the
/// checkpoint, so it creates an object that knows only the checkpoint; the birth transaction's
/// `life_epoch` merges in afterwards. Order does not matter -- the merge is a per-field maximum, and
/// no writer ever supplies a field it does not know (a guess here would be permanent, since the merge
/// can never lower it).
TEST(CASRefCheckpoint, EachWriterCreatesWithOnlyWhatItKnowsAndTheOtherFieldsMergeInLater)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_partial_create"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const RefCkpt publisher{.life_epoch = std::nullopt, .committed_through = ID_1_1, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = std::nullopt};

    ASSERT_EQ(publishCkpt(op, layout, life, publisher), CkptPublishOutcome::Published);
    const auto created = readCkpt(op, layout, life);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->ckpt.checkpoint_snapshot_id, ID_1_1);
    EXPECT_FALSE(created->ckpt.life_epoch.has_value()) << "the publisher must not invent a genesis epoch";

    const RefCkpt birth{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                        .last_epoch_seal = std::nullopt};
    ASSERT_EQ(publishCkpt(op, layout, life, birth), CkptPublishOutcome::Published);
    const auto completed = readCkpt(op, layout, life);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(completed->ckpt.life_epoch, 1u);
    EXPECT_EQ(completed->ckpt.checkpoint_snapshot_id, ID_1_1) << "and must not lose the checkpoint on the way in";
}

/// The conflict path is the whole reason the algorithm re-READS instead of retrying its bytes: the
/// winner's field must survive the loser's retry. Here a concurrent writer advances the seal between
/// our read and our write; our retry must merge onto the new body, not overwrite it.
TEST(CASRefCheckpoint, AConflictRereadsAndMergesOntoTheWinner)
{
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_conflict"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};

    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    CasOperation sealer_op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(sealer_op.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->watch(key);

    /// The concurrent sealer lands exactly ONCE, immediately after our first read -- so our first
    /// write carries a precondition that is no longer current, and our retry has to merge onto its
    /// body. It writes on its OWN operation: the interference is a different actor, not a reentrant
    /// call of the one under test.
    bool interfered = false;
    backend->after_read = [&]
    {
        if (interfered)
            return;
        interfered = true;
        const RefCkpt sealer{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_2_1, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = ID_2_1};
        overwriteObject(sealer_op, key, encodeRefCkpt(mergeCkpt(base, sealer)));
    };

    const RefCkpt publisher{.life_epoch = std::nullopt, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(publishCkpt(op, layout, life, publisher), CkptPublishOutcome::Published);

    backend->after_read = nullptr;
    const auto sample = readCkpt(op, layout, life);
    ASSERT_TRUE(sample.has_value());
    EXPECT_EQ(sample->ckpt.checkpoint_snapshot_id, ID_1_2) << "our own contribution must land";
    EXPECT_EQ(sample->ckpt.last_epoch_seal, ID_2_1)
        << "the concurrent writer's seal must survive our retry -- a retry that reused the body read "
           "before the conflict would silently drop it (TLC `_sab_sealclobbersbase`)";
    EXPECT_EQ(sample->ckpt.life_epoch, 1u);
    EXPECT_GE(std::count(backend->journal.begin(), backend->journal.end(), String{"WRITE"}), 2)
        << "the first write must have been refused, not skipped";
}

/// A contribution that adds nothing issues NO write. This is a correctness property, not a saving:
/// both writers publish on every snapshot and every seal, and a no-op write would mint a fresh
/// incarnation each time, turning every other writer's in-flight write into a conflict for identical
/// bytes.
TEST(CASRefCheckpoint, AnIdenticalMergedBodyIssuesNoWrite)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_noop"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.refCkptKey(life);
    const RefCkpt full{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_2_1, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = ID_2_1};

    ASSERT_EQ(publishCkpt(op, layout, life, full), CkptPublishOutcome::Published);
    const uint64_t writes_after_create = backend->writes(key);
    const auto meta_after_create = op.head(key, Retry::standard());
    ASSERT_TRUE(meta_after_create.has_value());

    /// The same contribution again, and a strictly OLDER one: neither adds anything.
    EXPECT_EQ(publishCkpt(op, layout, life, full), CkptPublishOutcome::IdenticalSkip);
    const RefCkpt older{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_1, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(publishCkpt(op, layout, life, older), CkptPublishOutcome::IdenticalSkip);

    EXPECT_EQ(backend->writes(key), writes_after_create) << "a skip must issue no write at all";
    const auto meta_after_skips = op.head(key, Retry::standard());
    ASSERT_TRUE(meta_after_skips.has_value());
    EXPECT_EQ(meta_after_skips->etag, meta_after_create->etag)
        << "and must not mint a new incarnation";
}

/// Admission is re-checked AFTER the read and BEFORE the write. A writer whose admission was lost in
/// that window has a stale merged body, so its write must never be sent.
TEST(CASRefCheckpoint, AnAdmissionLossBetweenTheReadAndTheWriteWritesNothing)
{
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_fenced"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.refCkptKey(life);

    auto backend = std::make_shared<CkptProbeBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    bool admitted = true;
    CasOperation op = requests.admit([&admitted] { return admitted; });
    CasOperation reader = requests.admit();

    const RefCkpt base{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_1, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = std::nullopt};
    ASSERT_EQ(publishCkpt(op, layout, life, base), CkptPublishOutcome::Published);
    const auto meta_before = reader.head(key, Retry::standard());
    ASSERT_TRUE(meta_before.has_value());

    /// Armed only now, so the loss lands inside the publish's own read-then-write window rather than
    /// before it began.
    backend->watch(key);
    backend->after_read = [&admitted] { admitted = false; };
    const uint64_t writes_before = backend->writes(key);

    const RefCkpt advance{.life_epoch = std::nullopt, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt};
    EXPECT_EQ(publishCkpt(op, layout, life, advance), CkptPublishOutcome::FencedOut);
    backend->after_read = nullptr;

    EXPECT_EQ(backend->writes(key), writes_before) << "the check precedes the write, so nothing is sent";
    const auto meta_after = reader.head(key, Retry::standard());
    ASSERT_TRUE(meta_after.has_value());
    EXPECT_EQ(meta_after->etag, meta_before->etag);
    EXPECT_EQ(readCkptOrFail(reader, layout, life), base);
}

/// Persistent contention fails CLOSED and says so. There is no partial state to clean up -- every
/// attempt either committed the complete merged body or changed nothing -- but the caller must be told
/// its contribution is unpublished rather than left to assume it landed.
TEST(CASRefCheckpoint, AnExhaustedDeadlineUnderPersistentConflictThrowsRetryLater)
{
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_exhausted"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_1, .checkpoint_snapshot_id = ID_1_1, .last_epoch_seal = std::nullopt};

    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    CasOperation setup = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(setup.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->watch(key);

    /// Every read is followed by a rewrite of the SAME body under a fresh incarnation, so the
    /// precondition this call holds is always stale and every write it issues is refused. Only the
    /// policy's deadline can end the loop, and the injected clock reaches it without sleeping.
    ///
    /// `overwriteObject` itself reads `key` (its own `readModifyWrite`'s precondition read), which
    /// would re-enter this very hook -- unlike the concurrent-actor fixtures elsewhere in this file,
    /// this rewrite is not a one-shot: it must keep firing on every OUTER read, so a plain one-shot
    /// latch would silently stop the persistent conflict after the first attempt. Guard only the
    /// reentrant call instead.
    bool rewriting = false;
    backend->after_read = [&]
    {
        if (rewriting)
            return;
        rewriting = true;
        overwriteObject(setup, key, encodeRefCkpt(base));
        rewriting = false;
    };

    const RefCkpt advance{.life_epoch = std::nullopt, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt};
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { publishCkpt(op, layout, life, advance); });
    backend->after_read = nullptr;
    EXPECT_FALSE(clock.sleeps.empty()) << "the loop must back off between attempts, not spin";
    EXPECT_EQ(readCkptOrFail(setup, layout, life), base) << "no partial state: every attempt either "
                                                            "committed the complete merged body or wrote nothing";
}

TEST(CASRefCheckpoint, AnAmbiguousCommittedWriteIsResolvedByOneExactReadWithoutBlindRetry)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/ckpt_ambiguous_committed"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt contribution{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                               .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->arm(key, CkptProbeBackend::Fault::CommitThenThrow);

    EXPECT_EQ(publishCkpt(op, layout, life, contribution), CkptPublishOutcome::Published);
    EXPECT_EQ(backend->journal, (std::vector<String>{"READ", "WRITE", "READ"}));
    backend->watched_key.clear();
    EXPECT_EQ(readCkptOrFail(op, layout, life).committed_through, ID_1_2);
}

TEST(CASRefCheckpoint, AnAmbiguousUncommittedWriteRetriesAgainstTheExactRead)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/ckpt_ambiguous_retry"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt contribution{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                               .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->arm(key, CkptProbeBackend::Fault::ThrowWithoutCommit);

    EXPECT_EQ(publishCkpt(op, layout, life, contribution), CkptPublishOutcome::Published);
    EXPECT_EQ(backend->journal, (std::vector<String>{"READ", "WRITE", "READ", "WRITE"}));
    backend->watched_key.clear();
    EXPECT_EQ(readCkptOrFail(op, layout, life).committed_through, ID_1_2);
}

/// The durable body a winner left behind already dominates this contribution, so nothing more is owed.
/// The verdict is `Published` rather than `IdenticalSkip` because an attempt of THIS call was sent:
/// `IdenticalSkip` promises no write was issued, and that promise has to stay true.
TEST(CASRefCheckpoint, AnAmbiguousWriteAcceptsAValidDominatingDurableFrontier)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/ckpt_ambiguous_dominating"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt contribution{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                               .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    const RefCkpt dominating{.life_epoch = 1, .committed_through = ID_2_1,
                             .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = ID_2_1};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->dominating_bytes = encodeRefCkpt(dominating);
    backend->arm(key, CkptProbeBackend::Fault::CommitThenThrow);

    EXPECT_EQ(publishCkpt(op, layout, life, contribution), CkptPublishOutcome::Published);
    EXPECT_EQ(backend->journal, (std::vector<String>{"READ", "WRITE", "READ"}));
    backend->watched_key.clear();
    EXPECT_EQ(readCkptOrFail(op, layout, life), dominating);
}

/// A resolve read that never answers leaves the attempt unproven, and the call must neither report it
/// committed nor send a second attempt on top of it. The engine reissues -- that is its contract -- but
/// every reissue is preceded by its own exact read.
TEST(CASRefCheckpoint, AFailedResolveReadNeverReportsACommitAndNeverSkipsTheRead)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    CasOperation reader = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/ckpt_ambiguous_read_failed"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, encodeRefCkpt(base), Retry::standard())));
    backend->arm(key, CkptProbeBackend::Fault::AlwaysThrowWithoutCommit);
    backend->fail_reads_after_the_first = true;

    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        publishCkpt(op, layout, life, RefCkpt{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                    .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    });
    /// `{READ, WRITE}` alone would satisfy "no adjacent writes" and "at least one write" without the
    /// resolving read ever having been attempted, let alone failed. Pin that at least one read follows
    /// the write, that the fault double actually fired on every one of them (a resolving read is itself
    /// retried against the policy deadline, so several follow, not just one), and that no reissue was
    /// sent while every resolution read fails.
    ASSERT_GE(backend->journal.size(), 3u);
    EXPECT_EQ(backend->journal.front(), "READ") << "the baseline read of the current state";
    EXPECT_EQ(backend->journal[1], "WRITE") << "the attempt that never committed";
    const size_t resolve_reads = backend->journal.size() - 2;
    EXPECT_TRUE(std::all_of(backend->journal.begin() + 2, backend->journal.end(),
                            [](const String & verb) { return verb == "READ"; }))
        << "no reissue can be sent while every resolution read fails, so nothing after the write is a WRITE";
    EXPECT_EQ(backend->read_fault_hits, resolve_reads)
        << "every read after the baseline failed -- the fault double actually fired on all of them, not just "
        << "the first";
    EXPECT_EQ(std::count(backend->journal.begin(), backend->journal.end(), String{"WRITE"}), 1)
        << "no reissue can be sent while every resolution read fails";
    backend->watched_key.clear();
    backend->fail_reads_after_the_first = false;
    EXPECT_EQ(readCkptOrFail(reader, layout, life), base);
}

/// Admission lost while the ambiguous write was in flight: the exact read that would settle it is
/// refused before it starts, so the call reports `FencedOut` and claims nothing about the object.
TEST(CASRefCheckpoint, AdmissionLostWithTheAmbiguousWritePreventsItsResolveRead)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    bool admitted = true;
    CasOperation op = requests.admit([&admitted] { return admitted; });
    CasOperation reader = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(
        RootNamespace{"srv1/ckpt_admission_lost_before_resolution"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_TRUE(std::holds_alternative<Committed>(reader.create(key, encodeRefCkpt(base), Retry::standard())));

    backend->after_write = [&admitted] { admitted = false; };
    backend->arm(key, CkptProbeBackend::Fault::CommitThenThrow);

    EXPECT_EQ(publishCkpt(op, layout, life,
                          RefCkpt{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                                  .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}),
              CkptPublishOutcome::FencedOut);
    EXPECT_EQ(backend->journal, (std::vector<String>{"READ", "WRITE"}))
        << "the resolve read started after admission was withdrawn";
}

/// Admission lost AFTER the resolve read proved the attempt durable: the object may well carry this
/// contribution, but a call whose admission is gone must never claim it.
TEST(CASRefCheckpoint, AdmissionLostAfterTheResolveReadStillRefusesToClaimTheCommit)
{
    auto backend = std::make_shared<CkptProbeBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    bool admitted = true;
    CasOperation op = requests.admit([&admitted] { return admitted; });
    CasOperation reader = requests.admit();
    const Layout layout{"p"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(RootNamespace{"srv1/ckpt_fence_after_resolution"});
    const String key = layout.refCkptKey(life);
    const RefCkpt base{.life_epoch = 1, .committed_through = ID_1_1,
                       .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    ASSERT_TRUE(std::holds_alternative<Committed>(reader.create(key, encodeRefCkpt(base), Retry::standard())));

    backend->arm(key, CkptProbeBackend::Fault::CommitThenThrow);
    /// The SECOND read is the resolve read; withdrawing after the first would refuse the write instead
    /// and never reach the point this test is about.
    size_t reads = 0;
    backend->after_read = [&]
    {
        if (++reads == 2)
            admitted = false;
    };

    EXPECT_EQ(publishCkpt(op, layout, life,
                          RefCkpt{.life_epoch = std::nullopt, .committed_through = ID_1_2,
                                  .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt}),
              CkptPublishOutcome::FencedOut);
    EXPECT_EQ(backend->journal, (std::vector<String>{"READ", "WRITE", "READ"}));
}

/// Both verdicts `publishCkpt` reaches WITHOUT writing consult admission before they speak, and both
/// answer `FencedOut` when it is gone. A writer the fence is about to refuse landed nothing anywhere:
/// telling it `IdenticalSkip` would claim its contribution is already durable, and telling it
/// `CORRUPTED_DATA` would turn a transient control signal into a permanent verdict on the namespace.
TEST(CASRefCheckpoint, DeclineTimeVerdictsReadAdmitted)
{
    const Layout layout{"p"};
    for (const bool decreasing : {false, true})
    {
        auto backend = std::make_shared<CkptProbeBackend>();
        CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
        bool admitted = true;
        CasOperation op = requests.admit([&admitted] { return admitted; });
        CasOperation reader = requests.admit();
        const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(
            RootNamespace{decreasing ? "srv1/ckpt_decline_decrease" : "srv1/ckpt_decline_identical"});
        const String key = layout.refCkptKey(life);
        /// A genesis epoch of 2 with the frontier in that same epoch: `checkRefCkptInvariants` refuses
        /// a `committed_through` preceding `life_epoch`, so the durable body a decrease is measured
        /// against has to be one the format would actually store.
        const RefCkpt durable{.life_epoch = std::optional<uint64_t>{2}, .committed_through = ID_2_1,
                              .checkpoint_snapshot_id = ID_2_1, .last_epoch_seal = std::nullopt};
        ASSERT_EQ(publishCkpt(reader, layout, life, durable), CkptPublishOutcome::Published);
        const uint64_t writes_before = backend->writes(key);

        /// Admission survives the read and is gone by the time the verdict is reached -- the exact
        /// window in which the answer must be `FencedOut` and nothing else.
        backend->watch(key);
        backend->after_read = [&admitted] { admitted = false; };
        const RefCkpt contribution = decreasing
            ? RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .checkpoint_snapshot_id = std::nullopt,
                      .last_epoch_seal = std::nullopt}
            : durable;
        EXPECT_EQ(publishCkpt(op, layout, life, contribution), CkptPublishOutcome::FencedOut)
            << (decreasing ? "a superseded epoch from an unadmitted writer is not corruption"
                           : "an identical body from an unadmitted writer is not a skip");
        backend->after_read = nullptr;

        EXPECT_EQ(backend->writes(key), writes_before) << "neither verdict may write";
        EXPECT_EQ(readCkptOrFail(reader, layout, life), durable);
    }
}

/// A `_ckpt` that does not decode is NEVER overwritten. It is the only record of recovery's base and
/// of what cleanup may delete, so replacing it with a body derived from the contribution alone would
/// erase the base and leave a well-formed object a reader would trust.
TEST(CASRefCheckpoint, ACorruptCheckpointIsNeverOverwritten)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RootNamespace ns{"srv1/ckpt_corrupt"};
    const NamespaceLifeId life = DB::Cas::tests::fixture::fixtureLife(ns);
    const String key = layout.refCkptKey(life);
    ASSERT_EQ(publishCkpt(op, layout, life,
                          RefCkpt{.life_epoch = std::optional<uint64_t>{1}, .committed_through = ID_1_2, .checkpoint_snapshot_id = ID_1_2, .last_epoch_seal = std::nullopt}),
              CkptPublishOutcome::Published);

    const String garbage = "not a cas object\n";
    overwriteObject(op, key, garbage);

    const RefCkpt birth{.life_epoch = std::optional<uint64_t>{5}, .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt};
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { publishCkpt(op, layout, life, birth); });
    const auto still_there = op.read(key, Retry::standard());
    ASSERT_TRUE(still_there.has_value());
    EXPECT_EQ(still_there->bytes, garbage) << "corruption must be surfaced, never laundered into a "
                                              "well-formed object";
}

/// ---------------------------------------------------------------------------------------------
/// The reader-side rules Task 6 and the cleanup call sites consume
/// ---------------------------------------------------------------------------------------------

/// INV-4's three-way revalidation of a base that turned out to be missing. The two incarnations come
/// from real reads of the same key across a rewrite, because an incarnation exists only as something a
/// request observed.
TEST(CASRefCheckpoint, AMissingSampledBaseRestartsOnAnAdvancedIncarnationAndIsCorruptionOnAnUnchangedOne)
{
    auto backend = std::make_shared<CountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const String key = "p/ckpt_incarnations";
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, "first", Retry::standard())));
    const auto first = op.read(key, Retry::standard());
    ASSERT_TRUE(first.has_value());
    overwriteObject(op, key, "second");
    const auto second = op.read(key, Retry::standard());
    ASSERT_TRUE(second.has_value());
    const Etag sampled = first->etag;
    const Etag advanced = second->etag;
    ASSERT_FALSE(sampled == advanced) << "the rewrite must mint a different incarnation";

    EXPECT_EQ(classifyMissingSampledBase(sampled, advanced), MissingBaseVerdict::RestartRecovery)
        << "cleanup legitimately moved the checkpoint while we read; restart from the newer base";
    EXPECT_EQ(classifyMissingSampledBase(sampled, sampled), MissingBaseVerdict::Corrupted)
        << "the checkpoint still names an object that is not there, which the strictly-below deletion "
           "gate makes unreachable in an honest run";
    EXPECT_EQ(classifyMissingSampledBase(sampled, std::nullopt), MissingBaseVerdict::Corrupted)
        << "a namespace with a sampled base and no checkpoint at all is worse, not better";
}

/// The deletion gate is STRICTLY below, because the checkpoint names the snapshot a recovery is
/// entitled to fetch by exact key. At-or-below is TLC counterexample `_sab_staleckptcorruption`.
TEST(CASRefCheckpoint, SnapshotsAreDeletableStrictlyBelowTheCheckpoint)
{
    EXPECT_TRUE(snapshotDeletableUnderCkpt(ID_1_1, ID_1_2));
    EXPECT_FALSE(snapshotDeletableUnderCkpt(ID_1_2, ID_1_2)) << "the checkpoint's own base is off limits";
    EXPECT_FALSE(snapshotDeletableUnderCkpt(ID_2_1, ID_1_2));
    /// Fail closed: a namespace with no checkpoint has established no covering base, so nothing is
    /// deletable -- a stale or absent pointer may only ever under-clean.
    EXPECT_FALSE(snapshotDeletableUnderCkpt(ID_1_1, std::nullopt));
}

/// ---------------------------------------------------------------------------------------------
/// The REAL call sites, through the ledger
/// ---------------------------------------------------------------------------------------------

/// The namespace-birth transaction creates the checkpoint, and it is the only writer that can: the
/// `life_epoch` is this transaction's own writer epoch.
TEST(CASRefCheckpoint, NamespaceBirthCreatesTheCheckpointCarryingItsLifeEpoch)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"srv1/ckpt_birth"};

    /// Stage B: the catalog carries no entry for `ns` before its first open, and the
    /// namespace's real incarnation does not exist to name a key with yet -- the pre-birth analog of
    /// "nothing exists" is "nothing is even NAMED", checked at the catalog rather than at a key this
    /// test cannot yet compute.
    EXPECT_TRUE(CasRefCatalog::read(op, store->layout()).catalog.entries.empty())
        << "nothing exists before the birth";
    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));

    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const auto sample = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(sample.has_value()) << "spec §3 creates the _ckpt before the namespace becomes Live";
    EXPECT_EQ(sample->ckpt.life_epoch, store->writerEpoch());
    EXPECT_FALSE(sample->ckpt.checkpoint_snapshot_id.has_value()) << "a newborn namespace has no base yet";
    EXPECT_FALSE(sample->ckpt.last_epoch_seal.has_value());
}

/// The snapshot publisher is INV-4's second writer: the body PUT commits, then the checkpoint names it.
TEST(CASRefCheckpoint, ACommittedSnapshotPublishAdvancesTheCheckpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/ckpt_publish"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    ASSERT_FALSE(readCkptOrFail(op, store->layout(), life).checkpoint_snapshot_id.has_value());

    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    const auto published = store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_TRUE(published.has_value());

    const auto sample = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(sample.has_value());
    EXPECT_EQ(sample->ckpt.checkpoint_snapshot_id, published);
    EXPECT_EQ(sample->ckpt.life_epoch, epoch) << "the publisher contributes nothing about life_epoch, so "
                                                 "the merge must preserve what the birth wrote";
    /// And the snapshot body it names really is there -- the checkpoint may never point at a key that
    /// does not exist, which is the premise the missing-base rule reasons from.
    EXPECT_TRUE(op.head(store->layout().refSnapshotKey(life, *published), Retry::standard()).has_value());
}

/// The body-PUT/cleanup/`_ckpt` race, decided by the ORDER of the two writes: cleanup planned in the
/// window between the snapshot body PUT and the checkpoint CAS still reads the OLD checkpoint, and the
/// gate is strictly below it -- so it cannot delete the snapshot just published.
TEST(CASRefCheckpoint, CleanupPlannedBetweenTheBodyPutAndTheCkptCasCannotDeleteTheNewSnapshot)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/ckpt_race"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    const RefTxnId first_snapshot = *store->newestPublishedSnapshotIdForTest(ns);

    ASSERT_EQ(publishRef(store, ns, "ref_2", 2), (RefTxnId{epoch, 2}));
    /// The checkpoint a cleanup pass sampled BEFORE the second publication -- the stale reading the
    /// race hands it.
    const std::optional<RefTxnId> stale_checkpoint = readCkptOrFail(op, store->layout(), life).checkpoint_snapshot_id;
    ASSERT_EQ(stale_checkpoint, first_snapshot);

    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    const RefTxnId second_snapshot = *store->newestPublishedSnapshotIdForTest(ns);
    ASSERT_LT(first_snapshot, second_snapshot);

    /// Planning against the STALE checkpoint: the just-published snapshot is not deletable, and neither
    /// is the one the stale checkpoint itself names. A stale pointer can only under-clean.
    EXPECT_FALSE(snapshotDeletableUnderCkpt(second_snapshot, stale_checkpoint));
    EXPECT_FALSE(snapshotDeletableUnderCkpt(first_snapshot, stale_checkpoint));
    /// Once the checkpoint is re-read, the older snapshot becomes reclaimable and the base does not.
    const std::optional<RefTxnId> fresh_checkpoint = readCkptOrFail(op, store->layout(), life).checkpoint_snapshot_id;
    EXPECT_TRUE(snapshotDeletableUnderCkpt(first_snapshot, fresh_checkpoint));
    EXPECT_FALSE(snapshotDeletableUnderCkpt(second_snapshot, fresh_checkpoint));
}

/// One `_ckpt` write per publication and not one more: the checkpoint is written where the snapshot is
/// published, and a publisher with nothing above its newest snapshot touches it at all.
TEST(CASRefCheckpoint, TheCheckpointIsWrittenOncePerPublicationAndNotOnIdleAttempts)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const uint64_t epoch = store->writerEpoch();
    const RootNamespace ns{"srv1/ckpt_republish"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{epoch, 1}));
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String key = store->layout().refCkptKey(life);
    const uint64_t writes_after_birth = backend->writes(key);
    EXPECT_EQ(writes_after_birth, 2u)
        << "birth publishes `life_epoch` before its log, then the durable log's committed frontier";

    ASSERT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_EQ(backend->writes(key), writes_after_birth + 1) << "one publication, one checkpoint write";
    const uint64_t writes_after_publish = backend->writes(key);
    const auto after_publish = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(after_publish.has_value());

    /// Nothing was appended since, so there is nothing above the newest snapshot: the publisher declines
    /// before it reaches the checkpoint at all, and repeating the attempt changes nothing.
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_EQ(backend->writes(key), writes_after_publish);
    EXPECT_EQ(readCkptOrFail(op, store->layout(), life), after_publish->ckpt);
}

TEST(CASRefCheckpoint, SnapshotPublisherRefusesEpochSealCandidateWithoutAnyWrite)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"srv1/no_snapshot_at_seal"};
    uint64_t predecessor_epoch = 0;
    {
        auto predecessor = openPool(backend);
        predecessor_epoch = predecessor->writerEpoch();
        ASSERT_EQ(publishRef(predecessor, ns, "ref_1", 1), (RefTxnId{predecessor_epoch, 1}));
    }

    auto store = openPool(backend);
    ASSERT_GT(store->writerEpoch(), predecessor_epoch);
    ASSERT_EQ(store->listRefs(ns).size(), 1u) << "recovery must close the predecessor epoch before publishing";

    const RefTxnId seal_id{predecessor_epoch, 2};
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String snapshot_key = store->layout().refSnapshotKey(life, seal_id);
    const String ckpt_key = store->layout().refCkptKey(life);
    ASSERT_EQ(store->lastEpochSealForTest(ns), std::make_optional(seal_id));
    ASSERT_EQ(readCkptOrFail(op, store->layout(), life).committed_through, std::make_optional(seal_id));
    const uint64_t snapshot_writes_before = backend->writes(snapshot_key);
    const uint64_t ckpt_writes_before = backend->writes(ckpt_key);

    /// Recovery installed the epoch seal as the runtime's greatest applied transaction. The publisher
    /// must decline it without reaching either durable write.
    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_EQ(backend->writes(snapshot_key), snapshot_writes_before);
    EXPECT_EQ(backend->writes(ckpt_key), ckpt_writes_before);

    /// Once an ordinary transaction advances the candidate beyond the seal, normal publication resumes.
    ASSERT_EQ(publishRef(store, ns, "ref_2", 2), (RefTxnId{store->writerEpoch(), 1}));
    EXPECT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
}

/// Publication replays a `NeedsRecovery` lane before it captures a snapshot and advances `_ckpt`.
TEST(CASRefCheckpoint, NeedsRecoveryReplaysBeforeCheckpointAdvance)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"srv1/ckpt_poisoned"};

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String key = store->layout().refCkptKey(life);
    const auto before = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(before.has_value());
    ASSERT_FALSE(before->ckpt.checkpoint_snapshot_id.has_value());
    const uint64_t writes_before = backend->writes(key);

    /// Enter `NeedsRecovery`: an install throws after its transaction is
    /// durable, leaving this cached table missing a transaction the log contains.
    auto planned = std::make_exception_ptr(DB::Exception(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED,
        "simulated allocation failure inside the post-durable install region"));
    auto fired = std::make_shared<std::atomic<bool>>(false);
    store->setInstallRegionProbeForTest([planned, fired]
    {
        if (fired->exchange(true))
            return;
        ALLOW_ALLOCATIONS_IN_SCOPE;
        std::rethrow_exception(planned);
    });
    expectThrowsCode(DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED, [&] { publishRef(store, ns, "ref_2", 2); });
    store->setInstallRegionProbeForTest(nullptr);
    ASSERT_EQ(store->laneStateForTest(ns), RefLaneState::NeedsRecovery);

    /// The publish entry point recovers first, so the snapshot covers the stranded transaction.
    EXPECT_TRUE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns));
    EXPECT_TRUE(store->resolveRef(ns, "ref_2", /*allow_stale=*/false).has_value())
        << "the stranded transaction is durable; the re-derivation must have applied it";
    EXPECT_EQ(store->laneStateForTest(ns), RefLaneState::Ready);
    EXPECT_GT(backend->writes(key), writes_before)
        << "and the checkpoint advances -- truthfully, over a snapshot that is not missing anything";
    EXPECT_TRUE(readCkptOrFail(op, store->layout(), life).checkpoint_snapshot_id.has_value());

}

/// A publish admitted under an incarnation that is replaced mid-attempt advances NOTHING, and does not
/// adopt the snapshot either -- adopting would suppress every later publication for it while the
/// checkpoint still pointed below it, leaving recovery on an older base with nothing to fix it.
TEST(CASRefCheckpoint, APublishFencedOutMidAttemptDoesNotAdvanceTheCheckpoint)
{
    const RootNamespace ns{"srv1/ckpt_stale_gen"};

    /// The watched key cannot be computed yet -- the real incarnation is minted only once the pool
    /// exists and this namespace's first open resolves it (`setWatchedKey` below, once it has).
    auto backend = std::make_shared<CkptProbeBackend>();
    DB::Cas::tests::seedPoolMetaForRestart(*backend);
    PoolPtr store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();

    ASSERT_EQ(publishRef(store, ns, "ref_1", 1), (RefTxnId{store->writerEpoch(), 1}));
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String ckpt_key = store->layout().refCkptKey(life);
    const auto before = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(before.has_value());
    ASSERT_FALSE(before->ckpt.checkpoint_snapshot_id.has_value());
    const uint64_t writes_before = backend->writes(ckpt_key);
    backend->watch(ckpt_key);

    /// Arm only after the precondition read above. The next watched `_ckpt` read is therefore the one
    /// inside this publish's read-then-CAS window, after the attempt captured its immutable runtime
    /// generation. Arming before `readCkpt` would stale the runtime before the operation began and test
    /// entry admission instead of the intended mid-attempt recheck.
    bool hook_fired = false;
    backend->after_read = [&]
    {
        if (hook_fired)
            return;
        hook_fired = true;
        DB::Cas::tests::rearmMountFenceAfterAnomalyForTest(store);
    };

    EXPECT_FALSE(store->tryPublishSnapshotAndAdvanceCheckpointOnce(ns))
        << "a publish whose checkpoint could not be advanced must not report success";
    EXPECT_TRUE(hook_fired) << "the checkpoint read-then-write seam was never exercised";
    backend->after_read = nullptr;
    EXPECT_EQ(backend->writes(ckpt_key), writes_before) << "nothing may be sent after the fence moved";
    EXPECT_FALSE(readCkptOrFail(op, store->layout(), life).checkpoint_snapshot_id.has_value());
    EXPECT_FALSE(store->newestPublishedSnapshotIdForTest(ns).has_value())
        << "the snapshot must not be adopted as the newest while its checkpoint is unpublished";
}

/// ===================================================================================
/// Equivalence fences for the `prepareRefChunk` extraction
/// ===================================================================================
///
/// An extraction is only safe to review if something pins what crosses its boundary. These three
/// fences are deliberately NOT red-first: they pass on the PRE-extraction tree and must keep passing
/// after it, which is the whole point -- the literals below were captured from a real append on the
/// pre-extraction tree and pasted in, so re-deriving them afterwards cannot silently measure the
/// change against itself.
///
/// They live in this TU rather than beside the pure preparation tests because all three need a real
/// backend and the real append lane, which this suite already drives through `publishRef` (including
/// the namespace birth, the one chunk shape whose first durable effect is the `_ckpt` and not the
/// ref-log `PUT`).
TEST(CASRefCheckpoint, CommitRefChunkDurableBytesUnchangedByExtraction)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"test/golden@cas@"};

    const RefTxnId id = publishRef(store, ns, "gold_ref", 7);
    ASSERT_EQ(id.writer_epoch, 1u);
    ASSERT_EQ(id.ref_sequence, 1u);

    /// The KEY carries the namespace incarnation, so its life segment is rendered rather than pasted;
    /// every other segment is literal. Stage B: the incarnation is now a REAL, randomly minted catalog
    /// value rather than the Stage-A sentinel, so it is learned back
    /// from the catalog (`liveLifeOrFail`) rather than pasted as a literal -- the shape assertion below
    /// is unaffected, since it names every OTHER segment literally and renders this one dynamically.
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String key = store->layout().refLogKey(life, id);
    EXPECT_EQ(key, "p/cas/ns/stream/" + renderIncarnation(life.incarnation)
                   + "/_log/0000000000000001-0000000000000001.zst")
        << "the canonical ref-log key the append lane derives";

    /// The BODY is checked as exact length plus a 128-bit SipHash of it -- not literally byte for byte,
    /// but any change that survives both is a 128-bit collision at a fixed length, which is the trade for
    /// keeping the assertion readable. It is a function of `{format generation, ns, id, ops,
    /// chain_link}` only -- no incarnation reaches it.
    const auto got = op.read(key, Retry::standard());
    ASSERT_TRUE(got.has_value()) << "the birth chunk must be durable at its canonical key";
    const String plaintext = openObject(FormatId::RefLog, got->bytes);
    EXPECT_EQ(plaintext, R"({"type":"cas_ref_log","v":1}
{"namespace":"test/golden@cas@","txn_epoch":"1","txn_seq":"1"}
{"op":"namespace_birth"}
{"op":"owner_transition","new_kind":"precommit","new_ref":"gold_ref","new_epoch":"1","new_build":"7","new_ord":1}
{"op":"owner_transition","old_kind":"precommit","old_ref":"gold_ref","old_epoch":"1","old_build":"7","old_ord":1,"new_kind":"committed","new_ref":"gold_ref","new_epoch":"1","new_build":"7","new_ord":1}
{"n":3}
)") << "the sealed ref-log plaintext changed";
    EXPECT_EQ(got->bytes.size(), 206u) << "the sealed ref-log body changed size";
    SipHash body_hash;
    body_hash.update(got->bytes.data(), got->bytes.size());
    EXPECT_EQ(getHexUIntLowercase(body_hash.get128()), "21c275ad44a6b47a4d6c389c0d71bb34")
        << "the sealed ref-log body changed content -- preparation must seal the same bytes it sealed "
           "before the extraction";
}

/// The directive's "preserve backend request counts", asserted rather than assumed: preparation is pure,
/// so lifting it out must not add or remove a single request. One birth chunk = exactly one write-once
/// `PUT` at the ref-log key, no read-back, plus the two ordered `_ckpt` CASes required by the protocol:
/// creation publishes `life_epoch` before the log and the append lane publishes `committed_through`
/// after the log is durable.
///
/// COUNTS per key. Request ORDER is not checked here and cannot be with these counters; the ordering
/// that matters for a birth -- `_ckpt` before the ref-log `PUT` -- is argued at the call site and would
/// need a sequence-recording backend to pin.
TEST(CASRefCheckpoint, AppendRequestCountUnchangedByExtraction)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"test/req@cas@"};

    const RefTxnId id = publishRef(store, ns, "req_ref", 1);
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    const String log_key = store->layout().refLogKey(life, id);
    const String ckpt_key = store->layout().refCkptKey(life);

    EXPECT_EQ(backend->writes(log_key), 1u) << "exactly one write-once request per committed chunk";
    /// ONE GET, not zero, since Stage B: `resolveNamespaceLife`'s `completeCreation` call
    /// publishes this life's `_ckpt.life_epoch` BEFORE the birth chunk is prepared, so this table's
    /// OWN recovery walk (also inside this `appendRefOps`, ahead of the commit) grounds itself at the
    /// genesis position `_ckpt` now names and confirms it absent by exact key -- which is `log_key`
    /// itself, the position the birth chunk is about to occupy. That GET precedes the Committed PUT;
    /// the PUT itself still owes no read-back.
    EXPECT_EQ(backend->getCount(log_key), 1u) << "one grounding probe from recovery, before the birth write";
    EXPECT_EQ(backend->writes(ckpt_key), 2u)
        << "the birth contributes `life_epoch` before its log and `committed_through` after the durable "
           "log; these are two different ordering obligations, not a duplicate publication";
}

/// The post-durable install region is the reason preparation has to happen where it does: once "this
/// object may be durable", recording it must not fail. The extraction moves work EARLIER, never into that
/// region.
///
/// The guarded region is NOT the whole window: between the `Committed` outcome and the swap,
/// `carve_hook_for_test(PostDurableInstall)`, the `state_mutex` acquisition and the `state_unchanged`
/// evaluation all run OUTSIDE `DENY_ALLOCATIONS_IN_SCOPE`. This fence does not prove allocation-freedom
/// for them or for anything else.
///
/// WHAT THIS TEST PROVES, and what it does NOT -- stated precisely, because a fence trusted for more
/// than it checks is worse than no fence.
///
/// `DENY_ALLOCATIONS_IN_SCOPE` is `static_assert(true)` unless `!defined(NDEBUG)` (`MemoryTracker.h`),
/// so it is inert in every build that leaves `NDEBUG` defined -- which includes this gate and CI's
/// sanitizer lanes, since those configure `CMAKE_BUILD_TYPE=None` and `CMakeLists.txt` maps that to
/// `RelWithDebInfo`. Only a `Debug` build, or a tidy lane (which adds `-UNDEBUG`), has the
/// no-allocation half live. Nothing here proves the region does not allocate.
///
/// What is left is weaker than "the install is still guarded": `install_region_probe_for_test` fires
/// as the FIRST statement inside the guarded scope, BEFORE `rt->state.swap(*candidate)`, and the SAME
/// probe is shared by BOTH probe-instrumented post-durable install regions (`CasRefLedger.cpp`: the
/// wedge-resolution adoption and `commitRefChunk`'s `Committed` install). So `probe_hits > 0` proves
/// only that SOME probe-instrumented region was entered on this path -- which on this path can only be
/// the commit install, since nothing here wedges. It goes red if the region stops being entered at all
/// (a lost commit path, a skipped install arm); a refactor that lifted the swap out of the scope while
/// leaving the guard shell and the probe behind would keep it GREEN.
TEST(CASRefCheckpoint, PostDurableInstallRegionStillEnteredAfterExtraction)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPool(backend);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"test/region@cas@"};

    unsigned probe_hits = 0;
    store->setInstallRegionProbeForTest([&probe_hits] { ++probe_hits; });
    const RefTxnId id = publishRef(store, ns, "region_ref", 1);
    store->setInstallRegionProbeForTest(nullptr);

    EXPECT_GT(probe_hits, 0u)
        << "no probe-instrumented post-durable install region was entered on a committing append -- "
           "the `Committed` install arm was not reached at all";
    const NamespaceLifeId life = liveLifeOrFail(op, store->layout(), ns);
    EXPECT_TRUE(op.read(store->layout().refLogKey(life, id), Retry::standard()).has_value());
}
