#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>

/// Task 9 (rev.7 spec §1 "empty-proof rule" [B3]): the last silent-empty-load killer. On a PRE-TERMINAL
/// (Live) or READ-ONLY pool, an enumeration about to answer EMPTY at a table root must first CONFIRM the
/// pool identity object (`_pool_meta`) exists with an AUTHORITATIVE, UNCACHED probe -- because "empty" at a
/// table root is exactly what a silently-erased backing looks like, and a read-only pool has no
/// keeper/lease/observer to catch that erasure any other way. These tests build a real
/// `ContentAddressedMetadataStorage` over a Local object storage (the gtest_cas_operation_gate.cpp harness)
/// and exercise the rule across the six cells the brief enumerates.

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int INVALID_STATE;
extern const int NETWORK_ERROR;
}

using namespace DB;
using DB::Cas::PoolLifecycle;
using DB::Cas::ProbeOutcome;
using DB::Cas::SentinelProbeResult;

namespace
{

/// A committed (non-empty) table dir + part reused across the tests (the exact shape
/// gtest_ca_transaction.cpp / gtest_cas_operation_gate.cpp use).
const std::string kTableDir = "g80/g80g80g8-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
/// A DIFFERENT, never-committed-to table dir: genuinely empty for every test, distinct uuid so a
/// commit to kTableDir can never make it non-empty.
const std::string kEmptyTableDir = "g99/g99g99g9-0909-4909-8909-090909090909";

std::shared_ptr<ContentAddressedMetadataStorage> openStorage()
{
    auto settings = Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_empty_proof_scratch");
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// Commit one real part into `kTableDir`, leaving that table dir non-empty (tmp -> final rename -> commit).
void commitOnePart(ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kTableDir + "/tmp_insert_all_1_1_0/data.bin", 65536, WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kTableDir + "/tmp_insert_all_1_1_0", kPartDir);
    tx->commit(NoCommitOptions{});
}

/// A read-only mount over a backing a writable mount already bootstrapped (`_pool_meta` present). The
/// writable mount minted the pool identity then shut down; the read-only mount validates `_pool_meta`,
/// takes NO lease and runs NO erasure observer (it stays `Live` forever) -- exactly the state in which
/// enumeration is the ONLY line of defense against a later erasure. Returns {ro storage, backing root}.
struct ReadOnlyMount
{
    std::shared_ptr<ContentAddressedMetadataStorage> ro;
    std::string root;
};

/// Delete ONLY the physical `_pool_meta` object under `root`, leaving the container directory and every
/// other object intact — so a subsequent authoritative `probeSentinel` verdicts `KeyAbsent` (the identity
/// key is gone while the container is alive), NOT `ContainerAbsent` (which a whole-root `remove_all` yields).
/// This models the realistic "someone rm'd just the identity object" / partial-erase shape. Returns whether
/// exactly one `_pool_meta` file was found and removed, so the test can guard against a vacuous pass.
bool deleteOnlyPoolMetaUnder(const std::string & root)
{
    size_t removed = 0;
    for (const auto & entry : std::filesystem::recursive_directory_iterator(root))
    {
        if (entry.is_regular_file() && entry.path().filename() == "_pool_meta")
        {
            std::filesystem::remove(entry.path());
            ++removed;
        }
    }
    return removed == 1;
}

/// The message thrown by `fn`, or a failure if it did not throw a `DB::Exception`.
std::string messageOf(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (const Exception & e)
    {
        return std::string(e.message());
    }
    ADD_FAILURE() << "expected a DB::Exception";
    return {};
}

ReadOnlyMount openReadOnlyOverBootstrappedBacking()
{
    auto settings = Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "ca_empty_proof_ro_scratch");

    /// (1) A writable mount bootstraps `_pool_meta` over a fresh backing, then shuts down.
    auto rw_os = Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = rw_os->getCommonKeyPrefix();
    {
        auto w = std::make_shared<ContentAddressedMetadataStorage>(
            rw_os, "pool", "srv1", "", nullptr, settings);
        w->startup();
        w->shutdown();
    }

    /// (2) A read-only mount over the SAME backing validates `_pool_meta` and mounts `Live` (no lease,
    /// no watermark, no observer -- read-only opens never enter the lifecycle machinery).
    DB::LocalObjectStorageSettings ro_settings("test", root, /*read_only_=*/true);
    auto ro_os = std::make_shared<DB::LocalObjectStorage>(std::move(ro_settings));
    auto ro = std::make_shared<ContentAddressedMetadataStorage>(
        ro_os, "pool", "srv1", "", nullptr, settings);
    ro->startup();
    return {std::move(ro), root};
}

}

/// (a) THE RO-ATTACH silent-empty killer: a read-only pool whose whole backing was erased must throw,
/// never answer empty. Both mandatory authorities disappear; table enumeration observes the missing
/// `cas/ref_catalog` first, so `CORRUPTED_DATA` takes precedence over the later `_pool_meta` empty-proof
/// check. The pool-meta-only companion below keeps the typed 668 contract pinned separately.
TEST(CASEmptyProof, ReadOnlyOverErasedBackingThrowsInsteadOfEmpty)
{
    auto mount = openReadOnlyOverBootstrappedBacking();

    /// Baseline while the backing is intact: the empty table root answers empty truthfully (`_pool_meta`
    /// present authorizes it), issuing exactly one confirming probe.
    mount.ro->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(mount.ro->listDirectory(kEmptyTableDir).empty());
    EXPECT_EQ(mount.ro->emptyProofProbeCountForTest(), 1u);

    /// Erase the backing out from under the (still Live) read-only mount: `_pool_meta` and everything.
    std::filesystem::remove_all(mount.root);

    /// Now the SAME empty listing must refuse on the first missing mandatory control it observes.
    Cas::tests::expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&] { mount.ro->listDirectory(kEmptyTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&] { mount.ro->iterateDirectory(kEmptyTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::CORRUPTED_DATA, [&] { mount.ro->isDirectoryEmpty(kEmptyTableDir); });
}

/// (a2, acceptance matrix — T9 review's KeyAbsent-specific real-backend follow-up) Test (a) erases the
/// WHOLE backing (`remove_all(root)`), so its probe verdicts `ContainerAbsent`. This test deletes ONLY the
/// `_pool_meta` object against the REAL Local backend — the container directory and every other object stay
/// intact — so the authoritative probe verdicts `KeyAbsent` instead. Both flavours must reach the SAME
/// "backing may be erased" refusal (distinct from the transient "transport or permission fault" one), so a
/// targeted deletion of just the identity object (a partial erase) is caught exactly like a whole-root wipe.
TEST(CASEmptyProof, ReadOnlyWithOnlyPoolMetaDeletedThrowsErasedFlavoredOnKeyAbsent)
{
    auto mount = openReadOnlyOverBootstrappedBacking();

    /// Baseline while the backing is intact: the empty table root answers empty truthfully with one probe.
    mount.ro->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(mount.ro->listDirectory(kEmptyTableDir).empty());
    EXPECT_EQ(mount.ro->emptyProofProbeCountForTest(), 1u);

    /// Delete ONLY `_pool_meta` (container + every sibling object intact) → the probe verdicts KeyAbsent.
    ASSERT_TRUE(deleteOnlyPoolMetaUnder(mount.root))
        << "expected exactly one _pool_meta object to remove; otherwise this test is vacuous";

    /// The KeyAbsent miss reaches the erased-flavored typed 668, NOT the transient one, and never answers empty.
    const std::string msg = messageOf([&] { mount.ro->listDirectory(kEmptyTableDir); });
    EXPECT_NE(msg.find("pool identity object absent"), std::string::npos) << msg;
    EXPECT_NE(msg.find("the backing may be erased"), std::string::npos) << msg;
    EXPECT_EQ(msg.find("transport or permission fault"), std::string::npos)
        << "a KeyAbsent miss must give the erased message, not the transient/retry one: " << msg;

    /// The other enumeration entry points refuse identically.
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { mount.ro->iterateDirectory(kEmptyTableDir); });
    Cas::tests::expectThrowsCode(ErrorCodes::INVALID_STATE, [&] { mount.ro->isDirectoryEmpty(kEmptyTableDir); });
}

/// (b) A Live pool over a genuinely-empty table dir with `_pool_meta` present answers empty AND issues
/// EXACTLY ONE uncached sentinel probe -- and it happens on the empty (`isDirectoryEmpty` == true) path.
TEST(CASEmptyProof, LiveEmptyTableDirAnswersEmptyWithExactlyOneProbe)
{
    auto storage = openStorage();

    storage->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(storage->isDirectoryEmpty(kEmptyTableDir));
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 1u)
        << "the empty table-root answer must confirm the pool identity with exactly one probe";

    /// listDirectory / iterateDirectory each independently issue exactly one confirming probe too.
    storage->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(storage->listDirectory(kEmptyTableDir).empty());
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 1u);
}

/// (c) The zero-cost hot path: a NON-empty table dir issues NO probe at all.
TEST(CASEmptyProof, LiveNonEmptyTableDirIssuesNoProbe)
{
    auto storage = openStorage();
    commitOnePart(*storage);

    storage->resetEmptyProofProbeCountForTest();
    EXPECT_FALSE(storage->listDirectory(kTableDir).empty());
    EXPECT_FALSE(storage->isDirectoryEmpty(kTableDir));
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 0u)
        << "the non-empty hot path must never touch the empty-proof probe";
}

/// (d) A Vanished pool answers truth-empty WITHOUT any probe: `checkOpAdmitted`'s Probe -> TruthAbsent
/// short-circuit answers before classification, so the terminal path never pays the empty-proof.
TEST(CASEmptyProof, VanishedPoolAnswersTruthEmptyWithoutProbe)
{
    auto storage = openStorage();
    commitOnePart(*storage);
    auto pool = storage->store();   /// captured while Live
    pool->setLifecycleForTest(PoolLifecycle::VanishedForgotten);

    storage->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(storage->listDirectory(kTableDir).empty());
    EXPECT_TRUE(storage->isDirectoryEmpty(kTableDir));
    EXPECT_FALSE(storage->iterateDirectory(kTableDir)->isValid());
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 0u)
        << "a Vanished pool answers truth-empty directly -- the gate short-circuits before the empty-proof";
}

/// (e) Scope discipline: a deeper (non-root) part-dir enumeration that answers empty is NOT gated.
TEST(CASEmptyProof, DeeperPartDirEmptyAnswerIsNotGated)
{
    auto storage = openStorage();

    /// A never-committed part dir under a table root: classifies as PartDir, answers empty, no probe.
    const std::string absent_part_dir = kEmptyTableDir + "/all_9_9_0";
    storage->resetEmptyProofProbeCountForTest();
    EXPECT_TRUE(storage->listDirectory(absent_part_dir).empty());
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 0u)
        << "only the TableDir/DetachedContainer roots are gated -- deeper part-dirs are not";
}

/// (f) A probe that cannot establish absence (transport/permission fault) throws the typed TRANSIENT
/// refusal, never an empty answer. Unproven absence is unavailability, so the refusal carries the
/// upstream-retryable class -- unlike the `KeyAbsent`/`ContainerAbsent` arm, where absence IS proven and
/// the 668 stands. The fault is injected through the empty-proof override seam.
TEST(CASEmptyProof, IndeterminateProbeThrowsTransientNeverEmpty)
{
    auto storage = openStorage();
    storage->setEmptyProofProbeOverrideForTest(
        [] { return SentinelProbeResult{ProbeOutcome::Indeterminate, std::nullopt}; });

    storage->resetEmptyProofProbeCountForTest();
    Cas::tests::expectThrowsCode(ErrorCodes::NETWORK_ERROR, [&] { storage->listDirectory(kEmptyTableDir); });
    EXPECT_EQ(storage->emptyProofProbeCountForTest(), 1u);

    /// The transient message names the fault (a retryable condition), distinct from the erased message.
    std::string msg;
    try
    {
        storage->listDirectory(kEmptyTableDir);
    }
    catch (const Exception & e)
    {
        msg = std::string(e.message());
    }
    EXPECT_NE(msg.find("transport or permission fault"), std::string::npos) << msg;
    EXPECT_NE(msg.find("TRANSIENT"), std::string::npos) << msg;
}
