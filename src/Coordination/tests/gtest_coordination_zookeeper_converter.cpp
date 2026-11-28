#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/ZooKeeperDataReader.h>

#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Disks/DiskLocal.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>

#include <functional>

namespace
{

/// One node record of a ZooKeeper (version 2) snapshot: path, data, ACL id and the persisted stat.
void writeZooKeeperNode(
    DB::WriteBuffer & out,
    const std::string & path,
    const std::string & data,
    int64_t zxid,
    int32_t cversion,
    int64_t ephemeral_owner,
    int64_t pzxid)
{
    Coordination::write(path, out);
    Coordination::write(data, out);
    Coordination::write(int64_t{-1}, out); /// ACL id
    Coordination::write(zxid, out); /// czxid
    Coordination::write(zxid, out); /// mzxid
    Coordination::write(int64_t{1000}, out); /// ctime
    Coordination::write(int64_t{1000}, out); /// mtime
    Coordination::write(int32_t{0}, out); /// version
    Coordination::write(cversion, out);
    Coordination::write(int32_t{0}, out); /// aversion
    Coordination::write(ephemeral_owner, out);
    Coordination::write(pzxid, out);
}

/// One transaction of a ZooKeeper (version 2) transaction log.
void writeZooKeeperTxn(
    DB::WriteBuffer & out, int64_t session_id, int64_t zxid, int32_t type, const std::function<void(DB::WriteBuffer &)> & write_body)
{
    DB::WriteBufferFromOwnString txn;
    Coordination::write(session_id, txn);
    Coordination::write(int32_t{1}, txn); /// xid
    Coordination::write(zxid, txn);
    Coordination::write(int64_t{2000} + zxid, txn); /// time
    Coordination::write(type, txn);
    write_body(txn);
    const std::string & bytes = txn.str();

    Coordination::write(int64_t{1}, out); /// checksum, 0 means the end of the log
    Coordination::write(static_cast<int32_t>(bytes.size()), out);
    out.write(bytes.data(), bytes.size());
    Coordination::write(int8_t{0x42}, out);
}

}

TEST(KeeperZooKeeperConverter, DigestMatchesRecalculation)
{
    ChangelogDirTest test_dir("./zookeeper_converter_data");
    const fs::path zookeeper_dir = fs::path(test_dir.path) / "zookeeper";
    const fs::path keeper_dir = fs::path(test_dir.path) / "keeper";
    fs::create_directory(zookeeper_dir);
    fs::create_directory(keeper_dir);

    constexpr int64_t snapshot_session = 0x100;
    constexpr int64_t log_session = 0x200;

    {
        DB::WriteBufferFromFile out(zookeeper_dir / "snapshot.10");
        Coordination::write(int32_t{1514885966}, out); /// "ZKSN"
        Coordination::write(int32_t{2}, out); /// version
        Coordination::write(int64_t{0}, out); /// dbid
        Coordination::write(int32_t{1}, out); /// sessions
        Coordination::write(snapshot_session, out);
        Coordination::write(int32_t{30000}, out);
        Coordination::write(int32_t{0}, out); /// ACL map
        writeZooKeeperNode(out, "", "", 0, 2, 0, 3);
        writeZooKeeperNode(out, "/a", "a", 1, 1, 0, 2);
        writeZooKeeperNode(out, "/a/b", "b", 2, 0, 0, 2);
        writeZooKeeperNode(out, "/e", "e", 3, 0, snapshot_session, 3);
        /// A parent that the log does not modify afterwards.
        writeZooKeeperNode(out, "/x", "x", 4, 1, 0, 5);
        writeZooKeeperNode(out, "/x/y", "y", 5, 0, 0, 5);
        Coordination::write(std::string("/"), out); /// end of nodes
        out.finalize();
    }

    {
        DB::WriteBufferFromFile out(zookeeper_dir / "log.11");
        Coordination::write(int32_t{1514884167}, out); /// "ZKLG"
        Coordination::write(int32_t{2}, out); /// version
        Coordination::write(int64_t{0}, out); /// dbid
        /// createSession
        writeZooKeeperTxn(out, log_session, 0x11, -10, [](DB::WriteBuffer & txn) { Coordination::write(int32_t{20000}, txn); });
        /// setData, the txn stores the new version
        writeZooKeeperTxn(
            out,
            log_session,
            0x12,
            5,
            [](DB::WriteBuffer & txn)
            {
                Coordination::write(std::string("/a"), txn);
                Coordination::write(std::string("a2"), txn);
                Coordination::write(int32_t{1}, txn);
            });
        /// create, ephemeral
        writeZooKeeperTxn(
            out,
            log_session,
            0x13,
            1,
            [](DB::WriteBuffer & txn)
            {
                Coordination::write(std::string("/a/c"), txn);
                Coordination::write(std::string("c"), txn);
                Coordination::write(Coordination::ACLs{{31, "world", "anyone"}}, txn);
                Coordination::write(uint8_t{1}, txn);
                Coordination::write(int32_t{2}, txn); /// parent cversion
            });
        /// delete
        writeZooKeeperTxn(out, log_session, 0x14, 2, [](DB::WriteBuffer & txn) { Coordination::write(std::string("/a/b"), txn); });
        out.finalize();
    }

    /// Same steps as `clickhouse keeper-converter`.
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, std::make_shared<DB::CoordinationSettings>());
    keeper_context->setDigestEnabled(true);
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("Keeper-snapshots", keeper_dir));
    auto storage = std::make_shared<DB::KeeperMemoryStorage>(500, "", keeper_context);
    const auto log = getLogger("KeeperZooKeeperConverterTest");
    DB::deserializeKeeperStorageFromSnapshotsDir(*storage, zookeeper_dir, log);
    storage->initializeSystemNodes();
    DB::deserializeLogsAndApplyToStorage(*storage, zookeeper_dir, log);

    EXPECT_EQ(storage->getZXID(), 0x14);
    EXPECT_EQ(committedNodeData(*storage, "/a"), "a2");
    EXPECT_FALSE(committedNodeExists(*storage, "/a/b"));
    DB::KeeperNodeStats stats;
    ASSERT_TRUE(storage->nodes_storage->getCommittedNodeSimple("/a/c", &stats, /*out_data=*/nullptr));
    EXPECT_EQ(stats.getEphemeralOwner(), log_session);
    ASSERT_TRUE(storage->nodes_storage->getCommittedNodeSimple("/e", &stats, /*out_data=*/nullptr));
    EXPECT_EQ(stats.getEphemeralOwner(), snapshot_session);
    ASSERT_TRUE(storage->nodes_storage->getCommittedNodeSimple("/x", &stats, /*out_data=*/nullptr));
    EXPECT_EQ(stats.getNumChildren(), 1);

    /// Keeper recalculates the digest when it loads a snapshot written without one.
    const uint64_t converter_digest = storage->nodes_digest;
    keeper_context->setDigestEnabled(false);
    DB::KeeperStorageSnapshot snapshot(
        storage.get(),
        std::make_shared<DB::SnapshotMetadata>(storage->getZXID(), 1, std::make_shared<nuraft::cluster_config>()),
        nullptr,
        keeper_context->getWriteSnapshotVersion());
    DB::KeeperSnapshotManager manager(1, keeper_context);
    auto buffer = manager.serializeSnapshotToBuffer(snapshot);

    keeper_context->setDigestEnabled(true);
    auto restored = DB::KeeperStorage::create(500, "", keeper_context, /*initialize_system_nodes=*/false);
    manager.deserializeSnapshotFromBuffer(buffer, *restored);
    EXPECT_EQ(converter_digest, restored->nodes_digest);
}

#endif
