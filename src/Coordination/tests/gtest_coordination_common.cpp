#include <Coordination/tests/gtest_coordination_common.h>

#if USE_NURAFT

#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/KeeperStateMachine.h>

DB::KeeperContextPtr makeKeeperContext(bool /*use_lsmt_storage*/, std::shared_ptr<DB::CoordinationSettings> settings)
{
    if (!settings)
        settings = std::make_shared<DB::CoordinationSettings>();
    /// TODO: translate use_lsmt_storage into a CoordinationSettings option here so
    ///       KeeperStorage::create picks the right implementation.
    /// Intentionally minimal: callers add setLocalLogsPreprocessed/disks/digest as they need them.
    return std::make_shared<DB::KeeperContext>(true, settings);
}

INSTANTIATE_TEST_SUITE_P(
    Storage,
    CoordinationTest,
    ::testing::Values(
        /*use_lsmt_storage*/ false
        /// TODO: Add `true` when LSMT storage is integrated.
    ),
    [](const ::testing::TestParamInfo<bool> & param_info) { return param_info.param ? "LSMT" : "Mem"; });

INSTANTIATE_TEST_SUITE_P(
    Storage,
    CoordinationTestWithCompression,
    ::testing::Values(
        StorageTypeAndCompression{.use_lsmt_storage = false, .enable_compression = false},
        StorageTypeAndCompression{.use_lsmt_storage = false, .enable_compression = true}
        /// TODO: StorageTypeAndCompression{.use_lsmt_storage = true, .enable_compression = true}
    ),
    [](const ::testing::TestParamInfo<StorageTypeAndCompression> & param_info) { return std::string(param_info.param.use_lsmt_storage ? "LSMT" : "Mem") + (param_info.param.enable_compression ? "Compressed" : "Uncompressed"); });

LogEntryPtr getLogEntry(const std::string & s, size_t term)
{
    DB::WriteBufferFromNuraftBuffer bufwriter;
    writeText(s, bufwriter);
    return nuraft::cs_new<nuraft::log_entry>(term, bufwriter.getBuffer());
}

void waitDurableLogs(nuraft::log_store & log_store)
{
    while (log_store.last_durable_index() != log_store.next_slot() - 1)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

void assertFileDeleted(std::string path)
{
    for (size_t i = 0; i < 100; ++i)
    {
        if (!fs::exists(path))
            return;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    FAIL() << "File " << path << " was not removed";
}

nuraft::ptr<nuraft::log_entry>
getLogEntryFromZKRequest(size_t term, int64_t session_id, int64_t zxid, const Coordination::ZooKeeperRequestPtr & request)
{
    DB::KeeperRequestForSession request_for_session;
    request_for_session.session_id = session_id;
    request_for_session.zxid = zxid;
    request_for_session.request = request;
    auto buffer = DB::KeeperStateMachine::getZooKeeperLogEntry(request_for_session);
    return nuraft::cs_new<nuraft::log_entry>(term, buffer);
}

void addNode(DB::KeeperStorage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner, DB::ACLId acl_id, int64_t seq_num)
{
    DB::KeeperNodeStats stats;
    if (ephemeral_owner)
        stats.makeEphemeral(ephemeral_owner);
    if (seq_num)
        stats.setSeqNum(seq_num);
    stats.acl_id = acl_id;
    storage.nodes_storage->addCommittedNodeIfNotExists(path, stats, data, /*update_parent_num_children=*/true, /*out_digest=*/nullptr);
}

Coordination::ACLs getUncommittedACLs(DB::KeeperStorage & storage, std::string_view path)
{
    Coordination::ACLId acl_id = 0;
    DB::KeeperNodeStats stats;
    if (storage.nodes_storage->getUncommittedNodeSimple(path, &stats, /*out_data=*/nullptr))
        acl_id = stats.acl_id;
    return storage.acl_map.convertNumber(acl_id);
}

bool committedNodeExists(DB::KeeperStorage & storage, std::string_view path)
{
    return storage.nodes_storage->getCommittedNodeSimple(path, /*out_stats=*/nullptr, /*out_data=*/nullptr);
}

std::string committedNodeData(DB::KeeperStorage & storage, std::string_view path)
{
    std::string data;
    if (!storage.nodes_storage->getCommittedNodeSimple(path, /*out_stats=*/nullptr, &data))
        data = "<NO NODE>";
    return data;
}

std::string uncommittedNodeData(DB::KeeperStorage & storage, std::string_view path)
{
    std::string data;
    if (!storage.nodes_storage->getUncommittedNodeSimple(path, /*out_stats=*/nullptr, &data))
        data = "<NO NODE>";
    return data;
}

#endif
