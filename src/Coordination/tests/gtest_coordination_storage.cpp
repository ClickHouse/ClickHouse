#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperStorage.h>

#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

TYPED_TEST(CoordinationTest, TestSystemNodeModify)
{
    using namespace Coordination;
    int64_t zxid{0};

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    // On INIT we abort when a system path is modified
    this->keeper_context->setServerState(KeeperContext::Phase::RUNNING);
    Storage storage{500, "", this->keeper_context};
    const auto assert_create = [&](const std::string_view path, const auto expected_code)
    {
        auto request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = path;
        storage.preprocessRequest(request, 0, 0, zxid);
        auto responses = storage.processRequest(request, 0, zxid);
        ASSERT_FALSE(responses.empty());

        const auto & response = responses[0];
        ASSERT_EQ(response.response->error, expected_code) << "Unexpected error for path " << path;

        ++zxid;
    };

    assert_create("/keeper", Error::ZBADARGUMENTS);
    assert_create("/keeper/with_child", Error::ZBADARGUMENTS);
    assert_create(DB::keeper_api_version_path, Error::ZBADARGUMENTS);

    assert_create("/keeper_map", Error::ZOK);
    assert_create("/keeper1", Error::ZOK);
    assert_create("/keepe", Error::ZOK);
    assert_create("/keeper1/test", Error::ZOK);
}

TYPED_TEST(CoordinationTest, TestCheckNotExistsRequest)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};

    int32_t zxid = 0;

    const auto create_path = [&](const auto & path)
    {
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        int new_zxid = ++zxid;
        create_request->path = path;
        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
    };

    const auto check_request = std::make_shared<ZooKeeperCheckRequest>();
    check_request->path = "/test_node";
    check_request->not_exists = true;

    {
        SCOPED_TRACE("CheckNotExists returns ZOK");
        int new_zxid = ++zxid;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZOK) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    create_path("/test_node");
    auto node_it = storage.container.find("/test_node");
    ASSERT_NE(node_it, storage.container.end());
    auto node_version = node_it->value.stats.version;

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS");
        int new_zxid = ++zxid;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZNODEEXISTS) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS for same version");
        int new_zxid = ++zxid;
        check_request->version = node_version;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZNODEEXISTS) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZOK for different version");
        int new_zxid = ++zxid;
        check_request->version = node_version + 1;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZOK) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }
}

TYPED_TEST(CoordinationTest, TestReapplyingDeltas)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    static constexpr int64_t initial_zxid = 100;

    const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
    create_request->path = "/test/data";
    create_request->is_sequential = true;

    const auto process_create = [](Storage & storage, const auto & request, int64_t zxid)
    {
        storage.preprocessRequest(request, 1, 0, zxid);
        auto responses = storage.processRequest(request, 1, zxid);
        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Error::ZOK);
    };

    const auto commit_initial_data = [&](auto & storage)
    {
        int64_t zxid = 1;

        const auto root_create = std::make_shared<ZooKeeperCreateRequest>();
        root_create->path = "/test";
        process_create(storage, root_create, zxid);
        ++zxid;

        for (; zxid <= initial_zxid; ++zxid)
            process_create(storage, create_request, zxid);
    };

    Storage storage1{500, "", this->keeper_context};
    commit_initial_data(storage1);

    for (int64_t zxid = initial_zxid + 1; zxid < initial_zxid + 50; ++zxid)
        storage1.preprocessRequest(create_request, 1, 0, zxid, /*check_acl=*/true, /*digest=*/std::nullopt, /*log_idx=*/zxid);

    /// create identical new storage
    Storage storage2{500, "", this->keeper_context};
    commit_initial_data(storage2);

    storage1.applyUncommittedState(storage2, initial_zxid);

    const auto commit_unprocessed = [&](Storage & storage)
    {
        for (int64_t zxid = initial_zxid + 1; zxid < initial_zxid + 50; ++zxid)
        {
            auto responses = storage.processRequest(create_request, 1, zxid);
            EXPECT_GE(responses.size(), 1);
            EXPECT_EQ(responses[0].response->error, Error::ZOK);
        }
    };

    commit_unprocessed(storage1);
    commit_unprocessed(storage2);

    const auto get_children = [&](Storage & storage)
    {
        const auto list_request = std::make_shared<ZooKeeperListRequest>();
        list_request->path = "/test";
        auto responses = storage.processRequest(list_request, 1, std::nullopt, /*check_acl=*/true, /*is_local=*/true);
        EXPECT_EQ(responses.size(), 1);
        const auto * list_response = dynamic_cast<const ListResponse *>(responses[0].response.get());
        EXPECT_TRUE(list_response);
        return list_response->names;
    };

    auto children1 = get_children(storage1);
    std::unordered_set<std::string> children1_set(children1.begin(), children1.end());

    auto children2 = get_children(storage2);
    std::unordered_set<std::string> children2_set(children2.begin(), children2.end());

    ASSERT_TRUE(children1_set == children2_set);
}

TYPED_TEST(CoordinationTest, TestRemoveRecursiveRequest)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};

    int32_t zxid = 0;

    const auto create = [&](const String & path, int create_mode)
    {
        int new_zxid = ++zxid;

        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        create_request->path = path;
        create_request->is_ephemeral = create_mode == zkutil::CreateMode::Ephemeral || create_mode == zkutil::CreateMode::EphemeralSequential;
        create_request->is_sequential = create_mode == zkutil::CreateMode::PersistentSequential || create_mode == zkutil::CreateMode::EphemeralSequential;

        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
    };

    const auto remove = [&](const String & path, int32_t version = -1)
    {
        int new_zxid = ++zxid;

        auto remove_request = std::make_shared<ZooKeeperRemoveRequest>();
        remove_request->path = path;
        remove_request->version = version;

        storage.preprocessRequest(remove_request, 1, 0, new_zxid);
        return storage.processRequest(remove_request, 1, new_zxid);
    };

    const auto remove_recursive = [&](const String & path, uint32_t remove_nodes_limit = 1)
    {
        int new_zxid = ++zxid;

        auto remove_request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
        remove_request->path = path;
        remove_request->remove_nodes_limit = remove_nodes_limit;

        storage.preprocessRequest(remove_request, 1, 0, new_zxid);
        return storage.processRequest(remove_request, 1, new_zxid);
    };

    const auto exists = [&](const String & path)
    {
        int new_zxid = ++zxid;

        const auto exists_request = std::make_shared<ZooKeeperExistsRequest>();
        exists_request->path = path;

        storage.preprocessRequest(exists_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(exists_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        return responses[0].response->error == Coordination::Error::ZOK;
    };

    {
        SCOPED_TRACE("Single Remove Single Node");
        create("/T1", zkutil::CreateMode::Persistent);

        auto responses = remove("/T1");
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZOK);
        ASSERT_FALSE(exists("/T1"));
    }

    {
        SCOPED_TRACE("Single Remove Tree");
        create("/T2", zkutil::CreateMode::Persistent);
        create("/T2/A", zkutil::CreateMode::Persistent);

        auto responses = remove("/T2");
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZNOTEMPTY);
        ASSERT_TRUE(exists("/T2"));
    }

    {
        SCOPED_TRACE("Recursive Remove Single Node");
        create("/T3", zkutil::CreateMode::Persistent);

        auto responses = remove_recursive("/T3", 100);
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZOK);
        ASSERT_FALSE(exists("/T3"));
    }

    {
        SCOPED_TRACE("Recursive Remove Tree Small Limit");
        create("/T5", zkutil::CreateMode::Persistent);
        create("/T5/A", zkutil::CreateMode::Persistent);
        create("/T5/B", zkutil::CreateMode::Persistent);
        create("/T5/A/C", zkutil::CreateMode::Persistent);

        auto responses = remove_recursive("/T5", 2);
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZNOTEMPTY);
        ASSERT_TRUE(exists("/T5"));
        ASSERT_TRUE(exists("/T5/A"));
        ASSERT_TRUE(exists("/T5/B"));
        ASSERT_TRUE(exists("/T5/A/C"));
    }

    {
        SCOPED_TRACE("Recursive Remove Tree Big Limit");
        create("/T6", zkutil::CreateMode::Persistent);
        create("/T6/A", zkutil::CreateMode::Persistent);
        create("/T6/B", zkutil::CreateMode::Persistent);
        create("/T6/A/C", zkutil::CreateMode::Persistent);

        auto responses = remove_recursive("/T6", 4);
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZOK);
        ASSERT_FALSE(exists("/T6"));
        ASSERT_FALSE(exists("/T6/A"));
        ASSERT_FALSE(exists("/T6/B"));
        ASSERT_FALSE(exists("/T6/A/C"));
    }

    {
        SCOPED_TRACE("Recursive Remove Ephemeral");
        create("/T7", zkutil::CreateMode::Ephemeral);
        ASSERT_EQ(storage.committed_ephemerals.size(), 1);

        auto responses = remove_recursive("/T7", 100);
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZOK);
        ASSERT_EQ(storage.committed_ephemerals.size(), 0);
        ASSERT_FALSE(exists("/T7"));
    }

    {
        SCOPED_TRACE("Recursive Remove Tree With Ephemeral");
        create("/T8", zkutil::CreateMode::Persistent);
        create("/T8/A", zkutil::CreateMode::Persistent);
        create("/T8/B", zkutil::CreateMode::Ephemeral);
        create("/T8/A/C", zkutil::CreateMode::Ephemeral);
        ASSERT_EQ(storage.committed_ephemerals.size(), 1);

        auto responses = remove_recursive("/T8", 4);
        ASSERT_EQ(responses.size(), 1);
        ASSERT_EQ(responses[0].response->error, Coordination::Error::ZOK);
        ASSERT_EQ(storage.committed_ephemerals.size(), 0);
        ASSERT_FALSE(exists("/T8"));
        ASSERT_FALSE(exists("/T8/A"));
        ASSERT_FALSE(exists("/T8/B"));
        ASSERT_FALSE(exists("/T8/A/C"));
    }
}

TYPED_TEST(CoordinationTest, TestRemoveRecursiveInMultiRequest)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};
    int zxid = 0;

    auto prepare_create_tree = []()
    {
        return Coordination::Requests{
            zkutil::makeCreateRequest("/A", "A", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/A/B", "B", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/A/C", "C", zkutil::CreateMode::Ephemeral),
            zkutil::makeCreateRequest("/A/B/D", "D", zkutil::CreateMode::Ephemeral),
        };
    };

    const auto exists = [&](const String & path)
    {
        int new_zxid = ++zxid;

        const auto exists_request = std::make_shared<ZooKeeperExistsRequest>();
        exists_request->path = path;

        storage.preprocessRequest(exists_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(exists_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        return responses[0].response->error == Coordination::Error::ZOK;
    };

    const auto is_multi_ok = [&](Coordination::ZooKeeperResponsePtr response)
    {
        const auto & multi_response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response);

        for (const auto & op_response : multi_response.responses)
            if (op_response->error != Coordination::Error::ZOK)
                return false;

        return true;
    };

    {
        SCOPED_TRACE("Remove In Multi Tx");
        int new_zxid = ++zxid;
        auto ops = prepare_create_tree();

        ops.push_back(zkutil::makeRemoveRequest("/A", -1));
        const auto request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});

        storage.preprocessRequest(request, 1, 0, new_zxid);
        auto responses = storage.processRequest(request, 1, new_zxid);
        ops.pop_back();

        ASSERT_EQ(responses.size(), 1);
        ASSERT_FALSE(is_multi_ok(responses[0].response));
    }

    {
        SCOPED_TRACE("Recursive Remove In Multi Tx");
        int new_zxid = ++zxid;
        auto ops = prepare_create_tree();

        ops.push_back(zkutil::makeRemoveRecursiveRequest("/A", 4));
        const auto request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});

        storage.preprocessRequest(request, 1, 0, new_zxid);
        auto responses = storage.processRequest(request, 1, new_zxid);
        ops.pop_back();

        ASSERT_EQ(responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(responses[0].response));
        ASSERT_FALSE(exists("/A"));
        ASSERT_FALSE(exists("/A/C"));
        ASSERT_FALSE(exists("/A/B"));
        ASSERT_FALSE(exists("/A/B/D"));
    }

    {
        SCOPED_TRACE("Recursive Remove With Regular In Multi Tx");
        int new_zxid = ++zxid;
        auto ops = prepare_create_tree();

        ops.push_back(zkutil::makeRemoveRequest("/A/C", -1));
        ops.push_back(zkutil::makeRemoveRecursiveRequest("/A", 3));
        const auto request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});

        storage.preprocessRequest(request, 1, 0, new_zxid);
        auto responses = storage.processRequest(request, 1, new_zxid);
        ops.pop_back();
        ops.pop_back();

        ASSERT_EQ(responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(responses[0].response));
        ASSERT_FALSE(exists("/A"));
        ASSERT_FALSE(exists("/A/C"));
        ASSERT_FALSE(exists("/A/B"));
        ASSERT_FALSE(exists("/A/B/D"));
    }

    {
        SCOPED_TRACE("Recursive Remove From Committed and Uncommitted states");
        int create_zxid = ++zxid;
        auto ops = prepare_create_tree();

        /// First create nodes
        const auto create_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
        storage.preprocessRequest(create_request, 1, 0, create_zxid);
        auto create_responses = storage.processRequest(create_request, 1, create_zxid);
        ASSERT_EQ(create_responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(create_responses[0].response));
        ASSERT_TRUE(exists("/A"));
        ASSERT_TRUE(exists("/A/C"));
        ASSERT_TRUE(exists("/A/B"));
        ASSERT_TRUE(exists("/A/B/D"));

        /// Remove node A/C as a single remove request.
        /// Remove all other as remove recursive request.
        /// In this case we should list storage to understand the tree topology
        /// but ignore already deleted nodes in uncommitted state.

        int remove_zxid = ++zxid;
        ops = {
            zkutil::makeRemoveRequest("/A/C", -1),
            zkutil::makeRemoveRecursiveRequest("/A", 3),
        };
        const auto remove_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});

        storage.preprocessRequest(remove_request, 1, 0, remove_zxid);
        auto remove_responses = storage.processRequest(remove_request, 1, remove_zxid);

        ASSERT_EQ(remove_responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(remove_responses[0].response));
        ASSERT_FALSE(exists("/A"));
        ASSERT_FALSE(exists("/A/C"));
        ASSERT_FALSE(exists("/A/B"));
        ASSERT_FALSE(exists("/A/B/D"));
    }

    {
        SCOPED_TRACE("Recursive Remove For Subtree With Updated Node");
        int create_zxid = ++zxid;
        auto ops = prepare_create_tree();

        /// First create nodes
        const auto create_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
        storage.preprocessRequest(create_request, 1, 0, create_zxid);
        auto create_responses = storage.processRequest(create_request, 1, create_zxid);
        ASSERT_EQ(create_responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(create_responses[0].response));

        /// Small limit
        int remove_zxid = ++zxid;
        ops = {
            zkutil::makeSetRequest("/A/B", "", -1),
            zkutil::makeRemoveRecursiveRequest("/A", 3),
        };
        auto remove_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
        storage.preprocessRequest(remove_request, 1, 0, remove_zxid);
        auto remove_responses = storage.processRequest(remove_request, 1, remove_zxid);

        ASSERT_EQ(remove_responses.size(), 1);
        ASSERT_FALSE(is_multi_ok(remove_responses[0].response));

        /// Big limit
        remove_zxid = ++zxid;
        ops[1] = zkutil::makeRemoveRecursiveRequest("/A", 4);
        remove_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
        storage.preprocessRequest(remove_request, 1, 0, remove_zxid);
        remove_responses = storage.processRequest(remove_request, 1, remove_zxid);

        ASSERT_EQ(remove_responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(remove_responses[0].response));
        ASSERT_FALSE(exists("/A"));
        ASSERT_FALSE(exists("/A/C"));
        ASSERT_FALSE(exists("/A/B"));
        ASSERT_FALSE(exists("/A/B/D"));
    }

    {
        SCOPED_TRACE("[BUG] Recursive Remove Level Sorting");
        int new_zxid = ++zxid;

        Coordination::Requests ops = {
            zkutil::makeCreateRequest("/a", "", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/a/bbbbbb", "", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/A", "", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/A/B", "", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/A/CCCCCCCCCCCC", "", zkutil::CreateMode::Persistent),
            zkutil::makeRemoveRecursiveRequest("/A", 3),
        };
        auto remove_request = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
        storage.preprocessRequest(remove_request, 1, 0, new_zxid);
        auto remove_responses = storage.processRequest(remove_request, 1, new_zxid);

        ASSERT_EQ(remove_responses.size(), 1);
        ASSERT_TRUE(is_multi_ok(remove_responses[0].response));
        ASSERT_TRUE(exists("/a"));
        ASSERT_TRUE(exists("/a/bbbbbb"));
        ASSERT_FALSE(exists("/A"));
        ASSERT_FALSE(exists("/A/B"));
        ASSERT_FALSE(exists("/A/CCCCCCCCCCCC"));
    }

}

TYPED_TEST(CoordinationTest, TestRemoveRecursiveWatches)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};
    int zxid = 0;

    const auto create = [&](const String & path, int create_mode)
    {
        int new_zxid = ++zxid;

        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        create_request->path = path;
        create_request->is_ephemeral = create_mode == zkutil::CreateMode::Ephemeral || create_mode == zkutil::CreateMode::EphemeralSequential;
        create_request->is_sequential = create_mode == zkutil::CreateMode::PersistentSequential || create_mode == zkutil::CreateMode::EphemeralSequential;

        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
    };

    const auto add_watch = [&](const String & path)
    {
        int new_zxid = ++zxid;

        const auto exists_request = std::make_shared<ZooKeeperExistsRequest>();
        exists_request->path = path;
        exists_request->has_watch = true;

        storage.preprocessRequest(exists_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(exists_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK);
    };

    const auto add_list_watch = [&](const String & path)
    {
        int new_zxid = ++zxid;

        const auto list_request = std::make_shared<ZooKeeperListRequest>();
        list_request->path = path;
        list_request->has_watch = true;

        storage.preprocessRequest(list_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(list_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK);
    };

    create("/A", zkutil::CreateMode::Persistent);
    create("/A/B", zkutil::CreateMode::Persistent);
    create("/A/C", zkutil::CreateMode::Ephemeral);
    create("/A/B/D", zkutil::CreateMode::Ephemeral);

    add_watch("/A");
    add_watch("/A/B");
    add_watch("/A/C");
    add_watch("/A/B/D");
    add_list_watch("/A");
    add_list_watch("/A/B");
    ASSERT_EQ(storage.watches.size(), 4);
    ASSERT_EQ(storage.list_watches.size(), 2);

    int new_zxid = ++zxid;

    auto remove_request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
    remove_request->path = "/A";
    remove_request->remove_nodes_limit = 4;

    storage.preprocessRequest(remove_request, 1, 0, new_zxid);
    auto responses = storage.processRequest(remove_request, 1, new_zxid);

    ASSERT_EQ(responses.size(), 7);
    /// request response is last
    ASSERT_EQ(dynamic_cast<Coordination::ZooKeeperWatchResponse *>(responses.back().response.get()), nullptr);

    std::unordered_map<std::string, std::vector<Coordination::Event>> expected_watch_responses
    {
        {"/A/B/D", {Coordination::Event::DELETED}},
        {"/A/B", {Coordination::Event::CHILD, Coordination::Event::DELETED}},
        {"/A/C", {Coordination::Event::DELETED}},
        {"/A", {Coordination::Event::CHILD, Coordination::Event::DELETED}},
    };

    std::unordered_map<std::string, std::vector<Coordination::Event>> actual_watch_responses;
    for (size_t i = 0; i < 6; ++i)
    {
        ASSERT_EQ(responses[i].response->error, Coordination::Error::ZOK);

        const auto & watch_response = dynamic_cast<Coordination::ZooKeeperWatchResponse &>(*responses[i].response);
        actual_watch_responses[watch_response.path].push_back(static_cast<Coordination::Event>(watch_response.type));
    }
    ASSERT_EQ(expected_watch_responses, actual_watch_responses);

    ASSERT_EQ(storage.watches.size(), 0);
    ASSERT_EQ(storage.list_watches.size(), 0);
}

TYPED_TEST(CoordinationTest, TestRemoveRecursiveAcls)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};
    int zxid = 0;

    {
        int new_zxid = ++zxid;
        String user_auth_data = "test_user:test_password";

        const auto auth_request = std::make_shared<ZooKeeperAuthRequest>();
        auth_request->scheme = "digest";
        auth_request->data = user_auth_data;

        storage.preprocessRequest(auth_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(auth_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to add auth to session";
    }

    const auto create = [&](const String & path)
    {
        int new_zxid = ++zxid;

        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        create_request->path = path;
        create_request->acls = {{.permissions = ACL::Create, .scheme = "auth", .id = ""}};

        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
    };

    /// Add nodes with only Create ACL
    create("/A");
    create("/A/B");
    create("/A/C");
    create("/A/B/D");

    {
        int new_zxid = ++zxid;

        auto remove_request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
        remove_request->path = "/A";
        remove_request->remove_nodes_limit = 4;

        storage.preprocessRequest(remove_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(remove_request, 1, new_zxid);

        EXPECT_EQ(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZNOAUTH);
    }
}

TYPED_TEST(CoordinationTest, TestListRequestTypes)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};

    int32_t zxid = 0;

    static constexpr std::string_view test_path = "/list_request_type/node";

    const auto create_path = [&](const auto & path, bool is_ephemeral, bool is_sequential = true)
    {
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        int new_zxid = ++zxid;
        create_request->path = path;
        create_request->is_sequential = is_sequential;
        create_request->is_ephemeral = is_ephemeral;
        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
        const auto & create_response = dynamic_cast<ZooKeeperCreateResponse &>(*responses[0].response);
        return create_response.path_created;
    };

    create_path(parentNodePath(StringRef{test_path}).toString(), false, false);

    static constexpr size_t persistent_num = 5;
    std::unordered_set<std::string> expected_persistent_children;
    for (size_t i = 0; i < persistent_num; ++i)
    {
        expected_persistent_children.insert(getBaseNodeName(create_path(test_path, false)).toString());
    }
    ASSERT_EQ(expected_persistent_children.size(), persistent_num);

    static constexpr size_t ephemeral_num = 5;
    std::unordered_set<std::string> expected_ephemeral_children;
    for (size_t i = 0; i < ephemeral_num; ++i)
    {
        expected_ephemeral_children.insert(getBaseNodeName(create_path(test_path, true)).toString());
    }
    ASSERT_EQ(expected_ephemeral_children.size(), ephemeral_num);

    const auto get_children = [&](const auto list_request_type)
    {
        const auto list_request = std::make_shared<ZooKeeperFilteredListRequest>();
        int new_zxid = ++zxid;
        list_request->path = parentNodePath(StringRef{test_path}).toString();
        list_request->list_request_type = list_request_type;
        storage.preprocessRequest(list_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(list_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        const auto & list_response = dynamic_cast<ZooKeeperListResponse &>(*responses[0].response);
        return list_response.names;
    };

    const auto persistent_children = get_children(ListRequestType::PERSISTENT_ONLY);
    EXPECT_EQ(persistent_children.size(), persistent_num);
    for (const auto & child : persistent_children)
    {
        EXPECT_TRUE(expected_persistent_children.contains(child)) << "Missing persistent child " << child;
    }

    const auto ephemeral_children = get_children(ListRequestType::EPHEMERAL_ONLY);
    EXPECT_EQ(ephemeral_children.size(), ephemeral_num);
    for (const auto & child : ephemeral_children)
    {
        EXPECT_TRUE(expected_ephemeral_children.contains(child)) << "Missing ephemeral child " << child;
    }

    const auto all_children = get_children(ListRequestType::ALL);
    EXPECT_EQ(all_children.size(), ephemeral_num + persistent_num);
    for (const auto & child : all_children)
    {
        EXPECT_TRUE(expected_ephemeral_children.contains(child) || expected_persistent_children.contains(child))
            << "Missing child " << child;
    }
}

TYPED_TEST(CoordinationTest, TestUncommittedStateBasicCrud)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};

    constexpr std::string_view path = "/test";

    const auto get_committed_data = [&]() -> std::optional<String>
    {
        auto request = std::make_shared<ZooKeeperGetRequest>();
        request->path = path;
        auto responses = storage.processRequest(request, 0, std::nullopt, true, true);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);

        if (get_response.error != Error::ZOK)
            return std::nullopt;

        return get_response.data;
    };

    const auto preprocess_get = [&](int64_t zxid)
    {
        auto get_request = std::make_shared<ZooKeeperGetRequest>();
        get_request->path = path;
        storage.preprocessRequest(get_request, 0, 0, zxid);
        return get_request;
    };

    const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
    create_request->path = path;
    create_request->data = "initial_data";
    storage.preprocessRequest(create_request, 0, 0, 1);
    storage.preprocessRequest(create_request, 0, 0, 2);

    ASSERT_EQ(get_committed_data(), std::nullopt);

    const auto after_create_get = preprocess_get(3);

    ASSERT_EQ(get_committed_data(), std::nullopt);

    const auto set_request = std::make_shared<ZooKeeperSetRequest>();
    set_request->path = path;
    set_request->data = "new_data";
    storage.preprocessRequest(set_request, 0, 0, 4);

    const auto after_set_get = preprocess_get(5);

    ASSERT_EQ(get_committed_data(), std::nullopt);

    const auto remove_request = std::make_shared<ZooKeeperRemoveRequest>();
    remove_request->path = path;
    storage.preprocessRequest(remove_request, 0, 0, 6);
    storage.preprocessRequest(remove_request, 0, 0, 7);

    const auto after_remove_get = preprocess_get(8);

    ASSERT_EQ(get_committed_data(), std::nullopt);

    {
        const auto responses = storage.processRequest(create_request, 0, 1);
        const auto & create_response = getSingleResponse<ZooKeeperCreateResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(create_request, 0, 2);
        const auto & create_response = getSingleResponse<ZooKeeperCreateResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZNODEEXISTS);
    }

    {
        const auto responses = storage.processRequest(after_create_get, 0, 3);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZOK);
        ASSERT_EQ(get_response.data, "initial_data");
    }

    ASSERT_EQ(get_committed_data(), "initial_data");

    {
        const auto responses = storage.processRequest(set_request, 0, 4);
        const auto & create_response = getSingleResponse<ZooKeeperSetResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(after_set_get, 0, 5);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZOK);
        ASSERT_EQ(get_response.data, "new_data");
    }

    ASSERT_EQ(get_committed_data(), "new_data");

    {
        const auto responses = storage.processRequest(remove_request, 0, 6);
        const auto & create_response = getSingleResponse<ZooKeeperRemoveResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(remove_request, 0, 7);
        const auto & create_response = getSingleResponse<ZooKeeperRemoveResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZNONODE);
    }

    {
        const auto responses = storage.processRequest(after_remove_get, 0, 8);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZNONODE);
    }

    ASSERT_EQ(get_committed_data(), std::nullopt);
}

TYPED_TEST(CoordinationTest, TestBlockACL)
{
    using namespace DB;
    using namespace Coordination;

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};

    int64_t zxid = 1;

    static constexpr std::string_view digest = "clickhouse:test";
    static constexpr std::string_view new_digest = "antonio:test";

    static constexpr int64_t session_id = 42;
    storage.committed_session_and_auth[session_id].push_back(KeeperStorageBase::AuthID{.scheme = "digest", .id = std::string{digest}});
    {
        static constexpr StringRef path = "/test";

        auto req_zxid = zxid++;
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        create_request->path = path.toString();
        create_request->acls = {Coordination::ACL{.permissions = Coordination::ACL::All, .scheme = "digest", .id = std::string{digest}}};
        storage.preprocessRequest(create_request, session_id, 0, req_zxid);
        auto acls = storage.uncommitted_state.getACLs(path);
        ASSERT_EQ(acls.size(), 1);
        ASSERT_EQ(acls[0].id, digest);
        storage.processRequest(create_request, session_id, req_zxid);
        ASSERT_NE(storage.container.getValue(path).acl_id, 0);

        req_zxid = zxid++;
        const auto set_acl_request = std::make_shared<ZooKeeperSetACLRequest>();
        set_acl_request->path = path.toString();
        set_acl_request->acls = {Coordination::ACL{.permissions = Coordination::ACL::All, .scheme = "digest", .id = std::string{new_digest}}};
        storage.preprocessRequest(set_acl_request, session_id, 0, req_zxid);
        acls = storage.uncommitted_state.getACLs(path);
        ASSERT_EQ(acls.size(), 1);
        ASSERT_EQ(acls[0].id, new_digest);
        storage.processRequest(set_acl_request, session_id, req_zxid);
        ASSERT_NE(storage.container.getValue(path).acl_id, 0);
    }

    {
        static constexpr StringRef path = "/test_blocked_acl";
        this->keeper_context->setBlockACL(true);

        auto req_zxid = zxid++;
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        create_request->path = path.toString();
        create_request->acls = {Coordination::ACL{.permissions = Coordination::ACL::All, .scheme = "digest", .id = std::string{digest}}};
        storage.preprocessRequest(create_request, session_id, 0, req_zxid);
        auto acls = storage.uncommitted_state.getACLs(path);
        ASSERT_EQ(acls.size(), 0);
        storage.processRequest(create_request, session_id, req_zxid);
        ASSERT_EQ(storage.container.getValue(path).acl_id, 0);

        req_zxid = zxid++;
        const auto set_acl_request = std::make_shared<ZooKeeperSetACLRequest>();
        set_acl_request->path = path.toString();
        set_acl_request->acls = {Coordination::ACL{.permissions = Coordination::ACL::All, .scheme = "digest", .id = std::string{new_digest}}};
        storage.preprocessRequest(set_acl_request, session_id, 0, req_zxid);
        acls = storage.uncommitted_state.getACLs(path);
        ASSERT_EQ(acls.size(), 0);
        storage.processRequest(set_acl_request, session_id, req_zxid);
        ASSERT_EQ(storage.container.getValue(path).acl_id, 0);
    }
}

#endif
