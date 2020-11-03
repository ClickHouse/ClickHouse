#include <Common/ZooKeeper/TestKeeperStorage.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/setThreadName.h>
#include <mutex>
#include <functional>
#include <common/logger_useful.h>

namespace zkutil
{
using namespace DB;

static String parentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}


TestKeeperStorage::TestKeeperStorage()
{
    container.emplace("/", Node());

    processing_thread = ThreadFromGlobalPool([this] { processingThread(); });
}

using Undo = std::function<void()>;

struct TestKeeperStorageRequest
{
    Coordination::ZooKeeperRequestPtr zk_request;

    TestKeeperStorageRequest(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : zk_request(zk_request_)
    {}
    virtual bool isMutable() const { return false; }
    virtual std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const = 0;
    virtual ~TestKeeperStorageRequest() {}
};

struct TestKeeperStorageHeartbeatRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & /* container */, int64_t /* zxid */) const override
    {
        return {zk_request->makeResponse(), {}};
    }
};


struct TestKeeperStorageCreateRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const override
    {
        LOG_DEBUG(&Poco::Logger::get("STORAGE"), "EXECUTING CREATE REQUEST");
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Undo undo;
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        if (container.count(request.path))
        {
            response.error = Coordination::Error::ZNODEEXISTS;
        }
        else
        {
            auto it = container.find(parentPath(request.path));

            if (it == container.end())
            {
                response.error = Coordination::Error::ZNONODE;
            }
            else if (it->second.is_ephemeral)
            {
                response.error = Coordination::Error::ZNOCHILDRENFOREPHEMERALS;
            }
            else
            {
                TestKeeperStorage::Node created_node;
                created_node.seq_num = 0;
                created_node.stat.czxid = zxid;
                created_node.stat.mzxid = zxid;
                created_node.stat.ctime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
                created_node.stat.mtime = created_node.stat.ctime;
                created_node.stat.numChildren = 0;
                created_node.stat.dataLength = request.data.length();
                created_node.data = request.data;
                created_node.is_ephemeral = request.is_ephemeral;
                created_node.is_sequental = request.is_sequential;
                std::string path_created = request.path;

                if (request.is_sequential)
                {
                    auto seq_num = it->second.seq_num;
                    ++it->second.seq_num;

                    std::stringstream seq_num_str;
                    seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

                    path_created += seq_num_str.str();
                }

                response.path_created = path_created;
                container.emplace(path_created, std::move(created_node));

                undo = [&container, path_created, is_sequential = request.is_sequential, parent_path = it->first]
                {
                    container.erase(path_created);
                    auto & undo_parent = container.at(parent_path);
                    --undo_parent.stat.cversion;
                    --undo_parent.stat.numChildren;

                    if (is_sequential)
                        --undo_parent.seq_num;
                };

                ++it->second.stat.cversion;
                ++it->second.stat.numChildren;

                response.error = Coordination::Error::ZOK;
            }
        }

        return { response_ptr, undo };
    }
};

struct TestKeeperStorageGetRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t /* zxid */) const override
    {
        LOG_DEBUG(&Poco::Logger::get("STORAGE"), "EXECUTING GET REQUEST");
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            response.stat = it->second.stat;
            response.data = it->second.data;
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};

void TestKeeperStorage::processingThread()
{
    setThreadName("TestKeeperSProc");

    LOG_DEBUG(&Poco::Logger::get("STORAGE"), "LOOPING IN THREAD");
    try
    {
        while (!shutdown)
        {
            RequestInfo info;

            UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());

            if (requests_queue.tryPop(info, max_wait))
            {
                if (shutdown)
                    break;

                ++zxid;

                auto zk_request = info.request->zk_request;
                LOG_DEBUG(&Poco::Logger::get("STORAGE"), "GOT REQUEST {}", zk_request->getOpNum());
                Coordination::ZooKeeperResponsePtr response;
                if (zk_request->xid == -2)
                {
                    response = std::make_shared<Coordination::ZooKeeperHeartbeatResponse>();
                    response->xid = zk_request->xid;
                    response->zxid = zxid;
                }
                else
                {
                    zk_request->addRootPath(root_path);
                    LOG_DEBUG(&Poco::Logger::get("STORAGE"), "PROCESSING REQUEST");
                    std::tie(response, std::ignore) = info.request->process(container, zxid);
                    response->xid = zk_request->xid;
                    LOG_DEBUG(&Poco::Logger::get("STORAGE"), "SENDING XID {}", response->xid);
                    response->zxid = zxid;

                    response->removeRootPath(root_path);
                }

                LOG_DEBUG(&Poco::Logger::get("STORAGE"), "SENDING RESPONSE");
                info.response_callback(response);
                LOG_DEBUG(&Poco::Logger::get("STORAGE"), "DONE");
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize();
    }
}


void TestKeeperStorage::finalize()
{
    {
        std::lock_guard lock(push_request_mutex);

        if (shutdown)
            return;
        shutdown = true;
    }
    try
    {
        RequestInfo info;
        while (requests_queue.tryPop(info))
        {
            auto response = info.request->zk_request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            try
            {
                info.response_callback(response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

}

TestKeeperStorage::AsyncResponse TestKeeperStorage::putRequest(const Coordination::ZooKeeperRequestPtr & request)
{
    auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
    auto future = promise->get_future();
    TestKeeperStorageRequestPtr storage_request;
    if (request->xid == -2)
        storage_request = std::make_shared<TestKeeperStorageHeartbeatRequest>(request);
    else if (request->getOpNum() == 1)
        storage_request = std::make_shared<TestKeeperStorageCreateRequest>(request);
    else if (request->getOpNum() == 4)
        storage_request = std::make_shared<TestKeeperStorageGetRequest>(request);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UNKNOWN EVENT WITH OPNUM {}", request->getOpNum())
;
    RequestInfo request_info;
    request_info.time = clock::now();
    request_info.request = storage_request;
    request_info.response_callback = [promise] (const Coordination::ZooKeeperResponsePtr & response) { promise->set_value(response); };

    std::lock_guard lock(push_request_mutex);
    if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::LOGICAL_ERROR);
    LOG_DEBUG(&Poco::Logger::get("STORAGE"), "PUSHED");
    return future;
}


TestKeeperStorage::~TestKeeperStorage()
{
    try
    {
        finalize();
        if (processing_thread.joinable())
            processing_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}



}
