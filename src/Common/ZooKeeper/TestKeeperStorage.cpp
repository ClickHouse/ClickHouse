#include <Common/ZooKeeper/TestKeeperStorage.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/setThreadName.h>
#include <mutex>
#include <functional>
#include <common/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>

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

static String baseName(const String & path)
{
    auto rslash_pos = path.rfind('/');
    return path.substr(rslash_pos + 1);
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

struct TestKeeperStorageRemoveRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t /*zxid*/) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*zk_request);
        Undo undo;

        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != it->second.stat.version)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else if (it->second.stat.numChildren)
        {
            response.error = Coordination::Error::ZNOTEMPTY;
        }
        else
        {
            auto prev_node = it->second;
            container.erase(it);
            auto & parent = container.at(parentPath(request.path));
            --parent.stat.numChildren;
            ++parent.stat.cversion;
            response.error = Coordination::Error::ZOK;

            undo = [prev_node, &container, path = request.path]
            {
                container.emplace(path, prev_node);
                auto & undo_parent = container.at(parentPath(path));
                ++undo_parent.stat.numChildren;
                --undo_parent.stat.cversion;
            };
        }

        return { response_ptr, undo };
    }
};

struct TestKeeperStorageExistsRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t /*zxid*/) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        auto it = container.find(request.path);
        if (it != container.end())
        {
            response.stat = it->second.stat;
            response.error = Coordination::Error::ZOK;
        }
        else
        {
            response.error = Coordination::Error::ZNONODE;
        }

        return { response_ptr, {} };
    }
};

struct TestKeeperStorageSetRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);
        Undo undo;

        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version == -1 || request.version == it->second.stat.version)
        {
            auto prev_node = it->second;

            it->second.data = request.data;
            ++it->second.stat.version;
            it->second.stat.mzxid = zxid;
            it->second.stat.mtime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
            it->second.data = request.data;
            ++container.at(parentPath(request.path)).stat.cversion;
            response.stat = it->second.stat;
            response.error = Coordination::Error::ZOK;

            undo = [prev_node, &container, path = request.path]
            {
                container.at(path) = prev_node;
                --container.at(parentPath(path)).stat.cversion;
            };
        }
        else
        {
            response.error = Coordination::Error::ZBADVERSION;
        }

        return { response_ptr, {} };
    }
};

struct TestKeeperStorageListRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t /*zxid*/) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);
        Undo undo;
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            auto path_prefix = request.path;
            if (path_prefix.empty())
                throw Coordination::Exception("Logical error: path cannot be empty", Coordination::Error::ZSESSIONEXPIRED);

            if (path_prefix.back() != '/')
                path_prefix += '/';

            /// Fairly inefficient.
            for (auto child_it = container.upper_bound(path_prefix);
                 child_it != container.end() && startsWith(child_it->first, path_prefix);
                ++child_it)
            {
                if (parentPath(child_it->first) == request.path)
                    response.names.emplace_back(baseName(child_it->first));
            }

            response.stat = it->second.stat;
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};

struct TestKeeperStorageCheckRequest final : public TestKeeperStorageRequest
{
    using TestKeeperStorageRequest::TestKeeperStorageRequest;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t /*zxid*/) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != it->second.stat.version)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else
        {
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};

struct TestKeeperStorageMultiRequest final : public TestKeeperStorageRequest
{
    std::vector<TestKeeperStorageRequestPtr> concrete_requests;
    TestKeeperStorageMultiRequest(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : TestKeeperStorageRequest(zk_request_)
    {
        Coordination::ZooKeeperMultiRequest & request = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*zk_request);
        concrete_requests.reserve(request.requests.size());

        for (const auto & zk_request : request.requests)
        {
            if (const auto * concrete_request_create = dynamic_cast<const Coordination::ZooKeeperCreateRequest *>(zk_request.get()))
            {
                concrete_requests.push_back(std::make_shared<TestKeeperStorageCreateRequest>(dynamic_pointer_cast<Coordination::ZooKeeperCreateRequest>(zk_request)));
            }
            else if (const auto * concrete_request_remove = dynamic_cast<const Coordination::ZooKeeperRemoveRequest *>(zk_request.get()))
            {
                concrete_requests.push_back(std::make_shared<TestKeeperStorageRemoveRequest>(dynamic_pointer_cast<Coordination::ZooKeeperRemoveRequest>(zk_request)));
            }
            else if (const auto * concrete_request_set = dynamic_cast<const Coordination::ZooKeeperSetRequest *>(zk_request.get()))
            {
                concrete_requests.push_back(std::make_shared<TestKeeperStorageSetRequest>(dynamic_pointer_cast<Coordination::ZooKeeperSetRequest>(zk_request)));
            }
            else if (const auto * concrete_request_check = dynamic_cast<const Coordination::ZooKeeperCheckRequest *>(zk_request.get()))
            {
                concrete_requests.push_back(std::make_shared<TestKeeperStorageCheckRequest>(dynamic_pointer_cast<Coordination::ZooKeeperCheckRequest>(zk_request)));
            }
            else
                throw Coordination::Exception("Illegal command as part of multi ZooKeeper request", Coordination::Error::ZBADARGUMENTS);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(TestKeeperStorage::Container & container, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);
        std::vector<Undo> undo_actions;

        try
        {
            for (const auto & concrete_request : concrete_requests)
            {
                auto [ cur_response, undo_action ] = concrete_request->process(container, zxid);
                response.responses.emplace_back(cur_response);
                if (cur_response->error != Coordination::Error::ZOK)
                {
                    response.error = cur_response->error;

                    for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                        if (*it)
                            (*it)();

                    return { response_ptr, {} };
                }
                else
                    undo_actions.emplace_back(std::move(undo_action));
            }

            response.error = Coordination::Error::ZOK;
            return { response_ptr, {} };
        }
        catch (...)
        {
            for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                if (*it)
                    (*it)();
            throw;
        }
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

                auto [response, _] = info.request->process(container, zxid);
                response->xid = zk_request->xid;
                response->zxid = zxid;
                response->removeRootPath(root_path);

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





class TestKeeperWrapperFactory final : private boost::noncopyable
{

public:
    using Creator = std::function<TestKeeperStorageRequestPtr(const Coordination::ZooKeeperRequestPtr &)>;
    using OpNumToRequest = std::unordered_map<int32_t, Creator>;

    static TestKeeperWrapperFactory & instance()
    {
        static TestKeeperWrapperFactory factory;
        return factory;
    }

    TestKeeperStorageRequestPtr get(const Coordination::ZooKeeperRequestPtr & zk_request) const
    {
        auto it = op_num_to_request.find(zk_request->getOpNum());
        if (it == op_num_to_request.end())
            throw Coordination::Exception("Unknown operation type " + std::to_string(zk_request->getOpNum()), Coordination::Error::ZBADARGUMENTS);

        return it->second(zk_request);
    }

    void registerRequest(int32_t op_num, Creator creator)
    {
        if (!op_num_to_request.try_emplace(op_num, creator).second)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Request with op num {} already registered", op_num);
    }

private:
    OpNumToRequest op_num_to_request;

private:
    TestKeeperWrapperFactory();
};

template<int32_t num, typename RequestT>
void registerTestKeeperRequestWrapper(TestKeeperWrapperFactory & factory)
{
    factory.registerRequest(num, [] (const Coordination::ZooKeeperRequestPtr & zk_request) { return std::make_shared<RequestT>(zk_request); });
}


TestKeeperWrapperFactory::TestKeeperWrapperFactory()
{
    registerTestKeeperRequestWrapper<11, TestKeeperStorageHeartbeatRequest>(*this);
    //registerTestKeeperRequestWrapper<100, TestKeeperStorageAuthRequest>(*this);
    //registerTestKeeperRequestWrapper<-11, TestKeeperStorageCloseRequest>(*this);
    registerTestKeeperRequestWrapper<1, TestKeeperStorageCreateRequest>(*this);
    registerTestKeeperRequestWrapper<2, TestKeeperStorageRemoveRequest>(*this);
    registerTestKeeperRequestWrapper<3, TestKeeperStorageExistsRequest>(*this);
    registerTestKeeperRequestWrapper<4, TestKeeperStorageGetRequest>(*this);
    registerTestKeeperRequestWrapper<5, TestKeeperStorageSetRequest>(*this);
    registerTestKeeperRequestWrapper<12, TestKeeperStorageListRequest>(*this);
    registerTestKeeperRequestWrapper<13, TestKeeperStorageCheckRequest>(*this);
    registerTestKeeperRequestWrapper<14, TestKeeperStorageMultiRequest>(*this);
}

TestKeeperStorage::AsyncResponse TestKeeperStorage::putRequest(const Coordination::ZooKeeperRequestPtr & request)
{
    auto promise = std::make_shared<std::promise<Coordination::ZooKeeperResponsePtr>>();
    auto future = promise->get_future();
    TestKeeperStorageRequestPtr storage_request = TestKeeperWrapperFactory::instance().get(request);
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
