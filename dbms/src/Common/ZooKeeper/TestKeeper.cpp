#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/setThreadName.h>
#include "iostream"

#include <typeinfo>

namespace Coordination
{
Watches watches;
Watches list_watches;
std::mutex watches_mutex;


String parentPath(const String& path)
{
    unsigned long rslash_pos = path.rfind('/');
    std::string parent_path;
    if (rslash_pos > 0)
    {
        parent_path = path.substr(0, rslash_pos);
    }
    return parent_path;
}

//    void TestKeeper::printArchitecture() {
//        std::cerr << "*************************************"  << std::endl;
//        std::cerr << architecture.size()  << std::endl;
//        for(auto elem : architecture)
//        {
//            std::cerr << elem.first << elem.second.data  << std::endl;
//        }
//        std::cerr << "*************************************"  << std::endl;
//    }

std::vector<String> children(const String & path)
{
    std::vector<String> children;
    for (auto it = architecture.begin(); it != architecture.end(); it++)
    {
        unsigned long rslash_pos = it->first.rfind('/');
        if (it->first.substr(0, rslash_pos) == path)
        {
            children.insert(children.end(), it->first.substr(rslash_pos + 1));
        }
    }
    return children;
}

std::map<std::string, TestNode> architecture;
long int sequential_number;
long int global_zxid;

struct TestKeeperResponse : virtual Response
{
    virtual ~TestKeeperResponse() {}

    void processWatch(const String& path) const;
};

struct TestKeeperWatchResponse final : WatchResponse, TestKeeperResponse
{
};

void TestKeeperResponse::processWatch(const String& path) const
{
    WatchResponse watch_response;



    watch_response.path = path;

    auto it = watches.find(watch_response.path);
    if (it != watches.end())
    {
        for (auto &callback : it->second)
            if (callback)
                callback(watch_response);

        watches.erase(it);
    }

    WatchResponse watch_list_response;
    watch_list_response.path = parentPath(path);

    it = list_watches.find(watch_list_response.path);
    if (it != list_watches.end())
    {
        for (auto & callback : it->second)
            if (callback)
                callback(watch_list_response);

        list_watches.erase(it);
    }
}

struct TestKeeperCreateRequest final : CreateRequest, TestKeeperRequest
{
    TestKeeperCreateRequest() {}
    TestKeeper::OpNum getOpNum() const override { return 1; }
    TestKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperCreateResponse final : CreateResponse, TestKeeperResponse
{
    TestKeeperCreateResponse() {}
};

struct TestKeeperRemoveRequest final : RemoveRequest, TestKeeperRequest
{
    TestKeeperRemoveRequest() {}
    TestKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
    TestKeeper::OpNum getOpNum() const override { return 2; }
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperRemoveResponse final : RemoveResponse, TestKeeperResponse
{

};

struct TestKeeperExistsRequest final : ExistsRequest, TestKeeperRequest
{
    TestKeeper::OpNum getOpNum() const override { return 3; }
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperExistsResponse final : ExistsResponse, TestKeeperResponse
{

};

struct TestKeeperGetRequest final : GetRequest, TestKeeperRequest
{
    TestKeeperGetRequest() {}
    TestKeeper::OpNum getOpNum() const override { return 4; }

    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperGetResponse final : GetResponse, TestKeeperResponse
{
};

struct TestKeeperSetRequest final : SetRequest, TestKeeperRequest
{
    TestKeeperSetRequest() {}
    TestKeeper::OpNum getOpNum() const override { return 5; }
    TestKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperSetResponse final : SetResponse, TestKeeperResponse
{

};

struct TestKeeperListRequest final : ListRequest, TestKeeperRequest
{
    TestKeeper::OpNum getOpNum() const override { return 6; }
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperListResponse final : ListResponse, TestKeeperResponse
{

};

struct TestKeeperCheckRequest final : CheckRequest, TestKeeperRequest
{
    TestKeeperCheckRequest() {}
    TestKeeper::OpNum getOpNum() const override { return 7; }
    TestKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};

struct TestKeeperCheckResponse final : CheckResponse, TestKeeperResponse
{

};

struct TestKeeperMultiRequest final : MultiRequest, TestKeeperRequest
{
    TestKeeper::OpNum getOpNum() const override { return 8; }
    TestKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls)
    {
        requests.reserve(generic_requests.size());

        for (const auto & generic_request : generic_requests)
        {
            if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
            {
                auto create = std::make_shared<TestKeeperCreateRequest>(*concrete_request_create);
                if (create->acls.empty())
                    create->acls = default_acls;
                requests.push_back(create);
            }
            else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperRemoveRequest>(*concrete_request_remove));
            }
            else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperSetRequest>(*concrete_request_set));
            }
            else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperCheckRequest>(*concrete_request_check));
            }
            else
                throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
        }
    }

    TestKeeperResponsePtr createResponse() const override;
    TestKeeperResponsePtr makeResponse() const override;
};


struct TestKeeperMultiResponse final : MultiResponse, TestKeeperResponse {};

TestKeeperResponsePtr TestKeeperCreateRequest::makeResponse() const
{
    TestKeeperCreateResponse response;
    String parent_path = Coordination::parentPath(path);
    if (architecture.find(path) != architecture.end())
    {
        response.error = Error::ZNODEEXISTS;
    } else if (parent_path != "" and architecture.find(parent_path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    } else if (parent_path != "" and architecture[parent_path].is_ephemeral)
    {
        response.error = Error::ZNOCHILDRENFOREPHEMERALS;
    } else
    {
        TestNode created_node;
        created_node.seq_num = 0;
        created_node.stat.czxid = zxid;
        created_node.stat.mzxid = zxid;
        created_node.stat.ctime =
                std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        created_node.stat.mtime = created_node.stat.ctime;
        created_node.stat.numChildren = 0;
        created_node.stat.dataLength = data.length();
        created_node.data = this->data;
        created_node.is_ephemeral = this->is_ephemeral;
        created_node.is_sequental = this->is_sequential;
        created_node.acls = this->acls;
        std::string path_created = path;
        if (this->is_sequential)
        {
            std::string number = std::to_string(architecture[parent_path].seq_num);
            architecture[parent_path].seq_num += 1;
            std::string zeros = "";
            for (size_t i = 0; i != 10 - number.size(); i++)
            {
                zeros += "0";
            }
            path_created += zeros + number;
        }
        architecture[path_created] = created_node;

        response.path_created = path_created;
        architecture[parent_path].stat.cversion++;
        architecture[parent_path].stat.numChildren += 1;
        response.error = Error::ZOK;
        response.processWatch(path);
    }
    return std::make_shared<TestKeeperCreateResponse>(response);
}

TestKeeperResponsePtr TestKeeperRemoveRequest::makeResponse() const
{
    std::cerr << watches.begin()->first << std::endl;
    TestKeeperRemoveResponse response;
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version != -1 && version != architecture[path].stat.version)
    {
        response.error = Error::ZBADVERSION;
    }
    else if (Coordination::children(path).size() != 0)
    {
        response.error = Error::ZNOTEMPTY;
    }
    else
    {
        String parent_path = Coordination::parentPath(path);

        auto it = architecture.find(path);
        architecture.erase(it);
        architecture[parent_path].stat.numChildren -= 1;
        response.error = Error::ZOK;
        response.processWatch(path);
    }
    return std::make_shared<TestKeeperRemoveResponse>(response);
}

TestKeeperResponsePtr TestKeeperExistsRequest::makeResponse() const
{
    TestKeeperExistsResponse response;
    if (architecture.find(path) != architecture.end())
    {
        response.stat = architecture[path].stat;
        response.error = Error::ZOK;
    }
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    return std::make_shared<TestKeeperExistsResponse>(response);
}

TestKeeperResponsePtr TestKeeperGetRequest::makeResponse() const
{
    TestKeeperGetResponse response;
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    else
    {
        response.stat = architecture[path].stat;
        response.data = architecture[path].data;
        response.error = Error::ZOK;
    }
    return std::make_shared<TestKeeperGetResponse>(response);
}

TestKeeperResponsePtr TestKeeperSetRequest::makeResponse() const
{
    TestKeeperSetResponse response;
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version == -1 || version == architecture[path].stat.version)
    {
        architecture[path].data = data;
        architecture[path].stat.version++;
        architecture[path].stat.mzxid = zxid;
        architecture[path].stat.mtime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        architecture[path].data = data;
        String parent_path = Coordination::parentPath(path);
        architecture[parent_path].stat.cversion++;
        response.stat = architecture[path].stat;
        std::cerr << "success" << std::endl;
        response.error = Error::ZOK;
        response.processWatch(path);
    }
    else
    {
        response.error = Error::ZBADVERSION;
    }
    return std::make_shared<TestKeeperSetResponse>(response);
}

TestKeeperResponsePtr TestKeeperListRequest::makeResponse() const
{
    TestKeeperListResponse response;
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    else
    {
        response.names = Coordination::children(path);
        response.stat = architecture[path].stat;
        response.error = Error::ZOK;
    }
    return std::make_shared<TestKeeperListResponse>(response);
}

TestKeeperResponsePtr TestKeeperCheckRequest::makeResponse() const
{
    TestKeeperCheckResponse response;
    if (architecture.find(path) == architecture.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version != -1 && version != architecture[path].stat.version)
    {
        response.error = Error::ZBADVERSION;
    }
    else
    {
        response.error = Error::ZOK;
    }

    return std::make_shared<TestKeeperCheckResponse>(response);
}

TestKeeperResponsePtr TestKeeperMultiRequest::makeResponse() const
{
    TestKeeperMultiResponse response;
    response.responses.reserve(requests.size());

    std::map<std::string, TestNode> architecture_copy = architecture;
    for (const auto & request : requests)
    {
        auto cur_response = dynamic_cast<const TestKeeperRequest &>(*request).makeResponse();
        response.responses.emplace_back(cur_response);
        if (cur_response->error != Error::ZOK)
        {
            response.error = cur_response->error;
            architecture = architecture_copy;
            return std::make_shared<TestKeeperMultiResponse>(response);
        }
    }
    response.error = Error::ZOK;
    return std::make_shared<TestKeeperMultiResponse>(response);
}

TestKeeperResponsePtr TestKeeperCreateRequest::createResponse() const { return std::make_shared<TestKeeperCreateResponse>(); }
TestKeeperResponsePtr TestKeeperRemoveRequest::createResponse() const { return std::make_shared<TestKeeperRemoveResponse>(); }
TestKeeperResponsePtr TestKeeperExistsRequest::createResponse() const { return std::make_shared<TestKeeperExistsResponse>(); }
TestKeeperResponsePtr TestKeeperGetRequest::createResponse() const { return std::make_shared<TestKeeperGetResponse>(); }
TestKeeperResponsePtr TestKeeperSetRequest::createResponse() const { return std::make_shared<TestKeeperSetResponse>(); }
TestKeeperResponsePtr TestKeeperListRequest::createResponse() const { return std::make_shared<TestKeeperListResponse>(); }
TestKeeperResponsePtr TestKeeperCheckRequest::createResponse() const { return std::make_shared<TestKeeperCheckResponse>(); }
TestKeeperResponsePtr TestKeeperMultiRequest::createResponse() const { return std::make_shared<TestKeeperMultiResponse>(); }

TestKeeper::~TestKeeper()
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

TestKeeper::TestKeeper(
        const String & root_path_,
        Poco::Timespan operation_timeout)
        : root_path(root_path_),
          operation_timeout(operation_timeout)
{
    if (!root_path.empty())
    {
        if (root_path.back() == '/')
            root_path.pop_back();
    }


    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    processing_thread = ThreadFromGlobalPool([this] { processingThread(); });
}

void TestKeeper::processingThread()
{
    setThreadName("TestKeeperProcessingThread");

    TestKeeperResponsePtr response;

    try
    {
        while (!expired)
        {
            RequestInfo info;

            UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());
            if (requests_queue.tryPop(info, max_wait))
            {

                if (expired)
                    break;

                if (info.watch)
                {
                    std::lock_guard lock(watches_mutex);
                    if (dynamic_cast<const ListRequest *>(info.request.get()))
                    {
                        list_watches[info.request->getPath()].emplace_back(std::move(info.watch));
                    }
                    else
                    {
                        watches[info.request->getPath()].emplace_back(std::move(info.watch));
                    }
                }

                info.request->addRootPath(root_path);
                response = info.request->makeResponse();
                response->removeRootPath(root_path);
                if (info.callback)
                    info.callback(*response);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize();
    }
}


void TestKeeper::finalize()
{
    {
        std::lock_guard lock(push_request_mutex);

        if (expired)
            return;
        expired = true;
    }
    try
    {
//            if (processing_thread.joinable()) {
//                processing_thread.join();
//            }

        {
            std::lock_guard lock(watches_mutex);

            for (auto & path_watches : watches)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = ZSESSIONEXPIRED;

                for (auto & callback : path_watches.second)
                {
                    if (callback)
                    {
                        try
                        {
                            callback(response);
                        }
                        catch (...)
                        {
                            tryLogCurrentException(__PRETTY_FUNCTION__);
                        }
                    }
                }
            }

            watches.clear();
        }

        RequestInfo info;
        while (requests_queue.tryPop(info))
        {
            if (info.callback)
            {
                ResponsePtr response = info.request->createResponse();
                response->error = ZSESSIONEXPIRED;
                try
                {
                    info.callback(*response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
            if (info.watch)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = ZSESSIONEXPIRED;
                try
                {
                    info.watch(response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void TestKeeper::pushRequest(RequestInfo && info)
{
    global_zxid++;
    info.request->zxid = global_zxid;
    try
    {
        info.time = clock::now();
        std::lock_guard lock(push_request_mutex);

        if (expired)
            throw Exception("Session expired", ZSESSIONEXPIRED);

        if (!requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
            throw Exception("Cannot push request to queue within operation timeout", ZOPERATIONTIMEOUT);
    }
    catch (...)
    {
        finalize();
        throw;
    }
}


void TestKeeper::create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs & acls,
        CreateCallback callback)
{
    TestKeeperCreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;
    request.acls = acls.empty() ? default_acls : acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperCreateRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };
    pushRequest(std::move(request_info));
}

void TestKeeper::remove(
        const String & path,
        int32_t version,
        RemoveCallback callback)
{
    TestKeeperRemoveRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperRemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };
    pushRequest(std::move(request_info));
}

void TestKeeper::exists(
        const String & path,
        ExistsCallback callback,
        WatchCallback watch)
{
    TestKeeperExistsRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperExistsRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
    request_info.watch = watch;
    pushRequest(std::move(request_info));
}

void TestKeeper::get(
        const String & path,
        GetCallback callback,
        WatchCallback watch)
{
    TestKeeperGetRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperGetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
    request_info.watch = watch;
    pushRequest(std::move(request_info));
}

void TestKeeper::set(
        const String & path,
        const String & data,
        int32_t version,
        SetCallback callback)
{
    TestKeeperSetRequest request;
    request.path = path;
    request.data = data;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperSetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };
    pushRequest(std::move(request_info));
}

void TestKeeper::list(
        const String & path,
        ListCallback callback,
        WatchCallback watch)
{
    TestKeeperListRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperListRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
    request_info.watch = watch;
    pushRequest(std::move(request_info));
}

void TestKeeper::check(
        const String & path,
        int32_t version,
        CheckCallback callback)
{
    TestKeeperCheckRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperCheckRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };
    pushRequest(std::move(request_info));
}

void TestKeeper::multi(
        const Requests & requests,
        MultiCallback callback)
{
    TestKeeperMultiRequest request(requests, default_acls);

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
    pushRequest(std::move(request_info));
}
};
