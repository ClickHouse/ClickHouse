#include "Common/ZooKeeper/IKeeper.h"
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <base/types.h>
#include <functional>

namespace Coordination
{

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


using Undo = std::function<void()>;


struct TestKeeperRequest : virtual Request
{
    virtual ResponsePtr createResponse() const = 0;
    virtual std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const = 0;
    virtual void processWatches(TestKeeper::Watches & /*watches*/, TestKeeper::Watches & /*list_watches*/) const {}
};


static void processWatchesImpl(const String & path, TestKeeper::Watches & watches, TestKeeper::Watches & list_watches)
{
    WatchResponse watch_response;
    watch_response.path = path;

    auto it = watches.find(watch_response.path);
    if (it != watches.end())
    {
        for (const auto & callback : it->second)
            if (callback)
                (*callback)(watch_response);

        watches.erase(it);
    }

    WatchResponse watch_list_response;
    watch_list_response.path = parentPath(path);

    it = list_watches.find(watch_list_response.path);
    if (it != list_watches.end())
    {
        for (const auto & callback : it->second)
            if (callback)
                (*callback)(watch_list_response);

        list_watches.erase(it);
    }
}


struct TestKeeperCreateRequest final : CreateRequest, TestKeeperRequest
{
    TestKeeperCreateRequest() = default;
    explicit TestKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;

    void processWatches(TestKeeper::Watches & node_watches, TestKeeper::Watches & list_watches) const override
    {
        processWatchesImpl(getPath(), node_watches, list_watches);
    }
};

struct TestKeeperRemoveRequest final : RemoveRequest, TestKeeperRequest
{
    TestKeeperRemoveRequest() = default;
    explicit TestKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;

    void processWatches(TestKeeper::Watches & node_watches, TestKeeper::Watches & list_watches) const override
    {
        processWatchesImpl(getPath(), node_watches, list_watches);
    }
};

struct TestKeeperExistsRequest final : ExistsRequest, TestKeeperRequest
{
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperGetRequest final : GetRequest, TestKeeperRequest
{
    TestKeeperGetRequest() = default;
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperSetRequest final : SetRequest, TestKeeperRequest
{
    TestKeeperSetRequest() = default;
    explicit TestKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;

    void processWatches(TestKeeper::Watches & node_watches, TestKeeper::Watches & list_watches) const override
    {
        processWatchesImpl(getPath(), node_watches, list_watches);
    }
};

struct TestKeeperListRequest : ListRequest, TestKeeperRequest
{
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperFilteredListRequest final : TestKeeperListRequest
{
    ListRequestType list_request_type;
};

struct TestKeeperCheckRequest final : CheckRequest, TestKeeperRequest
{
    TestKeeperCheckRequest() = default;
    explicit TestKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperSyncRequest final : SyncRequest, TestKeeperRequest
{
    TestKeeperSyncRequest() = default;
    explicit TestKeeperSyncRequest(const SyncRequest & base) : SyncRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperReconfigRequest final : ReconfigRequest, TestKeeperRequest
{
    TestKeeperReconfigRequest() = default;
    explicit TestKeeperReconfigRequest(const ReconfigRequest & base) : ReconfigRequest(base) {}
    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};

struct TestKeeperMultiRequest final : MultiRequest, TestKeeperRequest
{
    explicit TestKeeperMultiRequest(const Requests & generic_requests)
    {
        requests.reserve(generic_requests.size());

        for (const auto & generic_request : generic_requests)
        {
            if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
            {
                auto create = std::make_shared<TestKeeperCreateRequest>(*concrete_request_create);
                requests.push_back(create);
            }
            else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperRemoveRequest>(*concrete_request_remove));
            }
            else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperSetRequest>(*concrete_request_set));
            }
            else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<TestKeeperCheckRequest>(*concrete_request_check));
            }
            else
                throw Exception::fromMessage(Error::ZBADARGUMENTS, "Illegal command as part of multi ZooKeeper request");
        }
    }

    void processWatches(TestKeeper::Watches & node_watches, TestKeeper::Watches & list_watches) const override
    {
        for (const auto & generic_request : requests)
            dynamic_cast<const TestKeeperRequest &>(*generic_request).processWatches(node_watches, list_watches);
    }

    ResponsePtr createResponse() const override;
    std::pair<ResponsePtr, Undo> process(TestKeeper::Container & container, int64_t zxid) const override;
};


std::pair<ResponsePtr, Undo> TestKeeperCreateRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    CreateResponse response;
    response.zxid = zxid;
    Undo undo;

    if (container.contains(path))
    {
        response.error = Error::ZNODEEXISTS;
    }
    else
    {
        auto it = container.find(parentPath(path));

        if (it == container.end())
        {
            response.error = Error::ZNONODE;
        }
        else if (it->second.is_ephemeral)
        {
            response.error = Error::ZNOCHILDRENFOREPHEMERALS;
        }
        else
        {
            TestKeeper::Node created_node;
            created_node.seq_num = 0;
            created_node.stat.czxid = zxid;
            created_node.stat.mzxid = zxid;
            created_node.stat.ctime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
            created_node.stat.mtime = created_node.stat.ctime;
            created_node.stat.numChildren = 0;
            created_node.stat.dataLength = static_cast<int>(data.length());
            created_node.stat.ephemeralOwner = is_ephemeral ? 1 : 0;
            created_node.data = data;
            created_node.is_ephemeral = is_ephemeral;
            created_node.is_sequental = is_sequential;
            std::string path_created = path;

            if (is_sequential)
                path_created += fmt::format("{:0>10}", it->second.seq_num);

            /// Increment sequential number even if node is not sequential
            ++it->second.seq_num;

            response.path_created = path_created;
            container.emplace(path_created, std::move(created_node));

            undo = [&container, path_created, parent_path = it->first]
            {
                container.erase(path_created);
                auto & undo_parent = container.at(parent_path);
                --undo_parent.stat.cversion;
                --undo_parent.stat.numChildren;
                --undo_parent.seq_num;
            };

            ++it->second.stat.cversion;
            ++it->second.stat.numChildren;

            response.error = Error::ZOK;
        }
    }

    return { std::make_shared<CreateResponse>(response), undo };
}

std::pair<ResponsePtr, Undo> TestKeeperRemoveRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    RemoveResponse response;
    response.zxid = zxid;
    Undo undo;

    auto it = container.find(path);
    if (it == container.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version != -1 && version != it->second.stat.version)
    {
        response.error = Error::ZBADVERSION;
    }
    else if (it->second.stat.numChildren)
    {
        response.error = Error::ZNOTEMPTY;
    }
    else
    {
        auto prev_node = it->second;
        container.erase(it);
        auto & parent = container.at(parentPath(path));
        --parent.stat.numChildren;
        ++parent.stat.cversion;
        response.error = Error::ZOK;

        undo = [prev_node, &container, path = path]
        {
            container.emplace(path, prev_node);
            auto & undo_parent = container.at(parentPath(path));
            ++undo_parent.stat.numChildren;
            --undo_parent.stat.cversion;
        };
    }

    return { std::make_shared<RemoveResponse>(response), undo };
}

std::pair<ResponsePtr, Undo> TestKeeperExistsRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    ExistsResponse response;
    response.zxid = zxid;

    auto it = container.find(path);
    if (it != container.end())
    {
        response.stat = it->second.stat;
        response.error = Error::ZOK;
    }
    else
    {
        response.error = Error::ZNONODE;
    }

    return { std::make_shared<ExistsResponse>(response), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperGetRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    GetResponse response;
    response.zxid = zxid;

    auto it = container.find(path);
    if (it == container.end())
    {
        response.error = Error::ZNONODE;
    }
    else
    {
        response.stat = it->second.stat;
        response.data = it->second.data;
        response.error = Error::ZOK;
    }

    return { std::make_shared<GetResponse>(response), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperSetRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    SetResponse response;
    response.zxid = zxid;
    Undo undo;

    auto it = container.find(path);
    if (it == container.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version == -1 || version == it->second.stat.version)
    {
        auto prev_node = it->second;

        it->second.data = data;
        ++it->second.stat.version;
        it->second.stat.mzxid = zxid;
        it->second.stat.mtime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        it->second.data = data;
        ++container.at(parentPath(path)).stat.cversion;
        response.stat = it->second.stat;
        response.error = Error::ZOK;

        undo = [prev_node, &container, path = path]
        {
            container.at(path) = prev_node;
            --container.at(parentPath(path)).stat.cversion;
        };
    }
    else
    {
        response.error = Error::ZBADVERSION;
    }

    return { std::make_shared<SetResponse>(response), undo };
}

std::pair<ResponsePtr, Undo> TestKeeperListRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    ListResponse response;
    response.zxid = zxid;

    auto it = container.find(path);
    if (it == container.end())
    {
        response.error = Error::ZNONODE;
    }
    else
    {
        auto path_prefix = path;
        if (path_prefix.empty())
            throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "Logical error: path cannot be empty");

        if (path_prefix.back() != '/')
            path_prefix += '/';

        /// Fairly inefficient.
        for (auto child_it = container.upper_bound(path_prefix);
             child_it != container.end() && startsWith(child_it->first, path_prefix);
            ++child_it)
        {
            using enum ListRequestType;
            if (parentPath(child_it->first) == path)
            {
                ListRequestType list_request_type = ALL;
                if (const auto * filtered_list = dynamic_cast<const TestKeeperFilteredListRequest *>(this))
                    list_request_type = filtered_list->list_request_type;

                const auto is_ephemeral = child_it->second.stat.ephemeralOwner != 0;
                if (list_request_type == ALL || (is_ephemeral && list_request_type == EPHEMERAL_ONLY)
                    || (!is_ephemeral && list_request_type == PERSISTENT_ONLY))
                    response.names.emplace_back(baseName(child_it->first));
            }
        }

        response.stat = it->second.stat;
        response.error = Error::ZOK;
    }

    return { std::make_shared<ListResponse>(response), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperCheckRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    CheckResponse response;
    response.zxid = zxid;
    auto it = container.find(path);
    if (it == container.end())
    {
        response.error = Error::ZNONODE;
    }
    else if (version != -1 && version != it->second.stat.version)
    {
        response.error = Error::ZBADVERSION;
    }
    else
    {
        response.error = Error::ZOK;
    }

    return { std::make_shared<CheckResponse>(response), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperSyncRequest::process(TestKeeper::Container & /*container*/, int64_t zxid) const
{
    SyncResponse response;
    response.path = path;
    response.zxid = zxid;

    return { std::make_shared<SyncResponse>(std::move(response)), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperReconfigRequest::process(TestKeeper::Container &, int64_t) const
{
    // In TestKeeper we assume data is stored on one server, so this is a dummy implementation to
    // satisfy IKeeper interface.
    // We can't even check the validity of input data, neither can we create the /keeper/config znode
    // as we don't know the id of current "server".
    ReconfigResponse response;
    response.error = Error::ZOK;
    return { std::make_shared<ReconfigResponse>(std::move(response)), {} };
}

std::pair<ResponsePtr, Undo> TestKeeperMultiRequest::process(TestKeeper::Container & container, int64_t zxid) const
{
    MultiResponse response;
    response.zxid = zxid;
    response.responses.reserve(requests.size());
    std::vector<Undo> undo_actions;

    try
    {
        auto request_it = requests.begin();
        response.error = Error::ZOK;
        while (request_it != requests.end())
        {
            const TestKeeperRequest & concrete_request = dynamic_cast<const TestKeeperRequest &>(**request_it);
            ++request_it;
            auto [ cur_response, undo_action ] = concrete_request.process(container, zxid);
            response.responses.emplace_back(cur_response);
            if (cur_response->error != Error::ZOK)
            {
                response.error = cur_response->error;
                break;
            }

            undo_actions.emplace_back(std::move(undo_action));
        }

        if (response.error != Error::ZOK)
        {
            for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                if (*it)
                    (*it)();

            while (request_it != requests.end())
            {
                const TestKeeperRequest & concrete_request = dynamic_cast<const TestKeeperRequest &>(**request_it);
                ++request_it;
                response.responses.emplace_back(concrete_request.createResponse());
                response.responses.back()->error = Error::ZRUNTIMEINCONSISTENCY;
            }
        }

        return { std::make_shared<MultiResponse>(response), {} };
    }
    catch (...)
    {
        for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
            if (*it)
                (*it)();
        throw;
    }
}

ResponsePtr TestKeeperCreateRequest::createResponse() const { return std::make_shared<CreateResponse>(); }
ResponsePtr TestKeeperRemoveRequest::createResponse() const { return std::make_shared<RemoveResponse>(); }
ResponsePtr TestKeeperExistsRequest::createResponse() const { return std::make_shared<ExistsResponse>(); }
ResponsePtr TestKeeperGetRequest::createResponse() const { return std::make_shared<GetResponse>(); }
ResponsePtr TestKeeperSetRequest::createResponse() const { return std::make_shared<SetResponse>(); }
ResponsePtr TestKeeperListRequest::createResponse() const { return std::make_shared<ListResponse>(); }
ResponsePtr TestKeeperCheckRequest::createResponse() const { return std::make_shared<CheckResponse>(); }
ResponsePtr TestKeeperSyncRequest::createResponse() const { return std::make_shared<SyncResponse>(); }
ResponsePtr TestKeeperReconfigRequest::createResponse() const { return std::make_shared<ReconfigResponse>(); }
ResponsePtr TestKeeperMultiRequest::createResponse() const { return std::make_shared<MultiResponse>(); }


TestKeeper::TestKeeper(const zkutil::ZooKeeperArgs & args_)
    : args(args_)
{
    container.emplace("/", Node());

    if (!args.chroot.empty())
    {
        if (args.chroot.back() == '/')
            args.chroot.pop_back();
    }

    processing_thread = ThreadFromGlobalPool([this] { processingThread(); });
}


TestKeeper::~TestKeeper()
{
    try
    {
        finalize(__PRETTY_FUNCTION__);
        if (processing_thread.joinable())
            processing_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void TestKeeper::processingThread()
{
    setThreadName("TestKeeperProc");

    try
    {
        while (!expired)
        {
            RequestInfo info;

            UInt64 max_wait = static_cast<UInt64>(args.operation_timeout_ms);
            if (requests_queue.tryPop(info, max_wait))
            {
                if (expired)
                    break;


                ++zxid;

                info.request->addRootPath(args.chroot);
                auto [response, _] = info.request->process(container, zxid);

                if (info.watch)
                {
                    /// To be compatible with real ZooKeeper we add watch if request was successful (i.e. node exists)
                    /// or if it was exists request which allows to add watches for non existing nodes.
                    if (response->error == Error::ZOK)
                    {
                        auto & watches_type = dynamic_cast<const ListRequest *>(info.request.get())
                            ? list_watches
                            : watches;

                        watches_type[info.request->getPath()].insert(info.watch);
                    }
                    else if (response->error == Error::ZNONODE && dynamic_cast<const ExistsRequest *>(info.request.get()))
                    {
                        watches[info.request->getPath()].insert(info.watch);
                    }
                }

                if (response->error == Error::ZOK)
                    info.request->processWatches(watches, list_watches);

                response->removeRootPath(args.chroot);
                if (info.callback)
                    info.callback(*response);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize(__PRETTY_FUNCTION__);
    }
}


void TestKeeper::finalize(const String &)
{
    {
        std::lock_guard lock(push_request_mutex);

        if (expired)
            return;
        expired = true;
    }

    processing_thread.join();

    try
    {
        {
            for (auto & path_watch : watches)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = Error::ZSESSIONEXPIRED;

                for (const auto & callback : path_watch.second)
                {
                    if (callback)
                    {
                        try
                        {
                            (*callback)(response);
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
                response->error = Error::ZSESSIONEXPIRED;
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
                response.error = Error::ZSESSIONEXPIRED;
                try
                {
                    (*info.watch)(response);
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

void TestKeeper::pushRequest(RequestInfo && request)
{
    try
    {
        request.time = clock::now();

        /// We must serialize 'pushRequest' and 'finalize' (from processingThread) calls
        ///  to avoid forgotten operations in the queue when session is expired.
        /// Invariant: when expired, no new operations will be pushed to the queue in 'pushRequest'
        ///  and the queue will be drained in 'finalize'.
        std::lock_guard lock(push_request_mutex);

        if (expired)
            throw Exception::fromMessage(Error::ZSESSIONEXPIRED, "Session expired");

        if (!requests_queue.tryPush(std::move(request), args.operation_timeout_ms))
            throw Exception::fromMessage(Error::ZOPERATIONTIMEOUT, "Cannot push request to queue within operation timeout");
    }
    catch (...)
    {
        finalize(__PRETTY_FUNCTION__);
        throw;
    }
}


void TestKeeper::create(
        const String & path,
        const String & data,
        bool is_ephemeral,
        bool is_sequential,
        const ACLs &,
        CreateCallback callback)
{
    TestKeeperCreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;

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
        WatchCallbackPtr watch)
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
        WatchCallbackPtr watch)
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
        ListRequestType list_request_type,
        ListCallback callback,
        WatchCallbackPtr watch)
{
    TestKeeperFilteredListRequest request;
    request.path = path;
    request.list_request_type = list_request_type;

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

void TestKeeper::sync(
        const String & path,
        SyncCallback callback)
{
    TestKeeperSyncRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperSyncRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SyncResponse &>(response)); };
    pushRequest(std::move(request_info));
}

void TestKeeper::reconfig(
    std::string_view joining,
    std::string_view leaving,
    std::string_view new_members,
    int32_t version,
    ReconfigCallback callback)
{
    TestKeeperReconfigRequest req;
    req.joining = joining;
    req.leaving = leaving;
    req.new_members = new_members;
    req.version = version;

    pushRequest({
        .request = std::make_shared<TestKeeperReconfigRequest>(std::move(req)),
        .callback = [callback](const Response & response)
        {
            callback(dynamic_cast<const ReconfigResponse &>(response));
        },
        .watch = nullptr,
        .time = {}
    });
}

void TestKeeper::multi(
        const Requests & requests,
        MultiCallback callback)
{
    TestKeeperMultiRequest request(requests);

    RequestInfo request_info;
    request_info.request = std::make_shared<TestKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
    pushRequest(std::move(request_info));
}

}
