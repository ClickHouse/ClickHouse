#include <boost/algorithm/string.hpp>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <common/logger_useful.h>
 
#include <sstream>
#include <iomanip>
#include <iostream>


namespace Coordination
{
    std::atomic<int32_t> seq {1};

    enum class WatchConnType {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };

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

    void EtcdKeeper::EtcdNode::serialize() {
        std::vector<std::string> fields;
        fields.push_back(data);
        fields.push_back(is_ephemeral ? "1" : "0");
        fields.push_back(is_sequental ? "1" : "0");
        for (auto arg : stat.as_vector()) {
            fields.push_back(std::to_string(arg));
        }
        fields.push_back(std::to_string(seq_num));
        unpursed_data = "";
        for (auto field : fields) {
            unpursed_data += field + ';';
        }
    }

    void EtcdKeeper::EtcdNode::deserialize() {
        std::vector<std::string> fields;
        boost::split(fields, unpursed_data, boost::is_any_of(";"));
        data = fields[0];
        is_ephemeral = fields[1] == "1" ? true :false;
        is_sequental = fields[2] == "1" ? true :false;
        std::vector<std::string> stat_vector(fields.begin() + 2, fields.begin() + 13);
        seq_num = std::stoi(fields[14]);
    }

    PutRequest preparePutRequest(const std::string & key, const std::string & value)
    {
        PutRequest request = PutRequest();

        request.set_key(key);
        request.set_value(value);
        request.set_prev_kv(true);
        return request;
    }
        
    RangeRequest prepareRangeRequest(const std::string & key, bool with_prefix=false)
    {
        RangeRequest request = RangeRequest();
        request.set_key(key);
        std::string range_end(key);
        if(with_prefix)
        {
            int ascii = (int)range_end[range_end.length()-1];
            range_end.back() = ascii+1;
            request.set_range_end(range_end);
        }
        return request;
    }
        
        
    DeleteRangeRequest prepareDeleteRangeRequest(const std::string & key)
    {
        DeleteRangeRequest request = DeleteRangeRequest();
        request.set_key(key);
        request.set_prev_kv(true);
        return request;
    }
        
    Compare prepareCompare(
        const std::string & key,
        Compare::CompareTarget target,
        Compare::CompareResult result,
        Int64 version,
        Int64 create_revision,
        Int64 mod_revision
    )
    {
        Compare compare;
        compare.set_key(key);
        compare.set_target(target);
        compare.set_result(result);
        if (target == Compare::CompareTarget::Compare_CompareTarget_VERSION) {
            compare.set_version(version);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_CREATE) {
            compare.set_create_revision(create_revision);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_MOD) {
            compare.set_mod_revision(mod_revision);
        }
        return compare;
    }
        
    TxnRequest prepareTxnRequest(
        const std::string & key,
        Compare::CompareTarget target,
        Compare::CompareResult result,
        Int64 version,
        Int64 create_revision,
        Int64 mod_revision
        // std::string & value = nullptr)
    )
    {
        TxnRequest txn_request;
        Compare* compare = txn_request.add_compare();
        compare->set_key(key);
        compare->set_target(target);
        compare->set_result(result);
        if (target == Compare::CompareTarget::Compare_CompareTarget_VERSION) {
            compare->set_version(version);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_CREATE) {
            compare->set_create_revision(create_revision);
        }
        if (target == Compare::CompareTarget::Compare_CompareTarget_MOD) {
            compare->set_mod_revision(mod_revision);
        }
        // if (value) {
        //     compare->set_value(value);
        // }
        return txn_request;
    }

    void callPutRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        PutRequest request,
        const std::string & path)
    {
        EtcdKeeper::AsyncPutCall* call = new EtcdKeeper::AsyncPutCall(call_);
        call->path = path;
        call->response_reader =
            stub_->PrepareAsyncPut(&call->context, request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callRangeRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<RangeRequest> request) 
    {
        EtcdKeeper::AsyncRangeCall* call = new EtcdKeeper::AsyncRangeCall(call_);
        call->response_reader =
            stub_->PrepareAsyncRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callDeleteRangeRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<DeleteRangeRequest> request)
    {
        EtcdKeeper::AsyncDeleteRangeCall* call = new EtcdKeeper::AsyncDeleteRangeCall(call_);
        call->response_reader =
            stub_->PrepareAsyncDeleteRange(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void callRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        EtcdKeeper::TxnRequests requests)
    {
        TxnRequest txn_request;
        for (auto txn_compare: requests.compares) {
            Compare* compare = txn_request.add_compare();
            compare->CopyFrom(txn_compare);
        }
        RequestOp* req_success;
        for (auto success_range: requests.success_ranges)
        {
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_range(std::make_unique<RangeRequest>(success_range).release());
        }
        for (auto success_put: requests.success_puts)
        {
            std::cout << "CR" << success_put.key() << std::endl;
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_put(std::make_unique<PutRequest>(success_put).release());
        }
        for (auto success_delete_range: requests.success_delete_ranges)
        {
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(success_delete_range).release());
        }
        for (auto failure_range: requests.failure_ranges)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(std::make_unique<RangeRequest>(failure_range).release());
        }
        for (auto failure_put: requests.failure_puts)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_put(std::make_unique<PutRequest>(failure_put).release());
        }
        for (auto failure_delete_range: requests.failure_delete_ranges)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(failure_delete_range).release());
        }

        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub_->PrepareAsyncTxn(&call->context, txn_request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }


    void callTxnRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        std::unique_ptr<TxnRequest> request)
    {
        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub_->PrepareAsyncTxn(&call->context, *request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    void EtcdKeeper::callWatchRequest(
        const std::string & key,
        bool list_watch,
        std::unique_ptr<Watch::Stub> & stub_,
        CompletionQueue & cq_)
    {
        etcdserverpb::WatchRequest request;
        etcdserverpb::WatchResponse response;
        etcdserverpb::WatchCreateRequest create_request;
        create_request.set_key(key);
        request.mutable_create_request()->CopyFrom(create_request);
        stream_->Write(request, (void*)WatchConnType::WRITE);
        LOG_FATAL(log, "WATCH " << key << list_watch);
    }

    void EtcdKeeper::readWatchResponse()
    {
        stream_->Read(&response_, (void*)WatchConnType::READ);
        if (response_.created()) {
            LOG_ERROR(log, "Watch created");
        } else if (response_.events_size()) {
            std::cout << "WATCH RESP " << response_.DebugString() << std::endl;
            for (auto event : response_.events()) {
                String path = event.kv().key();
                WatchResponse watch_response;
                watch_response.path = path;

                auto it = watches.find(watch_response.path);
                if (it != watches.end())
                {
                    for (auto & callback : it->second)
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
        } else {
            LOG_ERROR(log, "Returned watch without created flag and without event.");
        }
    }
 
    struct EtcdKeeperRequest : virtual Request
    {
        EtcdKeeper::XID xid = 0;
        EtcdKeeper::TxnRequests txn_requests;
        virtual bool isMutable() const { return false; }
        virtual void testCall(EtcdKeeper::AsyncCall& call,
            std::unique_ptr<KV::Stub>& kv_stub_,
            CompletionQueue& kv_cq_) const
        { 
                callRequest(call, kv_stub_, kv_cq_, txn_requests);
        }
        virtual EtcdKeeperResponsePtr makeResponse() const = 0;
        virtual void prepareCall() const = 0;
    };

    using EtcdRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
    using EtcdRequests = std::vector<EtcdRequestPtr>;

    struct EtcdKeeperResponse : virtual Response {
        virtual ~EtcdKeeperResponse() {}
        virtual void readFromResponses(bool compare_result, std::vector<ResponseOp> responses) = 0;
        void readFromRepeatedPtrField(bool compare_result, google::protobuf::RepeatedPtrField<ResponseOp> fields)
        {
            std::vector<ResponseOp> responses;
            for (auto field : fields) {
                responses.push_back(field);
            }
            readFromResponses(compare_result, responses);
        }
        void readFromTag(void* got_tag)
        {
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            readFromRepeatedPtrField(call->response.succeeded(), call->response.responses());
        }
    };

    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        std::string process_path;

        EtcdKeeperCreateRequest() {}
        EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void setProcessPath()
        {
            std::string process_path_ = path;
            if (is_sequential)
            {
                seq.fetch_add(1);

                std::stringstream seq_num_str;
                seq_num_str << std::setw(10) << std::setfill('0') << seq;

                process_path_ += seq_num_str.str();
            }
            process_path = process_path_;
        }
        void prepareCall() const override {
            // TODO add creating metadata nodes
            EtcdKeeper::EtcdNode test_node;
            test_node.data = data;
            setProcessPath();
            std::cout << "CREATE " << process_path << std::endl;

            test_node.serialize();
            txn_requests.success_puts.push_back(preparePutRequest(process_path, data));
        }
    };

    struct EtcdKeeperCreateResponse final : CreateResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            std::cout << "readFromResponses" << std::endl;
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponsePut == resp.response_case())
                {
                    auto put_resp = resp.response_put();
                    error = Error::ZOK;
                }
            }
        }
    };
 
    struct EtcdKeeperRemoveRequest final : RemoveRequest, EtcdKeeperRequest
    {
        EtcdKeeperRemoveRequest() {}
        EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override {
            std::cout << "REMOVE " << path << std::endl;
            txn_requests.success_delete_ranges.emplace_back(prepareDeleteRangeRequest(path));
        }
    };

    struct EtcdKeeperRemoveResponse final : RemoveResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseDeleteRange == resp.response_case())
                {
                    auto response = resp.mutable_response_delete_range();
                    if (response->deleted())
                    {
                        error = Error::ZOK;
                    }
                    else
                    {
                        error = Error::ZNONODE;
                    }
                }
            }
        }
    };
 
    struct EtcdKeeperExistsRequest final : ExistsRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override
        {
            std::cout << "EXISTS " << path << std::endl;
            txn_requests.success_ranges.push_back(prepareRangeRequest(path));
        }
    };

    struct EtcdKeeperExistsResponse final : ExistsResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            if (responses.size() == 0) {
                error = Error::ZNONODE;
            }
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto response = resp.mutable_response_range();
                    if (response->count())
                    {
                        error = Error::ZOK;
                    }
                    else
                    {
                        error = Error::ZNONODE;
                    }
                }
            }
        }
    };
 
    struct EtcdKeeperGetRequest final : GetRequest, EtcdKeeperRequest
    {
        EtcdKeeperGetRequest() {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override
        {
            std::cout << "GET " << path << std::endl;
            txn_requests.success_ranges.push_back(prepareRangeRequest(path));
        }
    };

    struct EtcdKeeperGetResponse final : GetResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto response = resp.mutable_response_range();
                    std::cout << "COUNT" << response->count() << std::endl;
                    if (response->count() > 0)
                    {
                        data = "";
                        for (auto kv : response->kvs())
                        {
                            data = kv.value();
                        }
                        stat = Stat();
                        error = Error::ZOK;
                    }
                    else
                    {
                        error = Error::ZNONODE;
                    }
                }
            }
        }
    };
 
    struct EtcdKeeperSetRequest final : SetRequest, EtcdKeeperRequest
    {
        EtcdKeeperSetRequest() {}
        EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override
        {
            std::cout << "SET " << path << std::endl;
            // Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
            // if (version == -1)
            // {
            //     compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            // }
            // Compare::CompareTarget compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            // txn_requests.compares.push_back(prepareCompare(path, compare_target, compare_result, version, 0, 0));
            // txn_requests.failure_ranges.push_back(prepareRangeRequest(path));
            txn_requests.success_puts.push_back(preparePutRequest(path, data));
        }
    };

    struct EtcdKeeperSetResponse final : SetResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            if (compare_result)
            {
                error = Error::ZOK;
            }
            else if (responses[0].response_range().count() == 0)
            {
                error = Error::ZNONODE;
            }
            else
            {
                error = Error::ZBADVERSION;
            }
        }
    };
 
    struct EtcdKeeperListRequest final : ListRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override {
            std::cout << "LIST " << path << std::endl;
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            Compare::CompareTarget compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            txn_requests.compares.push_back(prepareCompare(path, compare_target, compare_result, -1, 0, 0));
            txn_requests.success_ranges.push_back(prepareRangeRequest(path + "/", true));
        }
    };

    struct EtcdKeeperListResponse final : ListResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            if (!compare_result) {
                error = Error::ZNONODE;
            }
            else
            {
                for (auto resp : responses) {
                    for (auto kv : resp.response_range().kvs()) {
                        names.emplace_back(baseName(kv.key()));
                    }
                }
                error = Error::ZOK;
            }
        }
    };
 
    struct EtcdKeeperCheckRequest final : CheckRequest, EtcdKeeperRequest
    {
        EtcdKeeperCheckRequest() {}
        EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void prepareCall() const override
        {
            std::cout << "CHECK " << path << std::endl;
            Compare::CompareResult compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
            if (version == -1) {
                compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
            }
            Compare::CompareTarget compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            txn_requests.compares.push_back(prepareCompare(path, compare_target, compare_result, version, 0, 0));
            txn_requests.failure_ranges.push_back(prepareRangeRequest(path));
        }
    };

    struct EtcdKeeperCheckResponse final : CheckResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses)
        {
            if (compare_result) {
                error = Error::ZOK;
            }
            else if (responses[0].response_range().count())
            {
                error = Error::ZNONODE;
            }
            else 
            {
                error = Error::ZBADVERSION;
            }
        }
    };
 
    struct EtcdKeeperMultiRequest final : MultiRequest, EtcdKeeperRequest
    {
        Responses resps;

        EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            std::cout << "MULTI " << std::endl;
            resps.reserve(generic_requests.size());
            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    auto request = std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create);
                    request->prepareCall();
                    txn_requests += request->txn_requests;
                    resps.push_back(request->makeResponse());
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    auto request = std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove);
                    request->prepareCall();
                    txn_requests += request->txn_requests;
                    resps.push_back(request->makeResponse());
                }
                else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    auto request = std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set);
                    request->prepareCall();
                    txn_requests += request->txn_requests;
                    resps.push_back(request->makeResponse());
                }
                else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    auto request = std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check);
                    request->prepareCall();
                    txn_requests += request->txn_requests;
                    resps.push_back(request->makeResponse());
                }
                else
                {
                    throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
                }
            }
        }

        void prepareCall() const override {
            // txn_requests.interaction();
        }
 
        EtcdKeeperResponsePtr makeResponse() const override;
    };

    struct EtcdKeeperMultiResponse final : MultiResponse, EtcdKeeperResponse
    {
        void readFromResponses(bool compare_result, std::vector<ResponseOp> responses_)
        {
            if (compare_result) {
                for (int i; i != responses.size(); i++) {
                    responses[i]->error = Error::ZOK;
                }
            }
            else
            {
                for (int i; i != responses.size(); i++) {
                    responses[i]->error = Error::ZNONODE;
                }
            }
        }
    };
  
    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponse() const
    {
        auto resp = std::make_shared<EtcdKeeperCreateResponse>();
        resp->path_created = process_path;
        return resp;
    }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponse() const { return std::make_shared<EtcdKeeperRemoveResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponse() const { return std::make_shared<EtcdKeeperExistsResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponse() const { return std::make_shared<EtcdKeeperGetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponse() const { return std::make_shared<EtcdKeeperSetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponse() const { return std::make_shared<EtcdKeeperListResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponse() const { return std::make_shared<EtcdKeeperCheckResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponse() const 
    { 
        auto resp = std::make_shared<EtcdKeeperMultiResponse>();
        resp->responses = resps;
        return resp;
    }
 
 
    EtcdKeeper::EtcdKeeper(const String & root_path_, Poco::Timespan operation_timeout_)
            : root_path(root_path_), operation_timeout(operation_timeout_)
    {
        log = &Logger::get("EtcdKeeper");
        LOG_FATAL(log, "INIT");

        std::string stripped_address = "localhost:2379";
        std::shared_ptr<Channel> channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        kv_stub_= KV::NewStub(channel);

        std::shared_ptr<Channel> watch_channel = grpc::CreateChannel(stripped_address, grpc::InsecureChannelCredentials());
        watch_stub_= Watch::NewStub(watch_channel);

        stream_ = watch_stub_->AsyncWatch(&context_, &watch_cq_, (void*)WatchConnType::CONNECT);
        readWatchResponse();
 
        if (!root_path.empty())
        {
            if (root_path.back() == '/')
                root_path.pop_back();
        }
 
        call_thread = ThreadFromGlobalPool([this] { callThread(); });
        complete_thread = ThreadFromGlobalPool([this] { completeThread(); });
        watch_complete_thread = ThreadFromGlobalPool([this] { watchCompleteThread(); });
    }
 
 
    EtcdKeeper::~EtcdKeeper()
    {

        LOG_FATAL(log, "DESTR");
        try
        {
            finalize();
            if (call_thread.joinable())
                call_thread.join();
            if (complete_thread.joinable())
                complete_thread.join();
            if (watch_complete_thread.joinable())
                watch_complete_thread.join();

        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void EtcdKeeper::callThread()
    {
        setThreadName("EtcdKeeperCall");
 
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
                        LOG_FATAL(log, "WATCH IN REQUEST");
                        bool list_watch = false;
                        if (dynamic_cast<const ListRequest *>(info.request.get())) {
                            list_watch = true;
                        }
                        callWatchRequest(info.request->getPath(), list_watch, watch_stub_, watch_cq_);
                    }

                    std::lock_guard lock(operations_mutex);
                    operations[info.request->xid] = info;

                    if (expired)
                        break;

                    info.request->addRootPath(root_path);

                    EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                    call->xid = info.request->xid;

                    info.request->prepareCall();
                    std::cout << "prepareCall" << std::endl;
                    info.request->testCall(*call, kv_stub_, kv_cq_);
                    std::cout << "testCall" << std::endl;
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
    }

    void EtcdKeeper::completeThread() {
        setThreadName("EtcdKeeperComplete");

        try
        {
            RequestInfo request_info;
            EtcdKeeperResponsePtr response;

            void* got_tag;
            bool ok = false;

            while (kv_cq_.Next(&got_tag, &ok)) {
                LOG_FATAL(log, "GOT TAG" << got_tag);

                GPR_ASSERT(ok);
                if (got_tag)
                {
                    auto call = static_cast<AsyncCall*>(got_tag);

                    XID xid = call->xid;

                    LOG_FATAL(log, "XID" << xid);

                    auto it = operations.find(xid);
                    if (it == operations.end())
                        throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

                    request_info = std::move(it->second);
                    operations.erase(it);

                    response = request_info.request->makeResponse();

                    if (!call->status.ok())
                    {
                        LOG_FATAL(log, "RPC FAILED" << call->status.error_message());
                    }
                    else
                    {
                        LOG_FATAL(log, "READ RPC RESPONSE");

                        response->readFromTag(got_tag);
                        response->removeRootPath(root_path);
                    }
                    
                    if (request_info.callback)
                        request_info.callback(*response);
                } 
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
        
    }

    void EtcdKeeper::watchCompleteThread() {
        setThreadName("EtcdKeeperWatchComplete");

        void* got_tag;
        bool ok = false;

        try
        {
            while (watch_cq_.Next(&got_tag, &ok))
            {
                if (ok) {
                    std::cout << std::endl
                            << "**** Processing completion queue tag " << got_tag
                            << std::endl;
                    switch (static_cast<WatchConnType>(reinterpret_cast<long>(got_tag))) {
                    case WatchConnType::READ:
                        std::cout << "Read a new message." << std::endl;
                        readWatchResponse();
                        break;
                    case WatchConnType::WRITE:
                        std::cout << "Sending message (async)." << std::endl;
                        break;
                    case WatchConnType::CONNECT:
                        std::cout << "Server connected." << std::endl;
                        break;
                    case WatchConnType::WRITES_DONE:
                        std::cout << "Server disconnecting." << std::endl;
                        break;
                    case WatchConnType::FINISH:
                        // std::cout << "Client finish; status = "
                        //         << (finish_status_.ok() ? "ok" : "cancelled")
                        //         << std::endl;
                        context_.TryCancel();
                        watch_cq_.Shutdown();
                        break;
                    default:
                        std::cerr << "Unexpected tag " << got_tag << std::endl;
                        GPR_ASSERT(false);
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
        
    }
 
    void EtcdKeeper::finalize()
    {
        LOG_FATAL(log, "FINALIZE");

        {
            std::lock_guard lock(push_request_mutex);
 
            if (expired)
                return;
            expired = true;
        }
 
        call_thread.join();
        complete_thread.join();
        watch_complete_thread.join();
 
        try
        {
            {
                for (auto & path_watch : watches)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = ZSESSIONEXPIRED;
 
                    for (auto & callback : path_watch.second)
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
                    ResponsePtr response = info.request->makeResponse();
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
 
    void EtcdKeeper::pushRequest(RequestInfo && info)
    {
        try
        {
            info.time = clock::now();
 
            if (!info.request->xid)
            {
                info.request->xid = next_xid.fetch_add(1);
                LOG_FATAL(log, "XID" << next_xid);

                if (info.request->xid < 0)
                    throw Exception("XID overflow", ZSESSIONEXPIRED);
            }

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
 
 
    void EtcdKeeper::create(
            const String & path,
            const String & data,
            bool is_ephemeral,
            bool is_sequential,
            const ACLs &,
            CreateCallback callback)
    {
        EtcdKeeperCreateRequest request;
        request.path = path;
        request.data = data;
        // request.process_path = request.getProcessPath();
        request.is_ephemeral = is_ephemeral;
        request.is_sequential = is_sequential;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperCreateRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::remove(
            const String & path,
            int32_t version,
            RemoveCallback callback)
    {
        EtcdKeeperRemoveRequest request;
        request.path = path;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperRemoveRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::exists(
            const String & path,
            ExistsCallback callback,
            WatchCallback watch)
    {
        EtcdKeeperExistsRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperExistsRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::get(
            const String & path,
            GetCallback callback,
            WatchCallback watch)
    { 
        EtcdKeeperGetRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperGetRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::set(
            const String & path,
            const String & data,
            int32_t version,
            SetCallback callback)
    {
        EtcdKeeperSetRequest request;
        request.path = path;
        request.data = data;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperSetRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::list(
            const String & path,
            ListCallback callback,
            WatchCallback watch)
    {
        EtcdKeeperListRequest request;
        request.path = path;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperListRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
        request_info.watch = watch;
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::check(
            const String & path,
            int32_t version,
            CheckCallback callback)
    { 
        EtcdKeeperCheckRequest request;
        request.path = path;
        request.version = version;
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperCheckRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
    void EtcdKeeper::multi(
            const Requests & requests,
            MultiCallback callback)
    {
        EtcdKeeperMultiRequest request(requests);
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperMultiRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
}
