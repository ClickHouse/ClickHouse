#include <boost/algorithm/string.hpp>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>
#include <common/logger_useful.h>
 
#include <sstream>
#include <iomanip>
#include <iostream>
#include <unordered_map>


namespace Coordination
{
    Logger * log = nullptr;
    std::unordered_map<std::string, int32_t> seqs;

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

    static String childName(const String & raw_child_path, const String & path)
    {
        String child_path = raw_child_path.substr(path.length() + 1);
        auto slash_pos = child_path.find('/');
        return child_path.substr(0, slash_pos);
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
            int ascii = (int)range_end[range_end.length() - 1];
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
        std::string target,
        std::string result,
        Int64 value
    )
    {
        Compare compare;
        compare.set_key(key);
        Compare::CompareResult compare_result;
        if (result == "equal")
        {
            compare_result = Compare::CompareResult::Compare_CompareResult_EQUAL;
        }
        else if (result == "not_equal")
        {
            compare_result = Compare::CompareResult::Compare_CompareResult_NOT_EQUAL;
        }
        compare.set_result(compare_result);
        Compare::CompareTarget compare_target;
        if (target == "version") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            compare.set_version(value);
        }
        if (target == "create") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_CREATE;
            compare.set_create_revision(value);
        }
        if (target == "mod") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_MOD;
            compare.set_mod_revision(value);
        }
        if (target == "value") {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VALUE;
            compare.set_value(std::to_string(value));
        }
        compare.set_target(compare_target);
        return compare;
    }

    void callRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub_,
        CompletionQueue & cq_,
        EtcdKeeper::TxnRequests requests)
    {
        TxnRequest txn_request;
        for (auto txn_compare: requests.compares) {
            LOG_DEBUG(log, "CMPR" << txn_compare.key());
            Compare* compare = txn_request.add_compare();
            compare->CopyFrom(txn_compare);
        }
        RequestOp* req_success;
        for (auto success_range: requests.success_ranges)
        {
            LOG_DEBUG(log, "RG" << success_range.key());
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_range(std::make_unique<RangeRequest>(success_range).release());
        }
        for (auto success_put: requests.success_puts)
        {
            LOG_DEBUG(log, "CR" << success_put.key() << " " << success_put.value());
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_put(std::make_unique<PutRequest>(success_put).release());
        }
        for (auto success_delete_range: requests.success_delete_ranges)
        {
            LOG_DEBUG(log, "DR" << success_delete_range.key());
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(success_delete_range).release());
        }
        for (auto failure_range: requests.failure_ranges)
        {
            LOG_DEBUG(log, "FRG" << failure_range.key());
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(std::make_unique<RangeRequest>(failure_range).release());
        }
        for (auto failure_put: requests.failure_puts)
        {
            LOG_DEBUG(log, "FCR" << failure_put.key());
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_put(std::make_unique<PutRequest>(failure_put).release());
        }
        for (auto failure_delete_range: requests.failure_delete_ranges)
        {
            LOG_DEBUG(log, "FDR" << failure_delete_range.key());
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(failure_delete_range).release());
        }

        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub_->PrepareAsyncTxn(&call->context, txn_request, &cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    enum class EtcdKeyPrefix {
        VALUE,
        SEQUENTIAL,
        CHILDS,
        IS_EPHEMERAL,
        IS_SEQUENTIAL
    };

    struct EtcdKey
    {
        String zk_path;
        String parent_zk_path;
        int32_t level;
        EtcdKey() {}
        EtcdKey(const String & path)
        {
            zk_path = path;
            level = std::count(zk_path.begin(), zk_path.end(), '/');
            parent_zk_path = parentPath(zk_path);
        }
        void setFullPath(const String & full_path)
        {
            int32_t slash = full_path.find("/", 10);
            level = std::stoi(full_path.substr(10, slash - 10));
            zk_path = full_path.substr(slash);
        }
        void updateZkPath(const String & new_zk_path)
        {
            zk_path = new_zk_path;
        }
        String generateFullPathFromParts(EtcdKeyPrefix prefix_type, int32_t level, const String & path)
        {
            String prefix;
            if (prefix_type == EtcdKeyPrefix::VALUE)
            {
                prefix = "/value/";
            }
            else if (prefix_type == EtcdKeyPrefix::SEQUENTIAL)
            {
                prefix = "/sequential/";
            }
            else if (prefix_type == EtcdKeyPrefix::CHILDS)
            {
                prefix = "/childs/";
            }
            else if (prefix_type == EtcdKeyPrefix::IS_EPHEMERAL)
            {
                prefix = "/is_ephemeral/";
            }
            else if (prefix_type == EtcdKeyPrefix::IS_SEQUENTIAL)
            {
                prefix = "/is_sequential/";
            }
            return "/zk" + prefix + std::to_string(level) + path;
        }
        String getSequentialKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::SEQUENTIAL, level, zk_path);
        }
        String getFullEtcdKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level, zk_path);
        }
        String getChildsFlagKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::CHILDS, level, zk_path);
        }
        String getEphimeralFlagKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_EPHEMERAL, level, zk_path);
        }
        String getSequentialFlagKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_SEQUENTIAL, level, zk_path);
        }
        std::vector<String> getRelatedKeys()
        {
            return std::vector<String> {getFullEtcdKey(), getSequentialKey(), getChildsFlagKey(), getEphimeralFlagKey(), getSequentialFlagKey()};
        }
        String getChildsPrefix()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level + 1, zk_path);
        }
        String getParentChildsFlagKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::CHILDS, level - 1, parent_zk_path);
        }
        String getParentKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level - 1, parent_zk_path);
        }
        String getParentEphimeralFlagKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_EPHEMERAL, level - 1, parent_zk_path);
        }
        String getParentSequentialKey()
        {
            return generateFullPathFromParts(EtcdKeyPrefix::SEQUENTIAL, level - 1, parent_zk_path);
        }
    };

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
        std::string range_end = key;
        if(list_watch)
        {
            LOG_DEBUG(log, "WATCH WITH PREFIX");
            int ascii = (int)range_end[range_end.length() - 1];
            range_end.back() = ascii+1;
            create_request.set_range_end(range_end);
        }
        request.mutable_create_request()->CopyFrom(create_request);
        stream_->Write(request, (void*)WatchConnType::WRITE);
        LOG_DEBUG(log, "WATCH " << key << list_watch);
    }

    void EtcdKeeper::readWatchResponse()
    {
        stream_->Read(&response_, (void*)WatchConnType::READ);
        if (response_.created()) {
            LOG_DEBUG(log, "Watch created");
        } else if (response_.events_size()) {
            // LOG_DEBUG(log, "WATCH RESP " << response_.DebugString());
            for (auto event : response_.events()) {
                String path = event.kv().key();
                EtcdKey etcd_key;
                etcd_key.setFullPath(path);
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
                watch_list_response.path = etcd_key.getParentKey();

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
            LOG_DEBUG(log, "Returned watch without created flag and without event.");
        }
    }

    struct EtcdKeeperResponse : virtual Response {
        bool finished = true;
        virtual ~EtcdKeeperResponse() {}
    };
    struct EtcdKeeperCreateResponse final : CreateResponse, EtcdKeeperResponse {};
    struct EtcdKeeperRemoveResponse final : RemoveResponse, EtcdKeeperResponse {};
    struct EtcdKeeperGetResponse final : GetResponse, EtcdKeeperResponse {};
    struct EtcdKeeperSetResponse final : SetResponse, EtcdKeeperResponse {};
    struct EtcdKeeperListResponse final : ListResponse, EtcdKeeperResponse {};
    struct EtcdKeeperCheckResponse final : CheckResponse, EtcdKeeperResponse {};
    struct EtcdKeeperMultiResponse final : MultiResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperRequest : virtual Request
    {
        String process_path;
        EtcdKeeper::XID xid = 0;
        bool composite = false;
        EtcdKey etcd_key;
        EtcdKeeper::TxnRequests txn_requests;
        bool pre_call_compares;
        std::vector<ResponseOp> pre_call_responses;
        virtual bool isMutable() const { return false; }
        bool pre_call_called = false;
        bool post_call_called = false;
        EtcdKeeperResponsePtr response_;
        String getEtcdKey() const
        {
            return etcd_key.getFullEtcdKey();
        }
        void clean()
        {
            composite = false;
            pre_call_called = false;
            post_call_called = false;
        }
        void setPreCall()
        {
            pre_call_called = true;
        }
        virtual void call(EtcdKeeper::AsyncCall& call,
            std::unique_ptr<KV::Stub>& kv_stub_,
            CompletionQueue& kv_cq_) const
        {
            callRequest(call, kv_stub_, kv_cq_, txn_requests);
            setPreCall();
            txn_requests.clear();
        }
        virtual EtcdKeeperResponsePtr makeResponse() const = 0;
        virtual void preparePostCall() const = 0;
        virtual void preparePreCall() {}
        virtual EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) = 0;
        EtcdKeeperResponsePtr makeResponseFromRepeatedPtrField(bool compare_result, google::protobuf::RepeatedPtrField<ResponseOp> fields)
        {
            LOG_DEBUG(log, "REPEATED_PTR");
            std::vector<ResponseOp> responses;
            for (auto field : fields) {
                responses.push_back(field);
            }
            LOG_DEBUG(log, "REPEATED_PTR_DONE");
            return makeResponseFromResponses(compare_result, responses);
        }
        EtcdKeeperResponsePtr makeResponseFromTag(void* got_tag)
        {
            if (response_->error != Error::ZOK)
            {
                return response_;
            }
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            if (!call->response.succeeded())
            {
                LOG_DEBUG(log, "ERROR");
            } 
            else
            {
                LOG_DEBUG(log, "SUCCESS");
            }
            std::cout << "DEBUG" << call->response.DebugString() << std::endl;
            return makeResponseFromRepeatedPtrField(call->response.succeeded(), call->response.responses());
        }
        bool callRequired(void* got_tag)
        {
            std::cout << "CHECK FOR REQ" << std::endl;
            if (response_->error == Error::ZOK && composite && !post_call_called) {
                EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
                // std::cout << "DEBUG REQ" << call->response.DebugString() << std::endl;
                pre_call_compares = call->response.succeeded();
                for (auto field : call->response.responses()) {
                    pre_call_responses.push_back(field);
                }
                post_call_called = true;
                LOG_DEBUG(log, "CALL REQ");
                return true;
            }
            else
            {
                LOG_DEBUG(log, "CALL NOT REQ");
                return false;
            }
        }
        virtual void checkRequestForComposite() {}
        virtual void setEtcdKey() {}
        virtual void setResponse() = 0;
        virtual void prepareCall() {
            setResponse();
            checkRequestForComposite();
            if (composite && !pre_call_called) {
                preparePreCall();
                return;
            }
            preparePostCall();
        }
    };

    using EtcdRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
    using EtcdRequests = std::vector<EtcdRequestPtr>;

    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        int32_t seq_num;
        int32_t children;
        bool parent_exists = true;
        EtcdKeeperCreateRequest() {}
        EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall()
        {
            if (parentPath(path) != "/")
            {
                txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getParentEphimeralFlagKey()));
            }
            else
            {
                parent_exists = true;
            }
            if (is_sequential)
            {
                txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getParentSequentialKey()));
            }
        }
        void parsePreResponses()
        {
            LOG_DEBUG(log, "PARSE PRE CREATE RESP");
            process_path = path;
            for (auto resp : pre_call_responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        LOG_DEBUG(log, "KEY" << kv.key() << " " << kv.value());
                        if (is_sequential && kv.key() == etcd_key.getParentSequentialKey())
                        {
                            seq_num = std::stoi(kv.value());
                            std::stringstream seq_num_str;
                            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;
                            process_path += seq_num_str.str();
                            etcd_key.updateZkPath(process_path);
                        }
                        else if (kv.key() == etcd_key.getParentEphimeralFlagKey())
                        {
                            if (kv.value() != "0")
                            {
                                response_->error = Error::ZNOCHILDRENFOREPHEMERALS;
                                return;
                            }
                            parent_exists = true;
                        }
                    }
                }
            }
        }
        void preparePostCall() const
        {
            parsePreResponses();
            LOG_DEBUG(log, "CREATE " << process_path);
            if (parent_exists || !composite)
            {
                if (is_sequential)
                {
                    txn_requests.compares.push_back(prepareCompare(etcd_key.getParentSequentialKey(), "value", "equal", seq_num));
                    txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getParentSequentialKey(), std::to_string(seq_num + 1)));
                }
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getFullEtcdKey(), data));
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getSequentialKey(), std::to_string(0)));
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getSequentialFlagKey(), std::to_string(is_sequential)));
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getEphimeralFlagKey(), std::to_string(is_ephemeral)));
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getChildsFlagKey(), ""));
                txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getParentChildsFlagKey(), "+" + etcd_key.getFullEtcdKey()));
            }
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        void checkRequestForComposite() {
            if (is_sequential || parentPath(path) != "/")
            {
                composite = true;
            }
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
 
    struct EtcdKeeperRemoveRequest final : RemoveRequest, EtcdKeeperRequest
    {
        int32_t children;
        int child_flag_version = -1;
        EtcdKeeperRemoveRequest() {}
        EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall()
        {
            LOG_DEBUG(log, "PRE REMOVE " << path);
            // txn_requests.compares.push_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
        void parsePreResponses()
        {
            LOG_DEBUG(log, "PARS PRE REMOVE " << path);
            response_->error = Error::ZNONODE;
            for (auto resp : pre_call_responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        LOG_DEBUG(log, "KEY" << kv.key() << " " << kv.value());
                        if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                        {
                            response_->error = Error::ZNOTEMPTY;
                            return;
                        }
                        else if (kv.key() == etcd_key.getChildsFlagKey())
                        {
                            child_flag_version = kv.version();
                        }
                        else if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            response_->error = Error::ZOK;
                            if (version != -1 && kv.version() != version)
                            {
                                response_->error = Error::ZBADVERSION;
                                return response_;
                            }
                        }
                    }
                }
            }
            std::cout << "ANS " << child_flag_version << std::endl;
        }
        void preparePostCall() const override {
            LOG_DEBUG(log, "POST REMOVE " << path);
            parsePreResponses();
            txn_requests.compares.push_back(prepareCompare(etcd_key.getChildsFlagKey(), "version", "equal", child_flag_version));
            txn_requests.compares.push_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));

            txn_requests.failure_ranges.push_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));

            for (auto key : etcd_key.getRelatedKeys())
            {
                txn_requests.success_delete_ranges.emplace_back(prepareDeleteRangeRequest(key));
            }
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        void checkRequestForComposite() {
            composite = true;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
 
    struct EtcdKeeperExistsRequest final : ExistsRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() const override
        {
            LOG_DEBUG(log, "EXISTS ");
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperExistsResponse final : ExistsResponse, EtcdKeeperResponse {};
 
    struct EtcdKeeperGetRequest final : GetRequest, EtcdKeeperRequest
    {
        EtcdKeeperGetRequest() {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() const override
        {
            LOG_DEBUG(log, "GET " << path);
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
 
    struct EtcdKeeperSetRequest final : SetRequest, EtcdKeeperRequest
    {
        EtcdKeeperSetRequest() {}
        EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
        bool isMutable() const override { return true; }
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() const override
        {
            LOG_DEBUG(log, "SET " << path);
            txn_requests.compares.push_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));
            txn_requests.failure_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_puts.push_back(preparePutRequest(etcd_key.getFullEtcdKey(), data));
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
 
    struct EtcdKeeperListRequest final : ListRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() const override {
            LOG_DEBUG(log, "LIST " << path);
            txn_requests.compares.push_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", "not_equal", -1));
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.push_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };
 
    struct EtcdKeeperCheckRequest final : CheckRequest, EtcdKeeperRequest
    {
        EtcdKeeperCheckRequest() {}
        EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() const override
        {
            LOG_DEBUG(log, "CHECK " << path << "    " << version);
            txn_requests.compares.push_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));
            txn_requests.failure_ranges.push_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
    };

    struct EtcdKeeperMultiRequest final : MultiRequest, EtcdKeeperRequest
    {
        String required_key = "";
        EtcdKeeperRequests etcd_requests;
        EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            LOG_DEBUG(log, "MULTI ");
            etcd_requests.reserve(generic_requests.size());
            std::unordered_map<String, int> create_map;
            std::unordered_map<String, int> remove_map;
            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    LOG_DEBUG(log, "concrete_request_create" << concrete_request_create->path);
                    create_map[concrete_request_create->path]++;
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    LOG_DEBUG(log, "concrete_request_remove" << concrete_request_remove->path);
                    remove_map[concrete_request_remove->path]++;
                }
            }

            auto it = create_map.begin();
            bool cr = false, rr = false;
            while (it != create_map.end())
            {
                String cur_path = it->first;
                if (create_map[cur_path] > 0 && remove_map[cur_path] > 0)
                {
                    std::cout << "REQUIRED KEY " << cur_path << std::endl;
                    required_key = cur_path;
                    cr = true;
                    rr = true;
                }
                it++;
            }

            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    LOG_DEBUG(log, "concrete_request_create" << concrete_request_create->path);
                    String cur_path = concrete_request_create->path;
                    if (cr && concrete_request_create->path == required_key)
                    {
                        cr = false;
                        continue;
                    }
                    // if (create_map[cur_path] == 2)
                    // {
                    //     create_map[cur_path]--;
                    //     continue;
                    // }
                    // if (std::find(create_exc.begin(), create_exc.end(), cur_path) != create_exc.end())
                    // {
                    //     continue;
                    // }
                    etcd_requests.push_back(std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create));
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    String cur_path = concrete_request_remove->path;
                    if (rr && concrete_request_remove->path == required_key)
                    {
                        rr = false;
                        continue;
                    }
                    // if (std::find(remove_exc.begin(), remove_exc.end(), cur_path) != remove_exc.end())
                    // {
                    //     continue;
                    // }
                    LOG_DEBUG(log, "concrete_request_remove" << concrete_request_remove->path);
                    etcd_requests.push_back(std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove));
                }
                else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    LOG_DEBUG(log, "concrete_request_set " << concrete_request_set->path);

                    etcd_requests.push_back(std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set));
                }
                else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    LOG_DEBUG(log, "concrete_request_check");
                    etcd_requests.push_back(std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check));
                }
                else
                {
                    throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
                }
            }
            EtcdKey ek = EtcdKey(required_key);
            required_key = ek.getFullEtcdKey();
        }
        void setEtcdKey() override
        {
            for (auto request : etcd_requests)
            {
                request->setEtcdKey();
            }
        }
        void preparePreCall()
        {
            LOG_DEBUG(log, "MULTI PRE CALL");
            for (auto request : etcd_requests)
            {
                request->checkRequestForComposite();
                if (request->composite)
                {
                    LOG_DEBUG(log, "COMPOSITE");
                    request->preparePreCall();
                    txn_requests += request->txn_requests;
                }
            }
            if (required_key != "")
            {
                txn_requests.success_ranges.push_back(prepareRangeRequest(required_key));
            }
        }
        void parsePreResponses()
        {
            LOG_DEBUG(log, "PARSE PRE CREATE RESP");
            if (required_key != "")
            {
                for (auto resp : pre_call_responses)
                {
                    if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                    {
                        auto range_resp = resp.response_range();
                        for (auto kv : range_resp.kvs())
                        {
                            LOG_DEBUG(log, "KEY" << kv.key() << " " << kv.value());
                            if (kv.key() == required_key)
                            {
                                response_->error = Error::ZNODEEXISTS;
                            }
                        }
                    }
                }
            }
        }
        void preparePostCall() const override {
            parsePreResponses();
            for (auto request : etcd_requests)
            {
                request->txn_requests.clear();
                request->pre_call_compares = pre_call_compares;
                request->pre_call_responses = pre_call_responses;
                request->preparePostCall();
                txn_requests += request->txn_requests;
            }
            txn_requests.take_last_create("/zk/childs");
        }
        void checkRequestForComposite() {
            LOG_DEBUG(log, "MULTI COMP");
            if (!txn_requests.empty())
            {
                LOG_DEBUG(log, "MULTI COMPOS");
                composite = true;
            }
        }
        void setResponse() override
        {
            response_ = makeResponse();
            response_->error = Error::ZOK;
        }
        void prepareCall() {
            setResponse();
            LOG_DEBUG(log, "PREPARE");
            if (!pre_call_called)
            {
                preparePreCall();
            }
            checkRequestForComposite();
            if (composite && !pre_call_called) {
                return;
            }
            preparePostCall();
        }

        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
    };

    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        LOG_DEBUG(log, "POST CREATE PARS");
        auto response = std::make_shared<EtcdKeeperCreateResponse>();
        if (!parent_exists)
        {
            response->error = Error::ZNONODE;
            return response;
        }
        if (!compare_result)
        {
            response->finished = false;
            LOG_DEBUG(log, "ERROR");
            return response;
        }
        response->path_created = process_path;
        for (auto resp: responses) {
            if(ResponseOp::ResponseCase::kResponsePut == resp.response_case())
            {
                auto put_resp = resp.response_put();
                if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey()){
                    response->error = Error::ZNODEEXISTS;
                }
                else
                {
                    response->error = Error::ZOK;
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperRemoveResponse>();
        if (!compare_result)
        {
            response_->error = Error::ZBADVERSION;
            for (auto resp: responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getChildsFlagKey()) {
                            if (kv.version() == child_flag_version)
                            {
                                response_->error = Error::ZOK;
                                response_->finished = false;
                            }
                            return response_;
                        }
                    }
                }
            }
        }
        for (auto resp: responses)
        {
            if(ResponseOp::ResponseCase::kResponseDeleteRange == resp.response_case())
            {
                auto delete_range_resp = resp.response_delete_range();
                if (delete_range_resp.deleted())
                {
                    for (auto kv : delete_range_resp.prev_kvs())
                    {
                        if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            response->error = Error::ZOK;
                            return response;
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperExistsResponse>();
        response->error = Error::ZNONODE;
        for (auto resp: responses) {
            if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey()) {
                        response->error = Error::ZOK;
                        response->stat.version = kv.version();
                        return response;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperGetResponse>();
        response->error = Error::ZNONODE;
        if (compare_result) {
            for (auto resp: responses)
            {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        LOG_DEBUG(log, "P" << kv.key());
                        if (kv.key() == etcd_key.getFullEtcdKey()) {
                            response->data = kv.value();
                            response->stat = Stat();
                            response->stat.version = kv.version();
                            response->error = Error::ZOK;
                            return response;
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperSetResponse>();
        response->error = Error::ZNONODE;
        if (!compare_result) {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getFullEtcdKey()) {
                            std::cout << "SET BAD VERSION" << std::endl;
                            response->error = Error::ZBADVERSION;
                            return response;
                        }
                    }
                }
            }
        }
        else
        {
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponsePut == resp.response_case())
                {
                    auto put_resp = resp.response_put();
                    if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey()) {
                        response->error = Error::ZOK;
                        response->stat.version = put_resp.prev_kv().version() + 1;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperListResponse>();
        if (!compare_result)
        {
            response->error = Error::ZNONODE;
        }
        else
        {
            response->error = Error::ZOK;
        }
        for (auto resp : responses) {
            if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    LOG_DEBUG(log, "KEY" << kv.key() << "\t" << kv.value());
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        response->stat.version = kv.version();
                    }
                    if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                    {
                        for (auto kv : range_resp.kvs())
                        {
                            std::string child = childName(kv.key(), etcd_key.getChildsPrefix());
                            std::cout << "CHILD" << childName(kv.key(), etcd_key.getChildsPrefix());
                            if (std::find(response->names.begin(), response->names.end(), child) == response->names.end())
                            {
                                response->names.emplace_back(childName(kv.key(), etcd_key.getChildsPrefix()));
                            }
                        }
                        response->error = Error::ZOK;
                        break;
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperCheckResponse>();
        if (compare_result) 
        {
            response->error = Error::ZOK;
        }
        else
        {
            response->error = Error::ZNONODE;
            for (auto resp: responses) {
                if(ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getFullEtcdKey()) {
                            std::cout << kv.version() << "\t" << version << std::endl;
                            if (kv.version() != version)
                            {
                                response->error = Error::ZBADVERSION;
                                return response;
                            }
                        }
                    }
                }
            }
        }
        return response;
    }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> responses)
    {
        auto response = std::make_shared<EtcdKeeperMultiResponse>();
        if (compare_result) {
            response->error = Error::ZOK;
            for (int i; i != etcd_requests.size(); i++) {
                response->responses.push_back(etcd_requests[i]->makeResponseFromResponses(true, responses));
            }
        }
        else
        {
            LOG_DEBUG(log, "MULTI COMP" << etcd_requests.size());
            response->responses.reserve(etcd_requests.size());
            for (int i; i != etcd_requests.size(); i++) {
                LOG_DEBUG(log, "MULTI COMPARE " << i);
                auto resp = etcd_requests[i]->makeResponseFromResponses(false, responses);
                response->responses.push_back(resp);
                if (resp->error != Error::ZOK)
                {
                    response->error = resp->error;
                    return response;
                }
            }
        }
        return response;
    }

    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponse() const { return std::make_shared<EtcdKeeperCreateResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponse() const { return std::make_shared<EtcdKeeperRemoveResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponse() const { return std::make_shared<EtcdKeeperExistsResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponse() const { return std::make_shared<EtcdKeeperGetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponse() const { return std::make_shared<EtcdKeeperSetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponse() const { return std::make_shared<EtcdKeeperListResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponse() const { return std::make_shared<EtcdKeeperCheckResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponse() const { return std::make_shared<EtcdKeeperMultiResponse>(); }
 
 
    EtcdKeeper::EtcdKeeper(const String & root_path_, Poco::Timespan operation_timeout_)
            : root_path(root_path_), operation_timeout(operation_timeout_)
    {
        log = &Logger::get("EtcdKeeper");
        LOG_DEBUG(log, "INIT");

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

        LOG_DEBUG(log, "DESTR");
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

                    std::lock_guard lock(operations_mutex);
                    LOG_DEBUG(log, "ADD XID" << info.request->xid);
                    operations[info.request->xid] = info;

                    if (expired)
                        break;

                    info.request->addRootPath(root_path);
                    info.request->setEtcdKey();

                    if (info.watch)
                    {
                        LOG_DEBUG(log, "WATCH IN REQUEST");
                        bool list_watch = false;
                        if (dynamic_cast<const ListRequest *>(info.request.get())) {
                            list_watch = true;
                        }
                        std::lock_guard lock(watches_mutex);
                        if (list_watch)
                        {
                            list_watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));

                        }
                        else
                        {
                            watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));
                        }
                        callWatchRequest(info.request->getEtcdKey(), list_watch, watch_stub_, watch_cq_);
                    }

                    EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                    call->xid = info.request->xid;

                    info.request->prepareCall();
                    info.request->call(*call, kv_stub_, kv_cq_);
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
                LOG_DEBUG(log, "GOT TAG" << got_tag);

                GPR_ASSERT(ok);
                if (got_tag)
                {
                    auto call = static_cast<AsyncCall*>(got_tag);

                    XID xid = call->xid;

                    LOG_DEBUG(log, "XID COMP" << xid);

                    auto it = operations.find(xid);
                    if (it == operations.end())
                        throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

                    request_info = std::move(it->second);
                    operations.erase(it);

                    if (!call->status.ok())
                    {
                        LOG_DEBUG(log, "RPC FAILED" << call->status.error_message());
                    }
                    else
                    {
                        LOG_DEBUG(log, "READ RPC RESPONSE");

                        if (!request_info.request->callRequired(got_tag))
                        {
                            LOG_DEBUG(log, "NOT RQUERED " << xid);
                            response = request_info.request->makeResponseFromTag(got_tag);
                            if (!response->finished)
                            {
                                request_info.request->clean();
                                if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
                                    throw Exception("Cannot push request to queue within operation timeout", ZOPERATIONTIMEOUT);
                            }
                            else
                            {
                                response->removeRootPath(root_path);
                                if (request_info.callback)
                                {
                                    request_info.callback(*response);
                                }
                            }
                        }
                        else
                        {
                            LOG_DEBUG(log, "RQUERED " << xid);
                            operations[request_info.request->xid] = request_info;
                            EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                            call->xid = request_info.request->xid;
                            request_info.request->prepareCall();
                            request_info.request->call(*call, kv_stub_, kv_cq_);
                        }
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

    void EtcdKeeper::watchCompleteThread() {
        setThreadName("EtcdKeeperWatchComplete");

        void* got_tag;
        bool ok = false;

        try
        {
            while (watch_cq_.Next(&got_tag, &ok))
            {
                if (ok) {
                    LOG_DEBUG(log, "**** Processing completion queue tag ");
                    switch (static_cast<WatchConnType>(reinterpret_cast<long>(got_tag))) {
                    case WatchConnType::READ:
                        LOG_DEBUG(log, "Read a new message.");
                        readWatchResponse();
                        break;
                    case WatchConnType::WRITE:
                        LOG_DEBUG(log, "Sending message (async).");
                        break;
                    case WatchConnType::CONNECT:
                        LOG_DEBUG(log, "Server connected.");
                        break;
                    case WatchConnType::WRITES_DONE:
                        LOG_DEBUG(log, "Server disconnecting.");
                        break;
                    case WatchConnType::FINISH:
                        // std::cout << "Client finish; status = "
                        //         << (finish_status_.ok() ? "ok" : "cancelled")
                        //         << std::endl;
                        context_.TryCancel();
                        watch_cq_.Shutdown();
                        break;
                    default:
                        LOG_ERROR(log, "Unexpected tag ");
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
        LOG_DEBUG(log, "FINALIZE");

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
                LOG_DEBUG(log, "PUSH XID" << next_xid);

                if (info.request->xid < 0)
                    throw Exception("XID overflow", ZSESSIONEXPIRED);
            }

            std::lock_guard lock(push_request_mutex);
 
            if (expired)
                throw Exception("Session expired", ZSESSIONEXPIRED);

            RequestInfo info_copy = info;
 
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
        LOG_DEBUG(log, "_CREATE");
        EtcdKeeperCreateRequest request;
        request.path = path;
        request.data = data;
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
        LOG_DEBUG(log, "_REMOVE");
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
        LOG_DEBUG(log, "_EXISTS");
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
        LOG_DEBUG(log, "_GET");
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
        LOG_DEBUG(log, "_SET");
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
        LOG_DEBUG(log, "_LIST");
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
        LOG_DEBUG(log, "_CHECK " << version);
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
        LOG_DEBUG(log, "_MULTI");
        EtcdKeeperMultiRequest request(requests);
 
        RequestInfo request_info;
        request_info.request = std::make_shared<EtcdKeeperMultiRequest>(std::move(request));
        request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };
        pushRequest(std::move(request_info));
    }
 
}
