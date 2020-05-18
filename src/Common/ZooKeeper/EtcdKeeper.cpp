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


#define KEEP_ALIVE_TTL_S 30


namespace Coordination
{
    Logger * log = nullptr;
    int64_t lease_id = -1;

    enum class BiDiTag
    {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };

    int64_t EtcdKeeper::getSessionID() const { return lease_id; }

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

    static String rangeEnd(const String & key)
    {
        std::string range_end = key;
        int ascii = (int)range_end[range_end.length() - 1];
        range_end.back() = ascii+1;
        return range_end;
    }

    PutRequest preparePutRequest(const std::string & key, const std::string & value, const int64_t lease_id = 0)
    {
        PutRequest request = PutRequest();

        request.set_key(key);
        request.set_value(value);
        request.set_prev_kv(true);
        request.set_lease(lease_id);
        return request;
    }

    RangeRequest prepareRangeRequest(const std::string & key, bool with_prefix=false)
    {
        RangeRequest request = RangeRequest();
        request.set_key(key);
        if (with_prefix)
        {
            request.set_range_end(rangeEnd(key));
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
        const std::string & target,
        const std::string & result,
        int32_t value
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
        if (target == "version")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            compare.set_version(value);
        }
        if (target == "create")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_CREATE;
            compare.set_create_revision(value);
        }
        if (target == "mod")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_MOD;
            compare.set_mod_revision(value);
        }
        if (target == "value")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VALUE;
            compare.set_value(std::to_string(value));
        }
        compare.set_target(compare_target);
        return compare;
    }

    struct EtcdKeeper::TxnRequests
    {
        std::vector<Compare> compares;
        std::vector<RangeRequest> success_ranges;
        std::vector<PutRequest> success_puts;
        std::vector<DeleteRangeRequest> success_delete_ranges;
        std::vector<RangeRequest> failure_ranges;
        std::vector<PutRequest> failure_puts;
        std::vector<DeleteRangeRequest> failure_delete_ranges;
        TxnRequests& operator+=(const TxnRequests& rv)
        {
            this->compares.insert(this->compares.end(), rv.compares.begin(), rv.compares.end());
            this->success_ranges.insert(this->success_ranges.end(), rv.success_ranges.begin(), rv.success_ranges.end());
            this->success_puts.insert(this->success_puts.end(), rv.success_puts.begin(), rv.success_puts.end());
            this->success_delete_ranges.insert(this->success_delete_ranges.end(), rv.success_delete_ranges.begin(), rv.success_delete_ranges.end());
            this->failure_ranges.insert(this->failure_ranges.end(), rv.failure_ranges.begin(), rv.failure_ranges.end());
            this->failure_puts.insert(this->failure_puts.end(), rv.failure_puts.begin(), rv.failure_puts.end());
            this->failure_delete_ranges.insert(this->failure_delete_ranges.end(), rv.failure_delete_ranges.begin(), rv.failure_delete_ranges.end());
            return *this;
        }
        bool empty()
        {
            return (success_ranges.size() + success_puts.size() + success_delete_ranges.size() + failure_ranges.size() + failure_puts.size() + failure_delete_ranges.size()) == 0;
        }
        void clear()
        {
            compares.clear();
            success_ranges.clear();
            success_puts.clear();
            success_delete_ranges.clear();
            failure_ranges.clear();
            failure_puts.clear();
            failure_delete_ranges.clear();
        }
        void take_last_create_request_with_prefix(const String & prefix)
        {
            std::unordered_map<String, String> create_requests;
            for (const auto success_put : success_puts)
            {
                if (startsWith(success_put.key(), prefix))
                {
                    create_requests[success_put.key()] = success_put.value();
                }
            }
            auto it = success_puts.begin();
            while (it != success_puts.end())
            {
                if (startsWith(it->key(), prefix) && it->value() != create_requests[it->key()])
                {
                    it = success_puts.erase(it);
                }
                else
                {
                    it++;
                }
            }
        }
    };

    void callRequest(
        EtcdKeeper::AsyncCall & call_,
        std::unique_ptr<KV::Stub> & stub,
        CompletionQueue & cq,
        EtcdKeeper::TxnRequests & requests)
    {
        TxnRequest txn_request;
        for (auto txn_compare: requests.compares)
        {
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
        call->response_reader = stub->PrepareAsyncTxn(&call->context, txn_request, &cq);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, (void*)call);
    }

    enum class EtcdKeyPrefix
    {
        VALUE,
        SEQUENTIAL,
        CHILDS,
        CTIME,
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
            String prefix = "/zk/value/";
            int32_t prefix_len = prefix.size();
            int32_t slash = full_path.find("/", prefix_len);
            level = std::stoi(full_path.substr(prefix_len, slash - prefix_len));
            zk_path = full_path.substr(slash);
            parent_zk_path = parentPath(zk_path);
        }
        void updateZkPath(const String & new_zk_path)
        {
            zk_path = new_zk_path;
        }
        String generateFullPathFromParts(EtcdKeyPrefix prefix_type, int32_t level, const String & path) const
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
            else if (prefix_type == EtcdKeyPrefix::CTIME)
            {
                prefix = "/ctime/";
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
        String getSequentialCounterKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::SEQUENTIAL, level, zk_path);
        }
        String getFullEtcdKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level, zk_path);
        }
        String getChildsFlagKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::CHILDS, level, zk_path);
        }
        String getCtimeKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::CTIME, level, zk_path);
        }
        String getEphimeralFlagKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_EPHEMERAL, level, zk_path);
        }
        String getSequentialFlagKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_SEQUENTIAL, level, zk_path);
        }
        std::vector<String> getRelatedKeys() const
        {
            return std::vector<String> {getFullEtcdKey(), getSequentialCounterKey(), getChildsFlagKey(), getEphimeralFlagKey(), getSequentialFlagKey()};
        }
        String getChildsPrefix() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level + 1, zk_path);
        }
        String getParentChildsFlagKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::CHILDS, level - 1, parent_zk_path);
        }
        String getParentKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level - 1, parent_zk_path);
        }
        String getParentEphimeralFlagKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::IS_EPHEMERAL, level - 1, parent_zk_path);
        }
        String getParentSequentialCounterKey() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::SEQUENTIAL, level - 1, parent_zk_path);
        }
    };

    void EtcdKeeper::callWatchRequest(
        const std::string & key,
        bool list_watch,
        std::unique_ptr<Watch::Stub> & stub,
        CompletionQueue & cq)
    {
        WatchRequest watch_request;
        etcdserverpb::WatchResponse response;
        WatchCreateRequest create_request;
        create_request.set_key(key);
        if (list_watch)
        {
            create_request.set_range_end(rangeEnd(key));
        }
        watch_request.mutable_create_request()->CopyFrom(create_request);
        watche_write_mutex.lock();
        watch_stream->Write(watch_request, (void*)BiDiTag::WRITE);
    }

    void EtcdKeeper::readWatchResponse()
    {
        watch_stream->Read(&watch_response, (void*)BiDiTag::READ);
        if (watch_response.created())
        {
            /// watch created
        } else if (watch_response.events_size())
        {
            for (auto event : watch_response.events())
            {
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
        }
        else
        {
            LOG_ERROR(log, "Returned etcd watch without created flag and without event.");
        }
    }

    void EtcdKeeper::readLeaseKeepAliveResponse()
    {
        keep_alive_stream->Read(&keep_alive_response, (void*)BiDiTag::READ);
    }

    void EtcdKeeper::requestLease(std::chrono::seconds ttl)
    {
        LeaseGrantRequest lease_request;
        lease_request.set_id(0);
        lease_request.set_ttl(ttl.count());
        
        LeaseGrantResponse lease_response;
        ClientContext lease_grand_context;

        Status status = lease_stub->LeaseGrant(&lease_grand_context, lease_request, &lease_response);

        if (status.ok()) {
            lease_id = lease_response.id();
        } else {
            throw Exception("Cannot get lease ID: " + lease_response.error(), ZRUNTIMEINCONSISTENCY);
        }
    }

    struct EtcdKeeperResponse : virtual Response
    {
        bool finished = true;
        virtual ~EtcdKeeperResponse() {}
    };

    struct EtcdKeeperCreateResponse final : CreateResponse, EtcdKeeperResponse {};
    struct EtcdKeeperRemoveResponse final : RemoveResponse, EtcdKeeperResponse {};
    struct EtcdKeeperGetResponse final : GetResponse, EtcdKeeperResponse {};
    struct EtcdKeeperSetResponse final : SetResponse, EtcdKeeperResponse {};
    struct EtcdKeeperExistsResponse final : ExistsResponse, EtcdKeeperResponse {};
    struct EtcdKeeperListResponse final : ListResponse, EtcdKeeperResponse {};
    struct EtcdKeeperCheckResponse final : CheckResponse, EtcdKeeperResponse {};
    struct EtcdKeeperMultiResponse final : MultiResponse, EtcdKeeperResponse {};

    struct EtcdKeeperRequest : virtual Request
    {
        int32_t retry = 0;
        EtcdKeeper::XID xid = 0;
        bool composite = false;
        EtcdKey etcd_key;
        EtcdKeeper::TxnRequests txn_requests;
        std::vector<ResponseOp> pre_call_responses;
        bool pre_call_called = false;
        bool post_call_called = false;
        EtcdKeeperResponsePtr response;

        virtual EtcdKeeperResponsePtr makeResponse() const = 0;
        virtual void preparePostCall() = 0;
        virtual EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) = 0;
        virtual void preparePreCall() {}
        virtual void checkRequestForComposite() {}
        virtual void setEtcdKey() {}
        virtual bool isMutable() const { return false; }

        String getEtcdKey() const
        {
            return etcd_key.getFullEtcdKey();
        }
        String getChildsPrefix() const
        {
            return etcd_key.getChildsPrefix();
        }
        virtual void clean()
        {
            composite = false;
            pre_call_called = false;
            post_call_called = false;
            response->error = Error::ZOK;
            txn_requests.clear();
            pre_call_responses.clear();
        }
        virtual void call(EtcdKeeper::AsyncCall & call,
            std::unique_ptr<KV::Stub> & kv_stub,
            CompletionQueue & kv_cq)
        {
            retry++;
            if (retry > 120)
            {
                response->error = Error::ZSYSTEMERROR;
                return;
            }
            callRequest(call, kv_stub, kv_cq, txn_requests);
            pre_call_called = true;
            txn_requests.clear();
        }
        EtcdKeeperResponsePtr makeResponseFromExisted()
        {
            return response;
        }
        EtcdKeeperResponsePtr makeResponseFromRepeatedPtrField(bool compare_result, google::protobuf::RepeatedPtrField<ResponseOp> fields)
        {
            std::vector<ResponseOp> responseOps;
            for (auto field : fields)
            {
                responseOps.emplace_back(field);
            }
            return makeResponseFromResponses(compare_result, responseOps);
        }
        EtcdKeeperResponsePtr makeResponseFromTag(void* got_tag)
        {
            if (response->error != Error::ZOK)
            {
                return response;
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
            return makeResponseFromRepeatedPtrField(call->response.succeeded(), call->response.responses());
        }
        bool callRequired(void* got_tag)
        {
            if (response->error == Error::ZOK && composite && !post_call_called)
            {
                EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
                for (auto field : call->response.responses())
                {
                    pre_call_responses.emplace_back(field);
                }
                post_call_called = true;
                return true;
            }
            else
            {
                return false;
            }
        }
        virtual void setResponse()
        {
            response = makeResponse();
            response->error = Error::ZOK;
        }
        virtual void prepareCall()
        {
            setResponse();
            checkRequestForComposite();
            if (composite && !pre_call_called)
            {
                preparePreCall();
                return;
            }
            preparePostCall();
        }
    };

    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        String process_path;
        int32_t seq_num;
        bool parent_exists = false;
        int32_t seq_delta = 1;
        EtcdKeeperCreateRequest() {}
        EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall()
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentEphimeralFlagKey()));
            if (is_sequential)
            {
                txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentSequentialCounterKey()));
            }
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
        }
        void setSequentialNumber(int32_t seq_num_)
        {
            seq_num = seq_num_;
            process_path = path;
            std::stringstream seq_num_str;
            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;
            process_path += seq_num_str.str();
            etcd_key.updateZkPath(process_path);
        }
        void parsePreResponses()
        {
            if (!parent_exists)
            {
                response->error = Error::ZNONODE;
            }
            for (auto resp : pre_call_responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (is_sequential && kv.key() == etcd_key.getParentSequentialCounterKey())
                        {
                            setSequentialNumber(std::stoi(kv.value()));
                        }
                        else if (kv.key() == etcd_key.getParentEphimeralFlagKey())
                        {
                            if (kv.value() != "0")
                            {
                                response->error = Error::ZNOCHILDRENFOREPHEMERALS;
                                return;
                            }
                            response->error = Error::ZOK;
                        }
                        else if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            response->error = Error::ZNODEEXISTS;
                            return;
                        }
                    }
                }
            }
        }
        void preparePostCall()
        {
            process_path = path;
            if (composite)
            {
                parsePreResponses();
            }
            if (is_sequential)
            {
                txn_requests.compares.emplace_back(prepareCompare(etcd_key.getParentSequentialCounterKey(), "value", "equal", seq_num));
                txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentSequentialCounterKey()));
                txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getParentSequentialCounterKey(), std::to_string(seq_num + seq_delta)));
            }
            // TODO add compare for childs flag version
            int64_t cur_lease_id = is_ephemeral ? lease_id : 0;
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getFullEtcdKey(), data, cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getSequentialCounterKey(), std::to_string(0), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getSequentialFlagKey(), std::to_string(is_sequential), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getEphimeralFlagKey(), std::to_string(is_ephemeral), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getChildsFlagKey(), "", cur_lease_id));
            auto time_now = std::chrono::high_resolution_clock::now();
            int64_t time = std::chrono::duration_cast<std::chrono::milliseconds>(time_now.time_since_epoch()).count();
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getCtimeKey(), std::to_string(time), cur_lease_id));
            if (parentPath(path) != "/")
            {
                txn_requests.compares.emplace_back(prepareCompare(etcd_key.getParentKey(), "version", "not_equal", -1));
                txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentKey()));
                txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getParentChildsFlagKey(), "+" + etcd_key.getFullEtcdKey()));
            }
        }
        void checkRequestForComposite()
        {
            if (is_sequential || parentPath(path) != "/")
            {
                composite = true;
            }
            else
            {
                parent_exists = true;
            }
        }
    };

    struct EtcdKeeperRemoveRequest final : RemoveRequest, EtcdKeeperRequest
    {
        int32_t children;
        int child_flag_version = -1;
        EtcdKeeperRemoveRequest() {}
        EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        bool isMutable() const override { return true; }
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall()
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
        void parsePreResponses()
        {
            response->error = Error::ZNONODE;
            for (auto resp : pre_call_responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                        {
                            response->error = Error::ZNOTEMPTY;
                            return;
                        }
                        else if (kv.key() == etcd_key.getChildsFlagKey())
                        {
                            response->error = Error::ZOK;
                            child_flag_version = kv.version();
                        }
                        else if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            response->error = Error::ZOK;
                            if (version != -1 && kv.version() != version)
                            {
                                response->error = Error::ZBADVERSION;
                                return;
                            }
                        }
                    }
                }
            }
        }
        void preparePostCall() override
        {
            parsePreResponses();
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getChildsFlagKey(), "version", "equal", child_flag_version));
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));

            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            for (auto key : etcd_key.getRelatedKeys())
            {
                txn_requests.success_delete_ranges.emplace_back(prepareDeleteRangeRequest(key));
            }
        }
        void checkRequestForComposite()
        {
            composite = true;
        }
    };

    struct EtcdKeeperExistsRequest final : ExistsRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() override
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getCtimeKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
    };

    struct EtcdKeeperGetRequest final : GetRequest, EtcdKeeperRequest
    {
        EtcdKeeperGetRequest() {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() override
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getCtimeKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
    };

    struct EtcdKeeperSetRequest final : SetRequest, EtcdKeeperRequest
    {
        EtcdKeeperSetRequest() {}
        EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        bool isMutable() const override { return true; }
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() override
        {
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getCtimeKey()));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getCtimeKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getFullEtcdKey(), data));
        }
    };

    struct EtcdKeeperListRequest final : ListRequest, EtcdKeeperRequest
    {
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() override
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getCtimeKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
    };

    struct EtcdKeeperCheckRequest final : CheckRequest, EtcdKeeperRequest
    {
        EtcdKeeperCheckRequest() {}
        EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePostCall() override
        {
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
        }
    };

    struct EtcdKeeperMultiRequest final : MultiRequest, EtcdKeeperRequest
    {
        std::unordered_set<String> checking_etcd_keys;
        EtcdKeeperRequests etcd_requests;
        std::unordered_map<String, int32_t> sequential_keys_map;
        std::unordered_map<String, int32_t> multiple_sequential_keys_map;
        EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            etcd_requests.reserve(generic_requests.size());
            std::unordered_set<String> created_keys;
            std::unordered_map<String, int> create_requests_count;
            std::unordered_map<String, int> remove_requests_count;
            std::unordered_set<String> required_keys;
            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    created_keys.insert(concrete_request_create->path);
                    create_requests_count[concrete_request_create->path]++;
                    if (concrete_request_create->is_sequential)
                    {
                        sequential_keys_map[concrete_request_create->path]++;
                    }
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    remove_requests_count[concrete_request_remove->path]++;
                }
            }

            for (auto it = sequential_keys_map.begin(); it != sequential_keys_map.end(); it++)
            {
                if (it->second > 1)
                {
                    multiple_sequential_keys_map[it->first] = -1;
                }
            }

            std::unordered_map<String, bool> create_request_to_skip, remove_request_to_skip;
            for (auto it = create_requests_count.begin(); it != create_requests_count.end(); it++)
            {
                String cur_path = it->first;
                if (create_requests_count[cur_path] > 0 && remove_requests_count[cur_path] > 0)
                {
                    required_keys.insert(cur_path);
                    create_request_to_skip[cur_path] = true;
                    remove_request_to_skip[cur_path] = true;
                }
            }

            for (const auto & generic_request : generic_requests)
            {
                if (auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    auto current_create_request = std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create);
                    String cur_path = current_create_request->path;
                    if (required_keys.count(cur_path) && create_request_to_skip[cur_path])
                    {
                        create_request_to_skip[cur_path] = false;
                        continue;
                    }
                    else if (multiple_sequential_keys_map[cur_path] == -1)
                    {
                        current_create_request->seq_delta = sequential_keys_map[cur_path];
                        multiple_sequential_keys_map[cur_path]++;
                    }
                    else if (sequential_keys_map[cur_path] > 1)
                    {
                        continue;
                    }
                    if (created_keys.count(parentPath(cur_path)) > 0)
                    {
                        current_create_request->parent_exists = true;
                    }
                    etcd_requests.emplace_back(current_create_request);
                }
                else if (auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    String cur_path = concrete_request_remove->path;
                    if (required_keys.count(cur_path) && remove_request_to_skip[cur_path])
                    {
                        remove_request_to_skip[cur_path] = false;
                        continue;
                    }
                    etcd_requests.emplace_back(std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove));
                }
                else if (auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    etcd_requests.emplace_back(std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set));
                }
                else if (auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    etcd_requests.emplace_back(std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check));
                }
                else
                {
                    throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
                }
            }
            if (required_keys.size())
            {
                for (auto key : required_keys)
                {
                    EtcdKey ek = EtcdKey(key);
                    checking_etcd_keys.insert(ek.getFullEtcdKey());
                }
            }
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void clean() override
        {
            composite = false;
            pre_call_called = false;
            post_call_called = false;
            response->error = Error::ZOK;
            txn_requests.clear();
            pre_call_responses.clear();
            for (int i = 0; i != etcd_requests.size(); i++)
            {
                etcd_requests[i]->clean();
            }
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
            for (auto request : etcd_requests)
            {
                request->checkRequestForComposite();
                if (request->composite)
                {
                    request->preparePreCall();
                    txn_requests += request->txn_requests;
                }
            }
            if (checking_etcd_keys.size())
            {
                for (auto key : checking_etcd_keys)
                {
                    txn_requests.success_ranges.emplace_back(prepareRangeRequest(key));
                }
            }
        }
        void parsePreResponses()
        {
            if (checking_etcd_keys.size())
            {
                for (auto resp : pre_call_responses)
                {
                    if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                    {
                        auto range_resp = resp.response_range();
                        for (auto kv : range_resp.kvs())
                        {
                            if (checking_etcd_keys.count(kv.key()))
                            {
                                EtcdKeeperMultiResponse multi_response;
                                multi_response.error = Error::ZNODEEXISTS;
                                std::shared_ptr<EtcdKeeperCreateResponse> create_resp = std::make_shared<EtcdKeeperCreateResponse>();
                                create_resp->error = Error::ZNODEEXISTS;
                                multi_response.responses.emplace_back(create_resp);
                                response = std::make_shared<EtcdKeeperMultiResponse>(multi_response);
                                return;
                            }
                        }
                    }
                }
            }
            for (auto request : etcd_requests)
            {
                request->txn_requests.clear();
                request->pre_call_responses = pre_call_responses;
                request->preparePostCall();
                if (request->response->error != Error::ZOK)
                {
                    EtcdKeeperMultiResponse multi_response;
                    multi_response.error = request->response->error;
                    for (auto request : etcd_requests)
                    {
                        multi_response.responses.emplace_back(request->makeResponseFromExisted());
                    }
                    response = std::make_shared<EtcdKeeperMultiResponse>(multi_response);
                    return;
                }
                txn_requests += request->txn_requests;
            }
        }
        void preparePostCall() override
        {
            parsePreResponses();
            txn_requests.take_last_create_request_with_prefix("/zk/childs");
        }
        void checkRequestForComposite()
        {
            if (!txn_requests.empty())
            {
                composite = true;
            }
        }
        void prepareCall()
        {
            setResponse();
            response->error = Error::ZOK;
            for (auto request : etcd_requests)
            {
                request->setResponse();
                request->response->error = Error::ZOK;
            }
            if (!pre_call_called)
            {
                preparePreCall();
            }
            checkRequestForComposite();
            if (composite && !pre_call_called)
            {
                return;
            }
            preparePostCall();
        }
        void setResponse() override
        {
            response = makeResponse();
            response->error = Error::ZOK;
            for (auto request : etcd_requests)
            {
                request->setResponse();
            }
        }
    };

    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperCreateResponse create_response;
        if (!compare_result)
        {
            bool parent_exists_ = false;
            if (parentPath(path) == "/")
            {
                parent_exists_ = true;
            }
            bool seq_num_matched = false;
            if (!is_sequential)
            {
                seq_num_matched = true;
            }
            for (auto resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getParentKey())
                        {
                            parent_exists_ = true;
                        }
                        else if (kv.key() == etcd_key.getParentSequentialCounterKey() && kv.value() == std::to_string(seq_num))
                        {
                            seq_num_matched = true;
                        }
                    }
                }
            }
            if (!parent_exists_)
            {
                create_response.error = Error::ZNONODE;
                return;
            }
            if (!seq_num_matched)
            {
                create_response.finished = false;
            }
            create_response.path_created = path;
            if (!create_response.finished)
            {
                LOG_DEBUG(log, "Create request for path: " << process_path << " does not finished.");
            }
        }
        else
        {
            create_response.path_created = process_path;
            for (auto resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponsePut == resp.response_case())
                {
                    auto put_resp = resp.response_put();
                    if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey())
                    {
                        create_response.error = Error::ZSYSTEMERROR;
                    }
                    else
                    {
                        create_response.error = Error::ZOK;
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperCreateResponse>(create_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperRemoveResponse remove_response;
        if (!compare_result)
        {
            remove_response.error = Error::ZBADVERSION;
            for (auto resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getChildsFlagKey())
                        {
                            if (kv.version() != child_flag_version)
                            {
                                remove_response.finished = false;
                            }
                            break;
                        }
                    }
                }
            }
        }
        else
        {
            for (auto resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseDeleteRange == resp.response_case())
                {
                    auto delete_range_resp = resp.response_delete_range();
                    if (delete_range_resp.deleted())
                    {
                        for (auto kv : delete_range_resp.prev_kvs())
                        {
                            if (kv.key() == etcd_key.getFullEtcdKey())
                            {
                                remove_response.error = Error::ZOK;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperRemoveResponse>(remove_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperExistsResponse exists_response;
        exists_response.error = Error::ZNONODE;
        for (auto resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        exists_response.error = Error::ZOK;
                        exists_response.stat.version = kv.version();
                        exists_response.stat.ephemeralOwner = kv.lease();
                    }
                    else if (kv.key() == etcd_key.getCtimeKey())
                    {
                        exists_response.stat.ctime = std::stoll(kv.value());
                    }
                    else if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                    {
                        exists_response.stat.numChildren = range_resp.count();
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperExistsResponse>(exists_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperGetResponse get_response;
        get_response.error = Error::ZNONODE;
        if (compare_result)
        {
            for (auto resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    auto range_resp = resp.response_range();
                    for (auto kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            get_response.data = kv.value();
                            get_response.stat.version = kv.version();
                            get_response.stat.ephemeralOwner = kv.lease();
                            get_response.error = Error::ZOK;
                        }
                        else if (kv.key() == etcd_key.getCtimeKey())
                        {
                            get_response.stat.ctime = std::stoll(kv.value());
                        }
                        else if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                        {
                            get_response.stat.numChildren = range_resp.count();
                        }
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperGetResponse>(get_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperSetResponse set_response;
        if (!compare_result)
        {
            set_response.error = Error::ZNONODE;
        }
        for (auto resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponsePut == resp.response_case())
            {
                auto put_resp = resp.response_put();
                if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey())
                {
                    set_response.error = Error::ZOK;
                    set_response.stat.version = put_resp.prev_kv().version() + 1;
                    set_response.stat.ephemeralOwner = put_resp.prev_kv().lease();
                }
                
            }
            else if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        if (version != -1 && kv.version() != version)
                        {
                            set_response.error = Error::ZBADVERSION;
                            break;
                        }
                    }
                    else if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                    {
                        set_response.stat.numChildren = range_resp.count();
                    }
                    else if (kv.key() == etcd_key.getCtimeKey())
                    {
                        set_response.stat.ctime = std::stoll(kv.value());
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperSetResponse>(set_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperListResponse list_response;
        list_response.error = Error::ZNONODE;
        for (auto resp : responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        list_response.error = Error::ZOK;
                        list_response.stat.version = kv.version();
                        list_response.stat.ephemeralOwner = kv.lease();
                    }
                    else if (kv.key() == etcd_key.getCtimeKey())
                    {
                        list_response.stat.ctime = std::stoll(kv.value());
                    }
                    else if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                    {
                        list_response.stat.numChildren = range_resp.count();
                        for (auto kv : range_resp.kvs())
                        {
                            list_response.names.emplace_back(childName(kv.key(), etcd_key.getChildsPrefix()));
                        }
                        list_response.error = Error::ZOK;
                        break;
                    }

                }
            }
        }
        return std::make_shared<EtcdKeeperListResponse>(list_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperCheckResponse check_response;
        check_response.error = Error::ZNONODE;
        for (auto resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                auto range_resp = resp.response_range();
                for (auto kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        check_response.error = Error::ZOK;
                        if (version != -1 && kv.version() != version)
                        {
                            check_response.error = Error::ZBADVERSION;
                            return response;
                        }
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperCheckResponse>(check_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses)
    {
        EtcdKeeperMultiResponse multi_response;
        for (auto key : checking_etcd_keys)
        {
            std::shared_ptr<EtcdKeeperCreateResponse> create_resp = std::make_shared<EtcdKeeperCreateResponse>();
            create_resp->path_created = key;
            multi_response.responses.emplace_back(create_resp);
            std::shared_ptr<EtcdKeeperRemoveResponse> remove_resp = std::make_shared<EtcdKeeperRemoveResponse>();
            multi_response.responses.emplace_back(remove_resp);
        }
        if (compare_result)
        {
            multi_response.error = Error::ZOK;
            for (int i; i != etcd_requests.size(); i++)
            {
                if (auto * etcd_request = dynamic_cast<EtcdKeeperCreateRequest *>(etcd_requests[i].get()))
                {
                    if (sequential_keys_map[etcd_request->path] > 1)
                    {
                        auto * cur_resp = dynamic_cast<const EtcdKeeperCreateResponse *>(etcd_request->makeResponseFromResponses(true, responses).get());
                        int32_t cur_seq_num = etcd_request->seq_num;
                        for (int i = 0; i != sequential_keys_map[etcd_request->path]; i++)
                        {
                            auto added_resp = std::make_shared<EtcdKeeperCreateResponse>(*cur_resp);
                            etcd_request->setSequentialNumber(cur_seq_num + i);
                            added_resp->path_created = etcd_request->process_path;
                            multi_response.responses.emplace_back(added_resp);
                        }
                        continue;
                    }
                }
                multi_response.responses.emplace_back(etcd_requests[i]->makeResponseFromResponses(true, responses));
            }
        }
        else
        {
            multi_response.error = Error::ZSYSTEMERROR;
            multi_response.responses.reserve(etcd_requests.size());
            for (int i; i != etcd_requests.size(); i++)
            {
                auto resp = etcd_requests[i]->makeResponseFromResponses(false, responses);
                multi_response.finished &= resp->finished;
                multi_response.responses.emplace_back(resp);
                if (resp->error != Error::ZOK)
                {
                    multi_response.error = resp->error;
                    break;
                }
            }
        }
        return std::make_shared<EtcdKeeperMultiResponse>(multi_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperCreateRequest::makeResponse() const { return std::make_shared<EtcdKeeperCreateResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperRemoveRequest::makeResponse() const { return std::make_shared<EtcdKeeperRemoveResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponse() const { return std::make_shared<EtcdKeeperExistsResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperGetRequest::makeResponse() const { return std::make_shared<EtcdKeeperGetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperSetRequest::makeResponse() const { return std::make_shared<EtcdKeeperSetResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponse() const { return std::make_shared<EtcdKeeperListResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponse() const { return std::make_shared<EtcdKeeperCheckResponse>(); }
    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponse() const { return std::make_shared<EtcdKeeperMultiResponse>(); }

    EtcdKeeper::EtcdKeeper(const String & root_path_, const String & host_, Poco::Timespan operation_timeout_)
            : root_path(root_path_), host(host_), operation_timeout(operation_timeout_)
    {
        log = &Logger::get("EtcdKeeper");

        std::shared_ptr<Channel> channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        kv_stub = KV::NewStub(channel);

        std::shared_ptr<Channel> watch_channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        watch_stub = Watch::NewStub(watch_channel);

        watch_stream = watch_stub->AsyncWatch(&watch_context, &watch_cq, (void*)BiDiTag::CONNECT);
        readWatchResponse();

        std::shared_ptr<Channel> lease_channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        lease_stub = Lease::NewStub(lease_channel);

        requestLease(std::chrono::seconds(KEEP_ALIVE_TTL_S));

        keep_alive_stream = lease_stub->AsyncLeaseKeepAlive(&keep_alive_context, &lease_cq, (void*)BiDiTag::CONNECT);
        readLeaseKeepAliveResponse();

        if (!root_path.empty())
        {
            if (root_path.back() == '/')
                root_path.pop_back();
        }

        call_thread = ThreadFromGlobalPool([this] { callThread(); });
        complete_thread = ThreadFromGlobalPool([this] { completeThread(); });
        watch_complete_thread = ThreadFromGlobalPool([this] { watchCompleteThread(); });
        lease_complete_thread = ThreadFromGlobalPool([this] { leaseCompleteThread(); });
    }

    EtcdKeeper::~EtcdKeeper()
    {

        LOG_DEBUG(log, "EtcdKeeper destructor");
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

        auto prev_heartbeat_time = clock::now();

        try
        {
            while (!expired)
            {
                auto now = clock::now();
                auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(KEEP_ALIVE_TTL_S * 1000 / 3);
                if (next_heartbeat_time > now)
                {
                    RequestInfo info;
                    UInt64 max_wait = UInt64(operation_timeout.totalMilliseconds());
                    if (requests_queue.tryPop(info, max_wait))
                    {

                        {
                            std::lock_guard lock(operations_mutex);
                            operations[info.request->xid] = info;
                        }

                        if (expired)
                            break;

                        info.request->addRootPath(root_path);
                        info.request->setEtcdKey();

                        if (info.watch)
                        {
                            bool list_watch = false;
                            if (dynamic_cast<const ListRequest *>(info.request.get()))
                            {
                                list_watch = true;
                            }
                            std::lock_guard lock(watches_mutex);
                            if (list_watch)
                            {
                                list_watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));
                                callWatchRequest(info.request->getChildsPrefix(), true, watch_stub, watch_cq);
                            }
                            else
                            {
                                watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));
                                callWatchRequest(info.request->getEtcdKey(), false, watch_stub, watch_cq);
                            }
                        }

                        EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                        call->xid = info.request->xid;

                        info.request->prepareCall();
                        info.request->call(*call, kv_stub, kv_cq);
                    }
                }
                else
                {
                    prev_heartbeat_time = clock::now();
                    LeaseKeepAliveRequest keep_alive_request;
                    keep_alive_request.set_id(lease_id);
                    keep_alive_stream->Write(keep_alive_request, (void*)BiDiTag::WRITE);
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            finalize();
        }
    }

    void EtcdKeeper::completeThread()
    {
        setThreadName("EtcdKeeperComplete");

        try
        {
            RequestInfo request_info;
            EtcdKeeperResponsePtr response;

            void* got_tag;
            bool ok = false;

            while (kv_cq.Next(&got_tag, &ok))
            {
                GPR_ASSERT(ok);
                if (got_tag)
                {
                    auto call = static_cast<AsyncCall*>(got_tag);

                    XID xid = call->xid;
                    {
                        std::lock_guard lock(operations_mutex);
                        auto it = operations.find(xid);
                        if (it == operations.end())
                            throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

                        request_info = std::move(it->second);
                        operations.erase(it);
                    }

                    if (!call->status.ok())
                    {
                        LOG_ERROR(log, "RPC FAILED: " << call->status.error_message());
                    }
                    else
                    {
                        if (!request_info.request->callRequired(got_tag))
                        {
                            response = request_info.request->makeResponseFromTag(got_tag);
                            if (!response->finished)
                            {
                                request_info.request->clean();
                                {
                                    std::lock_guard lock(push_request_mutex);
                                    if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
                                        throw Exception("Cannot push request to queue within operation timeout", ZOPERATIONTIMEOUT);
                                }
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
                            std::lock_guard lock(operations_mutex);
                            operations[request_info.request->xid] = request_info;
                            EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                            call->xid = request_info.request->xid;
                            request_info.request->prepareCall();
                            request_info.request->call(*call, kv_stub, kv_cq);
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

    void EtcdKeeper::watchCompleteThread()
    {
        setThreadName("EtcdKeeperWatchComplete");

        void* got_tag;
        bool ok = false;

        try
        {
            while (watch_cq.Next(&got_tag, &ok))
            {
                if (ok)
                {
                    switch (static_cast<BiDiTag>(reinterpret_cast<long>(got_tag)))
                    {
                    case BiDiTag::READ:
                        readWatchResponse();
                        break;
                    case BiDiTag::WRITE:
                        watche_write_mutex.unlock();
                        break;
                    case BiDiTag::CONNECT:
                        break;
                    case BiDiTag::WRITES_DONE:
                        break;
                    case BiDiTag::FINISH:
                        watch_stream = watch_stub->AsyncWatch(&watch_context, &watch_cq, (void*)BiDiTag::CONNECT);
                        break;
                    default:
                        LOG_ERROR(log, "Unknown BiDiTag.");
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

    void EtcdKeeper::leaseCompleteThread()
    {
        setThreadName("EtcdKeeperLeaseComplete");

        void* got_tag;
        bool ok = false;

        try
        {
            while (lease_cq.Next(&got_tag, &ok))
            {
                if (ok)
                {
                    switch (static_cast<BiDiTag>(reinterpret_cast<long>(got_tag)))
                    {
                    case BiDiTag::READ:
                        readLeaseKeepAliveResponse();
                        break;
                    case BiDiTag::WRITE:
                        break;
                    case BiDiTag::CONNECT:
                        break;
                    case BiDiTag::WRITES_DONE:
                        break;
                    case BiDiTag::FINISH:
                        keep_alive_stream = lease_stub->AsyncLeaseKeepAlive(&keep_alive_context, &lease_cq, (void*)BiDiTag::CONNECT);
                        break;
                    default:
                        LOG_ERROR(log, "Unknown BiDiTag.");
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
        LOG_DEBUG(log, "Finalize EtcdKeeper");

        {
            std::lock_guard lock(push_request_mutex);

            if (expired)
                return;
            expired = true;
        }

        call_thread.join();
        complete_thread.join();
        watch_complete_thread.join();

        LeaseRevokeRequest revoke_request;
        LeaseRevokeResponse revoke_response;
        ClientContext lease_revoke_context;
        revoke_request.set_id(lease_id);
        lease_stub->LeaseRevoke(&lease_revoke_context, revoke_request, &revoke_response);

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
