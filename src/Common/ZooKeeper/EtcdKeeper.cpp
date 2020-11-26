#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_GRPC

#include <boost/algorithm/string.hpp>

#include <Common/ZooKeeper/EtcdKeeper.h>
#include <Common/setThreadName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Core/Types.h>

#include <sstream>
#include <iomanip>
#include <iostream>
#include <unordered_map>


#define KEEP_ALIVE_TTL_S 30


namespace Coordination
{
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

    static String childName(const String & raw_child_path, const String & path)
    {
        String child_path = raw_child_path.substr(path.length());
        auto slash_pos = child_path.find('/');
        return child_path.substr(0, slash_pos);
    }

    static String rangeEnd(const String & key)
    {
        std::string range_end = key;
        int ascii = static_cast<int> (static_cast<unsigned char> (range_end[range_end.length() - 1]));
        range_end.back() = ascii+1;
        return range_end;
    }

    PutRequest preparePutRequest(const std::string & key, const std::string & value, const int64_t lease_id_ = 0)
    {
        PutRequest request = PutRequest();

        request.set_key(key);
        request.set_value(value);
        request.set_prev_kv(true);
        request.set_lease(lease_id_);
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
        else
        {
            throw Exception("Unknown txn result.", Coordination::Error::ZRUNTIMEINCONSISTENCY);
        }
        compare.set_result(compare_result);
        Compare::CompareTarget compare_target;
        if (target == "version")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VERSION;
            compare.set_version(value);
        }
        else if (target == "create")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_CREATE;
            compare.set_create_revision(value);
        }
        else if (target == "mod")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_MOD;
            compare.set_mod_revision(value);
        }
        else if (target == "value")
        {
            compare_target = Compare::CompareTarget::Compare_CompareTarget_VALUE;
            compare.set_value(std::to_string(value));
        }
        else
        {
            throw Exception("Unknown txn target.", Coordination::Error::ZRUNTIMEINCONSISTENCY);
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
        bool empty() const
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
        void takeLastCreateRequestWithPrefix(const String & prefix)
        {
            std::unordered_map<String, String> create_requests;
            for (const auto & success_put : success_puts)
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
        for (const auto & txn_compare: requests.compares)
        {
            Compare* compare = txn_request.add_compare();
            compare->CopyFrom(txn_compare);
        }
        for (const auto & success_range: requests.success_ranges)
        {
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_range(std::make_unique<RangeRequest>(success_range).release());
        }
        for (const auto & success_put: requests.success_puts)
        {
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_put(std::make_unique<PutRequest>(success_put).release());
        }
        for (const auto & success_delete_range: requests.success_delete_ranges)
        {
            RequestOp* req_success = txn_request.add_success();
            req_success->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(success_delete_range).release());
        }
        for (const auto & failure_range: requests.failure_ranges)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_range(std::make_unique<RangeRequest>(failure_range).release());
        }
        for (const auto & failure_put: requests.failure_puts)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_put(std::make_unique<PutRequest>(failure_put).release());
        }
        for (const auto & failure_delete_range: requests.failure_delete_ranges)
        {
            RequestOp* req_failure = txn_request.add_failure();
            req_failure->set_allocated_request_delete_range(std::make_unique<DeleteRangeRequest>(failure_delete_range).release());
        }

        EtcdKeeper::AsyncTxnCall* call = new EtcdKeeper::AsyncTxnCall(call_);
        call->response_reader = stub->PrepareAsyncTxn(&call->context, txn_request, &cq);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->response, &call->status, static_cast<void*> (call));
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
        EtcdKey() = default;
        explicit EtcdKey(const String & path)
        {
            zk_path = path;
            level = std::count(zk_path.begin(), zk_path.end(), '/');
            parent_zk_path = parentPath(zk_path);
        }
        void setFullPath(const String & full_path)
        {
            String prefix = "/zk/value/";
            int32_t prefix_len = prefix.size();
            int32_t slash = full_path.find('/', prefix_len);
            level = std::stoi(full_path.substr(prefix_len, slash - prefix_len));
            zk_path = full_path.substr(slash);
            parent_zk_path = parentPath(zk_path);
        }
        void updateZkPath(const String & new_zk_path)
        {
            zk_path = new_zk_path;
        }
        static String generateFullPathFromParts(EtcdKeyPrefix prefix_type, int32_t level_, const String & path)
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
            return "/zk" + prefix + std::to_string(level_) + path;
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
            return std::vector<String> {getFullEtcdKey(), getSequentialCounterKey(), getChildsFlagKey(), getEphimeralFlagKey(), getSequentialFlagKey(), getCtimeKey()};
        }
        String getChildsPrefix() const
        {
            return generateFullPathFromParts(EtcdKeyPrefix::VALUE, level + 1, zk_path + "/");
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
        bool list_watch)
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
        watch_stream->Write(watch_request, reinterpret_cast<void*> (BiDiTag::WRITE));
    }

    void EtcdKeeper::readWatchResponse()
    {
        watch_stream->Read(&watch_response, reinterpret_cast<void*> (BiDiTag::READ));
        if (watch_response.created())
        {
            /// watch created
        } else if (watch_response.events_size())
        {
            for (const auto & event : watch_response.events())
            {
                std::lock_guard lock(watches_mutex);
                String path = event.kv().key();
                EtcdKey etcd_key;
                etcd_key.setFullPath(path);
                WatchResponse cur_watch_response;
                cur_watch_response.path = path;

                auto it = watches.find(cur_watch_response.path);
                if (it != watches.end())
                {
                    for (auto & callback : it->second)
                        if (callback)
                            callback(cur_watch_response);

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
    }

    void EtcdKeeper::readLeaseKeepAliveResponse()
    {
        keep_alive_stream->Read(&keep_alive_response, reinterpret_cast<void*> (BiDiTag::READ));
    }

    void EtcdKeeper::requestLease(std::chrono::seconds ttl)
    {
        LeaseGrantRequest lease_request;
        lease_request.set_id(0);
        lease_request.set_ttl(ttl.count());
        LeaseGrantResponse lease_response;
        ClientContext lease_grand_context;

        Status status = lease_stub->LeaseGrant(&lease_grand_context, lease_request, &lease_response);

        if (status.ok())
        {
            lease_id = lease_response.id();
        }
        else
        {
            throw Exception("Cannot get lease ID: " + lease_response.error(), Coordination::Error::ZRUNTIMEINCONSISTENCY);
        }
    }

    struct EtcdKeeperResponse : virtual Response
    {
        bool finished = true;
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
        virtual void clear()
        {
            composite = false;
            pre_call_called = false;
            post_call_called = false;
            response = makeResponse();
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
        }
        EtcdKeeperResponsePtr makeResponseFromExisted() const
        {
            return response;
        }
        EtcdKeeperResponsePtr makeResponseFromRepeatedPtrField(bool compare_result, google::protobuf::RepeatedPtrField<ResponseOp> fields)
        {
            std::vector<ResponseOp> response_ops;
            for (auto & field : fields)
            {
                response_ops.emplace_back(field);
            }
            return makeResponseFromResponses(compare_result, response_ops);
        }
        EtcdKeeperResponsePtr makeResponseFromTag(void* got_tag)
        {
            if (response->error != Error::ZOK)
            {
                return response;
            }
            EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
            return makeResponseFromRepeatedPtrField(call->response.succeeded(), call->response.responses());
        }
        virtual void parsePreResponses() {}
        bool callRequired(void* got_tag)
        {
            if (composite && !post_call_called)
            {
                EtcdKeeper::AsyncTxnCall* call = static_cast<EtcdKeeper::AsyncTxnCall*>(got_tag);
                for (const auto & field : call->response.responses())
                {
                    pre_call_responses.emplace_back(field);
                }
                post_call_called = true;
                parsePreResponses();
                return response->error == Error::ZOK;
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
                txn_requests.clear();
                preparePreCall();
                return;
            }
            txn_requests.clear();
            preparePostCall();
        }
    };

    struct EtcdKeeperCreateRequest final : CreateRequest, EtcdKeeperRequest
    {
        String process_path;
        int32_t seq_num;
        bool parent_exists = false;
        bool force_parent_exists = false;
        bool seq_path_exists = false;
        int32_t seq_delta = 1;
        std::vector<String> seq_paths;
        EtcdKeeperCreateRequest() = default;
        explicit EtcdKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall() override
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
            seq_paths.clear();
            seq_num = seq_num_;
            for (int i = 0; i != seq_delta; i++)
            {
                String seq_path = path;
                std::stringstream seq_num_str;
                seq_num_str << std::setw(10) << std::setfill('0') << seq_num + i;
                seq_path += seq_num_str.str();
                seq_paths.emplace_back(seq_path);
            }
            process_path = seq_paths[0];
            etcd_key.updateZkPath(process_path);
        }
        void parsePreResponses() override
        {
            process_path = path;
            if (!composite)
            {
                return;
            }
            if (!parent_exists && !force_parent_exists)
            {
                response->error = Error::ZNONODE;
            }
            for (auto & resp : pre_call_responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    const auto & range_resp = resp.response_range();
                    for (const auto & kv : range_resp.kvs())
                    {
                        if (is_sequential && kv.key() == etcd_key.getParentSequentialCounterKey())
                        {
                            seq_path_exists = true;
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
        void addPutsForEtcdkey(const EtcdKey & etcd_key_)
        {
            // TODO add compare for childs flag version
            // int64_t cur_lease_id = is_ephemeral ? lease_id : 0;
            int64_t cur_lease_id = 0;
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getFullEtcdKey(), data, cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getSequentialCounterKey(), std::to_string(0), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getSequentialFlagKey(), std::to_string(is_sequential), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getEphimeralFlagKey(), std::to_string(is_ephemeral), cur_lease_id));
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getChildsFlagKey(), "", cur_lease_id));
            auto time_now = std::chrono::high_resolution_clock::now();
            int64_t time = std::chrono::duration_cast<std::chrono::milliseconds>(time_now.time_since_epoch()).count();
            txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key_.getCtimeKey(), std::to_string(time), cur_lease_id));
        }
        void preparePostCall() override
        {
            if (is_sequential)
            {
                txn_requests.compares.emplace_back(prepareCompare(etcd_key.getParentSequentialCounterKey(), "value", "equal", seq_num));
                txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentSequentialCounterKey()));
                txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getParentSequentialCounterKey(), std::to_string(seq_num + seq_delta)));
                for (auto & seq_path : seq_paths)
                {
                    EtcdKey seq_etcd_key(seq_path);
                    addPutsForEtcdkey(seq_etcd_key);
                }
            }
            else
            {
                addPutsForEtcdkey(etcd_key);
            }
            if (parentPath(path) != "/")
            {
                txn_requests.compares.emplace_back(prepareCompare(etcd_key.getParentKey(), "version", "not_equal", -1));
                txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getParentKey()));
                txn_requests.success_puts.emplace_back(preparePutRequest(etcd_key.getParentChildsFlagKey(), "+" + etcd_key.getFullEtcdKey()));
            }
        }
        void checkRequestForComposite() override
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
        int removing_childs_count = 0;
        EtcdKeeperRemoveRequest() = default;
        explicit EtcdKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        bool isMutable() const override { return true; }
        void setEtcdKey() override
        {
            etcd_key = EtcdKey(path);
        }
        void preparePreCall() override
        {
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));
            txn_requests.success_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsPrefix(), true));
        }
        void parsePreResponses() override
        {
            response->error = Error::ZNONODE;
            for (auto & resp : pre_call_responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    const auto & range_resp = resp.response_range();
                    for (const auto & kv : range_resp.kvs())
                    {
                        std::cout << "XID " << xid << "KEY " << kv.key();
                        if (startsWith(kv.key(), etcd_key.getChildsPrefix()))
                        {
                            if (range_resp.count() > removing_childs_count)
                            {
                                response->error = Error::ZNOTEMPTY;
                                return;
                            }
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
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getChildsFlagKey(), "version", "equal", child_flag_version));
            txn_requests.compares.emplace_back(prepareCompare(etcd_key.getFullEtcdKey(), "version", version == -1 ? "not_equal" : "equal", version));

            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getChildsFlagKey()));
            txn_requests.failure_ranges.emplace_back(prepareRangeRequest(etcd_key.getFullEtcdKey()));
            for (auto & key : etcd_key.getRelatedKeys())
            {
                txn_requests.success_delete_ranges.emplace_back(prepareDeleteRangeRequest(key));
            }
        }
        void checkRequestForComposite() override
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
        EtcdKeeperGetRequest() = default;
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
        EtcdKeeperSetRequest() = default;
        explicit EtcdKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}
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
        EtcdKeeperCheckRequest() = default;
        explicit EtcdKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}
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
        std::vector<ResponsePtr> responses;
        std::vector<bool> processed_responses;
        std::vector<bool> faked_responses;
        bool failed_compare_requered = false;
        explicit EtcdKeeperMultiRequest(const Requests & generic_requests)
        {
            etcd_requests.reserve(generic_requests.size());
            faked_responses.assign(generic_requests.size(), false);
            processed_responses.assign(generic_requests.size(), false);
            std::unordered_set<String> created_keys;
            std::unordered_map<String, int> create_requests_count;
            std::unordered_map<String, int> remove_requests_count;
            std::unordered_map<String, int> removing_childs;
            std::unordered_set<String> required_keys;
            for (const auto & generic_request : generic_requests)
            {
                if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    created_keys.insert(concrete_request_create->path);
                    create_requests_count[concrete_request_create->path]++;
                    if (concrete_request_create->is_sequential)
                    {
                        sequential_keys_map[concrete_request_create->path]++;
                    }
                }
                else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    remove_requests_count[concrete_request_remove->path]++;
                    removing_childs[parentPath(concrete_request_remove->path)]++;
                }
            }

            for (auto && [first,second] : sequential_keys_map)
            {
                if (second > 1)
                {
                    multiple_sequential_keys_map[first] = -1;
                }
            }

            std::unordered_map<String, bool> create_request_to_skip, remove_request_to_skip;
            for (auto && [first,second] : create_requests_count)
            {
                String cur_path = first;
                if (create_requests_count[cur_path] > 0 && remove_requests_count[cur_path] > 0)
                {
                    required_keys.insert(cur_path);
                    create_request_to_skip[cur_path] = true;
                    remove_request_to_skip[cur_path] = true;
                }
            }

            int i = -1;
            for (const auto & generic_request : generic_requests)
            {
                i++;
                if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
                {
                    const auto & current_create_request = std::make_shared<EtcdKeeperCreateRequest>(*concrete_request_create);
                    EtcdKeeperCreateResponse create_response;
                    create_response.path_created = current_create_request->path;
                    responses.emplace_back(std::make_shared<CreateResponse>(create_response));
                    String cur_path = current_create_request->path;
                    if (required_keys.count(cur_path) && create_request_to_skip[cur_path])
                    {
                        create_request_to_skip[cur_path] = false;
                        faked_responses[i] = true;
                    }
                    else if (multiple_sequential_keys_map[cur_path] == -1)
                    {
                        current_create_request->seq_delta = sequential_keys_map[cur_path];
                        multiple_sequential_keys_map[cur_path]++;
                    }
                    else if (sequential_keys_map[cur_path] > 1)
                    {
                        faked_responses[i] = true;
                    }
                    if (created_keys.count(parentPath(cur_path)) > 0)
                    {
                        current_create_request->force_parent_exists = true;
                    }
                    etcd_requests.emplace_back(current_create_request);
                }
                else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
                {
                    responses.emplace_back(std::make_shared<RemoveResponse>());
                    String cur_path = concrete_request_remove->path;
                    if (required_keys.count(cur_path) && remove_request_to_skip[cur_path])
                    {
                        remove_request_to_skip[cur_path] = false;
                        faked_responses[i] = true;
                    }
                    const auto & current_remove_request = std::make_shared<EtcdKeeperRemoveRequest>(*concrete_request_remove);
                    current_remove_request->removing_childs_count = removing_childs[concrete_request_remove->path];
                    etcd_requests.emplace_back(current_remove_request);
                }
                else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
                {
                    responses.emplace_back(std::make_shared<SetResponse>());
                    etcd_requests.emplace_back(std::make_shared<EtcdKeeperSetRequest>(*concrete_request_set));
                }
                else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
                {
                    responses.emplace_back(std::make_shared<CheckResponse>());
                    etcd_requests.emplace_back(std::make_shared<EtcdKeeperCheckRequest>(*concrete_request_check));
                }
                else
                {
                    throw Exception("Illegal command as part of multi ZooKeeper request", Coordination::Error::ZBADARGUMENTS);
                }
            }
            if (!required_keys.empty())
            {
                for (const auto & key : required_keys)
                {
                    EtcdKey ek = EtcdKey(key);
                    checking_etcd_keys.insert(ek.getFullEtcdKey());
                }
            }
        }
        EtcdKeeperResponsePtr makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses) override;
        EtcdKeeperResponsePtr makeResponse() const override;
        void clear() override
        {
            composite = false;
            pre_call_called = false;
            post_call_called = false;
            response->error = Error::ZOK;
            txn_requests.clear();
            pre_call_responses.clear();
            processed_responses.assign(responses.size(), false);
            for (size_t i = 0; i != etcd_requests.size(); i++)
            {
                etcd_requests[i]->clear();
            }
        }
        void setEtcdKey() override
        {
            for (auto & request : etcd_requests)
            {
                request->setEtcdKey();
            }
        }
        void preparePreCall() override
        {
            for (size_t i = 0; i != etcd_requests.size(); i++)
            {
                if (faked_responses[i])
                {
                    continue;
                }
                etcd_requests[i]->checkRequestForComposite();
                if (etcd_requests[i]->composite)
                {
                    etcd_requests[i]->txn_requests.clear();
                    etcd_requests[i]->preparePreCall();
                    txn_requests += etcd_requests[i]->txn_requests;
                }
            }
            if (!checking_etcd_keys.empty())
            {
                for (const auto & key : checking_etcd_keys)
                {
                    txn_requests.success_ranges.emplace_back(prepareRangeRequest(key));
                }
            }
        }
        void parsePreResponses() override
        {
            std::unordered_set<String> founded_keys;
            if (!checking_etcd_keys.empty())
            {
                for (auto & resp : pre_call_responses)
                {
                    if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                    {
                        const auto & range_resp = resp.response_range();
                        for (const auto & kv : range_resp.kvs())
                        {
                            if (checking_etcd_keys.count(kv.key()))
                            {
                                EtcdKey ek;
                                ek.setFullPath(kv.key());
                                founded_keys.insert(ek.zk_path);
                            }
                        }
                    }
                }
            }
            for (size_t i = 0; i != etcd_requests.size(); i++)
            {
                if (!faked_responses[i])
                {
                    etcd_requests[i]->pre_call_responses = pre_call_responses;
                    etcd_requests[i]->parsePreResponses();
                }
                else if (const auto * concrete_response = dynamic_cast<const CreateResponse *>(responses[i].get()))
                {
                    if (founded_keys.count(concrete_response->path_created))
                    {
                        responses[i]->error = Error::ZNODEEXISTS;
                    }
                }
            }
            size_t indx = 0;
            while (indx < responses.size() && faked_responses[indx] && responses[indx]->error == Error::ZOK)
            {
                indx++;
            }
            if (indx < responses.size() && responses[indx]->error != Error::ZOK)
            {
                EtcdKeeperMultiResponse multi_response;
                multi_response.error = responses[indx]->error;
                multi_response.responses = responses;
                response = std::make_shared<EtcdKeeperMultiResponse>(multi_response);
            }
        }
        void preparePostCall() override
        {
            for (size_t i = 0; i != etcd_requests.size(); i++)
            {
                if (etcd_requests[i]->response->error != Error::ZOK)
                {
                    failed_compare_requered = true;
                }
                else if (!faked_responses[i])
                {
                    etcd_requests[i]->txn_requests.clear();
                    etcd_requests[i]->preparePostCall();
                    txn_requests += etcd_requests[i]->txn_requests;
                }
            }
            if (failed_compare_requered)
            {
                txn_requests.compares.emplace_back(prepareCompare("/fake/path", "version", "equal", -1));
            }
            txn_requests.takeLastCreateRequestWithPrefix("/zk/childs");
        }
        void checkRequestForComposite() override
        {
            if (!txn_requests.empty())
            {
                composite = true;
            }
        }
        void prepareCall() override
        {
            setResponse();
            response->error = Error::ZOK;
            for (auto & request : etcd_requests)
            {
                request->setResponse();
                request->response->error = Error::ZOK;
            }
            if (!pre_call_called)
            {
                txn_requests.clear();
                preparePreCall();
            }
            checkRequestForComposite();
            if (composite && !pre_call_called)
            {
                return;
            }
            txn_requests.clear();
            preparePostCall();
        }
        void setResponse() override
        {
            response = makeResponse();
            response->error = Error::ZOK;
            for (auto & request : etcd_requests)
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
            create_response.error = Error::ZOK;
            bool parent_exists_in_resp = false;
            if (parentPath(path) == "/")
            {
                parent_exists_in_resp = true;
            }
            bool seq_num_matched = false;
            if (!is_sequential)
            {
                seq_num_matched = true;
            }
            for (auto & resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    const auto & range_resp = resp.response_range();
                    for (const auto & kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getParentKey())
                        {
                            parent_exists_in_resp = true;
                        }
                        else if (kv.key() == etcd_key.getParentSequentialCounterKey() && kv.value() == std::to_string(seq_num))
                        {
                            seq_num_matched = true;
                        }
                    }
                }
            }
            if (!parent_exists_in_resp && !force_parent_exists)
            {
                create_response.error = Error::ZNONODE;
                return std::make_shared<EtcdKeeperCreateResponse>(create_response);
            }
            if (!seq_num_matched)
            {
                create_response.finished = false;
            }
            // create_response.path_created = process_path;
            create_response.path_created = "error";
        }
        else
        {
            create_response.path_created = process_path;
            for (auto & resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponsePut == resp.response_case())
                {
                    const auto & put_resp = resp.response_put();
                    if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey())
                    {
                        create_response.error = Error::ZBADARGUMENTS;
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
            remove_response.error = Error::ZNONODE;
            for (auto & resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    const auto & range_resp = resp.response_range();
                    for (const auto & kv : range_resp.kvs())
                    {
                        if (kv.key() == etcd_key.getChildsFlagKey())
                        {
                            if (kv.version() != child_flag_version)
                            {
                                remove_response.error = Error::ZOK;
                                remove_response.finished = false;
                            }
                        }
                        else if (kv.key() == etcd_key.getFullEtcdKey())
                        {
                            if (version == -1)
                            {
                                remove_response.error = Error::ZOK;
                            }
                            else if (kv.version() != version)
                            {
                                remove_response.error = Error::ZBADVERSION;
                                break;
                            }
                        }
                    }
                }
            }
        }
        else
        {
            for (auto & resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseDeleteRange == resp.response_case())
                {
                    const auto & delete_range_resp = resp.response_delete_range();
                    if (delete_range_resp.deleted())
                    {
                        for (const auto & kv : delete_range_resp.prev_kvs())
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

    EtcdKeeperResponsePtr EtcdKeeperExistsRequest::makeResponseFromResponses(bool , std::vector<ResponseOp> & responses)
    {
        EtcdKeeperExistsResponse exists_response;
        exists_response.error = Error::ZNONODE;
        for (auto & resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                const auto & range_resp = resp.response_range();
                for (const auto & kv : range_resp.kvs())
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
        get_response.stat = Stat();
        if (compare_result)
        {
            for (auto & resp: responses)
            {
                if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
                {
                    const auto & range_resp = resp.response_range();
                    for (const auto & kv : range_resp.kvs())
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
        set_response.stat = Stat();
        if (!compare_result)
        {
            set_response.error = Error::ZNONODE;
        }
        for (auto & resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponsePut == resp.response_case())
            {
                const auto & put_resp = resp.response_put();
                if (put_resp.prev_kv().key() == etcd_key.getFullEtcdKey())
                {
                    set_response.error = Error::ZOK;
                    set_response.stat.version = put_resp.prev_kv().version() + 1;
                    set_response.stat.ephemeralOwner = put_resp.prev_kv().lease();
                }
            }
            else if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                const auto & range_resp = resp.response_range();
                for (const auto & kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        if (version != -1 && kv.version() != version)
                        {
                            set_response.error = Error::ZBADVERSION;
                            break;
                        }
                        set_response.error = Error::ZOK;
                        // set_response.stat.version =  kv.version();
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

    EtcdKeeperResponsePtr EtcdKeeperListRequest::makeResponseFromResponses(bool , std::vector<ResponseOp> & responses)
    {
        EtcdKeeperListResponse list_response;
        list_response.stat = Stat();
        list_response.error = Error::ZNONODE;
        std::unordered_set<String> children;
        for (auto & resp : responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                const auto & range_resp = resp.response_range();
                for (const auto & kv : range_resp.kvs())
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
                        for (const auto & cur_kv : range_resp.kvs())
                        {
                            children.insert(childName(cur_kv.key(), etcd_key.getChildsPrefix()));
                        }
                        list_response.error = Error::ZOK;
                        break;
                    }

                }
            }
        }
        for (const auto & child : children)
        {
            list_response.names.emplace_back(child);
        }
        return std::make_shared<EtcdKeeperListResponse>(list_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperCheckRequest::makeResponseFromResponses(bool , std::vector<ResponseOp> & responses)
    {
        EtcdKeeperCheckResponse check_response;
        check_response.error = Error::ZNONODE;
        for (auto & resp: responses)
        {
            if (ResponseOp::ResponseCase::kResponseRange == resp.response_case())
            {
                const auto & range_resp = resp.response_range();
                for (const auto & kv : range_resp.kvs())
                {
                    if (kv.key() == etcd_key.getFullEtcdKey())
                    {
                        check_response.error = Error::ZOK;
                        if (version != -1 && kv.version() != version)
                        {
                            check_response.error = Error::ZBADVERSION;
                            break;
                        }
                    }
                }
            }
        }
        return std::make_shared<EtcdKeeperCheckResponse>(check_response);
    }

    EtcdKeeperResponsePtr EtcdKeeperMultiRequest::makeResponseFromResponses(bool compare_result, std::vector<ResponseOp> & responses_)
    {
        EtcdKeeperMultiResponse multi_response;
        multi_response.error = Error::ZOK;
        for (size_t i = 0; i != etcd_requests.size(); i++)
        {
            if (auto * etcd_request = dynamic_cast<EtcdKeeperCreateRequest *>(etcd_requests[i].get()))
            {
                if (sequential_keys_map[etcd_request->path] > 1)
                {
                    size_t seq_paths_index = 0;
                    for (size_t j = 0; j != etcd_requests.size() && seq_paths_index < etcd_request->seq_paths.size(); j++)
                    {
                        if (processed_responses[j])
                        {
                            continue;
                        }
                        if (auto * concrete_response = dynamic_cast<CreateResponse *>(responses[j].get()))
                        {
                            if (startsWith(etcd_request->seq_paths[seq_paths_index], concrete_response->path_created))
                            {
                                auto r = etcd_request->makeResponseFromResponses(compare_result, responses_);
                                multi_response.finished &= r->finished;
                                multi_response.finished &= etcd_request->response->finished;
                                CreateResponse cur_create_resp = *concrete_response;
                                cur_create_resp.path_created = etcd_request->seq_paths[seq_paths_index];
                                cur_create_resp.error = etcd_request->response->error;
                                responses[j] = std::make_shared<CreateResponse>(cur_create_resp);
                                seq_paths_index++;
                                processed_responses[j] = true;
                            }
                        }
                    }
                }
            }
        }
        for (size_t i = 0; i != etcd_requests.size(); i++)
        {
            if (!faked_responses[i] && !processed_responses[i])
            {
                if (etcd_requests[i]->makeResponseFromExisted()->error != Error::ZOK)
                {
                    responses[i] = etcd_requests[i]->makeResponseFromExisted();
                }
                if (responses[i]->error == Error::ZOK)
                {
                    auto r = etcd_requests[i]->makeResponseFromResponses(compare_result, responses_);
                    multi_response.finished &= r->finished;
                    responses[i] = r;
                }
                multi_response.finished &= etcd_requests[i]->response->finished;
            }
            multi_response.error = responses[i]->error;
            if (multi_response.error != Error::ZOK)
            {
                break;
            }
        }
        multi_response.responses = responses;
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
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        kv_stub = KV::NewStub(channel);

        std::shared_ptr<Channel> watch_channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        watch_stub = Watch::NewStub(watch_channel);

        watch_stream = watch_stub->AsyncWatch(&watch_context, &watch_cq, reinterpret_cast<void*> (BiDiTag::CONNECT));
        readWatchResponse();

        std::shared_ptr<Channel> lease_channel = grpc::CreateChannel(host, grpc::InsecureChannelCredentials());
        lease_stub = Lease::NewStub(lease_channel);

        requestLease(std::chrono::seconds(KEEP_ALIVE_TTL_S));

        keep_alive_stream = lease_stub->AsyncLeaseKeepAlive(&keep_alive_context, &lease_cq, reinterpret_cast<void*> (BiDiTag::CONNECT));
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
                            String watch_path;
                            {
                                std::lock_guard lock(watches_mutex);
                                if (list_watch)
                                {
                                    list_watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));
                                    watch_path = info.request->getChildsPrefix();
                                }
                                else
                                {
                                    watches[info.request->getEtcdKey()].emplace_back(std::move(info.watch));
                                    watch_path = info.request->getEtcdKey();
                                }
                            }
                            callWatchRequest(watch_path, list_watch);
                        }

                        EtcdKeeper::AsyncCall* call = new EtcdKeeper::AsyncCall;
                        call->xid = info.request->xid;

                        info.request->prepareCall();
                        info.request->call(*call, kv_stub, kv_cq);
                        delete call;
                    }
                }
                else
                {
                    prev_heartbeat_time = clock::now();
                    LeaseKeepAliveRequest keep_alive_request;
                    keep_alive_request.set_id(lease_id);
                    keep_alive_stream->Write(keep_alive_request, reinterpret_cast<void*> (BiDiTag::WRITE));
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
                    auto * call = static_cast<AsyncCall*>(got_tag);

                    XID xid = call->xid;
                    {
                        std::lock_guard lock(operations_mutex);
                        auto it = operations.find(xid);
                        if (it == operations.end())
                            throw Exception("Received response for unknown xid", Coordination::Error::ZRUNTIMEINCONSISTENCY);

                        request_info = std::move(it->second);
                        operations.erase(it);
                    }

                    if (!call->status.ok())
                    {
                        // LOG_ERROR(log, "RPC FAILED: " << call->status.error_message());
                    }
                    else
                    {
                        if (!request_info.request->callRequired(got_tag))
                        {
                            response = request_info.request->makeResponseFromTag(got_tag);
                            if (!response->finished)
                            {
                                request_info.request->clear();
                                {
                                    std::lock_guard lock(push_request_mutex);
                                    if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
                                        throw Exception("Cannot push request to queue within operation timeout", Coordination::Error::ZOPERATIONTIMEOUT);
                                }
                            }
                            else
                            {
                                response->removeRootPath(root_path);
                                if (request_info.callback)
                                {
                                    ResponsePtr r = request_info.request->makeResponseFromTag(got_tag);
                                    r->removeRootPath(root_path);
                                    request_info.callback(*r);
                                }
                            }
                        }
                        else
                        {
                            std::lock_guard lock(operations_mutex);
                            operations[request_info.request->xid] = request_info;
                            EtcdKeeper::AsyncCall * cur_call = new EtcdKeeper::AsyncCall;
                            cur_call->xid = request_info.request->xid;
                            request_info.request->prepareCall();
                            request_info.request->call(*cur_call, kv_stub, kv_cq);
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
                    switch (static_cast<BiDiTag>(reinterpret_cast<Int64>(got_tag)))
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
                        watch_stream = watch_stub->AsyncWatch(&watch_context, &watch_cq, reinterpret_cast<void*> (BiDiTag::CONNECT));
                        break;
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
                    switch (static_cast<BiDiTag>(reinterpret_cast<Int64>(got_tag)))
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
                        keep_alive_stream = lease_stub->AsyncLeaseKeepAlive(&keep_alive_context, &lease_cq, reinterpret_cast<void*> (BiDiTag::CONNECT));
                        break;
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
                    response.error = Error::ZSESSIONEXPIRED;

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
                {
                    throw Exception("XID overflow", Coordination::Error::ZSESSIONEXPIRED);
                }
            }

            std::lock_guard lock(push_request_mutex);

            if (expired)
            {
                throw Exception("Session expired", Coordination::Error::ZSESSIONEXPIRED);
            }

            if (!requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
            {
                throw Exception("Cannot push request to queue within operation timeout", Coordination::Error::ZOPERATIONTIMEOUT);
            }
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
#endif
