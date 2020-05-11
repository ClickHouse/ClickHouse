#pragma once
 
#include <mutex>
#include <map>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>
#include <common/logger_useful.h>
 
#include <Poco/Timespan.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/StringUtils/StringUtils.h>

#include <grpcpp/grpcpp.h>
#include <Common/ZooKeeper/rpc.grpc.pb.h>
 
using etcdserverpb::PutRequest;
using etcdserverpb::PutResponse;
using etcdserverpb::DeleteRangeRequest;
using etcdserverpb::DeleteRangeResponse;
// using etcdserverpb::AsyncDeleteRangeResponse;
using etcdserverpb::RangeRequest;
using etcdserverpb::RangeResponse;
using etcdserverpb::RequestOp;
using etcdserverpb::ResponseOp;
using etcdserverpb::TxnRequest;
using etcdserverpb::TxnResponse;
using etcdserverpb::Compare;
using etcdserverpb::KV;
using etcdserverpb::Watch;
// using etcdserverpb::WatchRequest;
// using etcdserverpb::WatchResponse;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

 
namespace Coordination
{
    struct EtcdKey;
    struct EtcdKeeperRequest;
    using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>; 
 
/** Looks like ZooKeeper but stores all data in memory of server process.
  * All data is not shared between different servers and is lost after server restart.
  *
  * The only purpose is to more simple testing for interaction with ZooKeeper within a single server.
  * This still makes sense, because multiple replicas of a single table can be created on a single server,
  *  and it is used to test replication logic.
  *
  * Does not support ACLs. Does not support NULL node values.
  *
  * NOTE: You can add various failure modes for better testing.
  */
    class EtcdKeeper : public IKeeper
    {
    public:
        using XID = int32_t;

        EtcdKeeper(const String & root_path_, Poco::Timespan operation_timeout_);
        ~EtcdKeeper() override;
 
        bool isExpired() const override { return expired; }
        int64_t getSessionID() const override { return 0; }
 
 
        void create(
                const String & path,
                const String & data,
                bool is_ephemeral,
                bool is_sequential,
                const ACLs & acls,
                CreateCallback callback) override;
 
        void remove(
                const String & path,
                int32_t version,
                RemoveCallback callback) override;
 
        void exists(
                const String & path,
                ExistsCallback callback,
                WatchCallback watch) override;
 
        void get(
                const String & path,
                GetCallback callback,
                WatchCallback watch) override;
 
        void set(
                const String & path,
                const String & data,
                int32_t version,
                SetCallback callback) override;
 
        void list(
                const String & path,
                ListCallback callback,
                WatchCallback watch) override;
 
        void check(
                const String & path,
                int32_t version,
                CheckCallback callback) override;
 
        void multi(
                const Requests & requests,
                MultiCallback callback) override;
 
 
        struct EtcdNode
        {
            String data;
            ACLs acls;
            bool is_ephemeral = false;
            bool is_sequental = false;
            Stat stat{};
            int32_t seq_num = 0;
            String unpursed_data;
            void serialize();
            void deserialize();
        };

        struct Call { 
            Call() = default;
            Call(const Call &) = default;
            Call & operator=(const Call &) = default;
            virtual ~Call() = default;
        };

        struct AsyncCall : virtual Call { 
            Status status;
            XID xid;
            int responses;         
        };

        struct AsyncPutCall final : AsyncCall 
        {
            AsyncPutCall() {}
            AsyncPutCall(const AsyncCall & base) : AsyncCall(base) {}
            std::string path;
            ClientContext context;
            PutResponse response;
            std::unique_ptr<ClientAsyncResponseReader<PutResponse>> response_reader;
        };

        struct AsyncDeleteRangeCall final : AsyncCall 
        {
            AsyncDeleteRangeCall() {}
            AsyncDeleteRangeCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            DeleteRangeResponse response;
            std::unique_ptr<ClientAsyncResponseReader<DeleteRangeResponse>> response_reader;
        };

        struct AsyncRangeCall final : AsyncCall 
        {
            AsyncRangeCall() {}
            AsyncRangeCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            RangeResponse response;
            std::unique_ptr<ClientAsyncResponseReader<RangeResponse>> response_reader;
        };

        struct AsyncTxnCall final : AsyncCall 
        {
            AsyncTxnCall() {}
            AsyncTxnCall(const AsyncCall & base) : AsyncCall(base) {}
            ClientContext context;
            TxnResponse response;
            std::unique_ptr<ClientAsyncResponseReader<TxnResponse>> response_reader;
        };

        struct TxnRequests
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
            void take_last_create(const String prefix)
            {
                std::unordered_map<String, String> last_data;
                for (const auto success_put : success_puts)
                {
                    if (startsWith(success_put.key(), prefix))
                    {
                        last_data[success_put.key()] = success_put.value();
                    }
                }
                auto it = success_puts.begin();
                while (it != success_puts.end())
                {
                    if (startsWith(it->key(), prefix) && it->value() != last_data[it->key()])
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
  
        using WatchCallbacks = std::vector<WatchCallback>;
        using Watches = std::map<String /* path, relative of root_path */, WatchCallbacks>;

        std::unique_ptr<KV::Stub> kv_stub_;
        CompletionQueue kv_cq_;

        std::unique_ptr<Watch::Stub> watch_stub_;
        CompletionQueue watch_cq_;

        std::unique_ptr<KV::Stub> lease_stub_;
        CompletionQueue lease_cq_;

        void callWatchRequest(
            const std::string & key,
            bool list_watch,
            std::unique_ptr<Watch::Stub> & stub_,
            CompletionQueue & cq_);

        void readWatchResponse();
 
    private:
        std::atomic<XID> next_xid {1};

        using clock = std::chrono::steady_clock;
 
        struct RequestInfo
        {
            EtcdKeeperRequestPtr request;
            ResponseCallback callback;
            WatchCallback watch;
            clock::time_point time;
        };

        String root_path;
        ACLs default_acls;
 
        Poco::Timespan operation_timeout;
 
        std::mutex push_request_mutex;
        std::atomic<bool> expired{false};
 
        int64_t zxid = 0;
 
        Watches watches;
        Watches list_watches;   /// Watches for 'list' request (watches on children).
        std::mutex watches_mutex;
 
        void createWatchCallBack(const String & path);
 
        using RequestsQueue = ConcurrentBoundedQueue<RequestInfo>;
        RequestsQueue requests_queue{1};

        using Operations = std::map<XID, RequestInfo>;

        Operations operations;
        std::mutex operations_mutex;
 
        void pushRequest(RequestInfo && request);
 
        void finalize();
 
        ThreadFromGlobalPool call_thread;
 
        void callThread();

        ThreadFromGlobalPool complete_thread;

        void completeThread();

        ThreadFromGlobalPool watch_complete_thread;

        void watchCompleteThread();

        // tmp
        std::unique_ptr<ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>> stream;
        ClientContext context_;
        etcdserverpb::WatchResponse watch_response;

        std::unique_ptr<PutRequest> preparePutRequest(const std::string &, const std::string &);
        std::unique_ptr<RangeRequest> prepareRangeRequest(const std::string &);
        std::unique_ptr<DeleteRangeRequest> prepareDeleteRangeRequest(const std::string &);
        std::unique_ptr<TxnRequest> prepareTxnRequest(
            const std::string &,
            Compare::CompareTarget ,
            Compare::CompareResult ,
            Int64 ,
            Int64 ,
            Int64 );

        void callPutRequest(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<PutRequest> );

        void callRangeRequest(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<RangeRequest> );

        void callDeleteRangeRequest(
        EtcdKeeper::AsyncCall &,
        std::unique_ptr<KV::Stub> &,
        CompletionQueue &,
        std::unique_ptr<DeleteRangeRequest> );
        
    };

    struct EtcdKeeperResponse;
    using EtcdKeeperResponsePtr = std::shared_ptr<EtcdKeeperResponse>;
    
    using EtcdKeeperRequestPtr = std::shared_ptr<EtcdKeeperRequest>;
    using EtcdKeeperRequests = std::vector<EtcdKeeperRequestPtr>;
 
}
