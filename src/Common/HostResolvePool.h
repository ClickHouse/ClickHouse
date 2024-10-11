#pragma once

#include <Common/CurrentMetrics.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Net/IPAddress.h>

#include <mutex>
#include <memory>

// That class resolves host into multiply addresses
// Features:
// - balance address usage.
//    `selectBest()` chooses the address by random with weights.
//    The more ip is used the lesser weight it has. When new address is happened, it takes more weight.
//    But still not all requests are assigned to the new address.
// - join resolve results
//    In case when host is resolved into different set of addresses, this class join all that addresses and use them.
//    An address expires after `history_` time.
// - failed address pessimization
//    If an address marked with `setFail()` it is marked as faulty. Such address won't be selected until either
//    a) it still occurs in resolve set after `history_` time or b) all other addresses are pessimized as well.
// - resolve schedule
//    Addresses are resolved through `DB::DNSResolver::instance()`.
//    Usually it does not happen more often than once in `history_` time.
//    But also new resolve performed each `setFail()` call.

namespace DB
{

struct HostResolverMetrics
{
    const ProfileEvents::Event discovered = ProfileEvents::end();
    const ProfileEvents::Event expired = ProfileEvents::end();
    const ProfileEvents::Event failed = ProfileEvents::end();

    const CurrentMetrics::Metric active_count = CurrentMetrics::end();
    const CurrentMetrics::Metric banned_count = CurrentMetrics::end();
};

constexpr size_t DEFAULT_RESOLVE_TIME_HISTORY_SECONDS = 2*60;
constexpr size_t RECORD_CONSECTIVE_FAIL_COUNT_LIMIT = 6;


class HostResolver : public std::enable_shared_from_this<HostResolver>
{
private:
    using WeakPtr = std::weak_ptr<HostResolver>;

public:
    using Ptr = std::shared_ptr<HostResolver>;

    template<class... Args>
    static Ptr create(Args&&... args)
    {
        struct make_shared_enabler : public HostResolver
        {
            explicit make_shared_enabler(Args&&... args) : HostResolver(std::forward<Args>(args)...) {}
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    virtual ~HostResolver();

    class Entry
    {
    public:
        Entry(Entry && entry) = default;
        Entry(Entry & entry) = delete;

        // no access as r-value
        const String * operator->() && = delete;
        const String * operator->() const && = delete;
        const String & operator*() && = delete;
        const String & operator*() const && = delete;

        const String * operator->() & { return &resolved_host; }
        const String * operator->() const & { return &resolved_host; }
        const String & operator*() & { return resolved_host; }
        const String & operator*() const & { return resolved_host; }

        void setFail();
        ~Entry();

    private:
        friend class HostResolver;

        Entry(HostResolver & pool_, Poco::Net::IPAddress address_)
            : pool(pool_.getWeakFromThis())
            , address(address_)
            , resolved_host(address.toString())
        { }

        HostResolver::WeakPtr pool;
        const Poco::Net::IPAddress address;
        const String resolved_host;

        bool fail = false;
    };

    /// can throw NetException(ErrorCodes::DNS_ERROR, ...), Exception(ErrorCodes::BAD_ARGUMENTS, ...)
    Entry resolve();
    void update();
    void reset();

    static HostResolverMetrics getMetrics();

protected:
    explicit HostResolver(
        String host_,
        Poco::Timespan history_ = Poco::Timespan(DEFAULT_RESOLVE_TIME_HISTORY_SECONDS, 0));

    using ResolveFunction = std::function<std::vector<Poco::Net::IPAddress> (const String & host)>;
    HostResolver(ResolveFunction && resolve_function_,
                    String host_,
                    Poco::Timespan history_);

    friend class Entry;
    WeakPtr getWeakFromThis();

    void setSuccess(const Poco::Net::IPAddress & address);
    void setFail(const Poco::Net::IPAddress & address);

    struct Record
    {
        Record(Poco::Net::IPAddress address_, Poco::Timestamp resolve_time_)
            : address(address_)
            , resolve_time(resolve_time_)
        {}

        Record(Record && rec) = default;
        Record& operator=(Record && s) = default;

        Record(const Record & rec) = default;
        Record& operator=(const Record & s) = default;

        Poco::Net::IPAddress address;
        Poco::Timestamp resolve_time;
        size_t usage = 0;
        bool failed = false;
        Poco::Timestamp fail_time = 0;
        size_t consecutive_fail_count = 0;

        size_t weight_prefix_sum;

        bool operator <(const Record & r) const
        {
            return address < r.address;
        }

        bool operator ==(const Record & r) const
        {
            return address == r.address;
        }

        size_t getWeight() const
        {
            if (failed)
                return 0;

            /// There is no goal to make usage's distribution ideally even
            /// The goal is to chose more often new address, but still use old addresses as well
            /// when all addresses have usage counter greater than 10000,
            /// no more corrections are needed, just random choice is ok
            if (usage > 10000)
                return 1;
            if (usage > 1000)
                return 5;
            if (usage > 100)
                return 8;
            return 10;
        }

        bool setFail(const Poco::Timestamp & now)
        {
            bool was_ok = !failed;

            failed = true;
            fail_time = now;

            if (was_ok)
            {
                if (consecutive_fail_count < RECORD_CONSECTIVE_FAIL_COUNT_LIMIT)
                    ++consecutive_fail_count;
            }

            return was_ok;
        }

        void setSuccess()
        {
            consecutive_fail_count = 0;
            ++usage;
        }
    };

    using Records = std::vector<Record>;

    Poco::Net::IPAddress selectBest() TSA_REQUIRES(mutex);
    Records::iterator find(const Poco::Net::IPAddress & address) TSA_REQUIRES(mutex);
    bool isUpdateNeeded();

    void updateImpl(Poco::Timestamp now, std::vector<Poco::Net::IPAddress> & next_gen) TSA_REQUIRES(mutex);
    void updateWeights() TSA_REQUIRES(mutex);
    void updateWeightsImpl() TSA_REQUIRES(mutex);
    size_t getTotalWeight() const TSA_REQUIRES(mutex);
    Poco::Timespan getRecordHistoryTime(const Record&) const;

    const String host;
    const Poco::Timespan history;
    const HostResolverMetrics metrics = getMetrics();

    // for tests purpose
    const ResolveFunction resolve_function;

    std::mutex mutex;

    Poco::Timestamp last_resolve_time TSA_GUARDED_BY(mutex) = Poco::Timestamp::TIMEVAL_MIN;
    Records records TSA_GUARDED_BY(mutex);

    Poco::Logger * log = &Poco::Logger::get("ConnectionPool");
};

class HostResolversPool
{
private:
    HostResolversPool() = default;

public:
    HostResolversPool(const HostResolversPool &) = delete;
    HostResolversPool & operator=(const HostResolversPool &) = delete;

    static HostResolversPool & instance();

    void dropCache();

    HostResolver::Ptr getResolver(const String & host);
private:
    std::mutex mutex;
    std::unordered_map<String, HostResolver::Ptr> host_pools TSA_GUARDED_BY(mutex);
};

}

