#include <ACL/QuotasConsumer.h>
#include <ACL/IACLAttributableManager.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <common/DateLUT.h>
#include <pcg_random.hpp>
#include <mutex>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int QUOTA_DOESNT_ALLOW_KEYS;
    extern const int QUOTA_EXPIRED;
}


namespace
{
    constexpr size_t MAX_RESOURCE_TYPE = Quota2::MAX_RESOURCE_TYPE;

    std::chrono::system_clock::duration randomDuration(const std::chrono::seconds max, pcg64 & rnd_engine)
    {
        auto upper_bound = std::chrono::duration_cast<std::chrono::system_clock::duration>(max).count();
        --upper_bound;
        auto rnd_value = std::uniform_int_distribution<decltype(upper_bound)>(0, upper_bound)(rnd_engine);
        return std::chrono::system_clock::duration(rnd_value);
    }

    std::chrono::system_clock::time_point ceilTimePoint(
        std::chrono::system_clock::time_point x,
        std::chrono::seconds interval,
        std::chrono::system_clock::duration offset = std::chrono::system_clock::duration::zero())
    {
        auto interval_duration = std::chrono::duration_cast<std::chrono::system_clock::duration>(interval).count();
        return std::chrono::system_clock::time_point(std::chrono::system_clock::duration(
            ((x - offset).time_since_epoch().count() + interval_duration - 1) / interval_duration * interval_duration + offset.count()));
    }
}


using ConsumptionKey = Quota2::ConsumptionKey;
using Attributes = Quota2::Attributes;
using AttributesPtr = Quota2::AttributesPtr;


struct QuotasConsumer::Interval
{
    ResourceAmount max[MAX_RESOURCE_TYPE];
    ResourceAmount used[MAX_RESOURCE_TYPE];
    std::chrono::seconds duration;
    std::chrono::system_clock::duration offset;
    std::chrono::system_clock::time_point end_of_interval;
};


struct QuotasConsumer::ExceedInfo
{
    ResourceAmount max;
    std::chrono::seconds duration;
    std::chrono::system_clock::time_point end_of_interval;
    AttributesPtr attrs;
};


class QuotasConsumer::Intervals
{
public:
    Intervals()
    {
        for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
        {
            interval_with_min_free_amount[i] = nullptr;
            min_free_amount[i].store(std::numeric_limits<ResourceAmount>::max());
        }
    }

    void setAttributes(const AttributesPtr & attrs_)
    {
        std::lock_guard lock{mutex};
        if (attrs == attrs_)
            return;
        attrs = attrs_;
        size_t index = 0;
        auto now = std::chrono::system_clock::now();

        applyChangeOfMinFreeAmount();

        for (const auto & [duration, limits] : attrs->limits_for_duration)
        {
            if (duration <= std::chrono::seconds::zero())
                continue;

            while ((index < intervals.size()) && (intervals[index].duration < duration))
                intervals.erase(intervals.begin() + index);

            if ((index < intervals.size()) && (intervals[index].duration == duration))
            {
                Interval & interval = intervals[index];
                for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
                    interval.max[i] = limits.limits[i];
            }
            else
            {
                Interval & interval = *(intervals.insert(intervals.begin() + index, Interval{}));
                for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
                {
                    interval.max[i] = limits.limits[i];
                    interval.used[i] = 0;
                }
                interval.duration = duration;
                interval.offset = randomDuration(duration, rnd_engine);
                interval.end_of_interval = ceilTimePoint(now, interval.duration, interval.offset);
            }
            ++index;
        }

        recalculateMinFreeAmount();
    }

    bool consume(ResourceType resource_type, ResourceAmount & amount, std::chrono::system_clock::time_point now, ExceedInfo * info)
    {
        if (!amount)
            return true;

        auto & mfa_atomic = min_free_amount[static_cast<size_t>(resource_type)];
        ResourceAmount mfa_value = mfa_atomic.load();
        while (mfa_value >= amount)
        {
            if (mfa_atomic.compare_exchange_weak(mfa_value, mfa_value - amount))
            {
                amount = 0;
                return true;
            }
        }

        std::lock_guard lock{mutex};

        Interval * mfa_interval = interval_with_min_free_amount[static_cast<size_t>(resource_type)];
        if (!mfa_interval)
        {
            amount = 0;
            return true;
        }

        if (now >= mfa_interval->end_of_interval)
        {
            applyChangeOfMinFreeAmount();
            for (Interval & interval : intervals)
            {
                if (now >= interval.end_of_interval)
                {
                    interval.end_of_interval = ceilTimePoint(now, interval.duration, interval.offset);
                    for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
                        interval.used[i] = 0;
                }
            }
            recalculateMinFreeAmount();
            mfa_interval = interval_with_min_free_amount[static_cast<size_t>(resource_type)];
        }

        mfa_value = mfa_atomic.load();
        while(true)
        {
            if  (mfa_value >= amount)
            {
                if (mfa_atomic.compare_exchange_weak(mfa_value, mfa_value - amount))
                {
                    amount = 0;
                    return true;
                }
            }
            else
            {
                if (mfa_atomic.compare_exchange_weak(mfa_value, 0))
                {
                    amount -= mfa_value;
                    if (info)
                    {
                        info->attrs = attrs;
                        info->duration = mfa_interval->duration;
                        info->end_of_interval = mfa_interval->end_of_interval;
                        info->max = mfa_interval->max[static_cast<size_t>(resource_type)];
                    }
                    return false;
                }
            }
        }
    }

private:
    void applyChangeOfMinFreeAmount()
    {
        for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
        {
            ResourceAmount mfa_value = min_free_amount[i].exchange(0);
            Interval * mfa_interval = interval_with_min_free_amount[i];
            ResourceAmount used = (mfa_interval && (mfa_interval->max[i] > mfa_value)) ? (mfa_interval->max[i] - mfa_value) : 0;
            for (Interval & interval : intervals)
                interval.used[i] += used;
        }
    }

    void recalculateMinFreeAmount()
    {
        for (size_t i = 0; i != MAX_RESOURCE_TYPE; ++i)
        {
            ResourceAmount mfa_value = std::numeric_limits<ResourceAmount>::max();
            Interval * mfa_interval = nullptr;
            for (Interval & interval : intervals)
            {
                ResourceAmount free_amount = (interval.max > interval.used) ? (interval.max - interval.used) : 0;
                if (mfa_value > free_amount)
                {
                    mfa_interval = &interval;
                    mfa_value = free_amount;
                }
            }
            interval_with_min_free_amount[i] = mfa_interval;
            min_free_amount[i].store(mfa_value);
        }
    }

    Interval * interval_with_min_free_amount[MAX_RESOURCE_TYPE];
    std::atomic<ResourceAmount> min_free_amount[MAX_RESOURCE_TYPE];
    std::vector<Interval> intervals;
    AttributesPtr attrs;
    mutable pcg64 rnd_engine{randomSeed()};
    std::mutex mutex;
};


class QuotasConsumer::ConsumptionMap
{
public:
    static ConsumptionMap & instance()
    {
        static ConsumptionMap the_instance;
        return the_instance;
    }

    Intervals * get(const UUID & quota, const AttributesPtr & attrs, const String & user_name, const IPAddress & ip_address, const String & custom_consumption_key)
    {
        if (!attrs)
            return nullptr;
        std::lock_guard lock{mutex};
        auto & x = map[quota];
        Intervals * result = nullptr;
        if (custom_consumption_key.empty())
        {
            if (attrs->allow_custom_consumption_key)
                result = &x[custom_consumption_key];
            else
                throw Exception(
                    "Quota " + attrs->name + " (for user " + user_name + ") doesn't allow client supplied keys.", ErrorCodes::QUOTA_DOESNT_ALLOW_KEYS);
        }
        else
        {
            switch (attrs->consumption_key)
            {
                case ConsumptionKey::NONE: result = &x[String()]; break;
                case ConsumptionKey::USER_NAME: result = &x[user_name]; break;
                case ConsumptionKey::IP_ADDRESS: result = &x[ip_address.toString()]; break;
            }
            __builtin_unreachable();
        }
        result->setAttributes(attrs);
        return result;
    }

private:
    std::unordered_map<UUID, std::unordered_map<String, Intervals>> map;
    std::mutex mutex;
};


QuotasConsumer::QuotasConsumer(
    const std::vector<Quota2> & quotas_, const String & user_name_, const IPAddress & ip_address_, const String & custom_consumption_key_)
    : quotas(quotas_), user_name(user_name_), ip_address(ip_address_), custom_consumption_key(custom_consumption_key_)
{
    intervals_for_quotas = std::make_unique<std::atomic<Intervals *>[]>(quotas.size());
    subscriptions = std::make_unique<SubscriptionPtr[]>(quotas.size());
    for (size_t index = 0; index != quotas.size(); ++index)
    {
        const Quota2 & quota = quotas[index];
        const auto id = quota.getID();
        auto * storage = quota.getStorage();
        if (!storage)
            continue;

        auto on_got_attrs = [id, index, this](const AttributesPtr & attrs)
        {
            intervals_for_quotas[index].store(ConsumptionMap::instance().get(id, attrs, user_name, ip_address, custom_consumption_key));
        };

        subscriptions[index] = storage->subscribeForChanges(id, on_got_attrs);

        AttributesPtr attrs;
        storage->read(id, attrs);
        on_got_attrs(attrs);
    }
}


QuotasConsumer::~QuotasConsumer()
{
}


void QuotasConsumer::consume(ResourceType resource_type, ResourceAmount amount)
{
    consume(resource_type, amount, std::chrono::system_clock::now());
}


void QuotasConsumer::consume(ResourceType resource_type, ResourceAmount amount, std::chrono::system_clock::time_point current_time)
{
    if (!amount)
        return;

    ExceedInfo info;
    bool got_info = false;
    for (size_t index = 0; index != quotas.size(); ++index)
    {
        Intervals * intervals = intervals_for_quotas[index].load();
        if (intervals)
        {
            if (intervals->consume(resource_type, amount, current_time, got_info ? nullptr : &info))
                return;
            got_info = true;
        }
    }

    std::stringstream message;
    message << "Quota for user '" << user_name << "' for ";

    if (info.duration == std::chrono::hours(1))
        message << "1 hour";
    else if (info.duration == std::chrono::minutes(1))
        message << "1 minute";
    else if (info.duration == std::chrono::duration_cast<std::chrono::hours>(info.duration))
        message << std::chrono::duration_cast<std::chrono::hours>(info.duration).count() << " hours";
    else if (info.duration == std::chrono::duration_cast<std::chrono::minutes>(info.duration))
        message << std::chrono::duration_cast<std::chrono::minutes>(info.duration).count() << " minutes";
    else
        message << std::chrono::duration_cast<std::chrono::seconds>(info.duration).count() << " seconds";

    message << " has been exceeded. "
        << Quota2::getResourceName(resource_type) << ": " << (info.max + amount) << ", max: " << info.max << ". "
        << "Interval will end at " << DateLUT::instance().timeToString(std::chrono::system_clock::to_time_t(info.end_of_interval)) << ". "
        << "Name of quota template: '" << info.attrs->name << "'.";

    throw Exception(message.str(), ErrorCodes::QUOTA_EXPIRED);
}
}
