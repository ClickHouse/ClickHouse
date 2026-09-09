#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Context_fwd.h>
#include <boost/noncopyable.hpp>
#include <list>
#include <functional>
#include <mutex>


namespace DB
{

class ReplicasReconnector : private boost::noncopyable
{
public:
    using Reconnector = std::function<bool(UInt64)>;
    using ReconnectorsList = std::list<Reconnector>;

    ReplicasReconnector(const ReplicasReconnector &) = delete;

    ~ReplicasReconnector();

    [[nodiscard]]
    static std::unique_ptr<ReplicasReconnector> init(ContextPtr context);

    static ReplicasReconnector & instance();

    void add(const Reconnector & reconnector);

private:
    /// Defined out of line: a definition in the header gives every shared object its own copy.
    static ReplicasReconnector * instance_ptr;
    ReconnectorsList reconnectors;
    std::mutex mutex;
    std::atomic_bool emergency_stop{false};
    BackgroundSchedulePoolTaskHolder task_handle;
    LoggerPtr log = nullptr;

    explicit ReplicasReconnector(ContextPtr context);

    void run();
};

}
