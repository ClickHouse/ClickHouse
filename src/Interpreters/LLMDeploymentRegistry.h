#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <chrono>
#include <memory>
#include <mutex>

namespace DB
{

struct LLMDeployment
{
    String name;
    String route;
    String credentials;
    String model;
    UInt64 weight = 1;
    UInt64 max_requests_per_minute = 0;
    UInt64 max_concurrent_requests = 0;
};

struct LLMDeploymentSnapshot
{
    VectorWithMemoryTracking<LLMDeployment> deployments;
    UInt64 version = 0;
};

/// Keeper owns the configuration; admission counters belong to this server.
class LLMDeploymentRegistry
{
    struct AdmissionState
    {
        struct Counters
        {
            std::chrono::steady_clock::time_point window_start{};
            UInt64 requests = 0;
            UInt64 in_flight = 0;
        };

        std::mutex mutex;
        UnorderedMapWithMemoryTracking<String, Counters> counters;
    };

public:
    class Reservation
    {
    public:
        Reservation(std::shared_ptr<AdmissionState> state_, LLMDeployment deployment_, UInt64 version_);
        ~Reservation();

        Reservation(const Reservation &) = delete;
        Reservation & operator=(const Reservation &) = delete;

        const LLMDeployment deployment;
        const UInt64 version;

    private:
        friend class LLMDeploymentRegistry;
        std::shared_ptr<AdmissionState> state;
        bool acquired = false;
    };

    explicit LLMDeploymentRegistry(String keeper_path_);

    LLMDeploymentSnapshot getSnapshot(ContextPtr context) const;
    void insert(ContextPtr context, const VectorWithMemoryTracking<LLMDeployment> & deployments);
    void remove(ContextPtr context, const String & name);

    std::unique_ptr<Reservation> reserve(const LLMDeploymentSnapshot & snapshot, const String & route);

private:
    String keeper_path;
    std::shared_ptr<AdmissionState> admission = std::make_shared<AdmissionState>();
};

}
