#include <Interpreters/LLMDeploymentRegistry.h>

#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/thread_local_rng.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>
#include <random>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int KEEPER_EXCEPTION;
extern const int LIMIT_EXCEEDED;
}

namespace
{

constexpr size_t max_deployments = 1024;
constexpr size_t max_snapshot_bytes = 256 * 1024;

void validate(const LLMDeployment & deployment)
{
    if (deployment.name.empty() || deployment.route.empty() || deployment.credentials.empty() || deployment.model.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM deployment name, route, credentials and model must not be empty");
    if (deployment.weight > 1000000)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM deployment weight must not exceed 1000000");
}

String serialize(const VectorWithMemoryTracking<LLMDeployment> & deployments)
{
    if (deployments.size() > max_deployments)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "At most {} LLM deployments are supported", max_deployments);

    WriteBufferFromOwnString out;
    FormatSettings format_settings;
    writeCString("{\"format_version\":1,\"deployments\":[", out);
    bool first = true;
    for (const auto & deployment : deployments)
    {
        validate(deployment);
        if (!first)
            writeChar(',', out);
        first = false;
        writeCString("{\"name\":", out);
        writeJSONString(deployment.name, out, format_settings);
        writeCString(",\"route\":", out);
        writeJSONString(deployment.route, out, format_settings);
        writeCString(",\"credentials\":", out);
        writeJSONString(deployment.credentials, out, format_settings);
        writeCString(",\"model\":", out);
        writeJSONString(deployment.model, out, format_settings);
        writeCString(",\"weight\":", out);
        writeIntText(deployment.weight, out);
        writeCString(",\"max_requests_per_minute\":", out);
        writeIntText(deployment.max_requests_per_minute, out);
        writeCString(",\"max_concurrent_requests\":", out);
        writeIntText(deployment.max_concurrent_requests, out);
        writeChar('}', out);
        if (out.count() > max_snapshot_bytes)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "LLM deployment configuration exceeds {} bytes", max_snapshot_bytes);
    }
    writeCString("]}", out);
    String data = std::move(out.str());
    if (data.size() > max_snapshot_bytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "LLM deployment configuration exceeds {} bytes", max_snapshot_bytes);
    return data;
}

LLMDeploymentSnapshot deserialize(const String & data, Int32 version)
{
    if (data.size() > max_snapshot_bytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "LLM deployment configuration exceeds {} bytes", max_snapshot_bytes);
    auto root = Poco::JSON::Parser().parse(data).extract<Poco::JSON::Object::Ptr>();
    if (!root || root->getValue<UInt64>("format_version") != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported LLM deployment configuration version");
    auto rows = root->getArray("deployments");
    if (!rows || rows->size() > max_deployments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid LLM deployment configuration");

    LLMDeploymentSnapshot snapshot;
    snapshot.version = version;
    UnorderedSetWithMemoryTracking<String> names;
    for (size_t i = 0; i < rows->size(); ++i)
    {
        auto row = rows->getObject(static_cast<unsigned int>(i));
        if (!row)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid LLM deployment entry");
        LLMDeployment deployment{
            row->getValue<String>("name"),
            row->getValue<String>("route"),
            row->getValue<String>("credentials"),
            row->getValue<String>("model"),
            row->getValue<UInt64>("weight"),
            row->getValue<UInt64>("max_requests_per_minute"),
            row->getValue<UInt64>("max_concurrent_requests")};
        validate(deployment);
        if (!names.insert(deployment.name).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate LLM deployment '{}'", deployment.name);
        snapshot.deployments.push_back(std::move(deployment));
    }
    return snapshot;
}

}

LLMDeploymentRegistry::LLMDeploymentRegistry(String keeper_path_)
    : keeper_path(std::move(keeper_path_))
{
    if (keeper_path.size() < 2 || keeper_path.front() != '/' || keeper_path.back() == '/')
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "llm_query.keeper_path must be an absolute Keeper path without a trailing slash");
}

LLMDeploymentSnapshot LLMDeploymentRegistry::getSnapshot(ContextPtr context) const
{
    auto component_guard = Coordination::setCurrentComponent("LLMDeploymentRegistry::getSnapshot");
    auto keeper = context->getZooKeeper();
    Coordination::Stat stat;
    String data;
    if (!keeper->tryGet(keeper_path, data, &stat))
        return {};
    return deserialize(data, stat.version);
}

void LLMDeploymentRegistry::insert(ContextPtr context, const VectorWithMemoryTracking<LLMDeployment> & deployments)
{
    auto component_guard = Coordination::setCurrentComponent("LLMDeploymentRegistry::insert");
    for (const auto & deployment : deployments)
        validate(deployment);
    auto keeper = context->getZooKeeper();
    keeper->createAncestors(keeper_path);
    for (size_t attempt = 0; attempt < 10; ++attempt)
    {
        Coordination::Stat stat;
        String data;
        bool exists = keeper->tryGet(keeper_path, data, &stat);
        auto snapshot = exists ? deserialize(data, stat.version) : LLMDeploymentSnapshot{};
        UnorderedSetWithMemoryTracking<String> names;
        for (const auto & deployment : snapshot.deployments)
            names.insert(deployment.name);
        for (const auto & deployment : deployments)
        {
            if (!names.insert(deployment.name).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "LLM deployment '{}' already exists", deployment.name);
            snapshot.deployments.push_back(deployment);
        }
        data = serialize(snapshot.deployments);
        auto code = exists ? keeper->trySet(keeper_path, data, stat.version)
                           : keeper->tryCreate(keeper_path, data, zkutil::CreateMode::Persistent);
        if (code == Coordination::Error::ZOK)
            return;
        if (code != Coordination::Error::ZBADVERSION && code != Coordination::Error::ZNODEEXISTS)
            throw Coordination::Exception::fromPath(code, keeper_path);
    }
    throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Concurrent LLM deployment updates exceeded the retry limit");
}

void LLMDeploymentRegistry::remove(ContextPtr context, const String & name)
{
    auto component_guard = Coordination::setCurrentComponent("LLMDeploymentRegistry::remove");
    auto keeper = context->getZooKeeper();
    for (size_t attempt = 0; attempt < 10; ++attempt)
    {
        Coordination::Stat stat;
        String data;
        if (!keeper->tryGet(keeper_path, data, &stat))
            return;
        auto snapshot = deserialize(data, stat.version);
        auto removed = std::erase_if(
            snapshot.deployments,
            [&](const auto & deployment)
            {
                return deployment.name == name;
            });
        if (!removed)
            return;
        auto code = keeper->trySet(keeper_path, serialize(snapshot.deployments), stat.version);
        if (code == Coordination::Error::ZOK)
            return;
        if (code != Coordination::Error::ZBADVERSION)
            throw Coordination::Exception::fromPath(code, keeper_path);
    }
    throw Exception(ErrorCodes::KEEPER_EXCEPTION, "Concurrent LLM deployment updates exceeded the retry limit");
}

LLMDeploymentRegistry::Reservation::Reservation(std::shared_ptr<AdmissionState> state_, LLMDeployment deployment_, UInt64 version_)
    : deployment(std::move(deployment_))
    , version(version_)
    , state(std::move(state_))
{
}

LLMDeploymentRegistry::Reservation::~Reservation()
{
    if (acquired)
    {
        std::lock_guard lock(state->mutex);
        --state->counters.at(deployment.name).in_flight;
    }
}

std::unique_ptr<LLMDeploymentRegistry::Reservation>
LLMDeploymentRegistry::reserve(const LLMDeploymentSnapshot & snapshot, const String & route)
{
    auto now = std::chrono::steady_clock::now();
    std::lock_guard lock(admission->mutex);

    {
        /// The counters outlive this query; charge both allocation and pruning to the server.
        MemoryTrackerSwitcher switcher(&total_memory_tracker);
        std::erase_if(
            admission->counters,
            [&](const auto & item)
            {
                const auto & [name, counters] = item;
                return !counters.in_flight && now - counters.window_start >= std::chrono::minutes(1)
                    && std::none_of(
                        snapshot.deployments.begin(),
                        snapshot.deployments.end(),
                        [&](const auto & value)
                        {
                            return value.name == name;
                        });
            });
        for (const auto & deployment : snapshot.deployments)
        {
            if (deployment.route == route && deployment.weight)
                admission->counters.try_emplace(deployment.name);
        }
    }

    VectorWithMemoryTracking<const LLMDeployment *> eligible;
    UInt64 total_weight = 0;
    bool route_exists = false;
    for (const auto & deployment : snapshot.deployments)
    {
        if (deployment.route != route)
            continue;
        route_exists = true;
        if (!deployment.weight)
            continue;
        auto & counters = admission->counters.at(deployment.name);
        if (now - counters.window_start >= std::chrono::minutes(1))
        {
            counters.window_start = now;
            counters.requests = 0;
        }
        if ((deployment.max_concurrent_requests && counters.in_flight >= deployment.max_concurrent_requests)
            || (deployment.max_requests_per_minute && counters.requests >= deployment.max_requests_per_minute))
            continue;
        eligible.push_back(&deployment);
        total_weight += deployment.weight;
    }
    if (!route_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown LLM route '{}'", route);
    if (eligible.empty())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "LLM route '{}' has no deployment with available capacity on this server", route);

    auto pick = std::uniform_int_distribution<UInt64>(0, total_weight - 1)(thread_local_rng);
    for (const auto * deployment : eligible)
    {
        if (pick < deployment->weight)
        {
            /// Allocate before consuming admission capacity.
            auto reservation = std::make_unique<Reservation>(admission, *deployment, snapshot.version);
            auto & counters = admission->counters.at(deployment->name);
            ++counters.requests;
            ++counters.in_flight;
            reservation->acquired = true;
            return reservation;
        }
        pick -= deployment->weight;
    }
    std::unreachable();
}

}
