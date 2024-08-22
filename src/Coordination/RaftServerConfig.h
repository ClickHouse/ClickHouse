#pragma once
#include <base/defines.h>
#include <base/types.h>
#include <fmt/core.h>
#include <libnuraft/srv_config.hxx>

#include <optional>

namespace DB
{
// default- and copy-constructible version of nuraft::srv_config
struct RaftServerConfig
{
    int id;
    String endpoint;
    bool learner;
    int priority;

    constexpr RaftServerConfig() = default;
    constexpr RaftServerConfig(int id_, std::string_view endpoint_, bool learner_ = false, int priority_ = 1)
        : id(id_), endpoint(endpoint_), learner(learner_), priority(priority_)
    {
    }

    constexpr bool operator==(const RaftServerConfig &) const = default;
    explicit RaftServerConfig(const nuraft::srv_config & cfg) noexcept;
    explicit operator nuraft::srv_config() const noexcept;

    /// Parse server in format "server.id=host:port[;learner][;priority]"
    static std::optional<RaftServerConfig> parse(std::string_view server) noexcept;
};

using RaftServers = std::vector<RaftServerConfig>;
/// Parse comma-delimited servers. Check for duplicate endpoints and ids.
/// @returns {} on parsing or validation error.
RaftServers parseRaftServers(std::string_view servers);

struct AddRaftServer : RaftServerConfig
{
};

struct RemoveRaftServer
{
    int id;
};

struct UpdateRaftServerPriority
{
    int id;
    int priority;
};

using ClusterUpdateAction = std::variant<AddRaftServer, RemoveRaftServer, UpdateRaftServerPriority>;
using ClusterUpdateActions = std::vector<ClusterUpdateAction>;
}

template <>
struct fmt::formatter<DB::RaftServerConfig> : fmt::formatter<string_view>
{
    constexpr auto format(const DB::RaftServerConfig & server, format_context & ctx) const
    {
        return fmt::format_to(
            ctx.out(), "server.{}={};{};{}", server.id, server.endpoint, server.learner ? "learner" : "participant", server.priority);
    }
};

template <>
struct fmt::formatter<DB::ClusterUpdateAction> : fmt::formatter<string_view>
{
    constexpr auto format(const DB::ClusterUpdateAction & action, format_context & ctx) const
    {
        if (const auto * add = std::get_if<DB::AddRaftServer>(&action))
            return fmt::format_to(ctx.out(), "(Add server {})", add->id);
        if (const auto * remove = std::get_if<DB::RemoveRaftServer>(&action))
            return fmt::format_to(ctx.out(), "(Remove server {})", remove->id);
        if (const auto * update = std::get_if<DB::UpdateRaftServerPriority>(&action))
            return fmt::format_to(ctx.out(), "(Change server {} priority to {})", update->id, update->priority);
        UNREACHABLE();
    }
};
