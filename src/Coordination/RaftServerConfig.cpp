#include "RaftServerConfig.h"
#include <unordered_set>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

namespace DB
{
RaftServerConfig::RaftServerConfig(const nuraft::srv_config & cfg) noexcept
    : id(cfg.get_id()), endpoint(cfg.get_endpoint()), learner(cfg.is_learner()), priority(cfg.get_priority())
{
}

RaftServerConfig::operator nuraft::srv_config() const noexcept
{
    return {id, 0, endpoint, "", learner, priority};
}

std::optional<RaftServerConfig> RaftServerConfig::parse(std::string_view server) noexcept
{
    std::vector<std::string_view> parts;
    splitInto<';', '='>(parts, server);

    const bool with_id_endpoint = parts.size() == 2;
    const bool with_server_type = parts.size() == 3;
    const bool with_priority = parts.size() == 4;
    if (!with_id_endpoint && !with_server_type && !with_priority)
        return std::nullopt;

    std::string_view id_str = parts[0];
    if (!id_str.starts_with("server."))
        return std::nullopt;

    id_str = id_str.substr(7);
    if (auto eq_pos = id_str.find('='); std::string_view::npos != eq_pos)
        id_str = id_str.substr(0, eq_pos);

    Int32 id;
    if (!tryParse(id, id_str))
        return std::nullopt;
    if (id <= 0)
        return std::nullopt;

    const std::string_view endpoint = parts[1];
    const size_t port_delimiter = endpoint.find_last_of(':');
    if (port_delimiter == std::string::npos)
        return {};
    const std::string_view port = endpoint.substr(port_delimiter + 1);

    uint16_t port_tmp;
    if (!tryParse(port_tmp, port))
        return std::nullopt;

    RaftServerConfig out{id, endpoint};

    if (with_id_endpoint)
        return out;

    if (parts[2] != "learner" && parts[2] != "participant")
        return std::nullopt;
    out.learner = parts[2] == "learner";
    if (with_server_type)
        return out;

    const std::string_view priority = parts[3];
    if (!tryParse(out.priority, priority))
        return std::nullopt;
    if (out.priority < 0)
        return std::nullopt;

    return out;
}

RaftServers parseRaftServers(std::string_view servers)
{
    std::vector<std::string_view> server_arr;
    std::unordered_set<int32_t> ids;
    std::unordered_set<String> endpoints;
    RaftServers out;

    for (auto & server : splitInto<','>(server_arr, servers))
    {
        if (auto maybe_server = RaftServerConfig::parse(server))
        {
            String endpoint = maybe_server->endpoint;
            if (endpoints.contains(endpoint))
                return {};
            const int id = maybe_server->id;
            if (ids.contains(id))
                return {};

            out.emplace_back(std::move(*maybe_server));
            endpoints.emplace(std::move(endpoint));
            ids.emplace(id);
        }
        else
            return {};
    }

    return out;
}
}
