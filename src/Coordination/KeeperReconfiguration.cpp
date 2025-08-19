#include "KeeperReconfiguration.h"
#include <unordered_set>
#include <base/find_symbols.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ClusterUpdateActions joiningToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view joining)
{
    ClusterUpdateActions out;
    std::unordered_set<String> endpoints;

    for (const auto & server : cfg->get_servers())
        endpoints.emplace(server->get_endpoint());

    // We can either add new servers or change weight of existing ones.
    // It makes no sense having a server in _joining_ which is identical to existing one including
    // weight, so such requests are declined.
    for (const RaftServerConfig & update : parseRaftServers(joining))
        if (auto server_ptr = cfg->get_server(update.id))
        {
            if (update.endpoint != server_ptr->get_endpoint() || update.learner != server_ptr->is_learner()
                || update.priority == server_ptr->get_priority())
                return {}; // can't change server endpoint/type due to NuRaft API limitations
            out.emplace_back(UpdateRaftServerPriority{.id = update.id, .priority = update.priority});
        }
        else if (endpoints.contains(update.endpoint))
            return {};
        else
            out.emplace_back(AddRaftServer{update});

    return out;
}

ClusterUpdateActions leavingToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view leaving)
{
    std::vector<std::string_view> leaving_arr;
    splitInto<','>(leaving_arr, leaving);
    if (leaving_arr.size() >= cfg->get_servers().size())
        return {};

    std::unordered_set<int32_t> remove_ids;
    ClusterUpdateActions out;

    for (std::string_view leaving_server : leaving_arr)
    {
        int32_t id;
        if (!tryParse(id, leaving_server))
            return {};

        if (remove_ids.contains(id))
            continue;

        if (auto ptr = cfg->get_server(id))
            out.emplace_back(RemoveRaftServer{.id = id});
        else
            return {};

        remove_ids.emplace(id);
    }

    return out;
}

String serializeClusterConfig(const ClusterConfigPtr & cfg, const ClusterUpdateActions & updates)
{
    RaftServers new_config;
    std::unordered_set<int32_t> remove_update_ids;

    for (const auto & update : updates)
    {
        if (const auto * add = std::get_if<AddRaftServer>(&update))
            new_config.emplace_back(*add);
        else if (const auto * remove = std::get_if<RemoveRaftServer>(&update))
            remove_update_ids.insert(remove->id);
        else if (const auto * priority = std::get_if<UpdateRaftServerPriority>(&update))
        {
            remove_update_ids.insert(priority->id);
            new_config.emplace_back(RaftServerConfig{*cfg->get_server(priority->id)});
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected update");
    }

    for (const auto & item : cfg->get_servers())
        if (!remove_update_ids.contains(item->get_id()))
            new_config.emplace_back(RaftServerConfig{*item});

    return fmt::format("{}", fmt::join(new_config.begin(), new_config.end(), "\n"));
}
}
