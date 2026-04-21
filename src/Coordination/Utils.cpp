#include <Coordination/Utils.h>

#include <filesystem>
#include <unordered_map>

namespace DB
{

struct Node
{
    std::vector<std::shared_ptr<Node>> children;
    bool outdated;
    std::optional<bool> all_childs_are_outdated;
    String path;
};

using NodePtr = std::shared_ptr<Node>;

std::vector<NodePtr> buildGraph(const std::vector<std::pair<String, bool>> & nodes)
{
    std::unordered_map<std::filesystem::path, NodePtr> path_to_node;
    for (const auto & [path_str, is_outdated] : nodes)
    {
        std::filesystem::path path = path_str;
        auto node = std::make_shared<Node>();
        node->outdated = is_outdated;
        node->path = path_str;
        path_to_node[path] = node;
    }

    std::vector<NodePtr> result;
    result.reserve(path_to_node.size());
    for (const auto & [path_str, is_outdated] : nodes)
    {
        std::filesystem::path path = path_str;
        auto initial_node = path_to_node[path];
        result.push_back(initial_node);
        while (path.has_parent_path() && path != path.parent_path())
        {
            path = path.parent_path();
            if (path_to_node.contains(path))
            {
                path_to_node[path]->children.push_back(initial_node);
                break;
            }
        }
    }

    return result;
}

void traverse(NodePtr node)
{
    if (node->all_childs_are_outdated.has_value())
        return;
    node->all_childs_are_outdated = node->outdated;
    for (const auto & child : node->children)
    {
        traverse(child);
        *node->all_childs_are_outdated &= *child->all_childs_are_outdated;
    }
}

std::vector<String> findOldNodes(const std::vector<std::pair<String, bool>> & nodes)
{
    auto tree_nodes = buildGraph(nodes);
    for (const auto & node : tree_nodes)
        traverse(node);
    std::vector<String> result;
    for (const auto & node : tree_nodes)
        if (*node->all_childs_are_outdated)
            result.push_back(node->path);
    return result;
}

}
