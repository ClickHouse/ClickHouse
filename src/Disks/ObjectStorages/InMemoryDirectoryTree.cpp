#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

#include <base/scope_guard.h>

#include <mutex>
#include <filesystem>
#include <memory>
#include <ranges>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::filesystem::path normalizePath(std::string_view path)
{
    if (path.ends_with('/'))
        path.remove_suffix(1);

    if (path.starts_with('/'))
        path.remove_prefix(1);

    return std::filesystem::path(path);
}

struct InMemoryDirectoryTree::INode
{
    std::weak_ptr<INode> parent;
    std::string last_directory_name;

    std::optional<DirectoryRemoteInfo> remote_info = {};
    std::unordered_map<std::string, std::shared_ptr<INode>> subdirectories = {};

    explicit INode(std::shared_ptr<INode> parent_, std::string last_directory_name_)
        : parent(parent_)
        , last_directory_name(std::move(last_directory_name_))
    {
    }

    bool isVirtual() const
    {
        return !remote_info.has_value();
    }

    bool isRoot() const
    {
        return parent.lock() == nullptr;
    }
};

std::shared_ptr<InMemoryDirectoryTree::INode> InMemoryDirectoryTree::walk(const std::filesystem::path & path, bool create_missing) const
{
    chassert(normalizePath(path.string()) == path);
    std::shared_ptr<INode> node = root;

    for (const auto & step : fs::path(path))
    {
        if (auto it = node->subdirectories.find(step); it != node->subdirectories.end())
            node = it->second;
        else if (create_missing)
            node = node->subdirectories.emplace(step, std::make_shared<INode>(node, step)).first->second;
        else
            return nullptr;
    }

    return node;
}

void InMemoryDirectoryTree::traverseSubtree(const std::filesystem::path & path, const ObserveFunction & observe) const
{
    chassert(normalizePath(path.string()) == path);
    std::vector<std::pair<fs::path, std::shared_ptr<INode>>> unvisited;

    if (auto start_node = walk(path))
        unvisited.emplace_back("", std::move(start_node));

    while (!unvisited.empty())
    {
        auto [node_path, node] = std::move(unvisited.back());
        unvisited.pop_back();

        observe(node_path, node);

        unvisited.reserve(unvisited.size() + node->subdirectories.size());
        for (const auto & [subdir, subnode] : node->subdirectories)
            unvisited.emplace_back(node_path / subdir, subnode);
    }
}

std::filesystem::path InMemoryDirectoryTree::determineNodePath(std::shared_ptr<INode> node) const TSA_REQUIRES(mutex)
{
    std::vector<std::string> path_parts;
    while (auto parent = node->parent.lock())
    {
        path_parts.push_back(node->last_directory_name);
        node = parent;
    }

    fs::path nodes_path;
    for (const auto & step : path_parts | std::views::reverse)
        nodes_path /= step;

    return nodes_path;
}

std::shared_ptr<InMemoryDirectoryTree::INode> InMemoryDirectoryTree::trimDanglingVirtualPath(std::shared_ptr<INode> node)
{
    while (!node->isRoot() && node->isVirtual())
    {
        if (!node->subdirectories.empty())
            break;

        auto parent = node->parent.lock();
        parent->subdirectories.erase(node->last_directory_name);
        node = parent;
    }

    return node;
}

InMemoryDirectoryTree::InMemoryDirectoryTree(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name)
    : remote_layout_directories_count(metric_directories_name)
    , remote_layout_files_count(metric_files_name)
    , root(std::make_shared<INode>(nullptr, ""))
{
}

void InMemoryDirectoryTree::apply(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout)
{
    std::lock_guard guard(mutex);

    /// Unlink old tree
    remote_layout_directories_count.changeTo(0);
    remote_layout_files_count.changeTo(0);
    remote_path_to_inode.clear();
    root->remote_info.reset();
    root->subdirectories.clear();

    remote_path_to_inode.reserve(remote_layout.size());
    for (auto & [path, info] : remote_layout)
    {
        const auto inode = walk(normalizePath(path), /*create_missing=*/true);
        remote_path_to_inode[info.remote_path] = inode;
        inode->remote_info = std::move(info);
        remote_layout_directories_count.add();
        remote_layout_files_count.add(inode->remote_info->file_names.size());
    }
}

std::optional<std::pair<std::string, DirectoryRemoteInfo>> InMemoryDirectoryTree::lookupDirectoryIfNotChanged(const std::string & remote_path, const std::string & etag) const
{
    std::lock_guard guard(mutex);
    const auto it = remote_path_to_inode.find(remote_path);
    if (it == remote_path_to_inode.end())
        return std::nullopt;

    const auto & inode = it->second;
    if (!inode->remote_info.has_value())
        return std::nullopt;

    if (inode->remote_info->etag != etag)
        return std::nullopt;

    return std::make_pair(determineNodePath(inode), inode->remote_info.value());
}

std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> InMemoryDirectoryTree::getSubtreeRemoteInfo(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> subtree_info;
    traverseSubtree(normalized_path, [&subtree_info](const std::string & node_path, const std::shared_ptr<INode> & node)
    {
        subtree_info[node_path] = node->remote_info;
    });

    return subtree_info;
}

void InMemoryDirectoryTree::recordDirectoryPath(const std::string & path, DirectoryRemoteInfo info)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path, /*create_missing=*/true);

    if (!inode->isVirtual())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' was already recorded", normalized_path.string());

    if (!inode->subdirectories.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", normalized_path.string());

    remote_path_to_inode[info.remote_path] = inode;
    inode->remote_info = std::move(info);
    remote_layout_directories_count.add();
    remote_layout_files_count.add(inode->remote_info->file_names.size());
}

void InMemoryDirectoryTree::unlinkTree(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    if (inode->isRoot())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is root", normalized_path.string());

    traverseSubtree(normalized_path, [&](const std::string &, const std::shared_ptr<INode> & node) TSA_REQUIRES(mutex)
    {
        if (!node->remote_info.has_value())
            return;

        remote_path_to_inode.erase(node->remote_info->remote_path);
        remote_layout_files_count.sub(node->remote_info->file_names.size());
        remote_layout_directories_count.sub();
    });

    auto inode_parent = inode->parent.lock();
    inode_parent->subdirectories.erase(normalized_path.filename());
    trimDanglingVirtualPath(inode_parent);
}

void InMemoryDirectoryTree::moveDirectory(const std::string & from, const std::string & to)
{
    std::lock_guard guard(mutex);
    const auto normalized_from = normalizePath(from);
    const auto normalized_to = normalizePath(to);

    const auto inode_from = walk(normalized_from);
    const auto inode_to_parent = walk(normalized_to.parent_path(), /*create_missing=*/true);

    if (!inode_from)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_from.string());

    if (inode_from->isRoot())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is root", normalized_from.string());

    const auto inode_from_parent = inode_from->parent.lock();
    scope_guard trim_from = [&]() TSA_REQUIRES(mutex) { trimDanglingVirtualPath(inode_from_parent); };
    scope_guard trim_to = [&]() TSA_REQUIRES(mutex) { trimDanglingVirtualPath(inode_to_parent); };

    if (inode_to_parent->subdirectories.contains(normalized_to.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a subdirectory '{}' under the path '{}', can't move", normalized_to.filename().string(), normalized_to.parent_path().string());

    if (!inode_to_parent->isVirtual())
        if (inode_to_parent->remote_info->file_names.contains(normalized_to.filename()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a file '{}' under the path '{}', can't move", normalized_to.filename().string(), normalized_to.parent_path().string());

    inode_from_parent->subdirectories.erase(normalized_from.filename());
    inode_to_parent->subdirectories.emplace(normalized_to.filename(), inode_from);

    inode_from->parent = inode_to_parent;
    inode_from->last_directory_name = normalized_to.filename();
}

std::vector<std::string> InMemoryDirectoryTree::listDirectory(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path);

    if (!inode)
        return {};

    std::vector<std::string> result;
    result.append_range(inode->subdirectories | std::views::keys);

    if (!inode->isVirtual())
        result.append_range(inode->remote_info->file_names);

    return result;
}

std::pair<bool, std::optional<DirectoryRemoteInfo>> InMemoryDirectoryTree::existsDirectory(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path);

    if (!inode)
        return {false, std::nullopt};

    if (inode->isVirtual())
        return {true, std::nullopt};

    return {true, inode->remote_info};
}

bool InMemoryDirectoryTree::existsFile(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path());

    if (!inode)
        return false;

    if (inode->isVirtual())
        return false;

    return inode->remote_info->file_names.contains(normalized_path.filename());
}

void InMemoryDirectoryTree::addFile(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path());

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    if (inode->isVirtual())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Creation of a file under the virtual directory is not possible");

    if (inode->subdirectories.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a subdirectory '{}' under the path '{}'. Can't create file", normalized_path.filename().string(), normalized_path.parent_path().string());

    if (inode->remote_info->file_names.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File '{}' already exists", normalized_path.string());

    inode->remote_info->file_names.insert(normalized_path.filename());
    remote_layout_files_count.add();
}

void InMemoryDirectoryTree::removeFile(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path());

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    if (inode->isVirtual())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Removal of a file under the virtual directory is not possible");

    if (!inode->remote_info->file_names.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File '{}' does not exist", normalized_path.string());

    inode->remote_info->file_names.erase(normalized_path.filename());
    remote_layout_files_count.sub();
}

}
