#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>

#include <deque>
#include <mutex>
#include <unordered_set>
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
    if (path.starts_with('/'))
        path.remove_prefix(1);

    if (path.ends_with('/'))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid path '{}': Path should not ends with '/'", path);

    return std::filesystem::path(path);
}

struct InMemoryDirectoryTree::INode
{
    std::weak_ptr<INode> parent;
    std::optional<DirectoryRemoteInfo> remote_info = {};
    std::unordered_map<std::string, std::shared_ptr<INode>> subdirectories = {};
    std::unordered_set<std::string> files = {};

    explicit INode(std::shared_ptr<INode> parent_)
        : parent(parent_)
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
    std::shared_ptr<INode> node = root;

    for (const auto & step : fs::path(path))
    {
        if (auto it = node->subdirectories.find(step); it != node->subdirectories.end())
        {
            node = it->second;
        }
        else if (create_missing)
        {
            node = node->subdirectories.emplace(step, std::make_shared<INode>(node)).first->second;
            CurrentMetrics::add(metric_directories);
        }
        else
        {
            return nullptr;
        }
    }

    return node;
}

void InMemoryDirectoryTree::traverseSubtree(const std::filesystem::path & path, std::function<void(const std::string &, const std::shared_ptr<INode> &)> observe) const
{
    std::deque<std::pair<fs::path, std::shared_ptr<INode>>> unvisited;

    if (auto start_node = walk(path, /*create_missing=*/false))
        unvisited.emplace_back(path, std::move(start_node));

    while (!unvisited.empty())
    {
        auto [node_path, node] = std::move(unvisited.front());
        unvisited.pop_front();

        observe(node_path, node);

        for (const auto & [subdir, subnode] : node->subdirectories)
            unvisited.emplace_back(node_path / subdir, subnode);
    }
}

InMemoryDirectoryTree::InMemoryDirectoryTree(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name)
    : metric_directories(metric_directories_name)
    , metric_files(metric_files_name)
    , root(std::make_shared<INode>(nullptr))
{
}

bool InMemoryDirectoryTree::existsRemotePathUnchanged(const std::string & remote_path, const std::string & etag) const
{
    std::lock_guard guard(mutex);
    const auto it = remote_path_to_inode.find(remote_path);
    if (it == remote_path_to_inode.end())
        return false;

    const auto & inode = it->second;
    if (!inode->remote_info.has_value())
        return false;

    return inode->remote_info->etag == etag;
}

std::optional<DirectoryRemoteInfo> InMemoryDirectoryTree::getDirectoryRemoteInfo(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path, /*create_missing=*/false);
    return inode ? inode->remote_info : std::nullopt;
}

std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> InMemoryDirectoryTree::getSubtreeRemoteInfo(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);

    if (!walk(normalized_path, /*create_missing=*/false))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

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

    remote_path_to_inode[info.remote_path] = inode;
    inode->remote_info = std::move(info);
}

void InMemoryDirectoryTree::unlinkTree(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path, /*create_missing=*/false);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    traverseSubtree(normalized_path, [&](const std::string &, const std::shared_ptr<INode> & node) TSA_REQUIRES(mutex)
    {
        if (node->remote_info.has_value())
            remote_path_to_inode.erase(node->remote_info->remote_path);

        CurrentMetrics::sub(metric_directories);
        CurrentMetrics::sub(metric_files, node->files.size());
    });

    auto inode_parent = inode->parent.lock();
    inode_parent->subdirectories.erase(normalized_path.filename());
}

bool InMemoryDirectoryTree::existsDirectory(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path, /*create_missing=*/false);
    return inode != nullptr;
}

void InMemoryDirectoryTree::moveDirectory(const std::string & from, const std::string & to)
{
    std::lock_guard guard(mutex);
    const auto normalized_from = normalizePath(from);
    const auto normalized_to = normalizePath(to);

    auto inode_from = walk(normalized_from, /*create_missing=*/false);
    auto inode_to_parent = walk(normalized_to.parent_path(), /*create_missing=*/false);

    if (!inode_from)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_from.string());

    if (!inode_to_parent)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_to.parent_path().string());

    if (inode_to_parent->subdirectories.contains(normalized_to.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a subdirectory '{}' under the path '{}', can't move", normalized_to.filename().string(), normalized_to.parent_path().string());

    if (inode_to_parent->files.contains(normalized_to.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a file '{}' under the path '{}', can't move", normalized_to.filename().string(), normalized_to.parent_path().string());

    auto inode_from_parent = inode_from->parent.lock();
    inode_from_parent->subdirectories.erase(normalized_from.filename());
    inode_to_parent->subdirectories.emplace(normalized_to.filename(), inode_from);
}

std::vector<std::string> InMemoryDirectoryTree::listDirectory(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path, /*create_missing=*/false);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    std::vector<std::string> result;
    result.append_range(inode->subdirectories | std::views::keys);
    result.append_range(inode->files);

    return result;
}

bool InMemoryDirectoryTree::existsFile(const std::string & path) const
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path(), /*create_missing=*/false);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    return inode->files.contains(normalized_path.filename());
}

void InMemoryDirectoryTree::addFile(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path(), /*create_missing=*/false);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    if (inode->isVirtual())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Creation of a file under the virtual directory is not possible");

    if (inode->subdirectories.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is a subdirectory '{}' under the path '{}'. Can't create file", normalized_path.filename().string(), normalized_path.parent_path().string());

    if (inode->files.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File '{}' alredy exists", normalized_path.string());

    inode->files.insert(normalized_path.filename());
    CurrentMetrics::add(metric_files);
}

void InMemoryDirectoryTree::removeFile(const std::string & path)
{
    std::lock_guard guard(mutex);
    const auto normalized_path = normalizePath(path);
    const auto inode = walk(normalized_path.parent_path(), /*create_missing=*/false);

    if (!inode)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.string());

    if (!inode->files.contains(normalized_path.filename()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File '{}' does not exist", normalized_path.string());

    inode->files.erase(normalized_path.filename());
    CurrentMetrics::sub(metric_files);
}

}
