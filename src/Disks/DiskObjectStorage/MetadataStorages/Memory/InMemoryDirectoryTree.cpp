#include <Disks/DiskObjectStorage/MetadataStorages/Memory/InMemoryDirectoryTree.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

std::vector<std::string> splitPath(std::string_view path)
{
    std::vector<std::string> components;
    size_t pos = 0;
    while (pos < path.size())
    {
        size_t slash = path.find('/', pos);
        if (slash == std::string_view::npos)
            slash = path.size();
        if (slash > pos)
            components.emplace_back(path.substr(pos, slash - pos));
        pos = slash + 1;
    }
    return components;
}

std::string joinPath(std::string_view directory, std::string_view name)
{
    if (directory.empty())
        return std::string(name);
    std::string result(directory);
    if (!result.ends_with('/'))
        result += '/';
    result += name;
    return result;
}

std::string_view normalizePath(std::string_view path)
{
    while (!path.empty() && path.ends_with('/'))
        path = path.substr(0, path.size() - 1);
    return path;
}

}

InMemoryDirectoryTree::NodePtr InMemoryDirectoryTree::resolve(std::string_view path) const
{
    NodePtr node = root;
    for (const auto & component : splitPath(path))
    {
        if (node->isFile())
            return nullptr;
        auto it = node->children.find(component);
        if (it == node->children.end())
            return nullptr;
        node = it->second;
    }
    return node;
}

std::pair<InMemoryDirectoryTree::Node *, std::string> InMemoryDirectoryTree::resolveParent(std::string_view path) const
{
    auto components = splitPath(path);
    if (components.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The root directory has no parent");

    Node * node = root.get();
    for (size_t i = 0; i + 1 < components.size(); ++i)
    {
        auto it = node->children.find(components[i]);
        if (it == node->children.end() || it->second->isFile())
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
                "Cannot resolve `{}`: `{}` is not an existing directory", path, components[i]);
        node = it->second.get();
    }
    return {node, std::move(components.back())};
}

bool InMemoryDirectoryTree::existsFile(std::string_view path) const
{
    auto node = resolve(path);
    return node && node->isFile();
}

bool InMemoryDirectoryTree::existsDirectory(std::string_view path) const
{
    auto node = resolve(path);
    return node && !node->isFile();
}

bool InMemoryDirectoryTree::existsFileOrDirectory(std::string_view path) const
{
    return resolve(path) != nullptr;
}

InMemoryDirectoryTree::Record * InMemoryDirectoryTree::getRecord(std::string_view path)
{
    auto node = resolve(path);
    return node && node->isFile() ? &*node->record : nullptr;
}

const InMemoryDirectoryTree::Record * InMemoryDirectoryTree::getRecord(std::string_view path) const
{
    auto node = resolve(path);
    return node && node->isFile() ? &*node->record : nullptr;
}

std::vector<std::string> InMemoryDirectoryTree::listDirectory(std::string_view path) const
{
    auto node = resolve(path);
    if (!node || node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path);

    std::vector<std::string> result;
    result.reserve(node->children.size());
    for (const auto & [name, child] : node->children)
        result.push_back(joinPath(normalizePath(path), name));
    return result;
}

std::optional<InMemoryDirectoryTree::Record> InMemoryDirectoryTree::putFile(std::string_view path, Record record)
{
    auto [parent, leaf] = resolveParent(path);

    std::optional<Record> displaced;
    if (auto it = parent->children.find(leaf); it != parent->children.end())
    {
        if (!it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Cannot create file `{}`: a directory with this name exists", path);
        displaced.emplace(std::move(*it->second->record));
        parent->children.erase(it);
    }

    auto node = std::make_shared<Node>();
    node->record.emplace(std::move(record));
    parent->children.emplace(std::move(leaf), std::move(node));
    return displaced;
}

InMemoryDirectoryTree::Record InMemoryDirectoryTree::removeFile(std::string_view path)
{
    auto [parent, leaf] = resolveParent(path);

    auto it = parent->children.find(leaf);
    if (it == parent->children.end() || !it->second->isFile())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);

    Record record = std::move(*it->second->record);
    parent->children.erase(it);
    return record;
}

void InMemoryDirectoryTree::createDirectory(std::string_view path)
{
    auto [parent, leaf] = resolveParent(path);

    if (auto it = parent->children.find(leaf); it != parent->children.end())
    {
        if (it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Cannot create directory `{}`: a file with this name exists", path);
        return;
    }

    parent->children.emplace(std::move(leaf), std::make_shared<Node>());
}

void InMemoryDirectoryTree::createDirectoryRecursive(std::string_view path)
{
    Node * node = root.get();
    for (const auto & component : splitPath(path))
    {
        auto it = node->children.find(component);
        if (it == node->children.end())
            it = node->children.emplace(component, std::make_shared<Node>()).first;
        else if (it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot create directory `{}`: `{}` is a file", path, component);
        node = it->second.get();
    }
}

void InMemoryDirectoryTree::removeDirectory(std::string_view path)
{
    auto [parent, leaf] = resolveParent(path);

    auto it = parent->children.find(leaf);
    if (it == parent->children.end() || it->second->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path);

    if (!it->second->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot remove directory `{}`: `{}` remains under it", path, it->second->children.begin()->first);

    parent->children.erase(it);
}

void InMemoryDirectoryTree::removeSubtree(std::string_view path, const std::function<void(const std::string &, Record &)> & visitor)
{
    auto node = resolve(path);
    if (!node)
        return;

    std::function<void(Node &, const std::string &)> visit = [&](Node & current, const std::string & relative_path)
    {
        if (current.isFile())
        {
            visitor(relative_path.empty() ? "." : relative_path, *current.record);
            return;
        }
        for (auto & [name, child] : current.children)
            visit(*child, relative_path.empty() ? name : relative_path + "/" + name);
    };
    visit(*node, "");

    auto components = splitPath(path);
    if (components.empty())
    {
        root->children.clear();
        return;
    }
    auto [parent, leaf] = resolveParent(path);
    parent->children.erase(leaf);
}

std::optional<InMemoryDirectoryTree::Record> InMemoryDirectoryTree::moveFile(std::string_view from, std::string_view to, bool replace)
{
    auto [from_parent, from_leaf] = resolveParent(from);
    auto from_it = from_parent->children.find(from_leaf);
    if (from_it == from_parent->children.end() || !from_it->second->isFile())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", from);

    auto [to_parent, to_leaf] = resolveParent(to);

    std::optional<Record> displaced;
    if (auto to_it = to_parent->children.find(to_leaf); to_it != to_parent->children.end())
    {
        if (!replace || !to_it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File `{}` already exists", to);
        displaced.emplace(std::move(*to_it->second->record));
        to_parent->children.erase(to_it);
    }

    /// `from_it` stays valid: resolving `to` does not mutate the tree.
    auto node = std::move(from_it->second);
    from_parent->children.erase(from_it);
    to_parent->children.emplace(std::move(to_leaf), std::move(node));
    return displaced;
}

void InMemoryDirectoryTree::moveDirectory(std::string_view from, std::string_view to)
{
    auto from_node = resolve(from);
    if (!from_node || from_node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", from);

    /// A destination inside the moved directory would create a cycle.
    {
        NodePtr node = root;
        for (const auto & component : splitPath(to))
        {
            if (node == from_node)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory `{}` under itself (`{}`)", from, to);
            if (node->isFile())
                break;
            auto it = node->children.find(component);
            if (it == node->children.end())
                break;
            node = it->second;
        }
    }

    auto [to_parent, to_leaf] = resolveParent(to);
    if (to_parent->children.contains(to_leaf))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File or directory `{}` already exists", to);

    auto [from_parent, from_leaf] = resolveParent(from);
    auto node = std::move(from_parent->children.at(from_leaf));
    from_parent->children.erase(from_leaf);
    to_parent->children.emplace(std::move(to_leaf), std::move(node));
}

void InMemoryDirectoryTree::forEachRecordUnder(std::string_view path, const std::function<void(const std::string &, Record &)> & visitor) const
{
    auto node = resolve(path);
    if (!node || node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path);

    std::function<void(Node &, const std::string &)> visit = [&](Node & current, const std::string & full_path)
    {
        if (current.isFile())
        {
            visitor(full_path, *current.record);
            return;
        }
        for (auto & [name, child] : current.children)
            visit(*child, joinPath(full_path, name));
    };
    visit(*node, std::string(normalizePath(path)));
}

}
