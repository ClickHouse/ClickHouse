#include <Common/ZooKeeper/ZooKeeperPathUtils.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <vector>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace zkutil
{

String normalizeZooKeeperPath(std::string zookeeper_path, bool check_starts_with_slash, LoggerPtr log)
{
    if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);
    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (!zookeeper_path.empty() && zookeeper_path.front() != '/')
    {
        /// Do not allow this for new tables, print warning for tables created in old versions
        if (check_starts_with_slash)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must starts with '/', got '{}'", zookeeper_path);
        if (log)
            LOG_WARNING(log, "ZooKeeper path ('{}') does not start with '/'. It will not be supported in future releases", zookeeper_path);
        zookeeper_path = "/" + zookeeper_path;
    }

    return zookeeper_path;
}

String joinZooKeeperPath(std::string_view parent, std::string_view child)
{
    if (parent.empty())
        return String(child);
    if (child.empty())
        return String(parent);
    if (parent.ends_with('/'))
    {
        if (child.starts_with('/'))
            child.remove_prefix(1);
        return String(parent) + String(child);
    }
    if (child.starts_with('/'))
        return String(parent) + String(child);
    return String(parent) + "/" + String(child);
}

String parentZooKeeperPath(std::string_view path)
{
    const auto pos = path.rfind('/');
    if (pos == std::string_view::npos)
        return {};
    if (pos == 0)
        return "/";
    return String(path.substr(0, pos));
}

String zooKeeperNodeName(std::string_view path)
{
    const auto pos = path.rfind('/');
    if (pos == std::string_view::npos)
        return String(path);
    return String(path.substr(pos + 1));
}

String lexicallyNormalizeZooKeeperPath(std::string_view path)
{
    const bool absolute = path.starts_with('/');

    std::vector<std::string_view> components;
    size_t pos = absolute ? 1 : 0;
    while (pos < path.size())
    {
        const size_t next = path.find('/', pos);
        const std::string_view component
            = path.substr(pos, next == std::string_view::npos ? std::string_view::npos : next - pos);

        if (component == "..")
        {
            if (!components.empty() && components.back() != "..")
                components.pop_back();
            else if (!absolute)
                components.push_back(component);
            /// A `..` at the root of an absolute path has nothing to go up to, so it is dropped.
        }
        else if (!component.empty() && component != ".")
        {
            components.push_back(component);
        }

        if (next == std::string_view::npos)
            break;
        pos = next + 1;
    }

    String result = absolute ? "/" : "";
    for (size_t i = 0; i < components.size(); ++i)
    {
        if (i != 0)
            result += '/';
        result += components[i];
    }
    return result;
}

String extractZooKeeperName(const String & path)
{
    if (path.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path should not be empty");
    if (path[0] == '/')
        return String(DEFAULT_ZOOKEEPER_NAME);
    auto pos = path.find(":/");
    if (pos != String::npos && pos < path.find('/'))
    {
        auto zookeeper_name = path.substr(0, pos);
        if (zookeeper_name.empty())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Zookeeper path should start with '/' or '<auxiliary_zookeeper_name>:/'");
        return zookeeper_name;
    }
    return String(DEFAULT_ZOOKEEPER_NAME);
}

String extractZooKeeperPath(const String & path, bool check_starts_with_slash, LoggerPtr log)
{
    if (path.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path should not be empty");
    if (path[0] == '/')
        return normalizeZooKeeperPath(path, check_starts_with_slash, log);
    auto pos = path.find(":/");
    if (pos != String::npos && pos < path.find('/'))
    {
        return normalizeZooKeeperPath(path.substr(pos + 1, String::npos), check_starts_with_slash, log);
    }
    return normalizeZooKeeperPath(path, check_starts_with_slash, log);
}

String extractZooKeeperPathAndCollapseTrailingSlashes(const String & path, bool check_starts_with_slash, LoggerPtr log)
{
    String result = extractZooKeeperPath(path, check_starts_with_slash, log);
    /// extractZooKeeperPath (via normalizeZooKeeperPath) strips only a single trailing slash, so a path
    /// like "/a//" keeps a leftover one. Collapse all of them (keeping the leading root '/') to get a
    /// canonical form: the interpreter concatenates "path + /replicas", and comparisons must treat
    /// "/a", "/a/" and "/a//" as the same keeper path.
    while (result.size() > 1 && result.back() == '/')
        result.pop_back();
    return result;
}

}
