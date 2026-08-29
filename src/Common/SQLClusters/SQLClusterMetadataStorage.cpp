#include <Common/SQLClusters/SQLClusterMetadataStorage.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserSQLClusterQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/escapeForFileName.h>
#include <Common/ZooKeeper/KeeperException.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int CLUSTER_ALREADY_EXISTS;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

static const std::string cluster_metadata_config_path = "cluster_metadata";

namespace
{

String getFileName(const String & cluster_name)
{
    return escapeForFileName(cluster_name) + ".sql";
}

}

SQLClusterMetadataStorage::SQLClusterMetadataStorage(ContextPtr context_, String root_path_)
    : WithContext(context_)
    , root_path(std::move(root_path_))
{
    auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage");
    if (root_path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`cluster_metadata.path` cannot be empty");

    if (root_path != "/" && root_path.back() == '/')
        root_path.resize(root_path.size() - 1);
    if (root_path.front() != '/')
        root_path = "/" + root_path;

    auto client = getClient();
    if (root_path != "/" && !client->exists(root_path))
    {
        client->createAncestors(root_path);
        client->createIfNotExists(root_path, "");
    }
}

zkutil::ZooKeeperPtr SQLClusterMetadataStorage::getClient() const
{
    if (!zookeeper_client || zookeeper_client->expired())
    {
        zookeeper_client = getContext()->getZooKeeper();
        zookeeper_client->sync(root_path);
    }
    return zookeeper_client;
}

String SQLClusterMetadataStorage::getPath(const String & file_name) const
{
    if (fs::path(file_name).is_absolute())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);
    return fs::path(root_path) / file_name;
}

bool SQLClusterMetadataStorage::waitUpdateImpl(size_t timeout)
{
    auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::waitUpdate");
    if (!wait_event)
        return true;

    if (wait_event->tryWait(timeout))
        return true;

    String res;
    Coordination::Stat stat;
    if (!getClient()->tryGet(root_path, res, &stat))
        return false;

    return stat.cversion != node_cversion;
}

std::vector<String> SQLClusterMetadataStorage::list() const
{
    auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::list");
    if (!wait_event)
        wait_event = std::make_shared<Poco::Event>();

    Coordination::Stat stat;
    auto children = getClient()->getChildren(root_path, &stat, wait_event);
    node_cversion = stat.cversion;
    return children;
}

String SQLClusterMetadataStorage::read(const String & file_name) const
{
    return getClient()->get(getPath(file_name));
}

void SQLClusterMetadataStorage::write(const String & file_name, const String & data, bool replace)
{
    if (replace)
        getClient()->createOrUpdate(getPath(file_name), data, zkutil::CreateMode::Persistent);
    else
    {
        auto code = getClient()->tryCreate(getPath(file_name), data, zkutil::CreateMode::Persistent);
        if (code == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Metadata file {} for SQL cluster already exists", file_name);
    }
}

bool SQLClusterMetadataStorage::removeNodeIfExists(const String & file_name)
{
    auto code = getClient()->tryRemove(getPath(file_name));
    if (code == Coordination::Error::ZOK)
        return true;
    if (code == Coordination::Error::ZNONODE)
        return false;
    throw Coordination::Exception::fromPath(code, getPath(file_name));
}

std::vector<String> SQLClusterMetadataStorage::listClusterNames() const
{
    std::vector<String> result;
    for (const auto & path : list())
    {
        if (path.ends_with(".sql"))
            result.push_back(unescapeForFileName(fs::path(path).stem().string()));
    }
    return result;
}

bool SQLClusterMetadataStorage::exists(const String & cluster_name) const
{
    return getClient()->exists(getPath(getFileName(cluster_name)));
}

ASTCreateSQLClusterQuery SQLClusterMetadataStorage::readCreateQuery(const String & cluster_name) const
{
    const auto path = getFileName(cluster_name);
    const auto query = read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateSQLClusterQuery parser;
    auto ast = parseQuery(parser, query, "in file " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    return ast->as<const ASTCreateSQLClusterQuery &>();
}

void SQLClusterMetadataStorage::writeCreateQuery(const String & cluster_name, const String & create_statement, bool replace)
{
    write(getFileName(cluster_name), create_statement, replace);
}

void SQLClusterMetadataStorage::remove(const String & cluster_name)
{
    if (!removeNodeIfExists(getFileName(cluster_name)))
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", cluster_name);
}

bool SQLClusterMetadataStorage::removeIfExists(const String & cluster_name)
{
    return removeNodeIfExists(getFileName(cluster_name));
}

bool SQLClusterMetadataStorage::waitUpdate()
{
    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(cluster_metadata_config_path + ".update_timeout_ms", 5000);
    return waitUpdateImpl(timeout);
}

std::unique_ptr<SQLClusterMetadataStorage> SQLClusterMetadataStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    if (!config.has(cluster_metadata_config_path))
        return nullptr;

    const auto path = config.getString(cluster_metadata_config_path + ".path", "");
    if (path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`cluster_metadata.path` must be non-empty when `<cluster_metadata>` is configured");

    return std::make_unique<SQLClusterMetadataStorage>(context_, path);
}

}
