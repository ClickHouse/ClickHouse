#include <filesystem>
#include <Core/Settings.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateRewriteRuleQuery.h>
#include <Parsers/parseQuery.h>
#include <base/sort.h>
#include <boost/algorithm/hex.hpp>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/RewriteRules/RewriteRulesStorage.h>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int REWRITE_RULE_DOESNT_EXIST;
    extern const int REWRITE_RULE_ALREADY_EXISTS;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

static const std::string query_rules_storage_config_path = "query_rules_storage";

namespace
{
    std::string getFileName(const std::string & rule_name)
    {
        return escapeForFileName(rule_name) + ".sql";
    }
}

class RewriteRulesStorage::IRewriteRulesStorage
{
public:
    virtual ~IRewriteRulesStorage() = default;

    virtual bool exists(const std::string & path) const = 0;

    virtual std::vector<std::string> list() const = 0;

    virtual std::string read(const std::string & path) const = 0;

    virtual void write(const std::string & path, const std::string & data, bool replace) = 0;

    virtual void remove(const std::string & path) = 0;

    virtual bool removeIfExists(const std::string & path) = 0;

    virtual bool isReplicated() const = 0;

    virtual bool waitUpdate(size_t /* timeout */) { return false; }
};


class RewriteRulesStorage::LocalStorage : public IRewriteRulesStorage, protected WithContext
{
public:
    LocalStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        if (fs::exists(root_path))
            cleanup();
    }

    ~LocalStorage() override = default;

    bool isReplicated() const override { return false; }

    std::vector<std::string> list() const override
    {
        if (!fs::exists(root_path))
            return {};

        std::vector<std::string> elements;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
            {
                elements.push_back(it->path());
            }
            else
            {
                LOG_WARNING(
                    getLogger("LocalStorage"),
                    "Unexpected file {} in query rule directory",
                    current_path.filename().string()
                );
            }
        }
        return elements;
    }

    bool exists(const std::string & file_name) const override
    {
        return fs::exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        ReadBufferFromFile in(getPath(file_name));
        std::string data;
        readStringUntilEOF(data, in);
        return data;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        if (!replace && fs::exists(file_name))
        {
            throw Exception(
                ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                "File {} for query rule already exists",
                file_name
            );
        }

        fs::create_directories(root_path);

        auto tmp_path = getPath(file_name + ".tmp");
        WriteBufferFromFile out(tmp_path, data.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(data, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(tmp_path, getPath(file_name));
    }

    void remove(const std::string & file_name) override
    {
        if (!removeIfExists(file_name))
        {
            throw Exception(
                ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
                "Cannot remove `{}`, because it doesn't exist", file_name);
        }
    }

    bool removeIfExists(const std::string & file_name) override
    {
        return fs::remove(getPath(file_name));
    }

protected:
    std::string root_path;

    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);

        return fs::path(root_path) / file_name_as_path;
    }

private:
    void cleanup()
    {
        std::vector<std::string> files_to_remove;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".tmp")
                files_to_remove.push_back(current_path);
        }
        for (const auto & file : files_to_remove)
            fs::remove(file);
    }
};

class RewriteRulesStorage::ZooKeeperStorage : public IRewriteRulesStorage, protected WithContext
{
private:
    std::string root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable Coordination::EventPtr wait_event;
    mutable Int32 collections_node_cversion = 0;

public:
    ZooKeeperStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        if (root_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Collections path cannot be empty");

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

    ~ZooKeeperStorage() override = default;

    bool isReplicated() const override { return true; }

    bool waitUpdate(size_t timeout) override
    {
        if (!wait_event)
        {
            return true;
        }

        if (wait_event->tryWait(timeout))
        {
            return true;
        }

        std::string res;
        Coordination::Stat stat;

        if (!getClient()->tryGet(root_path, res, &stat))
        {
            chassert(false);
            return false;
        }

        return stat.cversion != collections_node_cversion;
    }

    std::vector<std::string> list() const override
    {
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        Coordination::Stat stat;
        auto children = getClient()->getChildren(root_path, &stat, wait_event);
        collections_node_cversion = stat.cversion;
        return children;
    }

    bool exists(const std::string & file_name) const override
    {
        return getClient()->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        return getClient()->get(getPath(file_name));
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        if (replace)
        {
            getClient()->createOrUpdate(getPath(file_name), data, zkutil::CreateMode::Persistent);
        }
        else
        {
            auto code = getClient()->tryCreate(getPath(file_name), data, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZNODEEXISTS)
            {
                throw Exception(
                    ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                    "File {} for query rule already exists",
                    file_name
                );
            }
        }
    }

    void remove(const std::string & file_name) override
    {
        getClient()->remove(getPath(file_name));
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto code = getClient()->tryRemove(getPath(file_name));
        if (code == Coordination::Error::ZOK)
            return true;
        if (code == Coordination::Error::ZNONODE)
            return false;
        throw Coordination::Exception::fromPath(code, getPath(file_name));
    }

private:
    zkutil::ZooKeeperPtr getClient() const
    {
        if (!zookeeper_client || zookeeper_client->expired())
        {
            zookeeper_client = getContext()->getZooKeeper();
            zookeeper_client->sync(root_path);
        }
        return zookeeper_client;
    }

    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);

        return fs::path(root_path) / file_name_as_path;
    }
};

RewriteRulesStorage::RewriteRulesStorage(
    std::shared_ptr<IRewriteRulesStorage> storage_,
    ContextPtr context_)
    : WithContext(context_)
    , impl_storage(std::move(storage_))
{
}

MutableRewriteRuleObjectPtr RewriteRulesStorage::get(const std::string & rule_name) const
{
    const auto query = readCreateQuery(rule_name);
    return RewriteRuleObject::create(query);
}

RewriteRuleObjectsMap RewriteRulesStorage::getAll() const
{
    RewriteRuleObjectsMap result;
    for (const auto & rule_name : listRules())
    {
        if (result.contains(rule_name))
        {
            throw Exception(
                ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                "Found duplicate rewrite rule `{}`",
                rule_name);
        }
        result.emplace(rule_name, get(rule_name));
    }
    return result;
}

void RewriteRulesStorage::create(const RewriteRuleObjectPtr & create_query)
{
    writeCreateQuery(create_query->getCreateQuery().rule_name, create_query->getCreateQuery().whole_query);
}

void RewriteRulesStorage::remove(const std::string & rule_name)
{
    impl_storage->remove(getFileName(rule_name));
}

bool RewriteRulesStorage::removeIfExists(const std::string & rule_name)
{
    return impl_storage->removeIfExists(getFileName(rule_name));
}

void RewriteRulesStorage::update(const RewriteRuleObjectPtr & update_query)
{
    writeCreateQuery(
        update_query->getCreateQuery().rule_name,
        update_query->getCreateQuery().whole_query,
        true
    );
}

std::vector<std::string> RewriteRulesStorage::listRules() const
{
    auto paths = impl_storage->list();
    std::vector<std::string> rules;
    rules.reserve(paths.size());
    for (const auto & path : paths)
        rules.push_back(unescapeForFileName(std::filesystem::path(path).stem()));
    return rules;
}

ASTCreateRewriteRuleQuery RewriteRulesStorage::readCreateQuery(const std::string & rule_name) const
{
    const auto path = getFileName(rule_name);
    auto query = impl_storage->read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateRewriteRuleQuery parser(query.data() + query.size());
    auto ast = parseQuery(parser, query.data(), query.data() + query.size(), "in file " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    const auto & create_query = ast->as<const ASTCreateRewriteRuleQuery &>();
    return create_query;
}

void RewriteRulesStorage::writeCreateQuery(const String & rule_name, const String & create_query, bool replace)
{
    impl_storage->write(getFileName(rule_name), create_query, replace);
}

bool RewriteRulesStorage::isReplicated() const
{
    return impl_storage->isReplicated();
}

bool RewriteRulesStorage::waitUpdate()
{
    if (!impl_storage->isReplicated())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported");
    }

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(query_rules_storage_config_path + ".update_timeout_ms", 5000);

    return impl_storage->waitUpdate(timeout);
}

std::unique_ptr<RewriteRulesStorage> RewriteRulesStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(query_rules_storage_config_path + ".type", "local");

    if (storage_type == "local")
    {
        const auto path = config.getString(
            query_rules_storage_config_path + ".path",
            std::filesystem::path(context_->getPath()) / "query_rules");

        LOG_TRACE(getLogger("RewriteRulesStorage"),
                  "Using local storage for named collections at path: {}", path);

        auto local_storage = std::make_unique<RewriteRulesStorage::LocalStorage>(context_, path);

        return std::unique_ptr<RewriteRulesStorage>(
            new RewriteRulesStorage(std::move(local_storage), context_));
    } else if (storage_type == "zookeeper" || storage_type == "keeper")
    {
        const auto path = config.getString(query_rules_storage_config_path + ".path");

        LOG_TRACE(getLogger("RewriteRulesStorage"),
                  "Using zookeeper storage for named collections at path: {}", path);

        auto zk_storage = std::make_unique<RewriteRulesStorage::ZooKeeperStorage>(context_, path);

        return std::unique_ptr<RewriteRulesStorage>(
            new RewriteRulesStorage(std::move(zk_storage), context_));
    }

    throw Exception(
        ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown storage for rewrite rules: {}", storage_type);
}

}
