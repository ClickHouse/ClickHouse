#include <algorithm>
#include <charconv>
#include <filesystem>
#include <optional>
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
#include <Common/ZooKeeper/ZooKeeperCommon.h>
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

    /// Local storage prefixes each stored rule with a monotonically increasing creation
    /// order on its own first line: `<order>\n<CREATE query>`. The order makes the load
    /// order match the creation order deterministically, without depending on the file's
    /// mtime (coarse granularity, not preserved across backup/restore). The Keeper backend
    /// does not need this: it orders by the znode `czxid`, a persisted monotonic creation id.
    constexpr char creation_order_separator = '\n';

    /// Splits the stored `<order>\n<query>` content into the creation order and the query.
    std::pair<UInt64, std::string> splitCreationOrder(const std::string & content)
    {
        const auto pos = content.find(creation_order_separator);
        if (pos == std::string::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query rule storage entry is missing its creation-order header");

        UInt64 order = 0;
        const auto * first = content.data();
        const auto * last = content.data() + pos;
        const auto result = std::from_chars(first, last, order);
        if (result.ec != std::errc{} || result.ptr != last)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query rule storage entry has a malformed creation-order header");

        return {order, content.substr(pos + 1)};
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

        std::vector<std::pair<fs::path, UInt64>> entries;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
            {
                const auto order = splitCreationOrder(readFileRaw(current_path)).first;
                entries.emplace_back(current_path, order);
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

        /// Sort by the persisted creation order so the load order matches the creation order
        /// deterministically. Tie-break by path for determinism (orders are unique in
        /// practice, since each `CREATE RULE` takes the next value under the global lock).
        std::sort(entries.begin(), entries.end(),
            [](const auto & a, const auto & b)
            {
                if (a.second != b.second)
                    return a.second < b.second;
                return a.first < b.first;
            });

        std::vector<std::string> elements;
        elements.reserve(entries.size());
        for (auto & [p, _] : entries)
            elements.push_back(p.string());
        return elements;
    }

    bool exists(const std::string & file_name) const override
    {
        return fs::exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        /// Strip the creation-order header; callers expect the bare `CREATE RULE` query.
        return splitCreationOrder(readFileRaw(getPath(file_name))).second;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        const auto target_path = getPath(file_name);

        UInt64 creation_order = 0;
        if (fs::exists(target_path))
        {
            if (!replace)
            {
                throw Exception(
                    ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                    "File {} for query rule already exists",
                    file_name
                );
            }
            /// `ALTER RULE` keeps the rule's place in creation order, so reuse the order
            /// already persisted for it.
            creation_order = splitCreationOrder(readFileRaw(target_path)).first;
        }
        else if (replace)
        {
            /// `ALTER RULE` must only update an existing rule, never (re)create one.
            throw Exception(
                ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
                "File {} for query rule doesn't exist",
                file_name
            );
        }
        else
        {
            /// `CREATE RULE` takes the next creation order. This runs under the global
            /// `RewriteRules::mutex` and local storage is not replicated, so reading the
            /// current maximum and writing the new file cannot race.
            creation_order = nextCreationOrder();
        }

        fs::create_directories(root_path);

        const auto stored = std::to_string(creation_order) + creation_order_separator + data;

        auto tmp_path = getPath(file_name + ".tmp");
        WriteBufferFromFile out(tmp_path, stored.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(stored, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(tmp_path, target_path);
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
    static std::string readFileRaw(const fs::path & path)
    {
        ReadBufferFromFile in(path);
        std::string data;
        readStringUntilEOF(data, in);
        return data;
    }

    /// The next creation order for a new rule: one past the maximum currently persisted, or 0
    /// when there are none.
    UInt64 nextCreationOrder() const
    {
        UInt64 max_order = 0;
        bool any = false;
        if (fs::exists(root_path))
        {
            for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
            {
                const auto & current_path = it->path();
                if (current_path.extension() != ".sql")
                    continue;
                max_order = std::max(max_order, splitCreationOrder(readFileRaw(current_path)).first);
                any = true;
            }
        }
        return any ? max_order + 1 : 0;
    }

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
    /// Serializes access to the mutable members below (`zookeeper_client`, `wait_event`,
    /// `collections_node_cversion`, `max_child_mzxid`). The background watcher calls
    /// `waitUpdate` without holding `RewriteRules::mutex`, while `CREATE`/`ALTER`/`DROP
    /// RULE` reach `write`/`remove`/`list` on the same instance. Without this lock both
    /// paths could enter `getClient` and read/reset `zookeeper_client` concurrently when
    /// the Keeper session expires. `RewriteRules::mutex` is always acquired before this
    /// one (DDL and reload paths), and `waitUpdate` takes only this one, so the lock
    /// order is consistent and cannot deadlock.
    mutable std::mutex zk_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable Coordination::EventPtr wait_event;
    mutable Int32 collections_node_cversion = 0;
    /// zxid of the most recent data modification across rule znodes, as observed by
    /// the last `list`. `ALTER RULE` changes only the data of a child znode, which
    /// does not affect the parent's `cversion`, so it has to be tracked separately.
    mutable Int64 max_child_mzxid = 0;

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

        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::ZooKeeperStorage");
        std::lock_guard lock(zk_mutex);
        auto client = getClient(lock);
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
        Coordination::EventPtr event;
        {
            std::lock_guard lock(zk_mutex);
            event = wait_event;
        }

        /// `list` has not run yet (nothing loaded), so report an update to trigger
        /// the initial load.
        if (!event)
        {
            return true;
        }

        /// Block for the watch WITHOUT holding `zk_mutex`. Holding it here would stall
        /// concurrent `CREATE`/`ALTER`/`DROP RULE` storage operations for the whole
        /// timeout. `Poco::Event::tryWait` is itself thread-safe.
        if (event->tryWait(timeout))
        {
            return true;
        }

        /// Timed out: the child-list watch did not fire, but a data-only `ALTER RULE`
        /// on another replica modifies only a child znode's data (no parent watch).
        /// Re-acquire `zk_mutex` to inspect Keeper and read the shared members.
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::waitUpdate");
        std::string res;
        Coordination::Stat stat;
        auto client = getClient(lock);

        if (!client->tryGet(root_path, res, &stat))
        {
            chassert(false);
            return false;
        }

        if (stat.cversion != collections_node_cversion)
            return true;

        /// The child list is unchanged, but `ALTER RULE` on another replica modifies
        /// only the data of a rule znode, which fires no child-list watch and keeps
        /// the parent's `cversion` intact. Compare the most recent data-modification
        /// zxid across the rule znodes to detect such updates.
        Int64 current_max_mzxid = 0;
        for (const auto & child : client->getChildren(root_path))
        {
            Coordination::Stat child_stat;
            if (client->exists(getPath(child), &child_stat))
                current_max_mzxid = std::max(current_max_mzxid, child_stat.mzxid);
        }
        return current_max_mzxid != max_child_mzxid;
    }

    std::vector<std::string> list() const override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::list");
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        Coordination::Stat stat;
        auto client = getClient(lock);
        auto children = client->getChildren(root_path, &stat, wait_event);
        collections_node_cversion = stat.cversion;

        /// Sort children by `czxid` so the load order matches the creation order on this cluster.
        Int64 current_max_mzxid = 0;
        std::vector<std::pair<std::string, int64_t>> entries;
        entries.reserve(children.size());
        for (const auto & child : children)
        {
            Coordination::Stat child_stat;
            /// Watch each rule znode as well: `ALTER RULE` on another replica changes
            /// only the data of a child, which fires no watch on the parent.
            if (client->exists(getPath(child), &child_stat, wait_event))
            {
                entries.emplace_back(child, child_stat.czxid);
                current_max_mzxid = std::max(current_max_mzxid, child_stat.mzxid);
            }
        }
        max_child_mzxid = current_max_mzxid;
        std::sort(entries.begin(), entries.end(),
            [](const auto & a, const auto & b)
            {
                if (a.second != b.second)
                    return a.second < b.second;
                return a.first < b.first;
            });

        std::vector<std::string> result;
        result.reserve(entries.size());
        for (auto & [name, _] : entries)
            result.push_back(std::move(name));
        return result;
    }

    bool exists(const std::string & file_name) const override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::exists");
        return getClient(lock)->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::read");
        return getClient(lock)->get(getPath(file_name));
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::write");
        if (replace)
        {
            /// `ALTER RULE` must only update an existing rule, never (re)create one.
            /// `RewriteRules::updateRule` checks only the (possibly stale) local cache
            /// before reaching this point, so a concurrent `DROP RULE` on one replica
            /// followed by `ALTER RULE` on another would, with `createOrUpdate`,
            /// resurrect the dropped znode and diverge the replicated state from the
            /// user's intent. Update the existing node and fail if it is gone instead.
            auto code = getClient(lock)->trySet(getPath(file_name), data);

            if (code == Coordination::Error::ZNONODE)
            {
                throw Exception(
                    ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
                    "File {} for query rule doesn't exist",
                    file_name
                );
            }

            if (code != Coordination::Error::ZOK)
                throw Coordination::Exception::fromPath(code, getPath(file_name));
        }
        else
        {
            auto code = getClient(lock)->tryCreate(getPath(file_name), data, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZNODEEXISTS)
            {
                throw Exception(
                    ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                    "File {} for query rule already exists",
                    file_name
                );
            }

            /// Require successful persistence. Any other code (e.g. ZNONODE if the
            /// root znode disappeared) must not be ignored, otherwise the rule would
            /// be added to local memory while missing in Keeper, diverging across
            /// replicas and after reload.
            if (code != Coordination::Error::ZOK)
                throw Coordination::Exception::fromPath(code, getPath(file_name));
        }
    }

    void remove(const std::string & file_name) override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::remove");
        getClient(lock)->remove(getPath(file_name));
    }

    bool removeIfExists(const std::string & file_name) override
    {
        std::lock_guard lock(zk_mutex);
        auto component_guard = Coordination::setCurrentComponent("RewriteRulesStorage::removeIfExists");
        auto code = getClient(lock)->tryRemove(getPath(file_name));
        if (code == Coordination::Error::ZOK)
            return true;
        if (code == Coordination::Error::ZNONODE)
            return false;
        throw Coordination::Exception::fromPath(code, getPath(file_name));
    }

private:
    /// Requires `zk_mutex` to be held: it reads and may reset `zookeeper_client`.
    zkutil::ZooKeeperPtr getClient(std::lock_guard<std::mutex> & /*zk_lock*/) const
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

RewriteRuleObjectsList RewriteRulesStorage::getAll() const
{
    RewriteRuleObjectsList result;
    for (const auto & rule_name : listRules())
    {
        const bool already_present = std::any_of(
            result.begin(), result.end(),
            [&](const auto & entry) { return entry.first == rule_name; });
        if (already_present)
        {
            throw Exception(
                ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
                "Found duplicate rewrite rule `{}`",
                rule_name);
        }
        try
        {
            result.emplace_back(rule_name, get(rule_name));
        }
        catch (const Coordination::Exception & e)
        {
            /// A concurrent `DROP RULE` on another replica may have removed the rule
            /// znode between listing and reading it. This is expected in a replicated
            /// setup - the next reload cycle will reflect the converged state.
            if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_DEBUG(
                    getLogger("RewriteRulesStorage"),
                    "Rewrite rule `{}` was removed while reading, skipping",
                    rule_name);
                continue;
            }
            throw;
        }
    }
    return result;
}

void RewriteRulesStorage::create(const RewriteRuleObjectPtr & create_query)
{
    writeCreateQuery(create_query->getCreateQuery().rule_name, create_query->getCreateQuery().whole_query);
}

void RewriteRulesStorage::remove(const std::string & rule_name)
{
    const auto file_name = getFileName(rule_name);
    impl_storage->remove(file_name);
    LOG_INFO(
        getLogger("RewriteRulesStorage"),
        "Removed rewrite rule `{}` ({} `{}`) from {} storage",
        rule_name,
        isReplicated() ? "znode" : "file",
        file_name,
        isReplicated() ? "ZooKeeper/Keeper" : "local");
}

bool RewriteRulesStorage::removeIfExists(const std::string & rule_name)
{
    const auto file_name = getFileName(rule_name);
    const bool removed = impl_storage->removeIfExists(file_name);
    if (removed)
        LOG_INFO(
            getLogger("RewriteRulesStorage"),
            "Removed rewrite rule `{}` ({} `{}`) from {} storage",
            rule_name,
            isReplicated() ? "znode" : "file",
            file_name,
            isReplicated() ? "ZooKeeper/Keeper" : "local");
    return removed;
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
