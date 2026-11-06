#include <Interpreters/DDLIdempotencyKey.h>

#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTRenameQuery.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/AlterCommands.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_ddl_idempotency_key;
    extern const SettingsString idempotency_key;
}

namespace ErrorCodes
{
    extern const int IDEMPOTENCY_KEY_MISMATCH;
    extern const int IDEMPOTENCY_KEY_NOT_SUPPORTED;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace FailPoints
{
    extern const char database_replicated_idempotency_key_pause_after_first_probe[];
}

String DDLIdempotencyKeyNode::toString() const
{
    WriteBufferFromOwnString out;
    out << "version: " << VERSION << "\n";
    out << "entry: " << entry << "\n";
    out << "digest: " << digest << "\n";
    out << "ttl_sec: " << ttl_sec << "\n";
    return out.str();
}

DDLIdempotencyKeyNode DDLIdempotencyKeyNode::parse(const String & data)
{
    DDLIdempotencyKeyNode node;
    ReadBufferFromString in(data);
    UInt64 version = 0;
    in >> "version: " >> version >> "\n";
    in >> "entry: " >> node.entry >> "\n";
    in >> "digest: " >> node.digest >> "\n";
    in >> "ttl_sec: " >> node.ttl_sec >> "\n";
    return node;
}

String getDDLIdempotencyKeyNodeName(const String & key)
{
    /// Any client string must give a valid node name.
    return sipHash128String(key);
}

bool isDDLIdempotencyKeyNodeExpired(Int64 mtime_ms, UInt64 ttl_sec, Int64 now)
{
    Int64 modified = mtime_ms / 1000;
    return now > modified && static_cast<UInt64>(now - modified) > ttl_sec;
}

std::optional<Int64> getDDLIdempotencyKeeperTime(const ZooKeeperPtr & zookeeper, const String & keys_path)
{
    Coordination::Stat stat;
    auto code = zookeeper->trySet(keys_path, "", -1, &stat);
    if (code == Coordination::Error::ZNONODE)
        return {};
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, keys_path);
    return stat.mtime / 1000;
}

std::optional<DDLIdempotencyKeyNode> tryReadDDLIdempotencyKeyNode(
    const ZooKeeperPtr & zookeeper, const String & path, Int64 now, std::optional<int32_t> * expired_version)
{
    String data;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(path, data, &stat))
        return {};

    auto node = DDLIdempotencyKeyNode::parse(data);
    if (isDDLIdempotencyKeyNodeExpired(stat.mtime, node.ttl_sec, now))
    {
        if (expired_version)
            *expired_version = stat.version;
        return {};
    }
    return node;
}

void addDDLIdempotencyKeyCommitRequests(
    Coordination::Requests & ops, const String & path, const DDLIdempotencyKeyNode & node, std::optional<int32_t> expired_version)
{
    if (expired_version)
        ops.emplace_back(zkutil::makeSetRequest(path, node.toString(), *expired_version));
    else
        ops.emplace_back(zkutil::makeCreateRequest(path, node.toString(), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeSetRequest(String(Coordination::parentNodePath(path)), "", -1));
}

std::optional<ExpiredDDLIdempotencyKeyNode> tryGetExpiredDDLIdempotencyKeyNode(const ZooKeeperPtr & zookeeper, const String & path, Int64 now)
{
    Coordination::Stat parent_stat;
    if (!zookeeper->exists(String(Coordination::parentNodePath(path)), &parent_stat))
        return {};

    String data;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(path, data, &stat) || !isDDLIdempotencyKeyNodeExpired(stat.mtime, DDLIdempotencyKeyNode::parse(data).ttl_sec, now))
        return {};

    return ExpiredDDLIdempotencyKeyNode{.version = stat.version, .parent_version = parent_stat.version};
}

bool tryRemoveExpiredDDLIdempotencyKeyNode(const ZooKeeperPtr & zookeeper, const String & path, const ExpiredDDLIdempotencyKeyNode & node)
{
    Coordination::Requests ops{
        zkutil::makeCheckRequest(String(Coordination::parentNodePath(path)), node.parent_version),
        zkutil::makeRemoveRequest(path, node.version)};
    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZOK)
        return true;
    if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNONODE)
        return false;
    zkutil::KeeperMultiException::check(code, ops, responses);
    return false;
}

void checkDDLIdempotencyKeyDigest(const DDLIdempotencyKey & key, const DDLIdempotencyKeyNode & node)
{
    if (node.digest != key.digest)
        throw Exception(
            ErrorCodes::IDEMPOTENCY_KEY_MISMATCH,
            "The idempotency key '{}' was already used by a different statement or user (log entry {})",
            key.key, node.entry);
}

void throwIfDDLIdempotencyKeyIsSet(const ContextPtr & context)
{
    if (context->getDDLIdempotencyKey())
        throw Exception(
            ErrorCodes::IDEMPOTENCY_KEY_NOT_SUPPORTED,
            "idempotency_key is supported only for statements that a Replicated database executes through its log");
}

namespace
{

struct Target
{
    String database_name;
    String table;
    std::shared_ptr<DatabaseReplicated> database;
};

[[noreturn]] void throwNotSupported(const String & what)
{
    throw Exception(ErrorCodes::IDEMPOTENCY_KEY_NOT_SUPPORTED, "idempotency_key is not supported for {}", what);
}

/// Engines that create no state in Keeper or outside of the table's own directory while the storage is built,
/// which happens before the statement commits.
bool isEngineSupported(const String & name)
{
    if (name.ends_with("MergeTree"))
        return !name.starts_with("Replicated") && !name.starts_with("Shared");
    return name == "Memory" || name == "Null" || name == "Log" || name == "TinyLog" || name == "StripeLog" || name == "Distributed"
        || name == "Merge";
}

void checkCreateQuery(const ASTCreateQuery & create)
{
    if (!create.table)
        throwNotSupported("database-level statements");
    if (create.attach)
        throwNotSupported("ATTACH");
    if (create.replace_table || create.create_or_replace || create.replace_view)
        throwNotSupported("CREATE OR REPLACE and REPLACE");
    if (create.is_populate)
        throwNotSupported("POPULATE");
    if (create.is_clone_as)
        throwNotSupported("CLONE AS");
    if (create.isTemporary())
        throwNotSupported("temporary tables");

    if (create.is_dictionary || create.is_ordinary_view)
        return;

    if (create.is_materialized_view)
    {
        if (!create.is_materialized_view_with_external_target())
            throwNotSupported("a materialized view without TO");
        if (create.refresh_strategy)
            throwNotSupported("a refreshable materialized view");
        return;
    }

    if (create.select)
        throwNotSupported("CREATE ... AS SELECT");
    if (create.is_time_series_table)
        throwNotSupported("TimeSeries tables");
    if (!create.storage || !create.storage->engine)
        throwNotSupported("CREATE TABLE without an explicit ENGINE");
    if (!isEngineSupported(create.storage->engine->name))
        throwNotSupported(fmt::format("CREATE TABLE with engine {}", create.storage->engine->name));
}

/// Throws for the statements the key cannot be honoured for: those with an effect before or outside the commit of their
/// log entry, and those that make more than one entry or none. Returns the names of the database and the table the
/// statement names, without looking the database up.
Target getTarget(const IAST & ast, const ContextPtr & context)
{
    String database_name;
    String table_name;
    if (const auto * create = typeid_cast<const ASTCreateQuery *>(&ast))
    {
        checkCreateQuery(*create);
        database_name = create->getDatabase();
        table_name = create->getTable();
    }
    else if (const auto * drop = typeid_cast<const ASTDropQuery *>(&ast))
    {
        if (drop->kind != ASTDropQuery::Kind::Drop)
            throwNotSupported("DETACH and TRUNCATE");
        if (drop->database_and_tables)
            throwNotSupported("dropping several tables");
        if (!drop->table)
            throwNotSupported("database-level statements");
        if (drop->if_exists)
            throwNotSupported("DROP ... IF EXISTS");
        if (drop->isTemporary())
            throwNotSupported("temporary tables");
        database_name = drop->getDatabase();
        table_name = drop->getTable();
    }
    else if (const auto * rename = typeid_cast<const ASTRenameQuery *>(&ast))
    {
        if (rename->database)
            throwNotSupported("database-level statements");
        if (rename->getElements().size() != 1)
            throwNotSupported("renaming several tables");
        const auto & element = rename->getElements().front();
        if (element.if_exists)
            throwNotSupported("RENAME ... IF EXISTS");
        if (rename->rename_if_cannot_exchange)
            throwNotSupported("this RENAME");
        database_name = element.from.getDatabase();
        String to_database_name = element.to.getDatabase();
        if ((database_name.empty() ? context->getCurrentDatabase() : database_name)
            != (to_database_name.empty() ? context->getCurrentDatabase() : to_database_name))
            throwNotSupported("renaming a table to another database");
        table_name = element.from.getTable();
    }
    else if (const auto * alter = typeid_cast<const ASTAlterQuery *>(&ast))
    {
        if (alter->alter_object != ASTAlterQuery::AlterObjectType::TABLE)
            throwNotSupported("ALTER of anything but a table");
        for (const auto & child : alter->command_list->children)
        {
            const auto * command = child->as<ASTAlterCommand>();
            if (command->partition || !AlterCommand::parse(command))
                throwNotSupported("ALTER commands that change data or partitions");
        }
        database_name = alter->getDatabase();
        table_name = alter->getTable();
    }
    else
    {
        throwNotSupported("this kind of statement");
    }

    if (!context->getSettingsRef()[Setting::enable_ddl_idempotency_key])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "idempotency_key is an experimental feature, set enable_ddl_idempotency_key = 1 to use it");

    if (const auto * on_cluster = dynamic_cast<const ASTQueryWithOnCluster *>(&ast); on_cluster && !on_cluster->cluster.empty())
        throwNotSupported("ON CLUSTER");

    if (database_name.empty())
    {
        if (!typeid_cast<const ASTCreateQuery *>(&ast) && context->tryResolveStorageID({"", table_name}, Context::ResolveExternal))
            throwNotSupported("temporary tables");
        database_name = context->getCurrentDatabase();
    }

    return {.database_name = std::move(database_name), .table = std::move(table_name), .database = nullptr};
}

/// The statement as it is written in the log of the database, without what may differ between attempts that are the
/// same operation: the name of the database (replicas may name it differently) and the output clauses.
String computeDigest(const IAST & ast, const ContextPtr & context)
{
    ASTPtr normalized;
    if (const auto * rename = typeid_cast<const ASTRenameQuery *>(&ast))
    {
        auto elements = rename->getElements();
        for (auto & element : elements)
        {
            element.from.database = nullptr;
            element.to.database = nullptr;
        }
        auto res = make_intrusive<ASTRenameQuery>(std::move(elements));
        res->exchange = rename->exchange;
        res->dictionary = rename->dictionary;
        normalized = res;
    }
    else
    {
        normalized = ast.clone();
        auto & query = dynamic_cast<ASTQueryWithTableAndOutput &>(*normalized);
        query.reset(query.database);
        ASTQueryWithOutput::resetOutputASTIfExist(query);
    }

    auto user_id = context->getUserID();
    String user = user_id ? toString(*user_id) : "";
    String statement = normalized->formatWithSecretsOneLine();

    SipHash hash;
    hash.update(user.size());
    hash.update(user);
    hash.update(statement.size());
    hash.update(statement);
    return getHexUIntLowercase(hash.get128());
}

/// The access the statement itself requires, so that a retry answered from the key is authorized as the statement would be,
/// and a user without it learns nothing about the database or the key.
AccessRightsElements getRequiredAccess(const IInterpreter & interpreter, const IAST & ast, const Target & target, const ContextPtr & context)
{
    if (const auto * create = typeid_cast<const InterpreterCreateQuery *>(&interpreter))
        return create->getRequiredAccess();
    if (const auto * rename = typeid_cast<const InterpreterRenameQuery *>(&interpreter))
        return rename->getRequiredAccess(InterpreterRenameQuery::RenameType::RenameTable);
    if (typeid_cast<const InterpreterAlterQuery *>(&interpreter))
    {
        AccessRightsElements required_access;
        for (const auto & child : ast.as<const ASTAlterQuery &>().command_list->children)
        {
            auto command_access = InterpreterAlterQuery::getRequiredAccessForCommand(
                child->as<const ASTAlterCommand &>(),
                target.database_name,
                target.table,
                InterpreterAlterQuery::getRowExistsColumnKind(nullptr, context),
                context);
            required_access.insert(required_access.end(), command_access.begin(), command_access.end());
        }
        return required_access;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected interpreter for a query with idempotency_key");
}

void checkRequiredAccess(const IInterpreter & interpreter, const IAST & ast, const Target & target, const ContextPtr & context)
{
    const auto * drop = typeid_cast<const ASTDropQuery *>(&ast);
    if (!drop)
    {
        context->checkAccess(getRequiredAccess(interpreter, ast, target, context));
        return;
    }

    /// A DROP checks the flag of the kind of the object, which may be gone now: `DROP TABLE` also drops a view or an
    /// `ENGINE = Dictionary` table.
    const String & database_name = target.database_name;
    const auto access = context->getAccess();
    if (drop->is_dictionary)
        context->checkAccess(AccessType::DROP_DICTIONARY, database_name, target.table);
    else if (drop->is_view)
        context->checkAccess(AccessType::DROP_VIEW, database_name, target.table);
    else if (!access->isGranted(AccessType::DROP_VIEW, database_name, target.table)
             && !access->isGranted(AccessType::DROP_DICTIONARY, database_name, target.table))
        context->checkAccess(AccessType::DROP_TABLE, database_name, target.table);
}

BlockIO answerFromKey(
    const DDLIdempotencyKey & key, const DDLIdempotencyKeyNode & node, const Target & target, const ContextMutablePtr & context)
{
    checkDDLIdempotencyKeyDigest(key, node);
    key.state = DDLIdempotencyKey::State::Answered;

    auto guard = DatabaseCatalog::instance().getDDLGuard(target.database->getDatabaseName(), target.table, target.database.get());
    guard->releaseTableLock();
    return target.database->getDDLIdempotencyKeyAnswer(node.entry, context, std::move(guard));
}

}

bool isDDLIdempotencyKeyAlterEngineSupported(const String & engine_name)
{
    /// `ReplicatedMergeTree` adds its own Keeper requests to that commit.
    return isEngineSupported(engine_name) || (engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree"))
        || engine_name == "Buffer" || engine_name == "View" || engine_name == "MaterializedView";
}

BlockIO executeWithDDLIdempotencyKey(IInterpreter & interpreter, const ASTPtr & ast, const ContextMutablePtr & context, bool nested)
{
    switch (ast->getQueryKind())
    {
        case IAST::QueryKind::Select:
        case IAST::QueryKind::Explain:
        case IAST::QueryKind::Show:
        case IAST::QueryKind::Describe:
        case IAST::QueryKind::Exists:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Use:
            return interpreter.execute();
        default:
            break;
    }

    if (nested)
        throwNotSupported("statements nested in another query, like PARALLEL WITH or EXECUTE AS");

    Target target = getTarget(*ast, context);
    checkRequiredAccess(interpreter, *ast, target, context);
    target.database = std::dynamic_pointer_cast<DatabaseReplicated>(DatabaseCatalog::instance().tryGetDatabase(target.database_name));
    if (!target.database)
        throwNotSupported("databases with an engine other than Replicated");

    auto key = std::make_shared<DDLIdempotencyKey>();
    key->key = context->getSettingsRef()[Setting::idempotency_key];
    key->digest = computeDigest(*ast, context);
    context->setDDLIdempotencyKey(key);

    /// Before the interpreter, because a committed attempt may have dropped or changed the table the statement names.
    if (auto node = target.database->tryReadDDLIdempotencyKey(key->key))
        return answerFromKey(*key, *node, target, context);

    FailPointInjection::pauseFailPoint(FailPoints::database_replicated_idempotency_key_pause_after_first_probe);

    BlockIO res;
    try
    {
        res = interpreter.execute();
    }
    catch (...)
    {
        if (key->state != DDLIdempotencyKey::State::Pending)
            throw;

        /// Nothing was enqueued. If the effect of a committed attempt made this one fail, its key is visible now.
        auto original_exception = std::current_exception();
        std::optional<DDLIdempotencyKeyNode> node;
        try
        {
            node = target.database->tryReadDDLIdempotencyKey(key->key);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("DDLIdempotencyKey"), "Cannot read the idempotency key");
        }
        if (!node)
            std::rethrow_exception(original_exception);
        return answerFromKey(*key, *node, target, context);
    }

    if (key->state == DDLIdempotencyKey::State::Pending)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A query with idempotency_key finished without making a log entry");

    return res;
}

}
