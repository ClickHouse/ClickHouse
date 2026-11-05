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
#include <Common/logger_useful.h>

#include <Poco/Timestamp.h>


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
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

bool isDDLIdempotencyKeyNodeExpired(Int64 ctime_ms, UInt64 ttl_sec)
{
    Int64 now = Poco::Timestamp().epochTime();
    Int64 created = ctime_ms / 1000;
    return now > created && static_cast<UInt64>(now - created) > ttl_sec;
}

std::optional<DDLIdempotencyKeyNode> tryReadDDLIdempotencyKeyNode(
    const ZooKeeperPtr & zookeeper, const String & path, std::optional<int32_t> * expired_version)
{
    String data;
    Coordination::Stat stat;
    if (!zookeeper->tryGet(path, data, &stat))
        return {};

    auto node = DDLIdempotencyKeyNode::parse(data);
    if (isDDLIdempotencyKeyNodeExpired(stat.ctime, node.ttl_sec))
    {
        if (expired_version)
            *expired_version = stat.version;
        return {};
    }
    return node;
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
            ErrorCodes::NOT_IMPLEMENTED,
            "idempotency_key is supported only for statements that a Replicated database executes through its log");
}

namespace
{

/// The database and the table a supported statement is about.
struct Target
{
    std::shared_ptr<DatabaseReplicated> database;
    String table;
};

[[noreturn]] void throwNotSupported(const String & what)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "idempotency_key is not supported for {}", what);
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
/// log entry, and those that make more than one entry or none. Returns the database and the table the statement names.
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

    auto database = std::dynamic_pointer_cast<DatabaseReplicated>(DatabaseCatalog::instance().tryGetDatabase(database_name));
    if (!database)
        throwNotSupported("databases with an engine other than Replicated");

    return {std::move(database), std::move(table_name)};
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

/// The access the statement itself requires, so that a retry answered from the key is authorized as the statement would be.
AccessRightsElements getRequiredAccess(const IInterpreter & interpreter, const IAST & ast, const Target & target, const ContextPtr & context)
{
    if (const auto * create = typeid_cast<const InterpreterCreateQuery *>(&interpreter))
        return create->getRequiredAccess();
    if (const auto * rename = typeid_cast<const InterpreterRenameQuery *>(&interpreter))
        return rename->getRequiredAccess(InterpreterRenameQuery::RenameType::RenameTable);
    if (typeid_cast<const InterpreterAlterQuery *>(&interpreter))
    {
        AccessRightsElements required_access;
        const String & database_name = target.database->getDatabaseName();
        for (const auto & child : ast.as<const ASTAlterQuery &>().command_list->children)
        {
            auto command_access = InterpreterAlterQuery::getRequiredAccessForCommand(
                child->as<const ASTAlterCommand &>(),
                database_name,
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
    const String & database_name = target.database->getDatabaseName();
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
    const DDLIdempotencyKey & key,
    const DDLIdempotencyKeyNode & node,
    const IInterpreter & interpreter,
    const IAST & ast,
    const Target & target,
    const ContextMutablePtr & context)
{
    checkDDLIdempotencyKeyDigest(key, node);
    checkRequiredAccess(interpreter, ast, target, context);
    key.state = DDLIdempotencyKey::State::Answered;

    auto guard = DatabaseCatalog::instance().getDDLGuard(target.database->getDatabaseName(), target.table, target.database.get());
    guard->releaseTableLock();
    return target.database->getDDLIdempotencyKeyAnswer(node.entry, context, std::move(guard));
}

}

BlockIO executeWithDDLIdempotencyKey(IInterpreter & interpreter, const ASTPtr & ast, const ContextMutablePtr & context)
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

    Target target = getTarget(*ast, context);

    auto key = std::make_shared<DDLIdempotencyKey>();
    key->key = context->getSettingsRef()[Setting::idempotency_key];
    key->digest = computeDigest(*ast, context);
    context->setDDLIdempotencyKey(key);

    /// Before the interpreter, because a committed attempt may have dropped or changed the table the statement names.
    if (auto node = target.database->tryReadDDLIdempotencyKey(key->key))
        return answerFromKey(*key, *node, interpreter, *ast, target, context);

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

        /// Nothing was enqueued, hence nothing was executed. A replica sees the effect of a committed attempt only after
        /// the commit, which also creates the key, so if such an effect made this attempt fail, the key is there now.
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
        return answerFromKey(*key, *node, interpreter, *ast, target, context);
    }

    if (key->state == DDLIdempotencyKey::State::Pending)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A query with idempotency_key finished without making a log entry");

    return res;
}

}
