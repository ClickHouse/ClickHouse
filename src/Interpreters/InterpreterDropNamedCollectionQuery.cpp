#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Disks/DiskFromAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <base/sort.h>

#include <algorithm>
#include <cstring>


namespace DB
{

namespace Setting
{
    extern const SettingsBool check_named_collection_dependencies;
}

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_IS_USED;
}

namespace
{

enum class DependentStatus : uint8_t
{
    /// Nothing that uses the collection is left under this `StorageID`.
    Stale,
    /// A table that uses the collection and is attached right now.
    Attached,
    /// A table that uses the collection, is not attached right now, but is attached again at the next
    /// server start.
    Detached,
};

/// The metadata of a table that is replayed on the next server start.
struct MetadataToReplay
{
    /// Whether there is a metadata file at all.
    bool exists = false;
    /// The query it contains, or `nullptr` if it cannot be parsed.
    ASTPtr query;
};

/// Where the table metadata of one database is replayed from at the next server start.
struct ReplaySource
{
    /// The disk the table metadata files are kept on, or `nullptr` if nothing of this database is
    /// replayed at startup.
    DiskPtr disk;
    std::filesystem::path metadata_dir;
};

/// The table metadata of a database does not have to be on the default database disk: `Ordinary` and
/// `Atomic` databases can be created with `SETTINGS disk = ...`. The setting cannot be read through
/// `DatabaseMetadataDiskSettings` here, because engines that do not support it (`Replicated`, ...) can
/// have settings of their own in the same clause, which it rejects as unknown.
DiskPtr tryGetMetadataDiskFromDefinition(const ASTCreateQuery * database_create, const ContextPtr & context)
{
    if (!database_create || !database_create->storage || !database_create->storage->settings)
        return nullptr;

    const auto * value = database_create->storage->settings->changes.tryGet("disk");
    if (!value)
        return nullptr;

    /// The setting is either the name of a disk from the configuration, or an inline `disk(...)` function.
    ASTPtr value_as_custom_ast;
    CustomType custom;
    if (value->tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

    String disk_name;
    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
        disk_name = DiskFromAST::createCustomDisk(value_as_custom_ast, context, /* attach= */ true);
    else
        disk_name = value->safeGet<String>();

    return context->getDisk(disk_name);
}

ReplaySource tryGetReplaySource(const String & database_name, const ContextPtr & context)
{
    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (database)
    {
        /// Only a database that keeps table metadata on disk can bring a detached table back at startup.
        /// The others (`Memory`, `Filesystem`, `S3`, ...) return an empty path here.
        const auto metadata_dir = database->getMetadataPath();
        if (metadata_dir.empty())
            return {};

        return {.disk = database->getDisk(), .metadata_dir = metadata_dir};
    }

    /// A whole database can be detached as well, and then the dependent table is not reachable
    /// through the catalog at all. `DETACH DATABASE` keeps the database metadata file, so the
    /// database and all of its tables are loaded again on the next server start (there is no
    /// `DETACH DATABASE ... PERMANENTLY`). `DROP DATABASE` removes that file, and it also drops the
    /// dependencies of the tables it contains.
    auto default_disk = context->getDatabaseDisk();
    const auto database_metadata_path = DatabaseCatalog::getMetadataFilePath(database_name);
    if (!default_disk->existsFile(database_metadata_path))
        return {};

    /// The database object, which normally knows where the metadata of its tables is kept, is gone, so
    /// the source has to be reconstructed from the database definition the same way
    /// `InterpreterCreateQuery::createDatabase` does it: a database that has a UUID (`Atomic`,
    /// `Replicated`, ...) keeps the metadata under `store`, a name-based one under `metadata/<database>`,
    /// and on the metadata disk from the definition when there is one.
    auto database_metadata = DatabaseOnDisk::parseQueryFromMetadata(
        getLogger("InterpreterDropNamedCollectionQuery"), context, default_disk, database_metadata_path, /* throw_on_error= */ false);
    const auto * database_create = database_metadata ? database_metadata->as<ASTCreateQuery>() : nullptr;
    const auto metadata_dir = database_create && database_create->uuid != UUIDHelpers::Nil
        ? DatabaseCatalog::getStoreDirPath(database_create->uuid)
        : DatabaseCatalog::getMetadataDirPath(database_name);

    auto disk = tryGetMetadataDiskFromDefinition(database_create, context);
    return {.disk = disk ? disk : default_disk, .metadata_dir = metadata_dir};
}

/// A table detached with a plain `DETACH TABLE` keeps its metadata file, so it is attached again on
/// the next server start, but it is gone from `DatabaseCatalog`, so `isTableExist` says `false` for
/// it. Dropping a named collection that such a table references makes the `ATTACH` replayed at
/// startup throw `NAMED_COLLECTION_DOESNT_EXIST`, which aborts loading the metadata and the server
/// does not start, so a detached table still counts as a dependent.
/// `DETACH TABLE ... PERMANENTLY` is a different case: the metadata file stays, but it is marked with
/// a flag file and it is not loaded at startup, and the dependency is removed right away anyway.
MetadataToReplay tryGetMetadataToReplay(const ReplaySource & source, const String & table_name, const ContextPtr & context)
{
    if (!source.disk)
        return {};

    const String metadata_path = source.metadata_dir / (escapeForFileName(table_name) + ".sql");
    if (!source.disk->existsFile(metadata_path))
        return {};

    if (source.disk->existsFile(metadata_path + DatabaseOnDisk::detached_suffix))
        return {};

    auto log = getLogger("InterpreterDropNamedCollectionQuery");
    return {
        .exists = true,
        .query = DatabaseOnDisk::parseQueryFromMetadata(log, context, source.disk, metadata_path, /* throw_on_error= */ false),
    };
}

/// Whether any metadata file of the source replays the table with this UUID at the next server start.
/// Only a metadata file that really carries the UUID counts: a file that cannot be parsed aborts the
/// next server start by itself, whether the collection stays or not, and a file that disappears midway
/// is a concurrently dropped table.
bool replaysUUID(const ReplaySource & source, const UUID & uuid, const ContextPtr & context)
{
    if (!source.disk || !source.disk->existsDirectory(source.metadata_dir))
        return false;

    auto log = getLogger("InterpreterDropNamedCollectionQuery");
    for (auto it = source.disk->iterateDirectory(source.metadata_dir); it->isValid(); it->next())
    {
        if (!it->name().ends_with(".sql"))
            continue;

        if (source.disk->existsFile(it->path() + DatabaseOnDisk::detached_suffix))
            continue;

        auto query = DatabaseOnDisk::parseQueryFromMetadata(log, context, source.disk, it->path(), /* throw_on_error= */ false);
        const auto * create = query ? query->as<ASTCreateQuery>() : nullptr;
        if (create && create->uuid == uuid)
            return true;
    }

    return false;
}

/// Whether the metadata of any database, attached or detached, replays the table with this UUID.
/// This is the last resort for a dependent whose stored names went stale completely, e.g. its whole
/// database was renamed and detached after that.
bool replaysUUIDAnywhere(const UUID & uuid, const ContextPtr & context)
{
    const auto & catalog = DatabaseCatalog::instance();

    /// Remote databases (`MySQL`, ...) keep no local table metadata, so there is no point reaching out
    /// to them.
    for (const auto & [database_name, database] : catalog.getDatabases({.with_datalake_catalogs = false, .with_remote_databases = false}))
    {
        const auto metadata_dir = database->getMetadataPath();
        if (metadata_dir.empty())
            continue;

        if (replaysUUID({.disk = database->getDisk(), .metadata_dir = metadata_dir}, uuid, context))
            return true;
    }

    /// The definitions of detached databases are only visible as `metadata/<database>.sql` files.
    auto default_disk = context->getDatabaseDisk();
    const auto metadata_root = DatabaseCatalog::getMetadataDirPath();
    if (!default_disk->existsDirectory(metadata_root))
        return false;

    for (auto it = default_disk->iterateDirectory(metadata_root); it->isValid(); it->next())
    {
        if (!it->name().ends_with(".sql"))
            continue;

        const auto database_name = unescapeForFileName(it->name().substr(0, it->name().size() - strlen(".sql")));
        if (catalog.isDatabaseExist(database_name))
            continue;

        if (replaysUUID(tryGetReplaySource(database_name, context), uuid, context))
            return true;
    }

    return false;
}

/// Whether the definition of a table mentions the named collection.
/// The check is deliberately coarse: it is a search for the name over the whole query, so anything that
/// even looks like a reference to the collection keeps the dependency. Letting a table that does not use
/// the collection block `DROP NAMED COLLECTION` is an annoyance, while dropping a collection that a table
/// does use breaks the next server start.
bool mentionsCollection(const IAST & query, const String & collection_name)
{
    const String text = query.formatWithSecretsOneLine();

    for (size_t pos = text.find(collection_name); pos != String::npos; pos = text.find(collection_name, pos + 1))
    {
        const size_t end = pos + collection_name.size();
        if ((pos == 0 || !isWordCharASCII(text[pos - 1])) && (end == text.size() || !isWordCharASCII(text[end])))
            return true;
    }

    return false;
}

/// A dependency is registered while the engine arguments of a table are resolved, which happens before
/// the table is created, so a failed `CREATE TABLE` leaves the dependency of a table that never came to
/// exist behind. Such a stale dependency has to be recognized, otherwise it blocks the drop of the
/// collection forever - and, for a name-based database, the name can even be taken by another table that
/// has nothing to do with the collection later on.
DependentStatus getDependentStatus(const StorageID & table_id, const String & collection_name, const ContextPtr & context)
{
    const auto & catalog = DatabaseCatalog::instance();

    /// In an `Atomic` database dependencies are tracked by UUID. It identifies the dependent table
    /// exactly: it does not change when the table is renamed, and a table that merely reuses the name is
    /// a different table.
    if (table_id.hasUUID())
    {
        if (catalog.tryGetByUUID(table_id.uuid).second)
            return DependentStatus::Attached;

        const auto source = tryGetReplaySource(table_id.database_name, context);
        auto metadata = tryGetMetadataToReplay(source, table_id.table_name, context);
        if (metadata.exists)
        {
            /// If the metadata cannot be parsed, keep the dependency: the collection is better kept than
            /// the server left unable to start.
            const auto * create = metadata.query ? metadata.query->as<ASTCreateQuery>() : nullptr;
            if (!create || create->uuid == table_id.uuid)
                return DependentStatus::Detached;
        }

        /// The names stored in a UUID-tracked dependency can be stale: `RENAME TABLE` and
        /// `RENAME DATABASE` deliberately do not rewrite them, because the UUID stays. The metadata file
        /// is renamed, however, so the file that replays the dependent has to be searched for by the
        /// UUID in the `ATTACH` query it contains.
        if (replaysUUID(source, table_id.uuid, context) || replaysUUIDAnywhere(table_id.uuid, context))
            return DependentStatus::Detached;

        return DependentStatus::Stale;
    }

    /// A name-based database (`Ordinary`, ...) has nothing but the name to identify the dependent by, so
    /// check that the table under this name is really the one that uses the collection.
    auto metadata = tryGetMetadataToReplay(tryGetReplaySource(table_id.database_name, context), table_id.table_name, context);
    if (metadata.exists)
    {
        if (metadata.query && !mentionsCollection(*metadata.query, collection_name))
            return DependentStatus::Stale;

        return catalog.isTableExist(table_id, context) ? DependentStatus::Attached : DependentStatus::Detached;
    }

    /// A database that does not keep table metadata on disk (`Memory`, ...) loads nothing at startup, so
    /// only a table that is attached right now can be a dependent.
    auto database = catalog.tryGetDatabase(table_id.database_name);
    if (!database || !database->isTableExist(table_id.table_name, context))
        return DependentStatus::Stale;

    auto create = database->tryGetCreateTableQuery(table_id.table_name, context);
    if (create && !mentionsCollection(*create, collection_name))
        return DependentStatus::Stale;

    return DependentStatus::Attached;
}

}

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.collection_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (current_context->getSettingsRef()[Setting::check_named_collection_dependencies])
    {
        auto dependents = NamedCollectionFactory::instance().getDependents(query.collection_name);
        if (!dependents.empty())
        {
            /// Filter out the dependencies of tables that do not exist anymore, or never did.
            std::vector<String> dependent_names;
            dependent_names.reserve(dependents.size());
            for (const auto & dep : dependents)
            {
                switch (getDependentStatus(dep, query.collection_name, current_context))
                {
                    case DependentStatus::Attached:
                        dependent_names.push_back(dep.getFullTableName());
                        break;
                    case DependentStatus::Detached:
                        dependent_names.push_back(dep.getFullTableName() + " (detached)");
                        break;
                    case DependentStatus::Stale:
                        /// Only the dependency on this collection is removed: the same table can depend
                        /// on other collections, and those dependencies can be perfectly valid.
                        NamedCollectionFactory::instance().removeDependency(query.collection_name, dep);
                        break;
                }
            }

            if (!dependent_names.empty())
            {
                /// A table can be registered as a dependent more than once (the `URL` engine, for
                /// example, resolves its arguments both to dispatch on the scheme and to create the
                /// delegate storage), and the order the dependencies come in is not defined.
                ::sort(dependent_names.begin(), dependent_names.end());
                dependent_names.erase(std::unique(dependent_names.begin(), dependent_names.end()), dependent_names.end());

                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_IS_USED,
                    "Named collection `{}` is used by tables: {}",
                    query.collection_name,
                    fmt::join(dependent_names, ", "));
            }
        }
    }

    NamedCollectionFactory::instance().removeFromSQL(query);

    return {};
}

void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory);
void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropNamedCollectionQuery", create_fn);
}

}
