#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseFilesystem.h>

#include <Access/ContextAccess.h>
#include <Access/Common/AccessFlags.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Disks/DiskType.h>
#include <Disks/IVolume.h>
#include <IO/Archives/ArchiveUtils.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFile.h>
#include <Storages/StorageProxy.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/filesystemHelpers.h>
#include <Formats/FormatFactory.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsString rename_files_after_processing;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int PATH_ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/// Whether the user is allowed to read local files, i.e. whether the resolution of a table of a
/// `Filesystem` database would pass the read source access check of its `file` delegate. The
/// decision is derived directly from the grants, without constructing the delegate: already the
/// parsing of the arguments of the `file` table function observes the filesystem
/// (`StorageFile::FileSource::parse` enumerates the matching paths), which must not happen before
/// the grant is confirmed. `TableFunctionFile` reports an empty URI for source-access filtering
/// (it does not override `getFunctionURI`), so a filtered grant applies iff its regexp matches the
/// empty string - replicated here by filtering against the empty string as well.
bool isFileReadGranted(const ContextPtr & context)
{
    return context->getAccess()->isGrantedWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");
}

void checkFileReadGranted(const ContextPtr & context)
{
    context->getAccess()->checkAccessWithFilter(AccessType::READ, toStringSource(AccessTypeObjects::Source::FILE), /* filter */ "");
}

/// A table of a `Filesystem` database is resolved into a `StorageFile` created by the `file` table
/// function, and the resolved storage is cached under the logical table name. The `file` delegate
/// is constructed identically for reads and writes (`TableFunctionFile::getStorage` ignores
/// `is_insert_query`), so a single cached delegate serves both - but the source access it checks
/// once, during its own construction, is the READ grant only, and a cache hit skips that check
/// altogether. The proxy restores the contract: the read grant is checked on every resolution (see
/// `DatabaseFilesystem::getTableImpl`) and the WRITE grant is checked here, on every write entry
/// point (INSERT, asynchronous insert, a materialized view target, `TRUNCATE`), which the
/// underlying `StorageFile` does not check on its own.
///
/// The proxy also carries the logical storage ID (`db`.`name`): the table function creates the
/// nested storage under the internal `_table_function` database, and privilege checks against the
/// storage ID must see the name the user's grants refer to.
class StorageFilesystemDatabaseTable final : public StorageProxy
{
public:
    StorageFilesystemDatabaseTable(const StorageID & storage_id_, StoragePtr nested_, TableFunctionPtr table_function_)
        : StorageProxy(storage_id_)
        , nested(std::move(nested_))
        , table_function(std::move(table_function_))
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(nullptr, false);
        setInMemoryMetadata(*nested_metadata);
    }

    StoragePtr getNested() const override { return nested; }
    String getName() const override { return nested->getName(); }

    /// Forward the delegate's planner-visible capability contracts, as `StorageURLDatabaseTable` does:
    /// `StorageProxy` forwards only a part of them, and without these overrides `SELECT count()` would
    /// parse the file instead of using the optimized count path (e.g. the `Parquet` metadata or the
    /// row count cache), and `PREWHERE` could be moved to columns that the delegate does not support.
    std::optional<NameSet> supportedPrewhereColumns() const override { return nested->supportedPrewhereColumns(); }
    bool canMoveConditionsToPrewhere() const override { return nested->canMoveConditionsToPrewhere(); }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override
    {
        return nested->supportsTrivialCountOptimization(storage_snapshot, query_context);
    }

    /// Read through a snapshot of the nested storage (like `StorageTableFunctionProxy` does):
    /// the storage may downcast `storage_snapshot->storage` to its own type.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr &,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, context);
        nested->read(query_plan, column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    /// Checked on the initiator, before the query is executed or queued for asynchronous insertion:
    /// with `async_insert = 1` the sink below is created in a background flush, after the query has
    /// already reported success to the user (`wait_for_async_insert = 0`) and possibly with
    /// different privileges than the ones the user had when the query was issued.
    void checkInsertIsAllowed(ContextPtr context) const override
    {
        table_function->checkSourceAccess(context, /* is_insert_query */ true);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        table_function->checkSourceAccess(context, /* is_insert_query */ true);
        return nested->write(query, metadata_snapshot, context, async_insert);
    }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock) override
    {
        table_function->checkSourceAccess(context, /* is_insert_query */ true);
        nested->truncate(query, metadata_snapshot, context, lock);
    }

private:
    StoragePtr nested;
    TableFunctionPtr table_function;
};

/// The archive and the path inside it a name resolves to through the archive path syntax
/// (`archive.tar.zst::data.native`). The first part is empty if the name is a plain path in this
/// context, and the second one is then the name itself.
std::pair<String, String> splitToArchiveParts(const String & table_path, const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::allow_archive_path_syntax])
        return {{}, table_path};

    return splitToArchivePathAndPathInArchive(table_path);
}

/// The path to the archive a name resolves to through the archive path syntax, or an empty string
/// if the name is a plain path in this context.
String getPathToArchive(const String & table_path, const ContextPtr & context)
{
    return splitToArchiveParts(table_path, context).first;
}

}

DatabaseFilesystem::DatabaseFilesystem(
    const String & name_, const String & path_, ContextPtr context_, bool is_internal_metadata_replay)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), path(path_), log(getLogger("DatabaseFileSystem(" + name_ + ")"))
{
    bool is_local = context_->getApplicationType() == Context::ApplicationType::LOCAL;
    const String user_files_path = is_local ? "" : getContext()->getUserFilesPath();

    /// When `user_files_policy` is configured with a non-local disk (e.g. `s3_plain`),
    /// `fs::exists` only checks the local filesystem and would reject valid paths
    /// that exist on the configured `IDisk`. Use disk-aware existence checks that
    /// fall back to `fs::exists` when no volume is configured.
    auto user_files_volume = is_local ? VolumePtr{} : getContext()->getUserFilesVolume();

    if (fs::path(path).is_relative())
    {
        /// For a disk-backed `user_files_policy`, `user_files_path` is the disk root
        /// (`disk->getPath()`), which is not necessarily a host-absolute directory -
        /// for `s3_plain` it is an object-key prefix. Calling `fs::absolute` here would
        /// prepend the server working directory and break the later disk-prefix match
        /// in `splitUserFilesAbsolutePath` (so valid directories on the disk would be
        /// reported as missing). Normalize only lexically in that case, preserving the
        /// disk-root prefix so the path stays resolvable through `IDisk`.
        ///
        /// A relative path is always resolved against the disk root, mirroring
        /// `getPathsListOnDisk`: for an object-storage disk the root is itself a
        /// relative object-key prefix, so a relative path that begins with that prefix
        /// (e.g. `Filesystem('<prefix>/nested')`) is legitimate user input naming
        /// `<prefix>/<prefix>/nested` - it must not be mistaken for an already-qualified
        /// path, or the database would silently point at a different directory at the
        /// disk root. Reloading is idempotent: database metadata preserves the original
        /// user input, and `getCreateDatabaseQueryImpl` serializes the disk-relative
        /// form, so the root is prepended exactly once per load either way.
        if (user_files_volume)
            path = (fs::path(user_files_path) / path).lexically_normal().string();
        else
            path = fs::absolute(fs::path(user_files_path) / path).lexically_normal().string();
    }
    else
        path = fs::absolute(path).lexically_normal();

    /// `ec` is set only if stat fails for a reason other than a missing path, e.g. a symlink loop.
    std::error_code ec;
    const bool path_exists = user_files_volume
        ? userFilesPathExists(path, user_files_volume->getDisks())
        : fs::exists(path, ec);

    /// Where a path stat fails on leads cannot be resolved, so it is not checked here. A user statement over it is
    /// refused below, and each table resolution checks its own path before touching a file.
    /// On a plain `user_files_path` the database root is checked after resolving symlinks
    /// (`pathStartsWith`), as before `user_files_policy` existed: the lexical
    /// `Context::isUserFilesPath` would accept a root that is an in-root symlink to an outside
    /// directory, and every table of the database would then be read from there.
    if (!is_local && !ec
        && !(user_files_volume ? getContext()->isUserFilesPath(path) : pathStartsWith(fs::path(path), fs::path(user_files_path))))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Path must be inside user-files path");
    }

    if (!path_exists)
    {
        /// Metadata loading stops at the first exception, so refusing the server's own startup replay here
        /// would make a directory removed since then enough to stop the server from starting. Tables resolve
        /// their file on access, so an unreachable path costs only the tables; the database still drops.
        if (!is_internal_metadata_replay)
        {
            if (ec)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot access path {}: {}", path, ec.message());
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path does not exist: {}", path);
        }

        if (ec)
            LOG_WARNING(log, "Cannot access path {}: {}. The database has no tables until it becomes accessible", path, ec.message());
        else
            LOG_WARNING(log, "Path does not exist: {}. The database has no tables until it reappears", path);
    }
}

std::string DatabaseFilesystem::getTablePath(const std::string & table_name) const
{
    fs::path table_path = fs::path(path) / table_name;
    return table_path.lexically_normal().string();
}

StoragePtr DatabaseFilesystem::addTable(const std::string & cache_key, StoragePtr table_storage) const
{
    std::lock_guard lock(mutex);
    /// `emplace` keeps the existing entry if the key is already there, so `first->second` is the storage
    /// a concurrent call for the same name inserted first. Nothing that locks `mutex` again may be called
    /// here: it is the non-recursive base `IDatabase::mutex`, shared with `getDatabaseName`.
    return loaded_tables.emplace(cache_key, table_storage).first->second;
}

bool DatabaseFilesystem::checkTableFilePath(const std::string & table_path, ContextPtr context_, bool throw_on_error) const
{
    /// If run in Local mode, no need for path checking.
    bool check_path = context_->getApplicationType() != Context::ApplicationType::LOCAL;
    auto user_files_volume = check_path ? context_->getUserFilesVolume() : VolumePtr{};

    /// Archive syntax is not supported with disk-based user files (the `file` delegate rejects it),
    /// so with a `user_files_policy` volume the whole name is the path probed on the disk.
    const auto [path_to_archive, path_in_archive] = user_files_volume
        ? std::pair<String, String>{{}, table_path}
        : splitToArchiveParts(table_path, context_);
    const String path_to_probe = path_to_archive.empty() ? table_path : path_to_archive;

    /// A table of the database is a file beneath the directory of the database, so that a name like
    /// `../other_dir/file.csv` (or an absolute path) does not reach another directory of `user_files`,
    /// e.g. the directory of another `Filesystem` database. The check is lexical, like the names of the
    /// tables. `clickhouse-local` accepts any path.
    if (check_path && !fileOrSymlinkPathStartsWith(path_to_probe, path))
    {
        /// Access denied is thrown regardless of 'throw_on_error'
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is not inside the directory of the database");
    }

    /// When `user_files_policy` is configured with a non-local disk (e.g. `s3_plain`),
    /// `fs::exists` only checks the local filesystem and would reject valid paths
    /// that exist on the configured `IDisk`. Resolve the disk + relative path once
    /// and route existence checks through `IDisk` when a volume is configured.
    DiskPtr disk;
    String disk_relative_path;
    if (user_files_volume)
    {
        std::tie(disk, disk_relative_path) = splitUserFilesAbsolutePath(table_path, user_files_volume->getDisks());
        if (!disk || !isDiskRelativePathInsideRoot(disk, disk_relative_path))
        {
            /// Access denied is thrown regardless of 'throw_on_error'
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is not inside user files path");
        }
    }
    else if (check_path && !context_->isUserFilesPath(path_to_probe))
    {
        /// Access denied is thrown regardless of 'throw_on_error'
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File is not inside user files path");
    }

    /// The result of this probe is reported to the user (`EXISTS TABLE` returns it, and the
    /// resolution of a table reports `FILE_DOESNT_EXIST` instead of `ACCESS_DENIED`), so it must
    /// not be performed without the read source grant: `EXISTS TABLE` requires only `SHOW TABLES`,
    /// which would otherwise turn a `Filesystem` database into an oracle for the contents of
    /// `user_files`. Claim the table instead: a resolution of it fails with the access error, and
    /// `EXISTS` answers what it answers for a path with globs, which is not probed either.
    if (!isFileReadGranted(context_))
        return true;

    if (!containsGlobs(path_to_probe))
    {
        const bool exists = disk ? disk->existsFileOrDirectory(disk_relative_path)
                                 : existsOrFileNameTooLong([&] { return fs::exists(path_to_probe); });
        if (!exists)
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File does not exist: {}", path_to_probe);
            return false;
        }

        const bool is_regular_file = disk ? disk->existsFile(disk_relative_path)
                                          : existsOrFileNameTooLong([&] { return fs::is_regular_file(path_to_probe); });
        if (!is_regular_file)
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File is directory, but expected a file: {}", path_to_probe);
            return false;
        }

        /// The name addresses a file stored inside the archive, so the table exists only if that file
        /// does. Answering with the presence of the archive alone would claim a table for a member
        /// that is not there, and its resolution would fail with a schema inference error instead of
        /// reporting the missing table, as this database does for a plain path.
        if (!path_to_archive.empty() && !archiveContainsFile(path_to_archive, path_in_archive))
        {
            if (throw_on_error)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
                                "File does not exist inside the archive {}: {}", path_to_archive, path_in_archive);
            return false;
        }
    }

    return true;
}

std::string DatabaseFilesystem::getCacheKey(const std::string & table_name, const ContextPtr & context_) const
{
    /// `allow_archive_path_syntax` is part of the resolution of a name: with it enabled,
    /// `a.tar::x.csv` is the file `x.csv` inside the archive `a.tar`, and with it disabled the same
    /// name is a file called `a.tar::x.csv`. Both files can exist at once, and the existence probe
    /// succeeds in both modes, so the two interpretations must not share a cache entry, or a query
    /// would read the source the previous query resolved. A file name cannot contain a zero byte,
    /// so the marker cannot collide with the name of another table.
    if (getPathToArchive(getTablePath(table_name), context_).empty())
        return table_name;

    return table_name + String(1, '\0') + "archive";
}

StoragePtr DatabaseFilesystem::tryGetTableFromCache(const std::string & name, const ContextPtr & context_) const
{
    const std::string key = getCacheKey(name, context_);

    StoragePtr table = nullptr;
    {
        std::lock_guard lock(mutex);
        auto it = loaded_tables.find(key);
        if (it != loaded_tables.end())
            table = it->second;
    }

    if (!table)
        return nullptr;

    /// Invalidate cache if file no longer exists. Route through `IDisk` when
    /// `user_files_policy` is configured so the existence probe matches the
    /// disk that backs the storage (archive syntax is not supported there).
    /// Otherwise, a name addressing a file inside an archive stays a table only
    /// while the archive contains that file: the archive can be rewritten without it,
    /// and a cached table would then claim the name and read nothing instead of
    /// reporting a missing table.
    const auto table_path = getTablePath(name);
    const auto user_files_volume = getContext()->getUserFilesVolume();
    bool exists = false;
    if (user_files_volume)
    {
        exists = userFilesPathExists(table_path, user_files_volume->getDisks());
    }
    else
    {
        const auto [path_to_archive, path_in_archive] = splitToArchiveParts(table_path, context_);
        const String path_to_probe = path_to_archive.empty() ? path_in_archive : path_to_archive;
        exists = existsOrFileNameTooLong([&] { return fs::exists(path_to_probe); })
            && (path_to_archive.empty() || archiveContainsFile(path_to_archive, path_in_archive));
    }

    if (!exists)
    {
        std::lock_guard lock(mutex);
        loaded_tables.erase(key);
        return nullptr;
    }

    return table;
}

bool DatabaseFilesystem::isTableExist(const String & name, ContextPtr context_) const
{
    /// Without the read source grant the cache is an oracle as well: an entry warmed by a
    /// privileged query would otherwise report the existence of a file to a user who may not
    /// look at `user_files` at all. Claim the table, as `checkTableFilePath` does.
    if (!isFileReadGranted(context_))
        return true;

    if (tryGetTableFromCache(name, context_))
        return true;

    return checkTableFilePath(getTablePath(name), context_, /* throw_on_error */ false);
}

StoragePtr DatabaseFilesystem::getTableImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    /// Neither the delegate may be constructed nor a cached one served without the read source
    /// grant: already the parsing of the arguments of the `file` table function observes the
    /// filesystem (see `isFileReadGranted`), and a cache entry warmed by a privileged query would
    /// otherwise let an unprivileged user read the file. Fail with the same access error the
    /// source-access check of the delegate produces after the parsing.
    checkFileReadGranted(context_);

    /// A renaming rule belongs to the one query that set it, while a cached table is shared with the
    /// later queries of every user. Such a table is therefore neither taken from the cache, where it
    /// would arrive without the rule, nor put into it, where it would rename for an unrelated query.
    const bool renames_after_processing
        = !context_->getSettingsRef()[Setting::rename_files_after_processing].value.empty();

    /// Check if table exists in loaded tables map.
    if (!renames_after_processing)
    {
        if (auto table = tryGetTableFromCache(name, context_))
            return table;
    }

    auto table_path = getTablePath(name);
    if (!checkTableFilePath(table_path, context_, throw_on_error))
        return {};

    /// Choose the path passed to the `file` table function so that
    /// `getPathsListOnDisk` resolves it unambiguously. For plain local disks `table_path`
    /// is host-absolute and is recognized by its disk-root prefix. For object-storage
    /// disks (e.g. `s3_plain`) the disk root is a relative object-key prefix, so a
    /// qualified path is itself relative and indistinguishable from raw user input;
    /// `file()` no longer strips a relative prefix (that would mis-target a different
    /// object). Wrapper disks (`DiskEncrypted`, cached) have a root naming the backing
    /// store. Pass the disk-relative path explicitly for every disk that is not plain
    /// local, which `file()` resolves against the disk root.
    String file_path = table_path;
    if (auto user_files_volume = context_->getUserFilesVolume())
    {
        auto [disk, relative] = splitUserFilesAbsolutePath(table_path, user_files_volume->getDisks());
        if (disk && !isPlainLocalDisk(*disk))
            file_path = relative;
    }

    auto ast_function_ptr = makeASTFunction("file", make_intrusive<ASTLiteral>(file_path));

    auto table_function = TableFunctionFactory::instance().get(ast_function_ptr, context_);
    if (!table_function)
        return nullptr;

    /// Every reader of a file in one query shares the counter that decides when the rename happens, so
    /// such a table is memoised for that query, the way the `file` table function is.
    if (renames_after_processing && context_->hasQueryContext())
    {
        auto query_context = context_->getQueryContext();
        /// The memo builds the table with the context it is handed and keys on that context's changed
        /// settings, so the query's context is what makes the references of one query share a table.
        /// A rule a sub-query set locally is not among the query's settings, so resolving it there
        /// would build a table that renames by another rule, or does not rename at all.
        const bool rule_is_the_query_setting
            = query_context->getSettingsRef()[Setting::rename_files_after_processing].value
            == context_->getSettingsRef()[Setting::rename_files_after_processing].value;

        auto memoised_storage = query_context->executeTableFunction(
            ast_function_ptr, table_function, rule_is_the_query_setting ? ContextPtr(query_context) : context_);
        if (!memoised_storage)
            return memoised_storage;

        /// Wrapped like the cached table below: the proxy is what re-checks the WRITE source grant on
        /// every write entry point, so the memoised path must not hand out the bare storage.
        return std::make_shared<StorageFilesystemDatabaseTable>(
            StorageID(getDatabaseName(), name), std::move(memoised_storage), table_function);
    }

    /// TableFunctionFile throws exceptions, if table cannot be created.
    auto table_storage = table_function->execute(ast_function_ptr, context_, name);
    if (!table_storage)
        return table_storage;

    /// Cache the proxy, not the raw `StorageFile`: the proxy re-checks the WRITE source grant on
    /// every write entry point, which neither the cached storage nor `StorageFile` itself does.
    auto proxy = std::make_shared<StorageFilesystemDatabaseTable>(
        StorageID(getDatabaseName(), name), std::move(table_storage), std::move(table_function));

    /// A table built under a renaming rule stays out of the cache, where it would rename for an
    /// unrelated later query.
    if (renames_after_processing)
        return proxy;

    return addTable(getCacheKey(name, context_), std::move(proxy));
}

StoragePtr DatabaseFilesystem::getTable(const String & name, ContextPtr context_) const
{
    /// getTableImpl can throw exceptions, do not catch them to show correct error to user.
    if (auto storage = getTableImpl(name, context_, true))
        return storage;

    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                    backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
}

StoragePtr DatabaseFilesystem::tryGetTable(const String & name, ContextPtr context_) const
{
    return getTableImpl(name, context_, false);
}

bool DatabaseFilesystem::empty() const
{
    std::lock_guard lock(mutex);
    return loaded_tables.empty();
}

ASTPtr DatabaseFilesystem::getCreateDatabaseQueryImpl() const
{
    const auto & settings = getContext()->getSettingsRef();

    /// For an object-storage user-files disk the qualified `path` is relative (the disk
    /// root is an object-key prefix), and the constructor always resolves a relative path
    /// against the disk root. Serialize the disk-relative form so that replaying this
    /// query (e.g. on RESTORE) prepends the root exactly once instead of doubling it.
    /// The same applies to wrapper disks (`DiskEncrypted`, cached): their root names the
    /// backing store, which may differ on the node that replays the query.
    /// A qualified path on a plain local disk is host-absolute and is re-normalized by the
    /// absolute branch of the constructor, so it is serialized as is.
    String serialized_path = path;
    if (auto user_files_volume = getContext()->getUserFilesVolume())
    {
        auto [disk, disk_relative_path] = splitUserFilesAbsolutePath(path, user_files_volume->getDisks());
        if (disk && !isPlainLocalDisk(*disk))
            serialized_path = disk_relative_path;
    }

    const String query = fmt::format("CREATE DATABASE {} ENGINE = Filesystem('{}')", backQuoteIfNeed(database_name), serialized_path);

    ParserCreateQuery parser;
    ASTPtr ast
        = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

    if (!comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, make_intrusive<ASTLiteral>(comment));
    }

    return ast;
}

void DatabaseFilesystem::shutdown()
{
    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = loaded_tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->flushAndShutdown();
    }

    std::lock_guard lock(mutex);
    loaded_tables.clear();
}

/**
 * Returns an empty vector because the database is read-only and no tables can be backed up
 */
std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseFilesystem::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    return {};
}

/**
 *
 * Returns an empty iterator because the database does not have its own tables
 * But only caches them for quick access
 */
DatabaseTablesIteratorPtr DatabaseFilesystem::getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const
{
    return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
}

void registerDatabaseFilesystem(DatabaseFactory & factory);
void registerDatabaseFilesystem(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;
        const String & engine_name = engine_define->engine->name;

        /// If init_path is empty, then the current path will be used
        std::string init_path;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            if (engine->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filesystem database requires at most 1 argument: filesystem_path");

            const auto & arguments = engine->arguments->children;
            init_path = safeGetLiteralValue<String>(arguments[0], engine_name);
        }

        /// The loader flag, not `internal`, is the discriminator: an internal query is not necessarily the
        /// server's own replay, because wrappers run user statements as internal ones.
        const bool is_internal_metadata_replay
            = args.is_metadata_replay && args.mode >= LoadingStrictnessLevel::ATTACH;

        return std::make_shared<DatabaseFilesystem>(args.database_name, init_path, args.context, is_internal_metadata_replay);
    };
    factory.registerDatabase("Filesystem", create_fn, SecretArgumentsSpec{}, {
        .supports_arguments = true,
        .is_external = true,
        .source_access_type = AccessTypeObjects::Source::FILE,
    }, Documentation{
        .description = R"DOCS_MD(
The `Filesystem` database engine exposes files in a local directory as read-only tables. A table name is resolved as a path relative to the database directory and is read using the [`file`](/reference/functions/table-functions/file) table function.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE files
ENGINE = Filesystem([path]);
```

`path` is the directory that contains the files. If it is omitted, ClickHouse uses the current directory in `clickhouse-local` and the `user_files` directory in ClickHouse server.

## Usage {#usage}

For example, with `data.csv` in the selected directory:

```sql
CREATE DATABASE files ENGINE = Filesystem('imports');

SELECT * FROM files.`data.csv`;
```

The table name can include a relative path beneath the database directory. The table schema and format are inferred in the same way as for the `file` table function.

The database owns no table definitions: tables are created when their files are first resolved and are only cached for subsequent access. `CREATE TABLE`, `INSERT`, and other writes through this database are not supported.

## Access control {#access-control}

On ClickHouse server, the database directory and every resolved file must be inside [`user_files_path`](/reference/settings/server-settings/settings#user_files_path); this restriction also applies after following symlinks. `clickhouse-local` is not restricted to `user_files_path`.

Creating this database requires `READ` and `WRITE` source grants on `FILE`, regardless of [`table_engines_require_grant`](/reference/settings/server-settings/settings/other#table_engines_require_grant). Grant them with, for example:

```sql
GRANT READ, WRITE ON FILE TO user_name;
```

See the [`SOURCES` privileges](/reference/statements/grant#sources) for version and compatibility details.

## See also {#see-also}

- [`file` table function](/reference/functions/table-functions/file)
- [S3 database engine](/reference/engines/database-engines/s3)
- [HDFS database engine](/reference/engines/database-engines/hdfs)
)DOCS_MD",
        .syntax = "ENGINE = Filesystem([path])",
        .related = {"S3", "HDFS"}});
}
}
