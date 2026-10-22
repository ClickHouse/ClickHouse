#include <Common/NamedCollections/NamedCollectionsBackup.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/ErrorCodes.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <base/sort.h>

#include <algorithm>
#include <filesystem>
#include <unordered_map>

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
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{

/// Entries of an encrypted metadata storage are stored in the backup as the `FileEncryption`
/// ciphertext the storage holds followed by an authentication code (see
/// `NamedCollectionsMetadataStorage::decryptStoredFromBackup`), with a distinct extension so that
/// a restoring server can tell them from the plaintext `.sql` entries of the unencrypted storage
/// types (and so that an old server never tries to parse ciphertext as SQL).
constexpr std::string_view sql_extension = ".sql";
constexpr std::string_view encrypted_sql_extension = ".sql.enc";

}


void backupNamedCollections(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup)
{
    const auto & backup_named_collections = backup_entries_collector.getBackupSettings().backup_named_collections;

    /// `backup_named_collections = false` turns the export off entirely, even for an explicitly
    /// named `system.named_collections` table.
    if (backup_named_collections == false)
        return;

    auto context = backup_entries_collector.getContext();

    /// The entries are the `CREATE NAMED COLLECTION` statements with their values. With an
    /// encrypted storage they are copied as ciphertext, but any server holding the same key
    /// restores them into collections whose values can then be read, so exporting them is
    /// equivalent to reading the values and requires the same rights `system.named_collections`
    /// requires to show the values: `SHOW_NAMED_COLLECTIONS_SECRETS` together with
    /// `displaySecretsInShowAndSelect` (the former alone does not allow reading the values back).
    const AccessFlags show_values_rights = AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS} | AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS_SECRETS};

    if (!backup_named_collections.has_value()
        && !backup_entries_collector.isTableExplicitlySelectedForBackup(
            QualifiedTableName{DatabaseCatalog::SYSTEM_DATABASE, "named_collections"}))
    {
        /// The default for a DATABASE or ALL element selecting this table implicitly: export the
        /// collections if the user may read their values, skip them with a warning otherwise, so
        /// that `BACKUP ALL` works for a user who may back up everything but may not read the
        /// values. For `BACKUP ON CLUSTER` this decision was already taken by the initiating host
        /// against the initiating user (see `BackupsWorker`), because this host may be running the
        /// query with full access.
        auto access = context->getAccess();
        if (!access->isGranted(show_values_rights) || !access->isGranted(AccessType::displaySecretsInShowAndSelect))
        {
            LOG_WARNING(
                getLogger("backupNamedCollections"),
                "Not exporting the named collections to the backup: the user has no rights to read their values "
                "(`SHOW NAMED COLLECTIONS`, `SHOW NAMED COLLECTIONS SECRETS` and `displaySecretsInShowAndSelect`). "
                "Set backup_named_collections = 1 to make this an error, or backup_named_collections = 0 to "
                "silence this warning");
            return;
        }
    }

    /// With `backup_named_collections = true` or the table named explicitly, the missing rights are an
    /// error. The same rights are also required of the initiating user of a `BACKUP ON CLUSTER` query
    /// before it is dispatched (see `BackupUtils::getRequiredAccessToBackup`), because the hosts
    /// running this check may execute the query with full access.
    context->checkAccess(show_values_rights);
    context->checkAccess(AccessType::displaySecretsInShowAndSelect);

    auto & factory = NamedCollectionFactory::instance();
    const auto storage_properties = factory.getMetadataStorageProperties();

    auto raw_entries = factory.getRawEntriesForBackup();

    if (raw_entries.empty())
        return;

    std::vector<std::pair<String, BackupEntryPtr>> backup_entries;
    backup_entries.reserve(raw_entries.size());
    const std::string_view extension = storage_properties.is_encrypted ? encrypted_sql_extension : sql_extension;
    for (auto & [collection_name, stored] : raw_entries)
    {
        /// The backup medium is less trusted than the metadata storage, and the stored CTR
        /// ciphertext alone is malleable, so an exported entry carries an authentication code.
        if (storage_properties.is_encrypted)
            stored = factory.authenticateStoredEntryForBackup(stored);
        backup_entries.emplace_back(
            escapeForFileName(collection_name) + String{extension}, std::make_shared<BackupEntryFromMemory>(std::move(stored)));
    }

    if (!storage_properties.is_replicated)
    {
        fs::path data_path_in_backup_fs{data_path_in_backup};
        for (const auto & [file_name, entry] : backup_entries)
            backup_entries_collector.addBackupEntry(data_path_in_backup_fs / file_name, entry);
        return;
    }

    /// The Keeper storage is shared by all replicas: register the directory with the backup
    /// coordination and emit the entries from a post task so only one host writes them.
    auto backup_coordination = backup_entries_collector.getBackupCoordination();
    backup_coordination->addReplicatedNamedCollectionsDir(storage_properties.replication_id, data_path_in_backup);

    // On the stage of running post tasks, all directories will already be added to the backup coordination object.
    // They will only be returned for one of the hosts below, for the rest an empty list.
    // See also BackupCoordinationReplicatedNamedCollections class.
    backup_entries_collector.addPostTask(
        [my_backup_entries = std::move(backup_entries),
         replication_id = storage_properties.replication_id,
         &backup_entries_collector,
         backup_coordination]
        {
            auto dirs = backup_coordination->getReplicatedNamedCollectionsDirs(replication_id);

            for (const auto & dir : dirs)
            {
                fs::path dir_fs{dir};
                for (const auto & [file_name, entry] : my_backup_entries)
                {
                    backup_entries_collector.addBackupEntry(dir_fs / file_name, entry);
                }
            }
        });
}


void registerNamedCollectionsDirForRestore(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    const auto storage_properties = NamedCollectionFactory::instance().getMetadataStorageProperties();
    if (!storage_properties.is_replicated)
        return;
    restorer.getRestoreCoordination()->addReplicatedNamedCollectionsDir(storage_properties.replication_id, data_path_in_backup);
}


bool restoreNamedCollections(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    auto context = restorer.getContext();
    auto & factory = NamedCollectionFactory::instance();
    const auto storage_properties = factory.getMetadataStorageProperties();

    Strings data_paths;
    if (storage_properties.is_replicated)
    {
        auto restore_coordination = restorer.getRestoreCoordination();

        if (data_path_in_backup.empty())
        {
            /// This host's part of the backup holds no named collections, but a host sharing this
            /// storage may restore the ones its own part holds, and then this host has to reload.
            return !restore_coordination->getReplicatedNamedCollectionsDirs(storage_properties.replication_id).empty();
        }

        if (!restore_coordination->acquireReplicatedNamedCollections(storage_properties.replication_id))
            return true; /// Other replica is already restoring named collections into the shared storage.

        /// The directories registered by every host sharing this storage. They hold identical
        /// entries when the backup was taken from hosts sharing a storage the same way, but a
        /// backup taken from hosts with per-host storages (e.g. the `local` type) holds the
        /// collections of each host in its own directory, and restoring just this host's
        /// directory would silently lose the collections of the others.
        data_paths = restore_coordination->getReplicatedNamedCollectionsDirs(storage_properties.replication_id);
        if (std::find(data_paths.begin(), data_paths.end(), data_path_in_backup) == data_paths.end())
            data_paths.push_back(data_path_in_backup);
        ::sort(data_paths.begin(), data_paths.end());
    }
    else
    {
        if (data_path_in_backup.empty())
            return false;
        data_paths = {data_path_in_backup};
    }

    auto backup = restorer.getBackup();

    std::vector<std::pair<String /* data path */, String /* filename */>> entry_files;
    bool backup_is_encrypted = false;
    for (const auto & data_path : data_paths)
    {
        Strings filenames = backup->listFiles(data_path, /*recursive*/ false);
        ::sort(filenames.begin(), filenames.end());
        for (auto & filename : filenames)
        {
            if (filename.ends_with(encrypted_sql_extension))
                backup_is_encrypted = true;
            /// The entry file names are the collection names, and seeing those requires a right
            /// (`SHOW NAMED COLLECTIONS`) which restoring does not: an operator must be able to restore
            /// a backup without being able to read it. So no error below names an entry.
            else if (!filename.ends_with(sql_extension))
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore named collections: backup {} contains an entry whose file name has neither the {} "
                    "nor the {} extension",
                    backup->getNameForLogging(),
                    sql_extension,
                    encrypted_sql_extension);
            entry_files.emplace_back(data_path, std::move(filename));
        }
    }

    if (entry_files.empty())
        return false; /// Nothing to restore (in particular, a backup taken before named collections were backed up).

    if (backup_is_encrypted && !storage_properties.is_encrypted)
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "The backup contains encrypted named collections, but the storage for named collections of this server "
            "is not encrypted (see `named_collections_storage` in the server configuration). Either configure an "
            "encrypted storage holding the key the collections were encrypted with, or skip them with "
            "SETTINGS restore_named_collections = false");

    /// Read, authenticate, decrypt and parse every entry before creating anything, so that a
    /// problem - a key mismatch, which `decryptStoredEntryFromBackup` detects by the key
    /// fingerprint of the entry's header, or a tampered entry, which it detects by the
    /// authentication code - fails the restore before it has touched anything. Because the
    /// collections are restored before any database or table is created, nothing has been
    /// restored at all at this point.
    std::vector<ASTPtr> create_queries;
    create_queries.reserve(entry_files.size());
    /// With several directories the same collection legitimately appears in more than one of them
    /// (the backup of a shared storage holds an identical directory for every host sharing it), so
    /// identical definitions collapse into one. The same name with a different definition means
    /// the directories hold collections which could not have coexisted in one storage; silently
    /// picking one of the definitions would restore wrong values, so such a backup is refused.
    std::unordered_map<String /* collection name */, String /* formatted statement */> statements_by_name;
    for (const auto & [entry_data_path, filename] : entry_files)
    {
        String filepath = fs::path(entry_data_path) / filename;
        String stored;
        try
        {
            /// An exception is recorded into `system.errors` (and `system.error_log`) when it is
            /// constructed, before the sanitizing catch below replaces it, so the suppression keeps
            /// the original out of the error statistics too. The sanitized replacement is
            /// constructed outside the scope and is recorded normally.
            Exception::SuppressErrorCodesScope suppress_error_codes;
            auto in = backup->readFile(filepath);
            readStringUntilEOF(stored, *in);
        }
        catch (...)
        {
            /// `readFile` names the entry in its errors - `BACKUP_ENTRY_NOT_FOUND` for an entry
            /// listed but missing or inconsistent, in particular - and the entry name is the
            /// collection's name, which the restoring user may not see (as explained above).
            /// Report the failure without the original exception and without the entry path;
            /// the error code alone cannot leak the name.
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot read a named collection entry from backup {} ({}), the backup may be corrupted or "
                "incomplete (SETTINGS restore_named_collections = false skips restoring named collections)",
                backup->getNameForLogging(),
                ErrorCodes::getName(getCurrentExceptionCode()));
        }

        String statement;
        try
        {
            statement = filename.ends_with(encrypted_sql_extension) ? factory.decryptStoredEntryFromBackup(stored) : stored;
        }
        catch (Exception & e)
        {
            e.addMessage(
                "While reading a named collection entry from backup {} (SETTINGS restore_named_collections = false "
                "skips restoring named collections)",
                backup->getNameForLogging());
            throw;
        }

        ASTPtr ast;
        try
        {
            /// Keeps the parser exception, which embeds the decrypted statement, out of the error
            /// statistics; see the read above.
            Exception::SuppressErrorCodesScope suppress_error_codes;
            ParserCreateNamedCollectionQuery parser;
            ast = parseQuery(
                parser,
                statement.data(),
                statement.data() + statement.size(),
                /* description= */ "",
                0,
                context->getSettingsRef()[Setting::max_parser_depth],
                context->getSettingsRef()[Setting::max_parser_backtracks]);
        }
        catch (const Exception &)
        {
            /// A parser exception embeds the text around the error position, and here that text is the
            /// statement with the collection's values - for an encrypted entry, decrypted secret material,
            /// while restoring requires only `CREATE NAMED COLLECTION`, not the rights to read the values.
            /// A plaintext `.sql` entry is read from the backup unauthenticated, so a corrupted or crafted
            /// entry reaches this parser (a tampered encrypted entry is rejected by its authentication
            /// code before it is decrypted). Report the failure without the original exception and
            /// without the entry path (which is the collection's name), so that nothing the restoring user
            /// may not read leaks to them or to the log.
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot parse a named collection entry from backup {} as a CREATE NAMED COLLECTION statement, "
                "the entry may be corrupted (SETTINGS restore_named_collections = false skips restoring named collections)",
                backup->getNameForLogging());
        }

        /// Like the errors above, the conflict error below names no collection.
        const auto & parsed_create_query = ast->as<const ASTCreateNamedCollectionQuery &>();
        String formatted_statement = ast->formatWithSecretsOneLine();
        if (auto [it, inserted] = statements_by_name.try_emplace(parsed_create_query.collection_name, formatted_statement); !inserted)
        {
            if (it->second != formatted_statement)
                throw Exception(
                    ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Cannot restore named collections: the directories of different hosts in backup {} define the same "
                    "named collection differently, and the named collections storage of this server is shared by all "
                    "the hosts restoring them. Restore the collections of one host with SETTINGS shard_num_in_backup / "
                    "replica_num_in_backup, or skip them with SETTINGS restore_named_collections = false",
                    backup->getNameForLogging());
            continue; /// The same collection from another host's identical directory.
        }

        create_queries.push_back(std::move(ast));
    }

    const auto & restore_settings = restorer.getRestoreSettings();
    bool throw_if_exists = (restore_settings.create_named_collection == RestoreNamedCollectionCreationMode::kCreate);
    bool replace_if_exists = (restore_settings.create_named_collection == RestoreNamedCollectionCreationMode::kReplace);

    size_t restored_count = 0;
    size_t kept_from_config_count = 0;

    for (auto & ast : create_queries)
    {
        auto & create_query = ast->as<ASTCreateNamedCollectionQuery &>();

        /// A collection defined in the server configuration is rendered from the configuration, not
        /// stored, so it is never part of a backup, and a backed up collection with the same name
        /// must not overwrite it. With the default `create_named_collection = 'if-not-exists'` the
        /// config-defined collection is kept and the entry is skipped; the explicit modes surface
        /// the conflict through `createFromSQL` below (`NAMED_COLLECTION_ALREADY_EXISTS` for
        /// 'create', `NAMED_COLLECTION_IS_IMMUTABLE` for 'replace').
        if (auto existing = factory.tryGet(create_query.collection_name);
            existing && existing->getSourceId() == NamedCollection::SourceId::CONFIG && !throw_if_exists && !replace_if_exists)
        {
            ++kept_from_config_count;
            continue;
        }

        create_query.if_not_exists = !throw_if_exists && !replace_if_exists;
        create_query.or_replace = replace_if_exists;
        /// `createFromSQL` honours both flags and writes through the metadata storage, so the
        /// entry is re-encrypted under this server's key with a fresh IV (and replicated by
        /// Keeper for the Keeper storage types).
        try
        {
            /// Keeps the name-bearing errors of `createFromSQL` out of the error statistics; see
            /// the read in the first loop.
            Exception::SuppressErrorCodesScope suppress_error_codes;
            factory.createFromSQL(create_query);
        }
        catch (...)
        {
            /// The errors of `createFromSQL` name the collection (`NAMED_COLLECTION_ALREADY_EXISTS`
            /// with the 'create' mode, `NAMED_COLLECTION_IS_IMMUTABLE` for a config-defined
            /// collection with the 'replace' mode, a Keeper error naming the entry's path), and the
            /// restoring user may not see the name (as explained above). Report the failure without
            /// the original exception and without the name; the error code alone cannot leak it.
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot restore a named collection from backup {} ({}) (SETTINGS restore_named_collections = false "
                "skips restoring named collections)",
                backup->getNameForLogging(),
                ErrorCodes::getName(getCurrentExceptionCode()));
        }
        ++restored_count;
    }

    /// The restoring user receives the log records of the query (`send_logs_level`) without
    /// `SHOW NAMED COLLECTIONS`, so, like the errors above, the log records name no collection.
    auto log = getLogger("restoreNamedCollections");
    if (kept_from_config_count)
        LOG_WARNING(
            log,
            "Not restoring {} named collections from the backup because collections with the same "
            "names are defined in the server configuration",
            kept_from_config_count);
    LOG_DEBUG(log, "Restored {} named collections from backup", restored_count);
    return true;
}


void reloadNamedCollectionsAfterRestore()
{
    auto & factory = NamedCollectionFactory::instance();
    if (factory.usesReplicatedStorage())
        factory.reloadFromSQL();
}

}
