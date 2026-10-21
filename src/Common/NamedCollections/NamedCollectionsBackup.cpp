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
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <base/sort.h>

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
    extern const int CANNOT_RESTORE_TABLE;
}

namespace
{

/// Entries of an encrypted metadata storage are stored in the backup as the `FileEncryption`
/// ciphertext the storage holds, with a distinct extension so that a restoring server can tell
/// them from the plaintext `.sql` entries of the unencrypted storage types (and so that an old
/// server never tries to parse ciphertext as SQL).
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
        backup_entries.emplace_back(
            escapeForFileName(collection_name) + String{extension}, std::make_shared<BackupEntryFromMemory>(std::move(stored)));

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


void restoreNamedCollections(RestorerFromBackup & restorer, const String & data_path_in_backup)
{
    auto context = restorer.getContext();
    auto & factory = NamedCollectionFactory::instance();
    const auto storage_properties = factory.getMetadataStorageProperties();

    if (storage_properties.is_replicated
        && !restorer.getRestoreCoordination()->acquireReplicatedNamedCollections(storage_properties.replication_id))
        return; /// Other replica is already restoring named collections.

    auto backup = restorer.getBackup();
    fs::path data_path_in_backup_fs{data_path_in_backup};

    Strings filenames = backup->listFiles(data_path_in_backup, /*recursive*/ false);
    if (filenames.empty())
        return; /// Nothing to restore (in particular, a backup taken before named collections were backed up).

    ::sort(filenames.begin(), filenames.end());

    bool backup_is_encrypted = false;
    for (const auto & filename : filenames)
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
    }

    if (backup_is_encrypted && !storage_properties.is_encrypted)
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "The backup contains encrypted named collections, but the storage for named collections of this server "
            "is not encrypted (see `named_collections_storage` in the server configuration). Either configure an "
            "encrypted storage holding the key the collections were encrypted with, or skip them with "
            "SETTINGS restore_named_collections = false");

    /// Read, decrypt and parse every entry before creating anything, so that a problem - in
    /// particular a key mismatch, which `decryptStoredEntry` detects by the key fingerprint of the
    /// entry's header - fails the restore before it has touched anything. Because the collections
    /// are restored before any database or table is created, nothing has been restored at all at
    /// this point.
    std::vector<ASTPtr> create_queries;
    create_queries.reserve(filenames.size());
    for (const auto & filename : filenames)
    {
        String filepath = data_path_in_backup_fs / filename;
        auto in = backup->readFile(filepath);
        String stored;
        readStringUntilEOF(stored, *in);

        String statement;
        try
        {
            statement = filename.ends_with(encrypted_sql_extension) ? factory.decryptStoredEntry(stored) : stored;
        }
        catch (Exception & e)
        {
            e.addMessage(
                "While reading a named collection entry from backup {} (SETTINGS restore_named_collections = false "
                "skips restoring named collections)",
                backup->getNameForLogging());
            throw;
        }

        try
        {
            ParserCreateNamedCollectionQuery parser;
            ASTPtr ast = parseQuery(
                parser,
                statement.data(),
                statement.data() + statement.size(),
                /* description= */ "",
                0,
                context->getSettingsRef()[Setting::max_parser_depth],
                context->getSettingsRef()[Setting::max_parser_backtracks]);
            create_queries.push_back(std::move(ast));
        }
        catch (const Exception &)
        {
            /// A parser exception embeds the text around the error position, and here that text is the
            /// statement with the collection's values - for an encrypted entry, decrypted secret material,
            /// while restoring requires only `CREATE NAMED COLLECTION`, not the rights to read the values.
            /// The CTR encryption of the storage is not authenticated (`decryptStoredEntry` verifies only
            /// the key fingerprint of the header), so a corrupted or tampered entry decrypts into garbled
            /// plaintext and reaches this parser. Report the failure without the original exception and
            /// without the entry path (which is the collection's name), so that nothing the restoring user
            /// may not read leaks to them or to the log.
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot parse a named collection entry from backup {} as a CREATE NAMED COLLECTION statement, "
                "the entry may be corrupted (SETTINGS restore_named_collections = false skips restoring named collections)",
                backup->getNameForLogging());
        }
    }

    const auto & restore_settings = restorer.getRestoreSettings();
    bool throw_if_exists = (restore_settings.create_named_collection == RestoreNamedCollectionCreationMode::kCreate);
    bool replace_if_exists = (restore_settings.create_named_collection == RestoreNamedCollectionCreationMode::kReplace);

    auto log = getLogger("restoreNamedCollections");

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
            LOG_WARNING(
                log,
                "Not restoring named collection `{}` from backup because a collection with this name "
                "is defined in the server configuration",
                create_query.collection_name);
            continue;
        }

        create_query.if_not_exists = !throw_if_exists && !replace_if_exists;
        create_query.or_replace = replace_if_exists;
        /// `createFromSQL` honours both flags and writes through the metadata storage, so the
        /// entry is re-encrypted under this server's key with a fresh IV (and replicated by
        /// Keeper for the Keeper storage types).
        factory.createFromSQL(create_query);
        LOG_TRACE(log, "Restored named collection `{}` from backup", create_query.collection_name);
    }
}


void reloadNamedCollectionsAfterRestore()
{
    auto & factory = NamedCollectionFactory::instance();
    if (factory.usesReplicatedStorage())
        factory.reloadFromSQL();
}

}
