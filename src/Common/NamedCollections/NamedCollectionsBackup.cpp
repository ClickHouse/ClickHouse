#include <Common/NamedCollections/NamedCollectionsBackup.h>

#include <Access/Common/AccessFlags.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupCoordination.h>
#include <Backups/IRestoreCoordination.h>
#include <Backups/RestorerFromBackup.h>
#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
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
    auto context = backup_entries_collector.getContext();
    auto & factory = NamedCollectionFactory::instance();
    const auto storage_properties = factory.getMetadataStorageProperties();

    /// The entries are the `CREATE NAMED COLLECTION` statements with their values. With an
    /// encrypted storage they are copied as ciphertext, but any server holding the same key
    /// restores them into collections whose values can then be read, so exporting them is
    /// still equivalent to reading the values and requires `SHOW_NAMED_COLLECTIONS_SECRETS`,
    /// the same right `system.named_collections` requires to show the values.
    context->checkAccess(AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS} | AccessFlags{AccessType::SHOW_NAMED_COLLECTIONS_SECRETS});

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
    backup_coordination->addReplicatedSQLObjectsDir(
        storage_properties.replication_id, UserDefinedSQLObjectType::NamedCollection, data_path_in_backup);

    // On the stage of running post tasks, all directories will already be added to the backup coordination object.
    // They will only be returned for one of the hosts below, for the rest an empty list.
    // See also BackupCoordinationReplicatedSQLObjects class.
    backup_entries_collector.addPostTask(
        [my_backup_entries = std::move(backup_entries),
         replication_id = storage_properties.replication_id,
         &backup_entries_collector,
         backup_coordination]
        {
            auto dirs = backup_coordination->getReplicatedSQLObjectsDirs(replication_id, UserDefinedSQLObjectType::NamedCollection);

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
        && !restorer.getRestoreCoordination()->acquireReplicatedSQLObjects(
            storage_properties.replication_id, UserDefinedSQLObjectType::NamedCollection))
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
        else if (!filename.ends_with(sql_extension))
            throw Exception(
                ErrorCodes::CANNOT_RESTORE_TABLE,
                "Cannot restore named collections: File name {} doesn't have the extension {} or {}",
                String{data_path_in_backup_fs / filename},
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
    /// are restored before any table is created, nothing has been restored at all at this point.
    std::vector<ASTPtr> create_queries;
    create_queries.reserve(filenames.size());
    for (const auto & filename : filenames)
    {
        String filepath = data_path_in_backup_fs / filename;
        auto in = backup->readFile(filepath);
        String stored;
        readStringUntilEOF(stored, *in);

        try
        {
            String statement = filename.ends_with(encrypted_sql_extension) ? factory.decryptStoredEntry(stored) : stored;

            ParserCreateNamedCollectionQuery parser;
            ASTPtr ast = parseQuery(
                parser,
                statement.data(),
                statement.data() + statement.size(),
                "in file " + filepath + " from backup " + backup->getNameForLogging(),
                0,
                context->getSettingsRef()[Setting::max_parser_depth],
                context->getSettingsRef()[Setting::max_parser_backtracks]);
            create_queries.push_back(std::move(ast));
        }
        catch (Exception & e)
        {
            e.addMessage(
                "While reading the named collection entry {} from backup {} (SETTINGS restore_named_collections = false "
                "skips restoring named collections)",
                filepath,
                backup->getNameForLogging());
            throw;
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
