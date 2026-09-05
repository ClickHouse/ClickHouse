#pragma once

#include "config.h"

#include <memory>
#include <optional>
#include <unordered_map>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Backups/BackupDataFileNameGeneratorType.h>
#include <Backups/BackupInfo.h>
#if CLICKHOUSE_CLOUD
#include <Backups/BackupResumeParams.h>
#endif
#include <Backups/IBackup.h>
#include <Core/Types.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Parsers/IAST_fwd.h>
#include <boost/noncopyable.hpp>


namespace DB
{
class IBackupCoordination;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Factory for implementations of the IBackup interface.
class BackupFactory : boost::noncopyable
{
public:
    using OpenMode = IBackup::OpenMode;

    struct CreateParams
    {
        OpenMode open_mode = OpenMode::WRITE;
        BackupInfo backup_info;
        std::optional<BackupInfo> base_backup_info;
        String compression_method;
        int compression_level = -1;
        String password;
        String s3_storage_class;
        ContextPtr context;
        bool is_internal_backup = false;
        bool is_lightweight_snapshot = false;
        BackupDataFileNameGeneratorType data_file_name_generator = BackupDataFileNameGeneratorType::FirstFileName;
        size_t data_file_name_prefix_length = 3;
        std::shared_ptr<IBackupCoordination> backup_coordination;
        std::optional<UUID> backup_uuid;
        String backup_id;
        bool deduplicate_files = true;
        bool allow_s3_native_copy = true;
        bool allow_azure_native_copy = true;
        bool use_same_s3_credentials_for_base_backup = false;
        bool use_same_password_for_base_backup = false;
        bool azure_attempt_to_create_container = true;
        ReadSettings read_settings;
        WriteSettings write_settings;
#if CLICKHOUSE_CLOUD
        /// Set only when this attempt continues an interrupted one; see `BackupResumer`.
        std::optional<BackupResumeParams> resume;
#endif

        CreateParams getCreateParamsForBaseBackup(BackupInfo base_backup_info_, String old_password) const;
    };

    static BackupFactory & instance();

    /// Creates a new backup or opens it.
    BackupMutablePtr createBackup(const CreateParams & params) const;

    /// Returns a versioned, credential-free identity of the effective backup location.
    /// Named collections must be frozen first so identity generation and backup creation
    /// observe the same collection state.
    String getDestinationIdentity(const BackupInfo & backup_info, ContextPtr context) const;

    /// The SOURCES grant required to open `backup_info` in `open_mode`, as declared by its engine.
    struct SourceAccessTarget
    {
        AccessTypeObjects::Source source;
        /// The URI to match regex-filtered grants against. Empty means a whole-source grant is required.
        String uri;
        AccessFlags flags;
    };

    /// `std::nullopt` for engines with no external location (`Memory`, `Null`).
    using SourceAccessFn
        = std::function<std::optional<SourceAccessTarget>(const BackupInfo &, ContextPtr, IBackup::OpenMode)>;

    /// Performs no I/O, so it is also callable as a preflight before a query is distributed or a
    /// database is created.
    void checkSourceAccess(const BackupInfo & backup_info, ContextPtr context, OpenMode open_mode) const;

    using CreatorFn = std::function<BackupMutablePtr(const CreateParams & params)>;
    using DestinationIdentityFn = std::function<Strings(const BackupInfo & backup_info, ContextPtr context)>;
    void registerBackupEngine(
        const String & engine_name,
        const CreatorFn & creator_fn,
        const DestinationIdentityFn & destination_identity_fn,
        const SourceAccessFn & source_access_fn);

private:
    struct RegisteredEngine
    {
        CreatorFn creator;
        DestinationIdentityFn destination_identity;
        SourceAccessFn source_access;
    };

    BackupFactory();

    std::unordered_map<String, RegisteredEngine> engines;
};

}
