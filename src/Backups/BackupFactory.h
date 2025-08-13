#pragma once

#include <Backups/IBackup.h>
#include <Backups/BackupInfo.h>
#include <Core/Types.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <Parsers/IAST_fwd.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <optional>
#include <unordered_map>


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
        std::shared_ptr<IBackupCoordination> backup_coordination;
        std::optional<UUID> backup_uuid;
        bool deduplicate_files = true;
        bool allow_s3_native_copy = true;
        bool allow_azure_native_copy = true;
        bool use_same_s3_credentials_for_base_backup = false;
        bool use_same_password_for_base_backup = false;
        bool azure_attempt_to_create_container = true;
        ReadSettings read_settings;
        WriteSettings write_settings;
    };

    static BackupFactory & instance();

    /// Creates a new backup or opens it.
    BackupMutablePtr createBackup(const CreateParams & params) const;

    using CreatorFn = std::function<BackupMutablePtr(const CreateParams & params)>;
    void registerBackupEngine(const String & engine_name, const CreatorFn & creator_fn);

private:
    BackupFactory();

    std::unordered_map<String, CreatorFn> creators;
};

}
