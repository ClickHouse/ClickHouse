#pragma once

#include <Backups/IBackup.h>
#include <Backups/BackupInfo.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <optional>
#include <unordered_map>


namespace DB
{
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
        ContextPtr context;
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
