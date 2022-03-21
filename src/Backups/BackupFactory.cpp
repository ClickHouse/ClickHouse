#include <Backups/BackupFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BACKUP_ENGINE_NOT_FOUND;
    extern const int LOGICAL_ERROR;
}

BackupFactory & BackupFactory::instance()
{
    static BackupFactory the_instance;
    return the_instance;
}

BackupMutablePtr BackupFactory::createBackup(const CreateParams & params) const
{
    const String & engine_name = params.backup_info.backup_engine_name;
    auto it = creators.find(engine_name);
    if (it == creators.end())
        throw Exception(ErrorCodes::BACKUP_ENGINE_NOT_FOUND, "Not found backup engine {}", engine_name);
    return (it->second)(params);
}

void BackupFactory::registerBackupEngine(const String & engine_name, const CreatorFn & creator_fn)
{
    if (creators.contains(engine_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Backup engine {} was registered twice", engine_name);
    creators[engine_name] = creator_fn;
}

void registerBackupEngines(BackupFactory & factory);

BackupFactory::BackupFactory()
{
    registerBackupEngines(*this);
}

}
