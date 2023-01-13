

namespace DB
{
class BackupFactory;

void registerBackupEngineFile(BackupFactory &);

void registerBackupEngines(BackupFactory & factory)
{
    registerBackupEngineFile(factory);
}

}
