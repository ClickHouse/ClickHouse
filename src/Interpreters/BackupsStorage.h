#pragma once

#include <Interpreters/SystemLogStorage.h>
#include <Interpreters/BackupInfoElement.h>

namespace DB
{

class BackupsStorage: public SystemLogStorage<BackupInfoElement>
{
public:
    BackupsStorage(ContextPtr context_, const String & database, const String & table, const String & engine, bool prepare = false);
    void update(const BackupOperationInfo & info);
};

}
