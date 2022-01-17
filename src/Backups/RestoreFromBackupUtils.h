#pragma once

#include <Parsers/ASTBackupQuery.h>


namespace DB
{

class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreFromBackupTask;
using RestoreFromBackupTaskPtr = std::unique_ptr<IRestoreFromBackupTask>;
using RestoreFromBackupTasks = std::vector<RestoreFromBackupTaskPtr>;
class Context;
using ContextMutablePtr = std::shared_ptr<Context>;

/// Prepares restore tasks.
RestoreFromBackupTasks makeRestoreTasks(ContextMutablePtr context, const BackupPtr & backup, const ASTBackupQuery::Elements & elements);

/// Executes restore tasks.
void executeRestoreTasks(RestoreFromBackupTasks && tasks, size_t num_threads);

}
