#pragma once

#include <Parsers/ASTBackupQuery.h>


namespace DB
{

class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IRestoreTask;
using RestoreTaskPtr = std::unique_ptr<IRestoreTask>;
using RestoreTasks = std::vector<RestoreTaskPtr>;
struct RestoreSettings;
class Context;
using ContextMutablePtr = std::shared_ptr<Context>;

/// Prepares restore tasks.
RestoreTasks makeRestoreTasks(ContextMutablePtr context, const BackupPtr & backup, const ASTBackupQuery::Elements & elements, const RestoreSettings & restore_settings);

/// Executes restore tasks.
void executeRestoreTasks(RestoreTasks && tasks, size_t num_threads);

}
