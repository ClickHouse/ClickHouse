#pragma once
#include <string>
#include <Coordination/KeeperStorage.h>
#include <common/logger_useful.h>

namespace DB
{

void deserializeKeeperStorageFromSnapshot(KeeperStorage & storage, const std::string & snapshot_path, Poco::Logger * log);

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorage & storage, const std::string & path, Poco::Logger * log);

void deserializeLogAndApplyToStorage(KeeperStorage & storage, const std::string & log_path, Poco::Logger * log);

void deserializeLogsAndApplyToStorage(KeeperStorage & storage, const std::string & path, Poco::Logger * log);

}
