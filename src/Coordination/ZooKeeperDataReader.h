#pragma once
#include <string>
#include <Coordination/KeeperStorage.h>
#include <common/logger_useful.h>

namespace DB
{

void deserializeKeeperStorageFromSnapshot(KeeperStorage & storage, const std::string & snapshot_path, Poco::Logger * log = nullptr);

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorage & storage, const std::string & path, Poco::Logger * log = nullptr);

void deserializeLogAndApplyToStorage(KeeperStorage & storage, const std::string & log_path, Poco::Logger * log = nullptr);

void deserializeLogsAndApplyToStorage(KeeperStorage & storage, const std::string & path, Poco::Logger * log = nullptr);

}
