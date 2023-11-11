#pragma once
#include <string>
#include <Coordination/KeeperStorage.h>

namespace DB
{

template<typename Storage>
void deserializeKeeperStorageFromSnapshot(Storage & storage, const std::string & snapshot_path, Poco::Logger * log);

template<typename Storage>
void deserializeKeeperStorageFromSnapshotsDir(Storage & storage, const std::string & path, Poco::Logger * log);

template<typename Storage>
void deserializeLogAndApplyToStorage(Storage & storage, const std::string & log_path, Poco::Logger * log);

template<typename Storage>
void deserializeLogsAndApplyToStorage(Storage & storage, const std::string & path, Poco::Logger * log);

}
