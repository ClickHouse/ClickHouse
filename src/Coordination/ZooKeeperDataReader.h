#pragma once
#include <string>
#include <memory>

#include <Coordination/KeeperStorage_fwd.h>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

void deserializeKeeperStorageFromSnapshot(KeeperMemoryStorage & storage, const std::string & snapshot_path, LoggerPtr log);

void deserializeKeeperStorageFromSnapshotsDir(KeeperMemoryStorage & storage, const std::string & path, LoggerPtr log);

void deserializeLogAndApplyToStorage(KeeperMemoryStorage & storage, const std::string & log_path, LoggerPtr log);

void deserializeLogsAndApplyToStorage(KeeperMemoryStorage & storage, const std::string & path, LoggerPtr log);

}
