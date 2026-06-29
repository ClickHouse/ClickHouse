#pragma once
#include <string>
#include <memory>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

class KeeperStorage;

void deserializeKeeperStorageFromSnapshot(KeeperStorage & storage, const std::string & snapshot_path, LoggerPtr log);

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorage & storage, const std::string & path, LoggerPtr log);

void deserializeLogAndApplyToStorage(KeeperStorage & storage, const std::string & log_path, LoggerPtr log);

void deserializeLogsAndApplyToStorage(KeeperStorage & storage, const std::string & path, LoggerPtr log);

}
