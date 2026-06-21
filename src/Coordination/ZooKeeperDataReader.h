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

class KeeperStorageImpl;

void deserializeKeeperStorageFromSnapshot(KeeperStorageImpl & storage, const std::string & snapshot_path, LoggerPtr log);

void deserializeKeeperStorageFromSnapshotsDir(KeeperStorageImpl & storage, const std::string & path, LoggerPtr log);

void deserializeLogAndApplyToStorage(KeeperStorageImpl & storage, const std::string & log_path, LoggerPtr log);

void deserializeLogsAndApplyToStorage(KeeperStorageImpl & storage, const std::string & path, LoggerPtr log);

}
