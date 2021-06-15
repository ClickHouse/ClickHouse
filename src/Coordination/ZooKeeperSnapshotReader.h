#pragma once
#include <string>
#include <filesystem>
#include <Coordination/KeeperStorage.h>
#include <IO/ReadBuffer.h>
#include <Coordination/ACLMap.h>

namespace DB
{

int64_t getZxidFromName(const std::string & filename);

void deserializeMagic(ReadBuffer & in);

int64_t deserializeSessionAndTimeout(KeeperStorage & storage, ReadBuffer & in);

void deserializeACLMap(KeeperStorage & storage, ReadBuffer & in);

int64_t deserializeStorageData(KeeperStorage & storage, ReadBuffer & in);

void deserializeKeeperStorage(KeeperStorage & storage, const std::string & path);

}
