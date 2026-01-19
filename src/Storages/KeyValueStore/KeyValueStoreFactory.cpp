#include <Storages/KeyValueStore/KeyValueStoreFactory.h>

#include "config.h"

#if USE_ROCKSDB
#include <Storages/KeyValueStore/RocksDB/KeyValueStoreRocksDB.h>
#endif

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STORAGE;
}

IKeyValueStorePtr KeyValueStoreFactory::create(
    const String & type_str,
    const String & dir,
    const std::vector<std::string> & expect_namespaces)
{
    if (type_str == "rocksdb")
    {
#if USE_ROCKSDB
        return KeyValueStoreRocksDB::create(dir, expect_namespaces);
#else
        throw Exception(ErrorCodes::UNKNOWN_STORAGE, "RocksDB support is not enabled. Rebuild with USE_ROCKSDB=1");
#endif
    }

    throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Unsupported KV store type: {}", type_str);
}

bool KeyValueStoreFactory::isTypeSupported(const String & type_str)
{
    if (type_str == "rocksdb")
    {
#if USE_ROCKSDB
        return true;
#else
        return false;
#endif
    }
    return false;
}

std::vector<String> KeyValueStoreFactory::getSupportedTypes()
{
    std::vector<String> types;
#if USE_ROCKSDB
    types.push_back("rocksdb");
#endif
    return types;
}

} // namespace DB

