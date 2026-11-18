#include <Storages/MergeTree/Manifest/ManifestStorageFactory.h>

#include "config.h"

#include <Common/Exception.h>

#if USE_ROCKSDB
#    include <Storages/MergeTree/Manifest/RocksDB/ManifestStorageRocksDB.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int BAD_ARGUMENTS;
}

ManifestStoragePtr ManifestStorageFactory::create(const String & type_str, const String & dir)
{
    if (type_str == "rocksdb")
    {
#if USE_ROCKSDB
        return ManifestStorageRocksDB::create(dir);
#else
        (void)dir;
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RocksDB support is not compiled. Rebuild with USE_ROCKSDB=1");
#endif
    }

    (void)dir;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported manifest storage type: {}.", type_str);
}

bool ManifestStorageFactory::isTypeSupported(const String & type_str)
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

std::vector<String> ManifestStorageFactory::getSupportedTypes()
{
    std::vector<String> types;
#if USE_ROCKSDB
    types.push_back("rocksdb");
#endif
    return types;
}

}
