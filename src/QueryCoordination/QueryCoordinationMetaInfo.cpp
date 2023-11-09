#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <QueryCoordination/QueryCoordinationMetaInfo.h>

namespace DB
{

void QueryCoordinationMetaInfo::write(WriteBuffer & out) const
{
    writeStringBinary(cluster_name, out);

    size_t table_size = storages.size();
    writeVarUInt(table_size, out);

    for (const auto & storage_id : storages)
    {
        writeStringBinary(storage_id.database_name, out);
        writeStringBinary(storage_id.table_name, out);
    }

    size_t key_size = sharding_keys.size();
    writeVarUInt(key_size, out);

    for (const auto & key : sharding_keys)
        writeStringBinary(key, out);
}

void QueryCoordinationMetaInfo::read(ReadBuffer & in)
{
    readStringBinary(cluster_name, in);

    size_t table_size = 0;
    readVarUInt(table_size, in);
    storages.reserve(table_size);

    for (size_t i = 0; i < table_size; ++i)
    {
        String database_name;
        String table_name;

        readStringBinary(database_name, in);
        readStringBinary(table_name, in);

        StorageID storage_id(database_name, table_name);
        storages.emplace_back(storage_id);
    }

    size_t key_size = 0;
    readVarUInt(key_size, in);
    sharding_keys.reserve(key_size);

    for (size_t i = 0; i < key_size; ++i)
    {
        String key;
        readStringBinary(key, in);
        sharding_keys.emplace_back(key);
    }
}

String QueryCoordinationMetaInfo::toString() const
{
    String res("Cluster: " + cluster_name);

    for (const auto & storage_id : storages)
        res += (", " + storage_id.database_name + "." + storage_id.table_name);

    for (const auto & key : sharding_keys)
        res += (", " + key);

    return res;
}

}
