#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergTableStateSnapshot.h>

using namespace DB;

namespace
{
std::vector<Iceberg::TableStateSnapshot> example_iceberg_states = {
    Iceberg::TableStateSnapshot{
        .metadata_file_path = "abacaba",
        .metadata_version = std::numeric_limits<Int32>::min(),
        .schema_id = std::numeric_limits<Int32>::max(),
        .snapshot_id = std::optional{std::numeric_limits<Int64>::max()},
    },
    Iceberg::TableStateSnapshot{
        .metadata_file_path = "",
        .metadata_version = 0,
        .schema_id = std::numeric_limits<Int32>::max(),
        .snapshot_id = std::nullopt,
    },
};
}

TEST(DatalakeStateSerde, IcebergStateSerde)
{

    for (const auto & iceberg_state : example_iceberg_states)
    {
        String str;
        {
            WriteBufferFromString write_buffer{str};
            iceberg_state.serialize(write_buffer);
        }
        ReadBufferFromMemory read_buffer(str.data(), str.size()) ;
        Iceberg::TableStateSnapshot serde_state = Iceberg::TableStateSnapshot::deserialize(read_buffer, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);
        ASSERT_TRUE(read_buffer.eof());
        ASSERT_EQ(iceberg_state, serde_state);
    }
}


TEST(DatalakeStateSerde, DataLakeStateSerde)
{

    for (const auto & iceberg_state : example_iceberg_states)
    {
        DataLakeTableStateSnapshot initial_state = iceberg_state;
        String str;
        {
            WriteBufferFromString write_buffer{str};
            serializeDataLakeTableStateSnapshot(initial_state, write_buffer);
        }
        ReadBufferFromMemory read_buffer(str.data(), str.size()) ;
        DataLakeTableStateSnapshot serde_state = deserializeDataLakeTableStateSnapshot(read_buffer);
        ASSERT_TRUE(read_buffer.eof());
        ASSERT_EQ(initial_state, serde_state);
    }
}

