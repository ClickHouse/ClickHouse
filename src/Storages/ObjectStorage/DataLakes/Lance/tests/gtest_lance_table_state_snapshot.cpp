#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>

#include <tuple>

using namespace DB;

TEST(LanceTableStateSnapshot, RoundTrip)
{
    Lance::TableStateSnapshot state{.snapshot_id = 42, .schema_id = 7};

    String serialized;
    WriteBufferFromString out(serialized);
    state.serialize(out);
    out.finalize();

    ReadBufferFromString in(serialized);
    const auto deserialized = Lance::TableStateSnapshot::deserialize(in, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    EXPECT_EQ(state, deserialized);
}

TEST(LanceTableStateSnapshot, DataLakeVariantRoundTrip)
{
    DataLakeTableStateSnapshot state = Lance::TableStateSnapshot{.snapshot_id = 100, .schema_id = 3};

    String serialized;
    WriteBufferFromString out(serialized);
    serializeDataLakeTableStateSnapshot(state, out);
    out.finalize();

    ReadBufferFromString in(serialized);
    const auto deserialized = deserializeDataLakeTableStateSnapshot(in);

    ASSERT_TRUE(std::holds_alternative<Lance::TableStateSnapshot>(deserialized));
    EXPECT_EQ(std::get<Lance::TableStateSnapshot>(state), std::get<Lance::TableStateSnapshot>(deserialized));
}

TEST(LanceWrapper, OpenMissingDatasetThrowsClickHouseException)
{
    Lance::DatasetOptions options{.uri = "/path/to/missing/lance/dataset"};

    try
    {
        std::ignore = Lance::Dataset::open(options);
        FAIL() << "Expected Lance::Dataset::open to throw";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(String(e.message()).find("path/to/missing/lance/dataset"), String::npos);
    }
}

#endif
