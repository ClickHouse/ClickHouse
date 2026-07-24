#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>

#include <limits>
#include <tuple>

using namespace DB;

TEST(LanceTableStateSnapshot, RoundTrip)
{
    Lance::TableStateSnapshot state{.version = 42};

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
    DataLakeTableStateSnapshot state = Lance::TableStateSnapshot{.version = std::numeric_limits<UInt64>::max()};

    String serialized;
    WriteBufferFromString out(serialized);
    serializeDataLakeTableStateSnapshot(state, out);
    out.finalize();

    ReadBufferFromString in(serialized);
    const auto deserialized = deserializeDataLakeTableStateSnapshot(in);

    ASSERT_TRUE(std::holds_alternative<Lance::TableStateSnapshot>(deserialized));
    EXPECT_EQ(std::get<Lance::TableStateSnapshot>(state), std::get<Lance::TableStateSnapshot>(deserialized));
}

TEST(LanceTableStateSnapshot, RejectsZeroVersion)
{
    Lance::TableStateSnapshot state;

    String serialized;
    {
        WriteBufferFromString out(serialized);
        EXPECT_THROW(state.serialize(out), Exception);
    }

    serialized.clear();
    WriteBufferFromString zero_out(serialized);
    writeVarUInt(0, zero_out);
    zero_out.finalize();

    ReadBufferFromString in(serialized);
    EXPECT_THROW(std::ignore = Lance::TableStateSnapshot::deserialize(in, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION), Exception);
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
