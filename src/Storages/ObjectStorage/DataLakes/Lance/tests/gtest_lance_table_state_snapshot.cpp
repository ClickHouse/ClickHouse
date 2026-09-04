#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceTableStateSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

#include <ch_lance.h>

#include <limits>
#include <tuple>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_OPEN_FILE;
extern const int FILE_DOESNT_EXIST;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int QUERY_WAS_CANCELLED;
extern const int S3_ERROR;
extern const int UNKNOWN_EXCEPTION;
}

namespace
{
Lance::TableStateSnapshot makeSnapshot(UInt64 version, UInt8 seed = 1)
{
    Lance::TableStateSnapshot snapshot;
    snapshot.version = version;
    snapshot.manifest_id.fill(seed);
    snapshot.manifest_size = 1024;
    snapshot.manifest_sha256.fill(seed + 1);
    return snapshot;
}
}

TEST(LanceTableStateSnapshot, RoundTrip)
{
    auto state = makeSnapshot(42);
    state.has_etag = true;
    state.etag_sha256.fill(7);

    String serialized;
    WriteBufferFromString out(serialized);
    state.serialize(out);
    out.finalize();

    ReadBufferFromString in(serialized);
    const auto deserialized = Lance::TableStateSnapshot::deserialize(in, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION);

    EXPECT_EQ(state, deserialized);
}

TEST(LanceTableStateSnapshot, RejectsTruncatedIdentity)
{
    const auto state = makeSnapshot(42);

    String serialized;
    WriteBufferFromString out(serialized);
    state.serialize(out);
    out.finalize();
    serialized.pop_back();

    ReadBufferFromString in(serialized);
    EXPECT_THROW(
        std::ignore = Lance::TableStateSnapshot::deserialize(in, DATA_LAKE_TABLE_STATE_SNAPSHOT_PROTOCOL_VERSION),
        Exception);
}

TEST(LanceTableStateSnapshot, DataLakeVariantRoundTrip)
{
    DataLakeTableStateSnapshot state = makeSnapshot(std::numeric_limits<UInt64>::max());

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

TEST(LanceTableStateSnapshot, RejectsInvalidIdentity)
{
    auto state = makeSnapshot(1);
    state.manifest_size = 0;
    EXPECT_THROW(state.validate(ErrorCodes::LOGICAL_ERROR), Exception);

    state = makeSnapshot(1);
    state.manifest_id.fill(0);
    EXPECT_THROW(state.validate(ErrorCodes::LOGICAL_ERROR), Exception);

    state = makeSnapshot(1);
    state.manifest_sha256.fill(0);
    EXPECT_THROW(state.validate(ErrorCodes::LOGICAL_ERROR), Exception);

    state = makeSnapshot(1);
    state.etag_sha256.fill(3);
    EXPECT_THROW(state.validate(ErrorCodes::LOGICAL_ERROR), Exception);

    state = makeSnapshot(1);
    state.has_etag = true;
    EXPECT_THROW(state.validate(ErrorCodes::LOGICAL_ERROR), Exception);
}

TEST(LanceTableStateSnapshot, RejectsLegacyVersionOnlyPayload)
{
    String serialized;
    WriteBufferFromString out(serialized);
    writeVarUInt(1, out);
    out.finalize();

    ReadBufferFromString in(serialized);
    EXPECT_THROW(std::ignore = Lance::TableStateSnapshot::deserialize(in, 1), Exception);
}

TEST(LanceConfiguration, PrewhereRemainsDisabled)
{
    EXPECT_FALSE(StorageLocalLanceConfiguration::SUPPORTS_PREWHERE);
}

TEST(LanceWrapper, OpenMissingDatasetThrowsClickHouseException)
{
    Lance::DatasetOptions options{.uri = "/path/to/missing/lance/dataset"};

    try
    {
        std::ignore = Lance::DatasetHandle::openEphemeral(options);
        FAIL() << "Expected Lance::DatasetHandle::openEphemeral to throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::FILE_DOESNT_EXIST);
        EXPECT_NE(String(e.message()).find("path/to/missing/lance/dataset"), String::npos);
    }
}

TEST(LanceWrapper, ProcessRuntimeStatsAndReuseCounters)
{
    const auto before = Lance::runtimeStats();
    Lance::ensureRuntime(0);
    const auto after_ensure = Lance::runtimeStats();
    EXPECT_GE(after_ensure.runtime_initialized, 1u);
    EXPECT_GE(after_ensure.runtime_initialized, before.runtime_initialized);

    /// Second ensure must not create another runtime.
    Lance::ensureRuntime(0);
    EXPECT_EQ(Lance::runtimeStats().runtime_initialized, after_ensure.runtime_initialized);
}

TEST(LanceWrapper, MapsFfiErrorKinds)
{
    using Lance::ErrorMapping::toClickHouseErrorCode;

    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_NONE, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_INVALID_ARGUMENT, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_NOT_FOUND, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::FILE_DOESNT_EXIST);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_PERMISSION_DENIED, CH_LANCE_ERROR_ORIGIN_S3), ErrorCodes::ACCESS_DENIED);
    EXPECT_EQ(
        toClickHouseErrorCode(CH_LANCE_ERROR_UNAUTHENTICATED, CH_LANCE_ERROR_ORIGIN_S3), ErrorCodes::AUTHENTICATION_FAILED);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_CORRUPT_DATA, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::INCORRECT_DATA);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_UNSUPPORTED, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_VERSION_NOT_FOUND, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::FILE_DOESNT_EXIST);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_STORAGE, CH_LANCE_ERROR_ORIGIN_S3), ErrorCodes::S3_ERROR);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_STORAGE, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::CANNOT_OPEN_FILE);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_STORAGE, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::UNKNOWN_EXCEPTION);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_INTERNAL, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::UNKNOWN_EXCEPTION);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_CANCELLED, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_SNAPSHOT_MISMATCH, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::INCORRECT_DATA);
    EXPECT_EQ(toClickHouseErrorCode(CH_LANCE_ERROR_MEMORY_LIMIT, CH_LANCE_ERROR_ORIGIN_LOCAL), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    EXPECT_EQ(toClickHouseErrorCode(1000, CH_LANCE_ERROR_ORIGIN_UNKNOWN), ErrorCodes::UNKNOWN_EXCEPTION);
}

#endif
