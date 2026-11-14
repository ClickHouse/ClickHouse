#include <Common/SettingsChanges.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <gtest/gtest.h>

using namespace DB;

/// `DataLakeStorageSettings` include all format settings, so `serialize` (`writeChangedBinary`) has to handle
/// their types too, for example `Char`, `UInt64Auto` and `URI`, besides the data lake's own settings.
GTEST_TEST(DataLakeStorageSettings, BinarySerializationRoundTrip)
{
    SettingsChanges changes;
    changes.emplace_back("format_csv_delimiter", Field(";"));
    changes.emplace_back("schema_inference_make_columns_nullable", Field(UInt64(1)));
    changes.emplace_back("format_avro_schema_registry_url", Field("http://localhost:8081/registry"));
    changes.emplace_back("iceberg_format_version", Field(UInt64(1)));
    changes.emplace_back("iceberg_metadata_file_path", Field("metadata/v3.metadata.json"));

    DataLakeStorageSettings sent;
    sent.loadFromSettingsChanges(changes);

    WriteBufferFromOwnString out;
    sent.serialize(out);
    const String wire = out.str();

    ReadBufferFromString in(wire);
    DataLakeStorageSettings received = DataLakeStorageSettings::deserialize(in);
    ASSERT_TRUE(in.eof());

    for (const auto & change : changes)
    {
        SCOPED_TRACE(change.name);
        ASSERT_TRUE(received.isChanged(change.name));
        ASSERT_EQ(received.get(change.name), sent.get(change.name));
    }
    ASSERT_FALSE(received.isChanged("format_tsv_null_representation"));
}

/// The layout written by `writeChangedBinary`: the number of settings, then the name, flags and binary value of each.
/// It is persisted by code outside of this repository, so the bytes must stay readable.
GTEST_TEST(DataLakeStorageSettings, ReadsPinnedBinaryLayout)
{
    WriteBufferFromOwnString out;
    writeVarUInt(2, out);
    writeStringBinary(std::string_view{"format_csv_delimiter"}, out);
    writeVarUInt(0, out);
    writeStringBinary(std::string_view{";"}, out);
    writeStringBinary(std::string_view{"iceberg_format_version"}, out);
    writeVarUInt(0, out);
    writeVarUInt(1, out);
    const String wire = out.str();

    ReadBufferFromString in(wire);
    DataLakeStorageSettings received = DataLakeStorageSettings::deserialize(in);
    ASSERT_TRUE(in.eof());
    ASSERT_EQ(received.get("format_csv_delimiter"), Field(";"));
    ASSERT_EQ(received.get("iceberg_format_version"), Field(UInt64(1)));
}
