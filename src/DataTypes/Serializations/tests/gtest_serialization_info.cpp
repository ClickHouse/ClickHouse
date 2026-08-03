#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/JSON/Object.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

SerializationInfoSettings defaultSettings()
{
    SerializationInfoSettings s;
    s.ratio_of_defaults_for_sparse = 0.9375;
    s.choose_kind = true;
    return s;
}

SerializationInfoSettings alwaysDefaultSettings()
{
    SerializationInfoSettings s;
    s.ratio_of_defaults_for_sparse = 1.0;
    s.choose_kind = false;
    return s;
}

SerializationInfo::Data makeData(size_t num_rows, size_t num_defaults)
{
    SerializationInfo::Data d;
    d.num_rows = num_rows;
    d.num_defaults = num_defaults;
    return d;
}

Poco::JSON::Object makeKindObj(const std::string & kind, size_t num_rows, size_t num_defaults)
{
    Poco::JSON::Object obj;
    obj.set("kind", kind);
    obj.set("num_rows", static_cast<Poco::UInt64>(num_rows));
    obj.set("num_defaults", static_cast<Poco::UInt64>(num_defaults));
    return obj;
}

void expectMalformedKindFails([[maybe_unused]] const std::string & kind)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test trigger LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    EXPECT_THROW(
        {
            try
            {
                SerializationInfo info({ISerialization::Kind::DEFAULT}, defaultSettings());
                auto obj = makeKindObj(kind, 100, 10);
                info.fromJSON(obj);
            }
            catch (const DB::Exception & e)
            {
                ASSERT_EQ(DB::ErrorCodes::LOGICAL_ERROR, e.code());
                EXPECT_THAT(e.what(), testing::HasSubstr("Unknown serialization kind"));
                throw;
            }
        },
        DB::Exception);
#endif
}
}

TEST(SerializationInfoJSON, RoundTripDefault)
{
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT};
    SerializationInfo info(kind_stack, defaultSettings(), makeData(1000, 100));

    Poco::JSON::Object obj;
    info.toJSON(obj);

    EXPECT_EQ(obj.getValue<std::string>("kind"), "Default");
    EXPECT_EQ(obj.getValue<size_t>("num_rows"), 1000);
    EXPECT_EQ(obj.getValue<size_t>("num_defaults"), 100);

    SerializationInfo restored(kind_stack, defaultSettings());
    restored.fromJSON(obj);

    EXPECT_EQ(restored.getKindStack(), kind_stack);
    EXPECT_EQ(restored.getData().num_rows, 1000);
    EXPECT_EQ(restored.getData().num_defaults, 100);
}

TEST(SerializationInfoJSON, RoundTripSparse)
{
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    SerializationInfo info(kind_stack, defaultSettings(), makeData(1000000, 950000));

    Poco::JSON::Object obj;
    info.toJSON(obj);

    EXPECT_EQ(obj.getValue<std::string>("kind"), "Sparse");
    EXPECT_EQ(obj.getValue<size_t>("num_rows"), 1000000);
    EXPECT_EQ(obj.getValue<size_t>("num_defaults"), 950000);

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    restored.fromJSON(obj);

    EXPECT_EQ(restored.getKindStack(), kind_stack);
    EXPECT_EQ(restored.getData().num_rows, 1000000);
    EXPECT_EQ(restored.getData().num_defaults, 950000);
}

TEST(SerializationInfoJSON, RoundTripDetachedOverSparse)
{
    /// kindStackToString reverses the non-Default elements:
    ///   [Default, Sparse, Detached] -> "DetachedOverSparse"
    /// stringToKindStack reads left-to-right:
    ///   "DetachedOverSparse" -> [Default, Detached, Sparse]
    /// So the round-trip reverses the inner order.
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE, ISerialization::Kind::DETACHED};
    SerializationInfo info(kind_stack, defaultSettings(), makeData(500, 490));

    Poco::JSON::Object obj;
    info.toJSON(obj);

    EXPECT_EQ(obj.getValue<std::string>("kind"), "DetachedOverSparse");

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    restored.fromJSON(obj);

    ISerialization::KindStack expected_after_roundtrip{
        ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED, ISerialization::Kind::SPARSE};
    EXPECT_EQ(restored.getKindStack(), expected_after_roundtrip);
    EXPECT_EQ(restored.getData().num_rows, 500);
    EXPECT_EQ(restored.getData().num_defaults, 490);
}

TEST(SerializationInfoJSON, RoundTripZeroRows)
{
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT};
    SerializationInfo info(kind_stack, defaultSettings(), makeData(0, 0));

    Poco::JSON::Object obj;
    info.toJSON(obj);

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    restored.fromJSON(obj);

    EXPECT_EQ(restored.getData().num_rows, 0);
    EXPECT_EQ(restored.getData().num_defaults, 0);
    EXPECT_EQ(restored.getKindStack(), kind_stack);
}

TEST(SerializationInfoJSON, FromJSONMissingFieldThrows)
{
    SerializationInfo info({ISerialization::Kind::DEFAULT}, defaultSettings());

    {
        Poco::JSON::Object obj;
        obj.set("kind", "Default");
        obj.set("num_rows", 100);
        /// missing num_defaults
        EXPECT_THROW(info.fromJSON(obj), DB::Exception);
    }

    {
        Poco::JSON::Object obj;
        obj.set("kind", "Default");
        obj.set("num_defaults", 10);
        /// missing num_rows
        EXPECT_THROW(info.fromJSON(obj), DB::Exception);
    }

    {
        Poco::JSON::Object obj;
        obj.set("num_rows", 100);
        obj.set("num_defaults", 10);
        /// missing kind
        EXPECT_THROW(info.fromJSON(obj), DB::Exception);
    }
}

TEST(SerializationInfoJSON, FromJSONOverwritesExistingData)
{
    ISerialization::KindStack initial_stack{ISerialization::Kind::DEFAULT};
    SerializationInfo info(initial_stack, defaultSettings(), makeData(1, 0));

    auto obj = makeKindObj("Sparse", 2000, 1999);
    info.fromJSON(obj);

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    EXPECT_EQ(info.getKindStack(), expected);
    EXPECT_EQ(info.getData().num_rows, 2000);
    EXPECT_EQ(info.getData().num_defaults, 1999);
}

TEST(SerializationInfoByNameJSON, WriteJSONCanBeReadBack)
{
    SerializationInfoSettings settings;
    settings.ratio_of_defaults_for_sparse = 0.5;
    settings.choose_kind = true;
    settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;
    settings.string_serialization_version = MergeTreeStringSerializationVersion::WITH_SIZE_STREAM;
    settings.propagate_types_serialization_versions_to_nested_types = true;

    auto string_type = std::make_shared<DataTypeString>();
    NamesAndTypesList columns
    {
        {"string\"with\\escapes", string_type},
        {"tuple", std::make_shared<DataTypeTuple>(DataTypes{string_type, string_type}, Strings{"a", "b"})},
    };

    SerializationInfoByName infos(columns, settings);

    WriteBufferFromOwnString out;
    infos.writeJSON(out);
    auto json = out.str();

    EXPECT_THAT(json, testing::HasSubstr(R"("name":"string\"with\\escapes")"));
    EXPECT_THAT(json, testing::HasSubstr(R"("subcolumns")"));
    EXPECT_THAT(json, testing::HasSubstr(R"("types_serialization_versions")"));

    auto restored = SerializationInfoByName::readJSONFromString(columns, json);
    EXPECT_EQ(restored.getVersion(), MergeTreeSerializationInfoVersion::WITH_TYPES);
    EXPECT_EQ(restored.getSettings().string_serialization_version, MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    EXPECT_TRUE(restored.getSettings().propagate_types_serialization_versions_to_nested_types);
    EXPECT_NE(restored.tryGet("string\"with\\escapes"), nullptr);
    EXPECT_NE(restored.tryGet("tuple"), nullptr);
}

/// Malformed kind tests.
/// stringToKind throws LOGICAL_ERROR which aborts in debug builds
/// but throws a catchable exception in release builds.

TEST(SerializationInfoJSON, FromJSONEmptyKind)
{
    expectMalformedKindFails("");
}
TEST(SerializationInfoJSON, FromJSONUnknownKind)
{
    expectMalformedKindFails("FooBar");
}
TEST(SerializationInfoJSON, FromJSONKindWrongCase)
{
    expectMalformedKindFails("sparse");
}
TEST(SerializationInfoJSON, FromJSONKindAllCaps)
{
    expectMalformedKindFails("SPARSE");
}
TEST(SerializationInfoJSON, FromJSONKindWithTrailingOver)
{
    expectMalformedKindFails("SparseOver");
}
TEST(SerializationInfoJSON, FromJSONKindWithLeadingOver)
{
    expectMalformedKindFails("OverSparse");
}
TEST(SerializationInfoJSON, FromJSONKindDoubleOver)
{
    expectMalformedKindFails("DetachedOverOverSparse");
}
TEST(SerializationInfoJSON, FromJSONKindWithWhitespace)
{
    expectMalformedKindFails(" Sparse");
}
TEST(SerializationInfoJSON, FromJSONKindJustOver)
{
    expectMalformedKindFails("Over");
}

/// chooseKindStack tests

TEST(SerializationInfoJSON, ChooseKindStackDefaultBelowThreshold)
{
    auto kind_stack = SerializationInfo::chooseKindStack(makeData(1000, 900), defaultSettings()); /// 0.9 < 0.9375

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

TEST(SerializationInfoJSON, ChooseKindStackSparseAboveThreshold)
{
    auto kind_stack = SerializationInfo::chooseKindStack(makeData(1000, 950), defaultSettings()); /// 0.95 > 0.9375

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    EXPECT_EQ(kind_stack, expected);
}

TEST(SerializationInfoJSON, ChooseKindStackAlwaysDefault)
{
    auto kind_stack = SerializationInfo::chooseKindStack(makeData(1000, 1000), alwaysDefaultSettings());

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

TEST(SerializationInfoJSON, ChooseKindStackZeroRows)
{
    auto kind_stack = SerializationInfo::chooseKindStack(makeData(0, 0), defaultSettings());

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

}
