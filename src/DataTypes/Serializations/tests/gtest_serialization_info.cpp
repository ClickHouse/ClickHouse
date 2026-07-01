#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/Serializations/EstimatesBuilder.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/Statistics/Statistics.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

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

Estimate makeEstimate(size_t num_rows, size_t num_defaults)
{
    Estimate estimate;
    estimate.rows_count = num_rows;
    estimate.num_defaults = num_defaults;
    return estimate;
}

/// A single-column set of estimates keyed by `key`, for the round-trip tests.
Estimates makeEstimates(const String & key, size_t num_rows, size_t num_defaults)
{
    Estimates estimates;
    estimates.emplace(key, makeEstimate(num_rows, num_defaults));
    return estimates;
}

Poco::JSON::Object makeKindObj(const std::string & kind, size_t num_rows, size_t num_defaults)
{
    Poco::JSON::Object obj;
    obj.set("kind", kind);
    obj.set("num_rows", static_cast<Poco::UInt64>(num_rows));
    obj.set("num_defaults", static_cast<Poco::UInt64>(num_defaults));
    return obj;
}

/// Serialize a single info (with the counts from `estimates`) and parse the result back into a JSON object.
Poco::JSON::Object::Ptr writeAndParse(const SerializationInfo & info, const String & key, const Estimates & estimates)
{
    WriteBufferFromOwnString out;
    info.writeJSON(out, nullptr, key, estimates);
    Poco::JSON::Parser parser;
    return parser.parse(out.str()).extract<Poco::JSON::Object::Ptr>();
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
                Estimates estimates;
                info.fromJSON(obj, "c", estimates);
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
    SerializationInfo info(kind_stack, defaultSettings());

    auto obj = writeAndParse(info, "c", makeEstimates("c", 1000, 100));

    EXPECT_EQ(obj->getValue<std::string>("kind"), "Default");
    EXPECT_EQ(obj->getValue<size_t>("num_rows"), 1000);
    EXPECT_EQ(obj->getValue<size_t>("num_defaults"), 100);

    SerializationInfo restored(kind_stack, defaultSettings());
    Estimates estimates;
    restored.fromJSON(*obj, "c", estimates);

    EXPECT_EQ(restored.getKindStack(), kind_stack);
    EXPECT_EQ(estimates.at("c").rows_count, 1000u);
    EXPECT_EQ(estimates.at("c").num_defaults.value_or(0), 100u);
}

TEST(SerializationInfoJSON, RoundTripSparse)
{
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    SerializationInfo info(kind_stack, defaultSettings());

    auto obj = writeAndParse(info, "c", makeEstimates("c", 1000000, 950000));

    EXPECT_EQ(obj->getValue<std::string>("kind"), "Sparse");
    EXPECT_EQ(obj->getValue<size_t>("num_rows"), 1000000);
    EXPECT_EQ(obj->getValue<size_t>("num_defaults"), 950000);

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    Estimates estimates;
    restored.fromJSON(*obj, "c", estimates);

    EXPECT_EQ(restored.getKindStack(), kind_stack);
    EXPECT_EQ(estimates.at("c").rows_count, 1000000u);
    EXPECT_EQ(estimates.at("c").num_defaults.value_or(0), 950000u);
}

TEST(SerializationInfoJSON, RoundTripDetachedOverSparse)
{
    /// kindStackToString reverses the non-Default elements:
    ///   [Default, Sparse, Detached] -> "DetachedOverSparse"
    /// stringToKindStack reads left-to-right:
    ///   "DetachedOverSparse" -> [Default, Detached, Sparse]
    /// So the round-trip reverses the inner order.
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE, ISerialization::Kind::DETACHED};
    SerializationInfo info(kind_stack, defaultSettings());

    auto obj = writeAndParse(info, "c", makeEstimates("c", 500, 490));

    EXPECT_EQ(obj->getValue<std::string>("kind"), "DetachedOverSparse");

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    Estimates estimates;
    restored.fromJSON(*obj, "c", estimates);

    ISerialization::KindStack expected_after_roundtrip{
        ISerialization::Kind::DEFAULT, ISerialization::Kind::DETACHED, ISerialization::Kind::SPARSE};
    EXPECT_EQ(restored.getKindStack(), expected_after_roundtrip);
    EXPECT_EQ(estimates.at("c").rows_count, 500u);
    EXPECT_EQ(estimates.at("c").num_defaults.value_or(0), 490u);
}

TEST(SerializationInfoJSON, RoundTripZeroRows)
{
    ISerialization::KindStack kind_stack{ISerialization::Kind::DEFAULT};
    SerializationInfo info(kind_stack, defaultSettings());

    auto obj = writeAndParse(info, "c", makeEstimates("c", 0, 0));

    SerializationInfo restored({ISerialization::Kind::DEFAULT}, defaultSettings());
    Estimates estimates;
    restored.fromJSON(*obj, "c", estimates);

    EXPECT_EQ(estimates.at("c").rows_count, 0u);
    EXPECT_EQ(estimates.at("c").num_defaults.value_or(0), 0u);
    EXPECT_EQ(restored.getKindStack(), kind_stack);
}

TEST(SerializationInfoJSON, WriteWithoutEstimateWritesZeroCounts)
{
    /// A column absent from the estimates is written with zero counts.
    SerializationInfo info({ISerialization::Kind::DEFAULT}, defaultSettings());
    auto obj = writeAndParse(info, "c", Estimates{});

    EXPECT_EQ(obj->getValue<size_t>("num_rows"), 0);
    EXPECT_EQ(obj->getValue<size_t>("num_defaults"), 0);
}

TEST(SerializationInfoJSON, FromJSONMissingFieldThrows)
{
    SerializationInfo info({ISerialization::Kind::DEFAULT}, defaultSettings());
    Estimates estimates;

    {
        Poco::JSON::Object obj;
        obj.set("kind", "Default");
        obj.set("num_rows", 100);
        /// missing num_defaults
        EXPECT_THROW(info.fromJSON(obj, "c", estimates), DB::Exception);
    }

    {
        Poco::JSON::Object obj;
        obj.set("kind", "Default");
        obj.set("num_defaults", 10);
        /// missing num_rows
        EXPECT_THROW(info.fromJSON(obj, "c", estimates), DB::Exception);
    }

    {
        Poco::JSON::Object obj;
        obj.set("num_rows", 100);
        obj.set("num_defaults", 10);
        /// missing kind
        EXPECT_THROW(info.fromJSON(obj, "c", estimates), DB::Exception);
    }
}

TEST(SerializationInfoJSON, FromJSONReadsKindAndCounts)
{
    SerializationInfo info({ISerialization::Kind::DEFAULT}, defaultSettings());

    auto obj = makeKindObj("Sparse", 2000, 1999);
    Estimates estimates;
    info.fromJSON(obj, "c", estimates);

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    EXPECT_EQ(info.getKindStack(), expected);
    EXPECT_EQ(estimates.at("c").rows_count, 2000u);
    EXPECT_EQ(estimates.at("c").num_defaults.value_or(0), 1999u);
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
    infos.writeJSON(out, {});
    auto json = out.str();

    EXPECT_THAT(json, testing::HasSubstr(R"("name":"string\"with\\escapes")"));
    EXPECT_THAT(json, testing::HasSubstr(R"("subcolumns")"));
    EXPECT_THAT(json, testing::HasSubstr(R"("types_serialization_versions")"));

    Estimates estimates;
    auto restored = SerializationInfoByName::readJSONFromString(columns, json, estimates);
    EXPECT_EQ(restored.getVersion(), MergeTreeSerializationInfoVersion::WITH_TYPES);
    EXPECT_EQ(restored.getSettings().string_serialization_version, MergeTreeStringSerializationVersion::WITH_SIZE_STREAM);
    EXPECT_TRUE(restored.getSettings().propagate_types_serialization_versions_to_nested_types);
    EXPECT_NE(restored.tryGet("string\"with\\escapes"), nullptr);
    EXPECT_NE(restored.tryGet("tuple"), nullptr);
}

TEST(SerializationInfoByNameJSON, WriteJSONCarriesEstimatesForColumnsAndSubcolumns)
{
    SerializationInfoSettings settings = defaultSettings();
    settings.version = MergeTreeSerializationInfoVersion::WITH_TYPES;

    auto uint_type = std::make_shared<DataTypeUInt64>();
    NamesAndTypesList columns
    {
        {"scalar", uint_type},
        {"tuple", std::make_shared<DataTypeTuple>(DataTypes{uint_type, uint_type}, Strings{"a", "b"})},
    };

    SerializationInfoByName infos(columns, settings);

    Estimates estimates;
    estimates.emplace("scalar", makeEstimate(1000, 100));
    estimates.emplace("tuple", makeEstimate(1000, 0));
    estimates.emplace(subcolumnEstimateKey("tuple", "a"), makeEstimate(1000, 900));
    estimates.emplace(subcolumnEstimateKey("tuple", "b"), makeEstimate(1000, 5));

    WriteBufferFromOwnString out;
    infos.writeJSON(out, estimates);

    Estimates restored;
    SerializationInfoByName::readJSONFromString(columns, out.str(), restored);

    EXPECT_EQ(restored.at("scalar").num_defaults.value_or(0), 100u);
    EXPECT_EQ(restored.at(subcolumnEstimateKey("tuple", "a")).num_defaults.value_or(0), 900u);
    EXPECT_EQ(restored.at(subcolumnEstimateKey("tuple", "b")).num_defaults.value_or(0), 5u);
    EXPECT_EQ(restored.at(subcolumnEstimateKey("tuple", "a")).rows_count, 1000u);
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

TEST(EstimatesBuilder, ChooseKindStackDefaultBelowThreshold)
{
    auto kind_stack = EstimatesBuilder::chooseKindStack(makeEstimate(1000, 900), defaultSettings()); /// 0.9 < 0.9375

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

TEST(EstimatesBuilder, ChooseKindStackSparseAboveThreshold)
{
    auto kind_stack = EstimatesBuilder::chooseKindStack(makeEstimate(1000, 950), defaultSettings()); /// 0.95 > 0.9375

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE};
    EXPECT_EQ(kind_stack, expected);
}

TEST(EstimatesBuilder, ChooseKindStackAlwaysDefault)
{
    auto kind_stack = EstimatesBuilder::chooseKindStack(makeEstimate(1000, 1000), alwaysDefaultSettings());

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

TEST(EstimatesBuilder, ChooseKindStackZeroRows)
{
    auto kind_stack = EstimatesBuilder::chooseKindStack(makeEstimate(0, 0), defaultSettings());

    ISerialization::KindStack expected{ISerialization::Kind::DEFAULT};
    EXPECT_EQ(kind_stack, expected);
}

/// The builder samples each tuple element independently, so the kind is chosen per element.
TEST(EstimatesBuilder, TuplePerElementKinds)
{
    auto uint_type = std::make_shared<DataTypeUInt64>();
    auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint_type, uint_type}, Strings{"a", "b"});
    NamesAndTypesList columns{{"t", tuple_type}};

    /// Element `a` is all-default (sparse), element `b` has no defaults (default).
    auto col_a = ColumnUInt64::create();
    auto col_b = ColumnUInt64::create();
    for (size_t i = 0; i < 100; ++i)
    {
        col_a->insertValue(0);
        col_b->insertValue(i + 1);
    }
    auto tuple_column = ColumnTuple::create(Columns{std::move(col_a), std::move(col_b)});

    Block block{{std::move(tuple_column), tuple_type, "t"}};

    SerializationInfoByName infos(columns, defaultSettings());
    EstimatesBuilder builder(columns, defaultSettings());
    builder.add(block);
    builder.chooseKinds(infos);

    const auto * info_tuple = typeid_cast<const SerializationInfoTuple *>(infos.tryGet("t").get());
    ASSERT_NE(info_tuple, nullptr);
    EXPECT_EQ(info_tuple->getElementKindStack(0), (ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE}));
    EXPECT_EQ(info_tuple->getElementKindStack(1), (ISerialization::KindStack{ISerialization::Kind::DEFAULT}));
}

/// `addDefaults` (used for columns missing from a source part) recurses into tuple elements.
TEST(EstimatesBuilder, AddDefaultsRecursesIntoTupleElements)
{
    auto uint_type = std::make_shared<DataTypeUInt64>();
    auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint_type, uint_type}, Strings{"a", "b"});
    NamesAndTypesList columns{{"t", tuple_type}};

    SerializationInfoByName infos(columns, defaultSettings());
    EstimatesBuilder builder(columns, defaultSettings());
    builder.addDefaults("t", 1000);
    builder.chooseKinds(infos);

    const auto * info_tuple = typeid_cast<const SerializationInfoTuple *>(infos.tryGet("t").get());
    ASSERT_NE(info_tuple, nullptr);
    /// Every element saw only default rows, so each is chosen sparse.
    EXPECT_EQ(info_tuple->getElementKindStack(0), (ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE}));
    EXPECT_EQ(info_tuple->getElementKindStack(1), (ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE}));
}

/// The exact default count from the explicit statistics overrides the sampled one.
TEST(EstimatesBuilder, MergeEstimatesPrefersExternalDefaults)
{
    auto uint_type = std::make_shared<DataTypeUInt64>();
    NamesAndTypesList columns{{"c", uint_type}};

    /// Sample a column with no default rows (would be chosen Default).
    auto col = ColumnUInt64::create();
    for (size_t i = 0; i < 1000; ++i)
        col->insertValue(i + 1);
    Block block{{std::move(col), uint_type, "c"}};

    SerializationInfoByName infos(columns, defaultSettings());
    EstimatesBuilder builder(columns, defaultSettings());
    builder.add(block);

    /// External statistics say the column is almost all-default; this must win.
    Estimates external;
    external.emplace("c", makeEstimate(1000, 990));
    builder.mergeEstimates(external);
    builder.chooseKinds(infos);

    EXPECT_EQ(infos.getKindStack("c"), (ISerialization::KindStack{ISerialization::Kind::DEFAULT, ISerialization::Kind::SPARSE}));
    EXPECT_EQ(builder.getEstimates().at("c").num_defaults.value_or(0), 990u);
}

}
