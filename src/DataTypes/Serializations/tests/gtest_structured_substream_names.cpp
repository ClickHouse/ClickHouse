#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/StructuredSubstreamNames.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <vector>

using namespace DB;

/** Substream naming for types containing `Nullable(Array(...))`.
  *
  * Every expectation here is derived from a real `enumerateStreams` walk rather than from a
  * hand-built `SubstreamPath`. That is not a stylistic preference. Naming decisions depend on what
  * precedes the final substream in the path, and `SerializationNullable` emits its null-map
  * substream and then pops it before recursing into the nested type. A constructed path such as
  * `[NullMap, NullableElements, ArraySizes]` therefore never occurs; the real path is
  * `[NullableElements, ArraySizes]`, and it takes a different branch. Expectations written against
  * constructed paths can be entirely self-consistent and still disagree with what is written to
  * disk, which is how the naming for this feature previously ended up with no accurate test.
  */

namespace
{

DataTypePtr parseType(const String & name)
{
    return DataTypeFactory::instance().get(name);
}

/// `Nullable(Array(...))` is not spellable as a type string at this stage - the type factory still
/// rejects it, deliberately - so types under test are composed programmatically.
DataTypePtr nullableArrayOf(const String & element_type_name)
{
    return makeNullableAllowingArray(parseType("Array(" + element_type_name + ")"));
}

DataTypePtr nullableArrayOf(const DataTypePtr & element)
{
    return makeNullableAllowingArray(std::make_shared<DataTypeArray>(element));
}

DataTypePtr arrayOf(const DataTypePtr & element)
{
    return std::make_shared<DataTypeArray>(element);
}

DataTypePtr tupleOf(const Names & names, const DataTypes & elements)
{
    return std::make_shared<DataTypeTuple>(elements, names);
}

DataTypePtr mapOf(const String & key_type_name, const DataTypePtr & value)
{
    return std::make_shared<DataTypeMap>(parseType(key_type_name), value);
}

DataTypePtr variantOf(const DataTypes & alternatives)
{
    return std::make_shared<DataTypeVariant>(alternatives);
}

struct EnumerationResult
{
    std::vector<String> file_names;
};

/// Walk `type` the way MergeTree does and collect, for every substream, the file name that would be
/// produced for a column named `c`.
EnumerationResult enumerate(
    const DataTypePtr & type,
    bool pass_column_type = true,
    bool use_specialized_prefixes_and_suffixes = false)
{
    EnumerationResult result;

    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = use_specialized_prefixes_and_suffixes;

    ISerialization::StreamFileNameSettings name_settings;
    if (pass_column_type)
        name_settings.column_type = type.get();

    auto serialization = type->getDefaultSerialization();
    auto data = ISerialization::SubstreamData(serialization).withType(type);

    serialization->enumerateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            result.file_names.push_back(ISerialization::getFileNameForStream("c", path, name_settings));
        },
        data);

    return result;
}

std::vector<String> fileNames(const DataTypePtr & type)
{
    return enumerate(type).file_names;
}

void expectAllDistinct(const std::vector<String> & values, const String & context)
{
    std::set<String> distinct(values.begin(), values.end());
    EXPECT_EQ(distinct.size(), values.size()) << context << ": " << ::testing::PrintToString(values);
}

/// Types that are legal on master today. Their stream names must not move by a single byte,
/// otherwise existing parts stop being readable.
std::vector<String> currentlyLegalTypeNames()
{
    return {
        "UInt8",
        "String",
        "Nullable(UInt32)",
        "Nullable(String)",
        "Nullable(IPv6)",
        "Array(UInt8)",
        "Array(Nullable(UInt32))",
        "Array(Array(Nullable(UInt32)))",
        "Array(Array(Array(UInt8)))",
        "Array(Nullable(Decimal(18, 4)))",
        "Tuple(a UInt8, b Nullable(String))",
        "Tuple(Nullable(UInt8), Nullable(UInt8))",
        "Tuple(a Array(Nullable(UInt8)), b Tuple(c Nullable(UInt8)))",
        "Nullable(Tuple(a UInt8, b Nullable(String)))",
        "Map(String, UInt8)",
        "Map(String, Array(Nullable(UInt8)))",
        "Map(String, Tuple(a Nullable(UInt8)))",
        "Array(Tuple(a UInt8, b Array(Nullable(String))))",
        "LowCardinality(String)",
        "LowCardinality(Nullable(String))",
        "Array(LowCardinality(Nullable(String)))",
        "Variant(UInt64, String, Array(UInt64))",
        "Array(Variant(UInt64, String))",
        "Variant(Array(Nullable(UInt8)), Tuple(a UInt8))",
        "Dynamic",
        "Array(Dynamic)",
        "JSON",
        "Nested(a UInt8, b Nullable(String))",
    };
}

/// Shapes that place `Nullable(Array)` in every structural position we intend to support.
std::vector<std::pair<String, DataTypePtr>> nullableArrayShapes()
{
    return {
        {"Nullable(Array(UInt32))", nullableArrayOf("UInt32")},
        {"Nullable(Array(Nullable(UInt32)))", nullableArrayOf("Nullable(UInt32)")},
        {"Nullable(Array(Array(Nullable(UInt32))))", nullableArrayOf("Array(Nullable(UInt32))")},
        {"Nullable(Array(Nullable(Array(Nullable(UInt32)))))", nullableArrayOf(nullableArrayOf("Nullable(UInt32)"))},
        {"Array(Nullable(Array(Nullable(UInt32))))", arrayOf(nullableArrayOf("Nullable(UInt32)"))},
        {"Array(Array(Nullable(Array(Nullable(UInt32)))))", arrayOf(arrayOf(nullableArrayOf("Nullable(UInt32)")))},
        {"Nullable(Array(Tuple(a Nullable(UInt8), b Nullable(UInt8))))",
         nullableArrayOf("Tuple(a Nullable(UInt8), b Nullable(UInt8))")},
        {"Tuple(a Nullable(Array(Nullable(UInt8))), b Nullable(Array(Nullable(UInt8))))",
         tupleOf({"a", "b"}, {nullableArrayOf("Nullable(UInt8)"), nullableArrayOf("Nullable(UInt8)")})},
        {"Tuple(a Nullable(Array(Nullable(UInt8))), b Array(Nullable(UInt8)))",
         tupleOf({"a", "b"}, {nullableArrayOf("Nullable(UInt8)"), parseType("Array(Nullable(UInt8))")})},
        {"Map(String, Nullable(Array(Nullable(UInt32))))", mapOf("String", nullableArrayOf("Nullable(UInt32)"))},
        {"Variant(Array(Nullable(Array(Nullable(UInt32)))), UInt8)",
         variantOf({arrayOf(nullableArrayOf("Nullable(UInt32)")), std::make_shared<DataTypeUInt8>()})},
        {"Variant(Array(Nullable(Array(UInt8))), Array(Nullable(Array(UInt16))))",
         variantOf({arrayOf(nullableArrayOf("UInt8")), arrayOf(nullableArrayOf("UInt16"))})},
        {"Array(Nullable(Array(Tuple(a Nullable(UInt8)))))",
         arrayOf(nullableArrayOf("Tuple(a Nullable(UInt8))"))},
    };
}

}

/// ---------------------------------------------------------------------------------------------
/// The predicate that selects the naming scheme.
/// ---------------------------------------------------------------------------------------------

TEST(StructuredSubstreamNames, NeedsStructuredForNullableArray)
{
    EXPECT_TRUE(needsStructuredSubstreamNames(*nullableArrayOf("Nullable(UInt32)")));
    EXPECT_TRUE(needsStructuredSubstreamNames(*nullableArrayOf("UInt32")));
}

/// The distinction the whole feature rests on: null *elements* are not a null *array*, and must keep
/// the naming they have today.
TEST(StructuredSubstreamNames, DoesNotNeedStructuredForArrayNullable)
{
    EXPECT_FALSE(needsStructuredSubstreamNames(*parseType("Array(Nullable(UInt32))")));
    EXPECT_FALSE(needsStructuredSubstreamNames(*parseType("Array(Array(Nullable(UInt32)))")));
}

TEST(StructuredSubstreamNames, NeedsStructuredThroughEveryCompoundType)
{
    EXPECT_TRUE(needsStructuredSubstreamNames(*arrayOf(nullableArrayOf("Nullable(UInt32)"))));
    EXPECT_TRUE(needsStructuredSubstreamNames(*mapOf("String", nullableArrayOf("Nullable(UInt32)"))));
    EXPECT_TRUE(needsStructuredSubstreamNames(
        *variantOf({arrayOf(nullableArrayOf("Nullable(UInt32)")), std::make_shared<DataTypeUInt8>()})));
    EXPECT_TRUE(needsStructuredSubstreamNames(
        *tupleOf({"a"}, {nullableArrayOf("Nullable(UInt8)")})));
}

/// ---------------------------------------------------------------------------------------------
/// Criterion 1: no currently-legal type changes its stream names.
/// ---------------------------------------------------------------------------------------------

/// Stated as a proof rather than a sample. If the structured scheme is never selected for a legal
/// type - neither by the type nor by any path enumerated for it - then the legacy naming code is the
/// only code that runs for such types, and this change does not modify that code.
TEST(StructuredSubstreamNames, StructuredSchemeNeverSelectedForCurrentlyLegalTypes)
{
    for (const auto & type_name : currentlyLegalTypeNames())
    {
        auto type = parseType(type_name);
        EXPECT_FALSE(needsStructuredSubstreamNames(*type)) << "type: " << type_name;

        ISerialization::EnumerateStreamsSettings enumerate_settings;
        auto serialization = type->getDefaultSerialization();
        auto data = ISerialization::SubstreamData(serialization).withType(type);

        serialization->enumerateStreams(
            enumerate_settings,
            [&](const ISerialization::SubstreamPath & path)
            { EXPECT_FALSE(needsStructuredSubstreamNamesForPath(path)) << "type: " << type_name; },
            data);
    }
}

/// Criterion 4, the part that can be settled at this stage. The scheme is chosen from the column
/// type when a caller has it and from the substream path when it does not. For a legal type the two
/// must agree, otherwise a column could be written under one set of names and read under another -
/// which is exactly the failure mode a caller that loses the type would produce.
TEST(StructuredSubstreamNames, TypedAndUntypedCallersAgreeForCurrentlyLegalTypes)
{
    for (const auto & type_name : currentlyLegalTypeNames())
    {
        auto type = parseType(type_name);
        EXPECT_EQ(enumerate(type, /*pass_column_type=*/true).file_names,
                  enumerate(type, /*pass_column_type=*/false).file_names)
            << "type: " << type_name;
    }
}

TEST(StructuredSubstreamNames, LegacyNamesForRepresentativeTypes)
{
    using Expected = std::vector<String>;

    EXPECT_EQ(fileNames(parseType("Nullable(UInt32)")), (Expected{"c.null", "c"}));
    EXPECT_EQ(fileNames(parseType("Array(UInt8)")), (Expected{"c.size0", "c"}));
    EXPECT_EQ(fileNames(parseType("Array(Nullable(UInt32))")), (Expected{"c.size0", "c.null", "c"}));
    EXPECT_EQ(
        fileNames(parseType("Array(Array(Nullable(UInt32)))")),
        (Expected{"c.size0", "c.size1", "c.null", "c"}));
}

/// ---------------------------------------------------------------------------------------------
/// Criterion 2: collision-free names for arbitrary nesting.
/// ---------------------------------------------------------------------------------------------

TEST(StructuredSubstreamNames, StreamNamesAreUniqueForEveryNullableArrayShape)
{
    for (const auto & [label, type] : nullableArrayShapes())
        expectAllDistinct(fileNames(type), label);
}

/// Prefixes and suffixes get their own substreams when a caller asks for them, and those must not
/// collide either.
TEST(StructuredSubstreamNames, StreamNamesAreUniqueWithSpecializedPrefixesAndSuffixes)
{
    for (const auto & [label, type] : nullableArrayShapes())
    {
        auto result = enumerate(type, /*pass_column_type=*/true, /*use_specialized_prefixes_and_suffixes=*/true);
        expectAllDistinct(result.file_names, label + " (with prefixes/suffixes)");
    }
}

/// ---------------------------------------------------------------------------------------------
/// The names themselves. Pinned in full, because they become permanent once a part is written.
/// ---------------------------------------------------------------------------------------------

TEST(StructuredSubstreamNames, ExactNamesForNullableArray)
{
    using Expected = std::vector<String>;

    /// The outer null map keeps the plain `.null` name and the offsets keep the plain `.size0` name.
    /// Only the streams below an array element acquire the `.array` component.
    EXPECT_EQ(
        fileNames(nullableArrayOf("UInt32")),
        (Expected{"c.null", "c.size0", "c.array.nested"}));

    /// The collision this feature exists to avoid: two null maps, two distinct names.
    EXPECT_EQ(
        fileNames(nullableArrayOf("Nullable(UInt32)")),
        (Expected{"c.null", "c.size0", "c.array.null", "c.array.nested"}));

    EXPECT_EQ(
        fileNames(nullableArrayOf("Array(Nullable(UInt32))")),
        (Expected{
            "c.null", "c.size0", "c.array.array.size0", "c.array.array.nested.null", "c.array.array.nested"}));

    /// A `Nullable(Array)` inside an ordinary array: the outer array keeps its legacy `.size0`.
    EXPECT_EQ(
        fileNames(arrayOf(nullableArrayOf("Nullable(UInt32)"))),
        (Expected{
            "c.size0", "c.array.null", "c.array.array.size0", "c.array.array.nested.null", "c.array.array.nested"}));
}

TEST(StructuredSubstreamNames, ExactNamesForTupleElements)
{
    using Expected = std::vector<String>;

    /// Tuple element names are escaped for the file system exactly as they are under legacy naming.
    EXPECT_EQ(
        fileNames(nullableArrayOf("Tuple(a Nullable(UInt8), b Nullable(UInt8))")),
        (Expected{
            "c.null",
            "c.size0",
            "c.array" + escapeForFileName(".a") + ".null",
            "c.array" + escapeForFileName(".a") + ".nested",
            "c.array" + escapeForFileName(".b") + ".null",
            "c.array" + escapeForFileName(".b") + ".nested"}));

    /// A `Nullable(Array)` sitting *inside* a tuple, rather than containing one.
    auto names = fileNames(tupleOf({"a", "b"}, {nullableArrayOf("Nullable(UInt8)"), parseType("Array(Nullable(UInt8))")}));
    expectAllDistinct(names, "Tuple(Nullable(Array(Nullable)), Array(Nullable))");
    EXPECT_NE(
        std::find(names.begin(), names.end(), "c" + escapeForFileName(".a") + ".null"),
        names.end())
        << ::testing::PrintToString(names);
}

/// The two null maps of a `Nullable(Array(Nullable(T)))` must never share a name at any depth. This
/// is the specific defect that made a new naming scheme necessary, so it is asserted directly rather
/// than only as part of a uniqueness sweep.
TEST(StructuredSubstreamNames, TwoNullMapsNeverShareAName)
{
    const std::vector<DataTypePtr> types = {
        nullableArrayOf("Nullable(UInt32)"),
        nullableArrayOf(nullableArrayOf("Nullable(UInt32)")),
        arrayOf(nullableArrayOf("Nullable(UInt32)")),
        arrayOf(arrayOf(nullableArrayOf("Nullable(UInt32)"))),
        mapOf("String", nullableArrayOf("Nullable(UInt32)")),
    };

    for (const auto & type : types)
    {
        auto names = fileNames(type);
        std::vector<String> null_map_names;
        for (const auto & name : names)
            if (name.ends_with(".null"))
                null_map_names.push_back(name);

        EXPECT_GE(null_map_names.size(), 2u) << type->getName() << ": " << ::testing::PrintToString(names);
        expectAllDistinct(null_map_names, type->getName() + " (null maps)");
    }
}

/// ---------------------------------------------------------------------------------------------
/// Subcolumn reads must resolve to the same files the writer produced.
/// ---------------------------------------------------------------------------------------------

/// A subcolumn is named from the type as stored in the part, not from the subcolumn's own type. If
/// these diverged, a subcolumn read would look for a file that was never written.
TEST(StructuredSubstreamNames, SubcolumnFileNamesMatchStorageStreams)
{
    auto storage_type = arrayOf(nullableArrayOf("Nullable(UInt32)"));
    auto storage_streams = fileNames(storage_type);
    std::set<String> storage_stream_set(storage_streams.begin(), storage_streams.end());

    for (const auto & subcolumn_name : storage_type->getSubcolumnNames())
    {
        auto subcolumn_type = storage_type->tryGetSubcolumnType(subcolumn_name);
        auto subcolumn_serialization = storage_type->getSubcolumnSerialization(
            subcolumn_name, storage_type->getDefaultSerialization());
        if (!subcolumn_type || !subcolumn_serialization)
            continue;

        NameAndTypePair column{"c", subcolumn_name, storage_type, subcolumn_type};

        ISerialization::EnumerateStreamsSettings enumerate_settings;
        ISerialization::StreamFileNameSettings name_settings;
        name_settings.column_type = storage_type.get();

        auto data = ISerialization::SubstreamData(subcolumn_serialization).withType(subcolumn_type);
        subcolumn_serialization->enumerateStreams(
            enumerate_settings,
            [&](const ISerialization::SubstreamPath & path)
            {
                auto file_name = ISerialization::getFileNameForStream(column, path, name_settings);
                EXPECT_TRUE(storage_stream_set.contains(file_name))
                    << "subcolumn " << subcolumn_name << " reads " << file_name
                    << " which the writer never produced; written: "
                    << ::testing::PrintToString(storage_streams);
            },
            data);
    }
}

/// ---------------------------------------------------------------------------------------------
/// Why the column type is required, and what happens when a caller does not supply one.
/// ---------------------------------------------------------------------------------------------

/// The same substream path needs different names for different column types, so no path-only rule can
/// replace the type. This is asserted directly because it is the reason the naming scheme is selected
/// per column rather than per path, and because a reader that loses the type does not fail loudly for
/// this particular path - it silently resolves to the legacy name.
TEST(StructuredSubstreamNames, SamePathNeedsDifferentNamesForDifferentTypes)
{
    ISerialization::SubstreamPath path;
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});

    auto legacy_type = parseType("Array(Nullable(UInt32))");
    auto structured_type = arrayOf(nullableArrayOf("Nullable(UInt32)"));

    ISerialization::StreamFileNameSettings legacy_settings;
    legacy_settings.column_type = legacy_type.get();

    ISerialization::StreamFileNameSettings structured_settings;
    structured_settings.column_type = structured_type.get();

    EXPECT_EQ(ISerialization::getFileNameForStream("c", path, legacy_settings), "c.null");
    EXPECT_EQ(ISerialization::getFileNameForStream("c", path, structured_settings), "c.array.null");
}

/// ---------------------------------------------------------------------------------------------
/// Regressions. Each of these shipped in CI because the corpus above did not reach the shape.
/// ---------------------------------------------------------------------------------------------

/// `Array(Nullable(T))` reached through a `Dynamic`/`Object` path must keep its legacy names. The
/// path carries an array and a null map, but in the opposite nesting to `Nullable(Array(T))`, and an
/// earlier version of the predicate matched on "array plus null map" alone. That renamed the streams
/// of existing JSON columns holding `Array(Nullable(Int64))` dynamic paths - a silent on-disk
/// incompatibility, caught by `03526_columns_substreams_in_wide_parts`.
TEST(StructuredSubstreamNames, ArrayOfNullableUnderADynamicPathKeepsLegacyNames)
{
    ISerialization::SubstreamPath path;
    path.push_back({ISerialization::Substream::ObjectDynamicPath});
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});
    EXPECT_FALSE(needsStructuredSubstreamNamesForPath(path));

    ISerialization::SubstreamPath sizes_path;
    sizes_path.push_back({ISerialization::Substream::ObjectDynamicPath});
    sizes_path.push_back({ISerialization::Substream::ArraySizes});
    EXPECT_FALSE(needsStructuredSubstreamNamesForPath(sizes_path));

    /// The genuine case - a `Nullable` directly above an `Array` under the same dynamic path - still
    /// selects structured naming.
    ISerialization::SubstreamPath nullable_array_path;
    nullable_array_path.push_back({ISerialization::Substream::ObjectDynamicPath});
    nullable_array_path.push_back({ISerialization::Substream::NullableElements});
    nullable_array_path.push_back({ISerialization::Substream::ArraySizes});
    EXPECT_TRUE(needsStructuredSubstreamNamesForPath(nullable_array_path));
}

/// A `Nullable` whose nested type exposes a `null` subcolumn of its own hides its null map, so that
/// the nested path wins. `Nullable(JSON).null` is the JSON path, not the `UInt8` null map. Listing
/// `NullMapHidden` as subcolumn-bearing defeats that, which `04269_nullable_json_null_subcolumn`
/// caught.
TEST(StructuredSubstreamNames, HiddenNullMapDoesNotClaimTheNullSubcolumn)
{
    ISerialization::SubstreamPath hidden;
    hidden.push_back({ISerialization::Substream::NullMapHidden});
    EXPECT_FALSE(ISerialization::hasSubcolumnForPath(hidden, hidden.size()));

    ISerialization::SubstreamPath visible;
    visible.push_back({ISerialization::Substream::NullMap});
    EXPECT_TRUE(ISerialization::hasSubcolumnForPath(visible, visible.size()));
}

/// `Nullable(Tuple(x Array(T)))` is legal today. Its substream path puts a `TupleElement` between the
/// `NullableElements` and the array, so it is *not* a `Nullable` sitting on an `Array` and must keep
/// legacy names. An earlier predicate matched it anyway and every string-only stream-resolution site
/// broke on it, which `03999_empty_to_equals_rewrite_for_nullable` caught.
///
/// This enumerates the nullable tuple itself. A previous version of this test built the type and then
/// enumerated the plain `Tuple(x Array(UInt16))`, so it asserted nothing about the shape it named.
TEST(StructuredSubstreamNames, NullableTupleWithArrayMemberStaysLegacy)
{
    auto type = parseType("Nullable(Tuple(x Array(UInt16)))");
    EXPECT_FALSE(needsStructuredSubstreamNames(*type));

    bool saw_tuple_element_before_array = false;

    ISerialization::EnumerateStreamsSettings enumerate_settings;
    auto serialization = type->getDefaultSerialization();
    auto data = ISerialization::SubstreamData(serialization).withType(type);
    serialization->enumerateStreams(
        enumerate_settings,
        [&](const ISerialization::SubstreamPath & path)
        {
            EXPECT_FALSE(needsStructuredSubstreamNamesForPath(path))
                << "path: " << path.toString();

            for (size_t i = 0; i + 1 < path.size(); ++i)
                if (path[i].type == ISerialization::Substream::TupleElement
                    && path[i + 1].type == ISerialization::Substream::ArraySizes)
                    saw_tuple_element_before_array = true;

            /// The string-only resolution sites take this route.
            ISerialization::StreamFileNameSettings without_type;
            EXPECT_NO_THROW(ISerialization::getFileNameForStream("c", path, without_type));
        },
        data);

    /// Guards the test itself: if the type ever stops producing that shape, the assertions above stop
    /// covering the case they were written for.
    EXPECT_TRUE(saw_tuple_element_before_array);

    EXPECT_EQ(
        enumerate(type, /*pass_column_type=*/true).file_names,
        enumerate(type, /*pass_column_type=*/false).file_names);
}
