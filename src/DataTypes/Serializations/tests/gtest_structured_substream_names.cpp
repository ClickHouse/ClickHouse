#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/StructuredSubstreamNames.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>

#include <set>
#include <vector>

using namespace DB;

namespace
{

ISerialization::SubstreamPath makeNullableArrayNullableElementPaths()
{
    ISerialization::SubstreamPath path;

    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::Regular});

    return path;
}

ISerialization::SubstreamPath makeNullableArrayNonNullableElementPaths()
{
    ISerialization::SubstreamPath path;

    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::Regular});

    return path;
}

ISerialization::SubstreamPath makeNullableArrayTupleElementInnerNullMapPaths(const String & tuple_element_name)
{
    ISerialization::SubstreamPath path;

    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});

    ISerialization::Substream tuple_element(ISerialization::Substream::TupleElement);
    tuple_element.name_of_substream = tuple_element_name;
    path.push_back(tuple_element);

    path.push_back({ISerialization::Substream::NullMap});

    return path;
}

ISerialization::SubstreamPath makeTupleNullableArrayElementPaths(const String & tuple_element_name)
{
    ISerialization::SubstreamPath path;

    ISerialization::Substream tuple_element(ISerialization::Substream::TupleElement);
    tuple_element.name_of_substream = tuple_element_name;
    path.push_back(tuple_element);
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::Regular});

    return path;
}

ISerialization::SubstreamPath makeNestedArrayNullableArrayPaths()
{
    ISerialization::SubstreamPath path;

    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::Regular});

    return path;
}

ISerialization::SubstreamPath makeNestedArrayNullableArrayOuterArraySizesPath()
{
    ISerialization::SubstreamPath path;
    path.push_back({ISerialization::Substream::ArraySizes});
    return path;
}

ISerialization::SubstreamPath makeNestedArrayNullableArrayTupleInnerNullMapPaths(const String & tuple_element_name)
{
    ISerialization::SubstreamPath path;

    path.push_back({ISerialization::Substream::ArrayElements});
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});
    path.push_back({ISerialization::Substream::ArrayElements});

    ISerialization::Substream tuple_element(ISerialization::Substream::TupleElement);
    tuple_element.name_of_substream = tuple_element_name;
    path.push_back(tuple_element);

    path.push_back({ISerialization::Substream::NullMap});

    return path;
}

ISerialization::SubstreamPath prefixPath(const ISerialization::SubstreamPath & path, size_t length)
{
    ISerialization::SubstreamPath result;
    result.assign(path.begin(), path.begin() + static_cast<ssize_t>(length));
    return result;
}

std::vector<String> collectStructuredSuffixesForNestedArrayNullableArray()
{
    const auto full_path = makeNestedArrayNullableArrayPaths();
    return {
        getStructuredSubstreamNameSuffix(makeNestedArrayNullableArrayOuterArraySizesPath()),
        getStructuredSubstreamNameSuffix(prefixPath(full_path, 2)),
        getStructuredSubstreamNameSuffix(prefixPath(full_path, 4)),
        getStructuredSubstreamNameSuffix(prefixPath(full_path, 6)),
        getStructuredSubstreamNameSuffix(full_path),
    };
}

}

TEST(StructuredSubstreamNames, NeedsStructuredForNullableArray)
{
    auto type = DataTypeFactory::instance().get("Nullable(Array(Nullable(UInt32)))");
    EXPECT_TRUE(needsStructuredSubstreamNames(*type));
}

TEST(StructuredSubstreamNames, DoesNotNeedStructuredForArrayNullable)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(UInt32))");
    EXPECT_FALSE(needsStructuredSubstreamNames(*type));
}

TEST(StructuredSubstreamNames, NeedsStructuredForNestedArrayNullableArray)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(Array(Nullable(UInt32))))");
    EXPECT_TRUE(needsStructuredSubstreamNames(*type));
}

TEST(StructuredSubstreamNames, NullableArrayStreamSuffixes)
{
    const auto full_path = makeNullableArrayNullableElementPaths();

    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 1)), ".null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 3)), ".array.size0");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 5)), ".array.nested.null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(full_path), ".array.nested");
}

TEST(StructuredSubstreamNames, MergeTreeFileNames)
{
    auto type = DataTypeFactory::instance().get("Nullable(Array(Nullable(UInt32)))");
    const auto full_path = makeNullableArrayNullableElementPaths();

    ISerialization::StreamFileNameSettings settings;
    settings.column_type = type.get();

    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 1), settings),
        "c.null");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 3), settings),
        "c.array.size0");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 5), settings),
        "c.array.nested.null");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", full_path, settings),
        "c.array.nested");
}

TEST(StructuredSubstreamNames, UsesStructuredWhenColumnTypeSet)
{
    auto type = DataTypeFactory::instance().get("Nullable(Array(UInt32))");

    ISerialization::StreamFileNameSettings settings;
    settings.column_type = type.get();

    ISerialization::SubstreamPath path;
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});

    EXPECT_EQ(ISerialization::getFileNameForStream("c", path, settings), "c.array.size0");
}

TEST(StructuredSubstreamNames, NullableArrayNonNullableElementStreamSuffix)
{
    const auto path = makeNullableArrayNonNullableElementPaths();

    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(path, 1)), ".null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(path, 3)), ".array.size0");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(path), ".array.nested");
}

TEST(StructuredSubstreamNames, NullableArrayTupleElementInnerNullMapSuffixes)
{
    const auto path_a = makeNullableArrayTupleElementInnerNullMapPaths("a");
    const auto path_b = makeNullableArrayTupleElementInnerNullMapPaths("b");

    EXPECT_EQ(getStructuredSubstreamNameSuffix(path_a), ".array.nested" + escapeForFileName(".a") + ".null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(path_b), ".array.nested" + escapeForFileName(".b") + ".null");
    EXPECT_NE(getStructuredSubstreamNameSuffix(path_a), getStructuredSubstreamNameSuffix(path_b));
}

TEST(StructuredSubstreamNames, TuplePrefixedNullableArrayStreamSuffixes)
{
    const auto path_a = makeTupleNullableArrayElementPaths("a");
    const auto path_b = makeTupleNullableArrayElementPaths("b");

    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(path_a, 2)), escapeForFileName(".a") + ".null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(path_b, 2)), escapeForFileName(".b") + ".null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(path_a), escapeForFileName(".a") + ".array.nested");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(path_b), escapeForFileName(".b") + ".array.nested");
    EXPECT_NE(getStructuredSubstreamNameSuffix(path_a), getStructuredSubstreamNameSuffix(path_b));
}

TEST(StructuredSubstreamNames, NestedArrayNullableArrayStreamSuffixes)
{
    const auto full_path = makeNestedArrayNullableArrayPaths();

    EXPECT_EQ(getStructuredSubstreamNameSuffix(makeNestedArrayNullableArrayOuterArraySizesPath()), ".size0");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 2)), ".array.null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 4)), ".array.array.size0");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(prefixPath(full_path, 6)), ".array.array.nested.null");
    EXPECT_EQ(getStructuredSubstreamNameSuffix(full_path), ".array.array.nested");

    EXPECT_NE(
        getStructuredSubstreamNameSuffix(prefixPath(full_path, 2)),
        getStructuredSubstreamNameSuffix(prefixPath(full_path, 6)));
}

TEST(StructuredSubstreamNames, NestedArrayNullableArrayMergeTreeFileNames)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(Array(Nullable(UInt32))))");
    const auto full_path = makeNestedArrayNullableArrayPaths();

    ISerialization::StreamFileNameSettings settings;
    settings.column_type = type.get();

    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", makeNestedArrayNullableArrayOuterArraySizesPath(), settings),
        "c.size0");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 2), settings),
        "c.array.null");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 4), settings),
        "c.array.array.size0");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", prefixPath(full_path, 6), settings),
        "c.array.array.nested.null");
    EXPECT_EQ(
        ISerialization::getFileNameForStream("c", full_path, settings),
        "c.array.array.nested");
}

TEST(StructuredSubstreamNames, NestedArrayNullableArrayAllSuffixesUnique)
{
    const auto suffixes = collectStructuredSuffixesForNestedArrayNullableArray();
    EXPECT_EQ(suffixes.size(), std::set<String>(suffixes.begin(), suffixes.end()).size());
}

TEST(StructuredSubstreamNames, NestedArrayNullableArrayTupleInnerNullMapSuffixes)
{
    const auto path_a = makeNestedArrayNullableArrayTupleInnerNullMapPaths("a");
    const auto path_b = makeNestedArrayNullableArrayTupleInnerNullMapPaths("b");

    EXPECT_EQ(
        getStructuredSubstreamNameSuffix(path_a),
        ".array.array.nested" + escapeForFileName(".a") + ".null");
    EXPECT_EQ(
        getStructuredSubstreamNameSuffix(path_b),
        ".array.array.nested" + escapeForFileName(".b") + ".null");
    EXPECT_NE(getStructuredSubstreamNameSuffix(path_a), getStructuredSubstreamNameSuffix(path_b));
    EXPECT_NE(
        getStructuredSubstreamNameSuffix(prefixPath(makeNestedArrayNullableArrayPaths(), 2)),
        getStructuredSubstreamNameSuffix(path_a));
}

TEST(StructuredSubstreamNames, SubstreamCacheKeysAreUniqueForNestedArrayNullableArray)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(Array(Nullable(UInt32))))");
    const auto outer_null_map_path = prefixPath(makeNestedArrayNullableArrayPaths(), 2);
    const auto inner_null_map_path = prefixPath(makeNestedArrayNullableArrayPaths(), 6);

    EXPECT_NE(
        ISerialization::getSubcolumnNameForStream(outer_null_map_path),
        ISerialization::getSubcolumnNameForStream(inner_null_map_path));

    EXPECT_NE(
        ISerialization::getSubstreamCacheKey(outer_null_map_path, false, type.get()),
        ISerialization::getSubstreamCacheKey(inner_null_map_path, false, type.get()));
}

TEST(StructuredSubstreamNames, UsesLegacyWithoutColumnType)
{
    auto type = DataTypeFactory::instance().get("Nullable(Array(UInt32))");

    ISerialization::StreamFileNameSettings settings;

    ISerialization::SubstreamPath path;
    path.push_back({ISerialization::Substream::NullMap});
    path.push_back({ISerialization::Substream::NullableElements});
    path.push_back({ISerialization::Substream::ArraySizes});

    EXPECT_EQ(ISerialization::getFileNameForStream("c", path, settings), "c.null.size0");

    settings.column_type = type.get();
    EXPECT_EQ(ISerialization::getFileNameForStream("c", path, settings), "c.array.size0");
}
