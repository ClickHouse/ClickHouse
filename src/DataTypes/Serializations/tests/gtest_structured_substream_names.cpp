#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/StructuredSubstreamNames.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>

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

ISerialization::SubstreamPath prefixPath(const ISerialization::SubstreamPath & path, size_t length)
{
    ISerialization::SubstreamPath result;
    result.assign(path.begin(), path.begin() + static_cast<ssize_t>(length));
    return result;
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
