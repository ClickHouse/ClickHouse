#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

ISerialization::SubstreamPath makePath(ISerialization::Substream stream)
{
    ISerialization::SubstreamPath path;
    path.push_back(std::move(stream));
    return path;
}

ISerialization::Substream namedStream(ISerialization::Substream::Type type, const String & name)
{
    ISerialization::Substream stream(type);
    stream.name_of_substream = name;
    return stream;
}

ISerialization::Substream namedV1Stream(ISerialization::Substream::Type type, const String & name)
{
    auto stream = namedStream(type, name);
    stream.map_key_stream_name_version = 1;
    return stream;
}

String fileName(const ISerialization::SubstreamPath & path)
{
    return ISerialization::getFileNameForStream("m", path, {});
}

}

TEST(MapWithKeyColumnsSubstreams, FileAndSubcolumnNames)
{
    {
        auto path = makePath(ISerialization::Substream(ISerialization::Substream::MapKeysInfo));
        EXPECT_EQ(fileName(path), "m.keys_info");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "keys_info");
        EXPECT_EQ(path.front().toString(), "MapKeysInfo");
        EXPECT_TRUE(ISerialization::isMetadataStream(path));
        EXPECT_TRUE(ISerialization::isSingleValuePerPartStream(path));
        EXPECT_TRUE(ISerialization::hasPrefix(path, false));
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, "foo"));
        EXPECT_EQ(fileName(path), "m.key_3_foo");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_foo");
        EXPECT_EQ(path.front().toString(), "MapKey(foo)");
        EXPECT_TRUE(ISerialization::hasSubcolumnForPath(path, path.size()));
        EXPECT_TRUE(ISerialization::isDynamicSubcolumn(path, path.size()));
        EXPECT_FALSE(ISerialization::isEphemeralSubcolumn(path, path.size()));
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyExists, "foo"));
        EXPECT_EQ(fileName(path), "m.exists_3_foo");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_foo");
        EXPECT_EQ(path.front().toString(), "MapKeyExists(foo)");
        EXPECT_TRUE(ISerialization::hasSubcolumnForPath(path, path.size()));
        EXPECT_TRUE(ISerialization::isDynamicSubcolumn(path, path.size()));
        EXPECT_FALSE(ISerialization::isEphemeralSubcolumn(path, path.size()));
    }

    {
        auto value_path = makePath(namedStream(ISerialization::Substream::MapKey, "template"));
        EXPECT_EQ(fileName(value_path), "m.key_8_template");
        auto exists_path = makePath(namedStream(ISerialization::Substream::MapKeyExists, "template"));
        EXPECT_EQ(fileName(exists_path), "m.exists_8_template");
        auto value_template_path = makePath(ISerialization::Substream(ISerialization::Substream::MapKeyValueTemplate));
        EXPECT_EQ(fileName(value_template_path), "m.key_template");
        auto exists_template_path = makePath(ISerialization::Substream(ISerialization::Substream::MapKeyExistsTemplate));
        EXPECT_EQ(fileName(exists_template_path), "m.exists_template");
        EXPECT_NE(fileName(value_path), fileName(value_template_path));
        EXPECT_NE(fileName(exists_path), fileName(exists_template_path));

        EXPECT_EQ(
            fileName(makePath(namedV1Stream(ISerialization::Substream::MapKey, "template"))),
            fileName(value_template_path));
    }
}

TEST(MapWithKeyColumnsSubstreams, EscapedKeyNames)
{
    const String dotted = "a.b";
    const String slashed = "a/b";
    const String empty;
    const String non_utf8 = String("\xC0\xAF", 2);

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, dotted));
        EXPECT_EQ(fileName(path), "m.key_3_" + escapeForFileName(dotted));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + dotted);
        EXPECT_EQ(path.front().toString(), "MapKey(" + dotted + ")");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, slashed));
        EXPECT_EQ(fileName(path), "m.key_3_" + escapeForFileName(slashed));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + slashed);
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, empty));
        EXPECT_EQ(fileName(path), "m.key_0_" + escapeForFileName(empty));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_");
        EXPECT_EQ(path.front().toString(), "MapKey()");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, non_utf8));
        EXPECT_EQ(fileName(path), "m.key_2_" + escapeForFileName(non_utf8));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + non_utf8);
        EXPECT_EQ(path.front().toString(), "MapKey(" + non_utf8 + ")");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyExists, dotted));
        EXPECT_EQ(fileName(path), "m.exists_3_" + escapeForFileName(dotted));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_" + dotted);
        EXPECT_EQ(path.front().toString(), "MapKeyExists(" + dotted + ")");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyExists, slashed));
        EXPECT_EQ(fileName(path), "m.exists_3_" + escapeForFileName(slashed));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_" + slashed);
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyExists, empty));
        EXPECT_EQ(fileName(path), "m.exists_0_" + escapeForFileName(empty));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyExists, non_utf8));
        EXPECT_EQ(fileName(path), "m.exists_2_" + escapeForFileName(non_utf8));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_" + non_utf8);
    }
}

TEST(MapWithKeyColumnsSubstreams, KeyBoundaryBeforeNestedValue)
{
    ISerialization::SubstreamPath first_v1;
    first_v1.push_back(namedV1Stream(ISerialization::Substream::MapKey, "k"));
    first_v1.push_back(namedStream(ISerialization::Substream::TupleElement, "a.b"));

    ISerialization::SubstreamPath second_v1;
    second_v1.push_back(namedV1Stream(ISerialization::Substream::MapKey, "k.a"));
    second_v1.push_back(namedStream(ISerialization::Substream::TupleElement, "b"));
    EXPECT_EQ(fileName(first_v1), fileName(second_v1));

    auto first_v2 = first_v1;
    first_v2.front().map_key_stream_name_version = MapKeyManifest::CURRENT_STREAM_NAME_VERSION;
    auto second_v2 = second_v1;
    second_v2.front().map_key_stream_name_version = MapKeyManifest::CURRENT_STREAM_NAME_VERSION;
    EXPECT_NE(fileName(first_v2), fileName(second_v2));
}
