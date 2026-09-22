#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>

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
        EXPECT_EQ(fileName(path), "m.key_foo");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_foo");
        EXPECT_EQ(path.front().toString(), "MapKey(foo)");
        EXPECT_TRUE(ISerialization::hasSubcolumnForPath(path, path.size()));
        EXPECT_TRUE(ISerialization::isDynamicSubcolumn(path, path.size()));
        EXPECT_FALSE(ISerialization::isEphemeralSubcolumn(path, path.size()));
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyPresence, "foo"));
        EXPECT_EQ(fileName(path), "m.key_presence");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_foo");
        EXPECT_EQ(path.front().toString(), "MapKeyPresence(foo)");
        EXPECT_TRUE(ISerialization::hasSubcolumnForPath(path, path.size()));
        EXPECT_TRUE(ISerialization::isDynamicSubcolumn(path, path.size()));
        EXPECT_FALSE(ISerialization::isSpecialCompressionAllowed(path));
        EXPECT_FALSE(ISerialization::isEphemeralSubcolumn(path, path.size()));
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
        EXPECT_EQ(fileName(path), "m.key_" + escapeForFileName(dotted));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + dotted);
        EXPECT_EQ(path.front().toString(), "MapKey(" + dotted + ")");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, slashed));
        EXPECT_EQ(fileName(path), "m.key_" + escapeForFileName(slashed));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + slashed);
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, empty));
        EXPECT_EQ(fileName(path), "m.key_" + escapeForFileName(empty));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_");
        EXPECT_EQ(path.front().toString(), "MapKey()");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKey, non_utf8));
        EXPECT_EQ(fileName(path), "m.key_" + escapeForFileName(non_utf8));
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "key_" + non_utf8);
        EXPECT_EQ(path.front().toString(), "MapKey(" + non_utf8 + ")");
    }

    {
        auto path = makePath(namedStream(ISerialization::Substream::MapKeyPresence, dotted));
        EXPECT_EQ(fileName(path), "m.key_presence");
        EXPECT_EQ(ISerialization::getSubcolumnNameForStream(path), "exists_" + dotted);
    }
}
