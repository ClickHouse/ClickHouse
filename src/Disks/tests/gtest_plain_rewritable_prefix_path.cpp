#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritablePrefixPath.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(PlainRewritablePrefixPath, SerializeImplicit)
{
    PlainRewritablePrefixPath path{.logical_path = "hello/world", .explicit_files = false, .files = {}};
    EXPECT_EQ(serializePlainRewritablePrefixPath(path), "hello/world/");

    path.logical_path = "/hello/world/";
    EXPECT_EQ(serializePlainRewritablePrefixPath(path), "/hello/world/");
}

TEST(PlainRewritablePrefixPath, ParseImplicit)
{
    auto parsed = parsePlainRewritablePrefixPath("hello/world/");
    EXPECT_FALSE(parsed.explicit_files);
    EXPECT_EQ(parsed.logical_path, "hello/world/");
    EXPECT_TRUE(parsed.files.empty());

    parsed = parsePlainRewritablePrefixPath("hello/world");
    EXPECT_FALSE(parsed.explicit_files);
    EXPECT_EQ(parsed.logical_path, "hello/world/");
}

TEST(PlainRewritablePrefixPath, RoundTripExplicit)
{
    PlainRewritablePrefixPath path{
        .logical_path = "/hello/world/",
        .explicit_files = true,
        .files = {
            {"hello.json", "gfkoqxvyhaasroiodbeurnftnwieiihy/hello.json"},
            {"upyachka.bin", "aaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin"},
        },
    };

    const auto serialized = serializePlainRewritablePrefixPath(path);
    EXPECT_EQ(
        serialized,
        "/hello/world/\n"
        "files: 2\n"
        "hello.json\tgfkoqxvyhaasroiodbeurnftnwieiihy/hello.json\n"
        "upyachka.bin\taaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin");

    const auto parsed = parsePlainRewritablePrefixPath(serialized);
    EXPECT_TRUE(parsed.explicit_files);
    EXPECT_EQ(parsed.logical_path, "/hello/world/");
    ASSERT_EQ(parsed.files.size(), 2);
    EXPECT_EQ(parsed.files[0].first, "hello.json");
    EXPECT_EQ(parsed.files[0].second, "gfkoqxvyhaasroiodbeurnftnwieiihy/hello.json");
    EXPECT_EQ(parsed.files[1].first, "upyachka.bin");
    EXPECT_EQ(parsed.files[1].second, "aaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin");
}

TEST(PlainRewritablePrefixPath, ParseExplicitWithSpaces)
{
    constexpr std::string_view content =
        "/hello/world/\n"
        "files: 2\n"
        "upyachka.bin    aaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin\n"
        "hello.json      gfkoqxvyhaasroiodbeurnftnwieiihy/hello.json\n";

    const auto parsed = parsePlainRewritablePrefixPath(content);
    EXPECT_TRUE(parsed.explicit_files);
    EXPECT_EQ(parsed.logical_path, "/hello/world/");
    ASSERT_EQ(parsed.files.size(), 2);
    EXPECT_EQ(parsed.files[0].first, "upyachka.bin");
    EXPECT_EQ(parsed.files[1].first, "hello.json");
}

TEST(PlainRewritablePrefixPath, ParseExplicitEmptyFileList)
{
    const auto parsed = parsePlainRewritablePrefixPath("A/\nfiles: 0");
    EXPECT_TRUE(parsed.explicit_files);
    EXPECT_EQ(parsed.logical_path, "A/");
    EXPECT_TRUE(parsed.files.empty());
}

TEST(PlainRewritablePrefixPath, RejectInvalid)
{
    EXPECT_THROW(parsePlainRewritablePrefixPath(""), Exception);
    EXPECT_THROW(parsePlainRewritablePrefixPath("A/\nfiles:"), Exception);
    EXPECT_THROW(parsePlainRewritablePrefixPath("A/\nfiles: 1"), Exception);
    EXPECT_THROW(parsePlainRewritablePrefixPath("A/\nfiles: 1\nonly_name"), Exception);
    EXPECT_THROW(parsePlainRewritablePrefixPath("A/\nfiles: 1\na\tb\nc\td"), Exception);

    PlainRewritablePrefixPath invalid_implicit{.logical_path = "A/", .explicit_files = false, .files = {{"a", "b/a"}}};
    EXPECT_THROW(serializePlainRewritablePrefixPath(invalid_implicit), Exception);
}
