#include <gtest/gtest.h>

#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>

using Path = DB::StorageObjectStorageConfiguration::Path;

TEST(ObjectStoragePathCutGlobs, LegacyRawScan)
{
    /// The legacy scan cuts at the first raw '*', '?' or '{', even when the brace
    /// group is literal text under the AST classification.
    const Path path("dir/data_{x}/part-*.parquet");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true), "dir/data_");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false), "dir");

    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true, /*use_glob_ast=*/ false), "dir/data_");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false, /*use_glob_ast=*/ false), "dir");
}

TEST(ObjectStoragePathCutGlobs, ASTLiteralBracesStayInPrefix)
{
    /// Under the AST classification a literal brace group such as "{x}" is not a glob,
    /// so the listing prefix keeps it and is cut at the first real glob expression.
    const Path path("dir/data_{x}/part-*.parquet");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true, /*use_glob_ast=*/ true), "dir/data_{x}/part-");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false, /*use_glob_ast=*/ true), "dir/data_{x}");
}

TEST(ObjectStoragePathCutGlobs, ASTGlobBracesStillCut)
{
    /// An actual enum glob cuts the prefix in both modes.
    const Path path("dir/file{a,b}.csv");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true, /*use_glob_ast=*/ false), "dir/file");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true, /*use_glob_ast=*/ true), "dir/file");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false, /*use_glob_ast=*/ true), "dir");
}

TEST(ObjectStoragePathCutGlobs, NoPrefixDirectoryFallsBackToRoot)
{
    /// Without a '/' before the first glob, the non-partial-prefix mode lists from the root.
    const Path path("data_{x}-*.csv");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false, /*use_glob_ast=*/ false), "/");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ false, /*use_glob_ast=*/ true), "/");
    EXPECT_EQ(path.cutGlobs(/*supports_partial_prefix=*/ true, /*use_glob_ast=*/ true), "data_{x}-");
}
