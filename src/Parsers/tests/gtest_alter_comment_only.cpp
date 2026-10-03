#include <gtest/gtest.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>

using namespace DB;

namespace
{

bool isSettingsOrCommentAlter(const String & query)
{
    ParserAlterQuery parser;
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    return ast->as<ASTAlterQuery &>().isSettingsOrCommentAlter();
}

bool isSettingsOrTableCommentAlter(const String & query)
{
    ParserAlterQuery parser;
    ASTPtr ast = parseQuery(parser, query, 0, 0, 0);
    return ast->as<ASTAlterQuery &>().isSettingsOrTableCommentAlter();
}

}

/// `isSettingsOrCommentAlter` recognises `ALTER`s that only touch settings or comments,
/// including a comment-only `MODIFY COLUMN`.
/// It must stay in sync with the storage-side decision (`AlterCommand::isCommentAlter` for the
/// properties `ALTER` supports, `checkColumnDeclarationIsSupportedByAlter` for the ones it
/// rejects): any column property next to the `COMMENT` disqualifies the fast path.
TEST(AlterCommentOnly, CommentOnlyModifyColumn)
{
    EXPECT_TRUE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x'"));
    EXPECT_TRUE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY SETTING max_part_loading_threads = 8"));

    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c UInt64 COMMENT 'x'"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' CODEC(ZSTD)"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' TTL d + INTERVAL 1 DAY"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' STATISTICS(tdigest)"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' SETTINGS (max_compress_block_size = 1024)"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' COLLATE utf8_bin"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' PRIMARY KEY"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c DEFAULT 1 COMMENT 'x'"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' FIRST"));
    EXPECT_FALSE(isSettingsOrCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x' AFTER d"));
}

/// `isSettingsOrTableCommentAlter` drives the DDL routing of `ON CLUSTER` queries
/// (`DDLWorker::taskShouldBeExecutedOnLeader`): settings and table comments are local metadata
/// executed on every replica, while column comments are replicated through ZooKeeper and take
/// the single-leader path.
TEST(AlterCommentOnly, SettingsOrTableComment)
{
    EXPECT_TRUE(isSettingsOrTableCommentAlter("ALTER TABLE t MODIFY COMMENT 'x'"));
    EXPECT_TRUE(isSettingsOrTableCommentAlter("ALTER TABLE t MODIFY SETTING max_part_loading_threads = 8"));
    EXPECT_TRUE(isSettingsOrTableCommentAlter("ALTER TABLE t MODIFY COMMENT 'x', RESET SETTING max_part_loading_threads"));

    EXPECT_FALSE(isSettingsOrTableCommentAlter("ALTER TABLE t COMMENT COLUMN c 'x'"));
    EXPECT_FALSE(isSettingsOrTableCommentAlter("ALTER TABLE t MODIFY COLUMN c COMMENT 'x'"));
    EXPECT_FALSE(isSettingsOrTableCommentAlter("ALTER TABLE t COMMENT COLUMN c 'x', MODIFY SETTING max_part_loading_threads = 8"));
}
