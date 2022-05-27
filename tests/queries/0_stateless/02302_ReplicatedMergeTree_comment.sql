-- Validate that setting/removing and getting comments on ReplicatedMergeTree works
-- https://github.com/ClickHouse/ClickHouse/issues/36377


CREATE TABLE 02302_ReplicatedMergeTree_comment
(
    key UInt64 COMMENT 'The PK'
)
ENGINE = ReplicatedMergeTree('/test/02302_ReplicatedMergeTree_comment/{database}/source', '1')
PARTITION BY key
ORDER BY tuple()
COMMENT 'Comment text for test table';

# Check that comment is present
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name == '02302_ReplicatedMergeTree_comment';

# Change to a different value and check if it was changed
ALTER TABLE 02302_ReplicatedMergeTree_comment MODIFY COMMENT 'Some new more detailed text of comment';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name == '02302_ReplicatedMergeTree_comment';

# Remove the comment and check if it is empty now
ALTER TABLE 02302_ReplicatedMergeTree_comment MODIFY COMMENT '';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name == '02302_ReplicatedMergeTree_comment';
