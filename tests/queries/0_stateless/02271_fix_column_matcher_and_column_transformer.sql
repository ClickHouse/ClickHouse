SET enable_analyzer = 1;

DROP TABLE IF EXISTS github_events;

CREATE TABLE github_events
(
    `file_time` DateTime,
    `event_type` Enum8('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    `actor_login` LowCardinality(String),
    `repo_name` LowCardinality(String),
    `created_at` DateTime,
    `updated_at` DateTime,
    `action` Enum8('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    `comment_id` UInt64,
    `body` String,
    `path` String,
    `position` Int32,
    `line` Int32,
    `ref` LowCardinality(String),
    `ref_type` Enum8('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    `creator_user_login` LowCardinality(String),
    `number` UInt32,
    `title` String,
    `labels` Array(LowCardinality(String)),
    `state` Enum8('none' = 0, 'open' = 1, 'closed' = 2),
    `locked` UInt8,
    `assignee` LowCardinality(String),
    `assignees` Array(LowCardinality(String)),
    `comments` UInt32,
    `author_association` Enum8('NONE' = 0, 'CONTRIBUTOR' = 1, 'OWNER' = 2, 'COLLABORATOR' = 3, 'MEMBER' = 4, 'MANNEQUIN' = 5),
    `closed_at` DateTime,
    `merged_at` DateTime,
    `merge_commit_sha` String,
    `requested_reviewers` Array(LowCardinality(String)),
    `requested_teams` Array(LowCardinality(String)),
    `head_ref` LowCardinality(String),
    `head_sha` String,
    `base_ref` LowCardinality(String),
    `base_sha` String,
    `merged` UInt8,
    `mergeable` UInt8,
    `rebaseable` UInt8,
    `mergeable_state` Enum8('unknown' = 0, 'dirty' = 1, 'clean' = 2, 'unstable' = 3, 'draft' = 4),
    `merged_by` LowCardinality(String),
    `review_comments` UInt32,
    `maintainer_can_modify` UInt8,
    `commits` UInt32,
    `additions` UInt32,
    `deletions` UInt32,
    `changed_files` UInt32,
    `diff_hunk` String,
    `original_position` UInt32,
    `commit_id` String,
    `original_commit_id` String,
    `push_size` UInt32,
    `push_distinct_size` UInt32,
    `member_login` LowCardinality(String),
    `release_tag_name` String,
    `release_name` String,
    `review_state` Enum8('none' = 0, 'approved' = 1, 'changes_requested' = 2, 'commented' = 3, 'dismissed' = 4, 'pending' = 5)
)
ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at);

with
    top_repos as ( select repo_name from github_events where event_type = 'WatchEvent' and toDate(created_at) = today() - 1 group by repo_name order by count() desc limit 100 union distinct select repo_name from github_events where event_type = 'WatchEvent' and toMonday(created_at) = toMonday(today() - interval 1 week) group by repo_name order by count() desc limit 100 union distinct select repo_name from github_events where event_type = 'WatchEvent' and toStartOfMonth(created_at) = toStartOfMonth(today()) - interval 1 month group by repo_name order by count() desc limit 100 union distinct select repo_name from github_events where event_type = 'WatchEvent' and toYear(created_at) = toYear(today()) - 1 group by repo_name order by count() desc limit 100 ),
    last_day as ( select repo_name, count() as count_last_day, rowNumberInAllBlocks() + 1 as position_last_day from github_events where repo_name in (select repo_name from top_repos) and toDate(created_at) = today() - 1 group by repo_name order by count_last_day desc ),
    last_week as ( select repo_name, count() as count_last_week, rowNumberInAllBlocks() + 1 as position_last_week from github_events where repo_name in (select repo_name from top_repos) and toMonday(created_at) = toMonday(today()) - interval 1 week group by repo_name order by count_last_week desc ),
    last_month as ( select repo_name, count() as count_last_month, rowNumberInAllBlocks() + 1 as position_last_month from github_events where repo_name in (select repo_name from top_repos) and toStartOfMonth(created_at) = toStartOfMonth(today()) - interval 1 month group by repo_name order by count_last_month desc )
select d.repo_name, columns('count') from last_day d join last_week w on d.repo_name = w.repo_name join last_month m on d.repo_name = m.repo_name;

set allow_suspicious_low_cardinality_types=1;

CREATE TABLE github_events__fuzz_0 (`file_time` Int64, `event_type` Enum8('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22), `actor_login` LowCardinality(String), `repo_name` LowCardinality(Nullable(String)), `created_at` DateTime, `updated_at` DateTime, `action` Array(Enum8('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20)), `comment_id` UInt64, `body` String, `path` LowCardinality(String), `position` Int32, `line` Int32, `ref` String, `ref_type` Enum8('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4), `creator_user_login` Int16, `number` UInt32, `title` String, `labels` Array(Array(LowCardinality(String))), `state` Enum8('none' = 0, 'open' = 1, 'closed' = 2), `locked` UInt8, `assignee` Array(LowCardinality(String)), `assignees` Array(LowCardinality(String)), `comments` UInt32, `author_association` Array(Enum8('NONE' = 0, 'CONTRIBUTOR' = 1, 'OWNER' = 2, 'COLLABORATOR' = 3, 'MEMBER' = 4, 'MANNEQUIN' = 5)), `closed_at` UUID, `merged_at` DateTime, `merge_commit_sha` Nullable(String), `requested_reviewers` Array(LowCardinality(Int64)), `requested_teams` Array(String), `head_ref` String, `head_sha` String, `base_ref` String, `base_sha` String, `merged` Nullable(UInt8), `mergeable` Nullable(UInt8), `rebaseable` LowCardinality(UInt8), `mergeable_state` Array(Enum8('unknown' = 0, 'dirty' = 1, 'clean' = 2, 'unstable' = 3, 'draft' = 4)), `merged_by` LowCardinality(String), `review_comments` UInt32, `maintainer_can_modify` Nullable(UInt8), `commits` UInt32, `additions` Nullable(UInt32), `deletions` UInt32, `changed_files` UInt32, `diff_hunk` Nullable(String), `original_position` UInt32, `commit_id` String, `original_commit_id` String, `push_size` UInt32, `push_distinct_size` UInt32, `member_login` LowCardinality(String), `release_tag_name` LowCardinality(String), `release_name` String, `review_state` Int16) ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at) settings allow_nullable_key=1;

EXPLAIN PIPELINE header = true, compact = true WITH top_repos AS (SELECT repo_name FROM github_events__fuzz_0 WHERE (event_type = 'WatchEvent') AND (toDate(created_at) = (today() - 1)) GROUP BY repo_name ORDER BY count() DESC LIMIT 100 UNION DISTINCT SELECT repo_name FROM github_events__fuzz_0 WHERE (event_type = 'WatchEvent') AND (toMonday(created_at) = toMonday(today() - toIntervalWeek(1))) GROUP BY repo_name ORDER BY count() DESC LIMIT 100 UNION DISTINCT SELECT repo_name FROM github_events__fuzz_0 PREWHERE (event_type = 'WatchEvent') AND (toStartOfMonth(created_at) = (toStartOfMonth(today()) - toIntervalMonth(1))) GROUP BY repo_name ORDER BY count() DESC LIMIT 100 UNION DISTINCT SELECT repo_name FROM github_events WHERE (event_type = 'WatchEvent') AND (toYear(created_at) = (toYear(today()) - 1)) GROUP BY repo_name ORDER BY count() DESC LIMIT 100), last_day AS (SELECT repo_name, count() AS count_last_day, rowNumberInAllBlocks() + 1 AS position_last_day FROM github_events WHERE (repo_name IN (SELECT repo_name FROM top_repos)) AND (toDate(created_at) = (today() - 1)) GROUP BY repo_name ORDER BY count_last_day DESC), last_week AS (SELECT repo_name, count() AS count_last_week, rowNumberInAllBlocks() + 1 AS position_last_week FROM github_events WHERE (repo_name IN (SELECT repo_name FROM top_repos)) AND (toMonday(created_at) = (toMonday(today()) - toIntervalWeek(2))) GROUP BY repo_name ORDER BY count_last_week DESC), last_month AS (SELECT repo_name, count() AS count_last_month, rowNumberInAllBlocks() + 1 AS position_last_month FROM github_events__fuzz_0 WHERE ('deleted' = 4) AND in(repo_name) AND (toStartOfMonth(created_at) = (toStartOfMonth(today()) - toIntervalMonth(1))) GROUP BY repo_name ORDER BY count_last_month DESC) SELECT d.repo_name, COLUMNS(count) FROM last_day AS d INNER JOIN last_week AS w ON d.repo_name = w.repo_name INNER JOIN last_month AS m ON d.repo_name = m.repo_name format Null; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE github_events;
