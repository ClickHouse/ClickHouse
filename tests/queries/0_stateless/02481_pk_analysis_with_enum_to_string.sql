CREATE TABLE gen
(
   repo_name String,
   event_type Enum8('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
   actor_login String,
   created_at DateTime,
   action Enum8('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
   number UInt32,
   merged_at DateTime
)
ENGINE = GenerateRandom;

CREATE TABLE github_events AS gen ENGINE=MergeTree ORDER BY (event_type, repo_name, created_at) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO github_events SELECT * FROM gen LIMIT 100000;

INSERT INTO github_events VALUES ('apache/pulsar','PullRequestEvent','hangc0276','2021-01-22 06:58:03','opened',9276,'1970-01-01 00:00:00') ('apache/pulsar','PullRequestEvent','hangc0276','2021-01-25 02:38:07','closed',9276,'1970-01-01 00:00:00') ('apache/pulsar','PullRequestEvent','hangc0276','2021-01-25 02:38:09','reopened',9276,'1970-01-01 00:00:00') ('apache/pulsar','PullRequestEvent','hangc0276','2021-04-22 06:05:09','closed',9276,'2021-04-22 06:05:08') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-23 00:32:09','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-23 02:52:11','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-24 03:02:31','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-25 02:16:42','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-26 06:52:42','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-27 01:10:33','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-01-29 02:11:41','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-02-02 07:35:40','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-02-03 00:44:26','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','IssueCommentEvent','hangc0276','2021-02-03 02:14:26','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','PullRequestReviewEvent','codelipenghui','2021-03-29 14:31:25','created',9276,'1970-01-01 00:00:00') ('apache/pulsar','PullRequestReviewEvent','eolivelli','2021-03-29 16:34:02','created',9276,'1970-01-01 00:00:00');

OPTIMIZE TABLE github_events FINAL;

SELECT count()
FROM github_events
WHERE (repo_name = 'apache/pulsar') AND (toString(event_type) IN ('PullRequestEvent', 'PullRequestReviewCommentEvent', 'PullRequestReviewEvent', 'IssueCommentEvent')) AND (actor_login NOT IN ('github-actions[bot]', 'codecov-commenter')) AND (number = 9276);
