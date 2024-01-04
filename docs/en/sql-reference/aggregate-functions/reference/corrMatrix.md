---
---

# corrMatrix

Syntax: `corrMatrix(x, y, z, ...)`

Used to compute correlation matrices. A correlation matrix is a table that contains the correlation coefficients between all possible pairs of variables in a dataset. It provides a summary of the relationships between different variables in the dataset and helps identify pairs of variables that are either strongly or negatively correlated.

This function was introduced in ClickHouse version 23.2 and it simplifies the computation of correlation matrices, which traditionally required complex or multiple queries in ClickHouse. Now, this calculation can be done with a single function call, making it easier and more efficient to analyze large datasets. 

Here is an example of how to use the `corrMatrix` function:

```sql
SELECT arrayMap(x -> round(x, 3), arrayJoin(corrMatrix(stars, issue_raised, follows, prs, forks, commit_comment_event, issues_commented))) AS correlation_matrix
FROM
(
    SELECT
        countIf(event_type = 'WatchEvent') AS stars,
        countIf(event_type = 'IssuesEvent') AS issue_raised,
        countIf(event_type = 'FollowEvent') AS follows,
        countIf(event_type = 'PullRequestEvent') AS prs,
        countIf(event_type = 'ForkEvent') AS forks,
        countIf(event_type = 'CommitCommentEvent') AS commit_comment_event,
        countIf(event_type = 'IssueCommentEvent') AS issues_commented
    FROM github_events
    GROUP BY repo_name
    HAVING stars > 100
)
```

This will output something like:

```response
┌─correlation_matrix──────────────────────┐
│ [1,0.298,0.003,0.22,0.552,0.063,0.291]  │
│ [0.298,1,0.035,0.376,0.223,0.104,0.557] │
│ [0.003,0.035,1,0.004,0.004,0.015,0.003] │
│ [0.22,0.376,0.004,1,0.277,0.151,0.685]  │
│ [0.552,0.223,0.004,0.277,1,0.057,0.262] │
│ [0.063,0.104,0.015,0.151,0.057,1,0.175] │
│ [0.291,0.557,0.003,0.685,0.262,0.175,1] │
└─────────────────────────────────────────┘

7 rows in set. Elapsed: 44.847 sec. Processed 5.72 billion rows, 51.60 GB (127.65 million rows/s., 1.15 GB/s.)
```
