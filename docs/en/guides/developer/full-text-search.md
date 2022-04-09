---
sidebar_label: Full-text Search with Quickwit
sidebar_position: 2
---

# Full-text search with ClickHouse and Quickwit

If you are using ClickHouse and you end up in a situation wishing you had full
text search abilities you have to know that it is possible to use a search
engine within ClickHouse. Quickwit is a distributed search engine. It is
designed from the ground up to offer cost-efficiency and high reliability on
large data sets and the good news is that Quickwit can be used within
ClickHouse. In this guide we are going to show you how you can add full text
search to ClickHouse.


## Installing ClickHouse

The  first step is to install Quickwit and ClickHouse if you don’t have them
installed already, follow the instruction below to install ClickHouse:


```
sudo apt-get install apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4

echo "deb https://repo.clickhouse.com/deb/stable/ main/" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

sudo service clickhouse-server start
```

## Installing Quickwit

Quickwit is relying on two external libraries to work correctly. You will need
to install them before installing Quickwit

```
# Quickwit depends on the following external libraries to work correctly
sudo apt-get -y update
sudo apt-get -y install libpq-dev libssl-dev
```

Once these two libraries are installed you can go ahead and install Quickwit:

```
curl -L https://install.quickwit.io | sh
# Quickwit detects the config from CLI args or the QW_CONFIG env variable.
# Let's set QW_CONFIG to the default config.
cd quickwit-v*/
export QW_CONFIG=./config/quickwit.yaml
```

You can test that Quickwit has been properly installed by running the following
command:


```
./quickwit --version
```

Now that both ClickHouse and Quickwit are installed and run all we have to do is
add some data to both of them.

## Indexing Data in QuickWit

The first thing we need to do is provide a data schema for the data we are going
to use. We are going to use a subset of the data provided by GitHub. You can
find the original data here, the dataset we are going to use has slightly been
modified in order to be more practical to use.


```
curl -o gh-archive-index-config.yaml https://datasets-documentation.s3.eu-west-3.amazonaws.com/full-text-search/gh-archive-index-config.yaml
./quickwit index create --index-config gh-archive-index-config.yaml
```

Now that the data schema is defined, let’s download and index some data in
Quickwit:

```
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/full-text-search/gh-archive-2021-12-text-only.json.gz
gunzip -c gh-archive-2021-12-text-only.json.gz | ./quickwit index ingest --index gh-archive
```

You can search through your data within Quickwit:

```
./quickwit index search --index gh-archive --query "clickhouse"
```

But we want to use it in conjunction with ClickHouse, so in order to do so, we
will need to create a searcher.

```
./quickwit service run searcher
```

This command will start an HTTP server with a REST API. We are now ready to
fetch some ids with the search stream endpoint. Let's start by streaming them on
a simple query and with a CSV output format.

```
curl "http://0.0.0.0:7280/api/v1/gh-archive/search/stream?query=clickhouse&outputFormat=csv&fastField=id"
```

In the remaining of this guide we will be using the ClickHouse binary output
format to speed up queries using ClickHouse.

## Adding Data to ClickHouse

First thing first, we need to connect to the ClickHouse database. Let’s use the
`clickhouse-client` to do it.

```
clickhouse-client –password <PASSWORD>
```

The first thing we need to do is to create a database:

```
CREATE DATABASE "github";
USE github;
```

Now we need to create the table that’s going to store our data:

```
CREATE TABLE github.github_events
(
    `id` UInt64,
    `event_type` Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    `actor_login` LowCardinality(String),
    `repo_name` LowCardinality(String),
    `created_at` Int64,
    `action` Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    `comment_id` UInt64,
    `body` String,
    `ref` LowCardinality(String),
    `number` UInt32,
    `title` String,
    `labels` Array(LowCardinality(String)),
    `additions` UInt32,
    `deletions` UInt32,
    `commit_id` String
)
ENGINE = MergeTree
ORDER BY (event_type, repo_name, created_at)
```

We are going to add some data to ClickHouse. It’s the same dataset as the one we
have indexed in Quickwit but this time it does not include text fields since
they are indexed already.


```
wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/full-text-search/gh-archive-2021-12.json.gz
gunzip -c gh-archive-2021-12.json.gz | clickhouse-client --query="INSERT INTO github.github_events FORMAT JSONEachRow" --password <PASSWORD>
```

Now that the data is in ClickHouse we can query them using the `clikchouse-client`.

```
SELECT
    repo_name,
    count() AS stars
FROM github.github_events
WHERE event_type = 'WatchEvent'
GROUP BY repo_name
ORDER BY stars DESC
LIMIT 5
```

## Full-text search within ClickHouse

So we have data both in ClickHouse and in QuickWit all there is to do now is to
query them! The url function allows you to fetch ids using the Quickwit search
stream:

```
SELECT count(*)
FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=clickhouse&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')
```

In this query above we are counting the number of ID returned by the ClickHouse
query executed in QuickWit. As you can see below, it’s returning the following:

```
┌─count()─┐
│   2012  │
└─────────┘

1 rows in set. Elapsed: 0.010 sec. Processed 2.01 thousand rows, 16.10 KB (210.35 thousand rows/s., 1.68 MB/s.)
```

We can search multiple tokens by separating them with a `+` :

```
SELECT count(*) FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=clickhouse+cloki&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')
```

In the query above we are searching for documents containing the words
`ClickHouse AND cloki`. Now we can tweak the query around to search for
`ClickHouse OR cloki`:

```
SELECT count(*)
FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=clickhouse+OR+cloki&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')
```

So the full text search is working, now let’s combine it with some `GROUP BY`
that would be done on the ClickHouse side. Here we want to know how many rows
match the words: `ClickHouse`, `cloki` or `quickwit` and in which GitHub
repository there located in.

```
SELECT
      count(*) AS count,
      repo_name AS repo
FROM github.github_events
WHERE id IN (
    SELECT id
    FROM url('http://127.0.0.1:7280/api/v1/gh-archive/search/stream?query=cloki+OR+clickhouse+OR+quickwit&fastField=id&outputFormat=clickHouseRowBinary', RowBinary, 'id UInt64')
)
GROUP BY repo ORDER BY count DESC
```

And as you can see below, it is fast:

```
┌─count─┬─repo──────────────────────────────────────────────┐
│   874 │ ClickHouse/ClickHouse                             │
│   112 │ traceon/ClickHouse                                │
│   112 │ quickwit-inc/quickwit                             │
│   110 │ PostHog/posthog                                   │
│    73 │ PostHog/charts-clickhouse                         │
│    64 │ datafuselabs/databend                             │
│    54 │ airbytehq/airbyte                                 │
│    53 │ ClickHouse/clickhouse-jdbc                        │
│    37 │ getsentry/snuba                                   │
│    37 │ PostHog/posthog.com                               │
…
…
│     1 │ antrea-io/antrea                                  │
│     1 │ python/typeshed                                   │
│     1 │ Sunt-ing/database-system-readings                 │
│     1 │ duckdb/duckdb                                     │
│     1 │ open-botech/ClickHouse                            │
└───────┴───────────────────────────────────────────────────┘

195 rows in set. Elapsed: 0.518 sec. Processed 45.43 million rows, 396.87 MB (87.77 million rows/s., 766.79 MB/s.)
```

The query is really fast, returning the result in 0.5 second.

## Conclusion

Using Quickwit within ClickHouse gives a lot of flexibility in how you work with
your data, especially when your data contains textual information and you need
to be able to search through them very quickly. You can find more information on
how to use Quickwit directly on their documentation. 
