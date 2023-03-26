---
slug: /en/getting-started/example-datasets/youtube-dislikes
sidebar_label: YouTube Dislikes
description: A collection is dislikes of YouTube videos.
---

# YouTube dataset of dislikes

In November of 2021, YouTube removed the public ***dislike*** count from all of its videos. While creators can still see the number of dislikes, viewers can only see how many ***likes*** a video has received.

:::important
The dataset has over 4.5 billion records, so be careful just copying-and-pasting the commands below unless your resources can handle that type of volume. The commands below were executed on a **Development** instance of [ClickHouse Cloud](https://clickhouse.cloud).
:::

The data is in a JSON format and can be downloaded from [archive.org](https://archive.org/download/dislikes_youtube_2021_12_video_json_files). We have made this same data available in S3 so that it can be downloaded much more efficiently into a ClickHouse Cloud instance.

Here are the steps to create a table in ClickHouse Cloud and insert the data.

:::note
The steps below will easily work on a local install of ClickHouse too. The only change would be to use the `s3` function instead of `s3cluster` (unless you have a cluster configured - in which case change `default` to the name of your cluster).
:::

## Step-by-step instructions

1. Let's see what the data looks like. The `s3cluster` table function returns a table, so we can `DESCRIBE` the reult:

```sql
DESCRIBE s3Cluster('default', 'https://clickhouse-public-datasets.s3.amazonaws.com/youtube/original/files/*.zst', 'JSONLines');
```

ClickHouse infers the following schema from the JSON file:

```response
┌─name────────────────┬─type─────────────────────────────────┐
│ id                  │ Nullable(String)                     │
│ fetch_date          │ Nullable(Int64)                      │
│ upload_date         │ Nullable(String)                     │
│ title               │ Nullable(String)                     │
│ uploader_id         │ Nullable(String)                     │
│ uploader            │ Nullable(String)                     │
│ uploader_sub_count  │ Nullable(Int64)                      │
│ is_age_limit        │ Nullable(Bool)                       │
│ view_count          │ Nullable(Int64)                      │
│ like_count          │ Nullable(Int64)                      │
│ dislike_count       │ Nullable(Int64)                      │
│ is_crawlable        │ Nullable(Bool)                       │
│ is_live_content     │ Nullable(Bool)                       │
│ has_subtitles       │ Nullable(Bool)                       │
│ is_ads_enabled      │ Nullable(Bool)                       │
│ is_comments_enabled │ Nullable(Bool)                       │
│ description         │ Nullable(String)                     │
│ rich_metadata       │ Array(Map(String, Nullable(String))) │
│ super_titles        │ Array(Map(String, Nullable(String))) │
│ uploader_badges     │ Nullable(String)                     │
│ video_badges        │ Nullable(String)                     │
└─────────────────────┴──────────────────────────────────────┘
```

2. Based on the inferred schema, we cleaned up the data types and added a primary key. Define the following table:

```sql
CREATE TABLE youtube
(
    `id` String,
    `fetch_date` DateTime,
    `upload_date` String,
    `title` String,
    `uploader_id` String,
    `uploader` String,
    `uploader_sub_count` Int64,
    `is_age_limit` Bool,
    `view_count` Int64,
    `like_count` Int64,
    `dislike_count` Int64,
    `is_crawlable` Bool,
    `has_subtitles` Bool,
    `is_ads_enabled` Bool,
    `is_comments_enabled` Bool,
    `description` String,
    `rich_metadata` Array(Map(String, String)),
    `super_titles` Array(Map(String, String)),
    `uploader_badges` String,
    `video_badges` String
)
ENGINE = MergeTree
ORDER BY (upload_date, uploader);
```

3. The following command streams the records from the S3 files into the `youtube` table.

:::important
This inserts a lot of data - 4.65 billion rows. If you do not want the entire dataset, simply add a `LIMIT` clause with the desired number of rows.
:::

```sql
INSERT INTO youtube
SETTINGS input_format_null_as_default = 1
SELECT
    id,
    parseDateTimeBestEffortUS(toString(fetch_date)) AS fetch_date,
    upload_date,
    ifNull(title, '') AS title,
    uploader_id,
    ifNull(uploader, '') AS uploader,
    uploader_sub_count,
    is_age_limit,
    view_count,
    like_count,
    dislike_count,
    is_crawlable,
    has_subtitles,
    is_ads_enabled,
    is_comments_enabled,
    ifNull(description, '') AS description,
    rich_metadata,
    super_titles,
    ifNull(uploader_badges, '') AS uploader_badges,
    ifNull(video_badges, '') AS video_badges
FROM s3Cluster('default','https://clickhouse-public-datasets.s3.amazonaws.com/youtube/original/files/*.zst', 'JSONLines');
```

4. Open a new tab in the SQL Console of ClickHouse Cloud (or a new `clickhouse-client` window) and watch the count increase:

```sql
select formatReadableQuantity(count()) from youtube;
```

5. It will take a while to insert 4.56B rows, depending on your server resources. Once the data is inserted, go ahead and count the number of dislikes of your favorite videos or channels. Let's see how many videos were uploaded by ClickHouse:

```sql
SELECT *
FROM youtube
WHERE uploader ILIKE '%ClickHouse%';
```

6. Here is a search for videos with **ClickHouse** in the `title` or `description` fields:

```sql
SELECT
    view_count,
    like_count,
    dislike_count,
    concat('https://youtu.be/', id) AS url,
    title
FROM youtube
WHERE (title ILIKE '%ClickHouse%') OR (description ILIKE '%ClickHouse%')
ORDER BY
    like_count DESC,
    view_count DESC;
```

