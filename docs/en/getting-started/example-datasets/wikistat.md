---
sidebar_label: WikiStat
---

# WikiStat

The dataset contains 0.5 trillion records.

See the video from FOSDEM 2023: https://www.youtube.com/watch?v=JlcI2Vfz_uk

And the presentation: https://presentations.clickhouse.com/fosdem2023/

Data source: https://dumps.wikimedia.org/other/pageviews/

Getting the list of links:
``` shell
for i in {2015..2023}; do
  for j in {01..12}; do
    echo "${i}-${j}" >&2
    curl -sSL "https://dumps.wikimedia.org/other/pageviews/$i/$i-$j/" \
      | grep -oE 'pageviews-[0-9]+-[0-9]+\.gz'
  done
done | sort | uniq | tee links.txt
```

Downloading the data:
``` shell
sed -r 's!pageviews-([0-9]{4})([0-9]{2})[0-9]{2}-[0-9]+\.gz!https://dumps.wikimedia.org/other/pageviews/\1/\1-\2/\0!' \
  links.txt | xargs -P3 wget --continue
```

(it will take about 3 days)

Creating a table:

``` sql
CREATE TABLE wikistat
(
    time DateTime CODEC(Delta, ZSTD(3)),
    project LowCardinality(String),
    subproject LowCardinality(String),
    path String CODEC(ZSTD(3)),
    hits UInt64 CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (path, time);
```

Loading the data:

``` shell
clickhouse-local --query "
  WITH replaceRegexpOne(_path, '^.+pageviews-(\\d{4})(\\d{2})(\\d{2})-(\\d{2})(\\d{2})(\\d{2}).gz$', '\1-\2-\3 \4-\5-\6')::DateTime AS time, 
       extractGroups(line, '^([^ \\.]+)(\\.[^ ]+)? +([^ ]+) +(\\d+) +(\\d+)$') AS values
  SELECT 
    time, 
    values[1] AS project,
    values[2] AS subproject,
    values[3] AS path,
    (values[4])::UInt64 AS hits
  FROM file('pageviews*.gz', LineAsString)
  WHERE length(values) = 5 FORMAT Native
" | clickhouse-client --query "INSERT INTO wikistat FORMAT Native"
```

Or loading the cleaning data:

``` sql
INSERT INTO wikistat WITH
    parseDateTimeBestEffort(extract(_file, '^pageviews-([\\d\\-]+)\\.gz$')) AS time,
    splitByChar(' ', line) AS values,
    splitByChar('.', values[1]) AS projects
SELECT
    time,
    projects[1] AS project,
    projects[2] AS subproject,
    decodeURLComponent(values[2]) AS path,
    CAST(values[3], 'UInt64') AS hits
FROM s3(
    'https://clickhouse-public-datasets.s3.amazonaws.com/wikistat/original/pageviews*.gz',
    LineAsString)
WHERE length(values) >= 3
```
