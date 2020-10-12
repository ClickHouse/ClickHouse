# Instruction to run benchmark for OmniSci on web-analytics dataset

OmniSci (former name "MapD") is open-source (open-core) in-memory analytical DBMS with support for GPU processing.
It can run on CPU without GPU as well. It can show competitive performance on simple queries (like - simple aggregation on a single column).

# How to install

https://docs.omnisci.com/installation-and-configuration/installation/installing-on-ubuntu

# Caveats

- Dataset (at least needed columns) must fit in memory.
- It does not support data compression (only dictionary encoding for strings).
- First query execution is very slow because uncompressed data is read from disk.
- It does not support index for quick range queries.
- It does not support NOT NULL for data types.
- It does not support BLOB.
- No support for UNSIGNED data type (it's Ok according to SQL standard).
- Lack of string processing functions.
- Strings are limited to 32767 bytes.
- GROUP BY on text data type is supported only if it has dictionary encoding.
`Exception: Cannot group by string columns which are not dictionary encoded`
- Some aggregate functions are not supported for strings at all.
`Aggregate on TEXT is not supported yet.`
- Sometimes I hit a bug when query is run in infinite loop and does not finish (after retry it's finished successfully).
- One query executed in hours even with retries.
- Sorting is slow and disabled with default settings for large resultsets.
`Exception: Sorting the result would be too slow`
`Cast from dictionary-encoded string to none-encoded would be slow`
- There is approximate count distinct function but the precision is not documented.

To enable sorting of large resultsets, see:
https://stackoverflow.com/questions/62977734/omnissci-sorting-the-result-would-be-too-slow

The list of known issues is here:
https://github.com/omnisci/omniscidb/issues?q=is%3Aissue+author%3Aalexey-milovidov

# How to prepare data

Download the 100 million rows dataset from here and insert into ClickHouse:
https://clickhouse.tech/docs/en/getting-started/example-datasets/metrica/

Convert the CREATE TABLE query:

```
clickhouse-client --query "SHOW CREATE TABLE hits_100m" --format TSVRaw |
    tr '`' '"' |
    sed -r -e '
        s/U?Int64/BIGINT/;
        s/U?Int32/INTEGER/;
        s/U?Int16/SMALLINT/;
        s/U?Int8/TINYINT/;
        s/DateTime/TIMESTAMP ENCODING FIXED(32)/;
        s/ Date/ DATE ENCODING DAYS(16)/;
        s/FixedString\(2\)/TEXT ENCODING DICT(16)/;
        s/FixedString\(3\)/TEXT ENCODING DICT/;
        s/FixedString\(\d+\)/TEXT ENCODING DICT/;
        s/String/TEXT ENCODING DICT/;'
```
And cut `ENGINE` part.

The resulting CREATE TABLE query:
```
CREATE TABLE hits
(
    "WatchID" BIGINT,
    "JavaEnable" TINYINT,
    "Title" TEXT ENCODING DICT,
    "GoodEvent" SMALLINT,
    "EventTime" TIMESTAMP ENCODING FIXED(32),
    "EventDate" ENCODING DAYS(16) Date,
    "CounterID" INTEGER,
    "ClientIP" INTEGER,
    "RegionID" INTEGER,
    "UserID" BIGINT,
    "CounterClass" TINYINT,
    "OS" TINYINT,
    "UserAgent" TINYINT,
    "URL" TEXT ENCODING DICT,
    "Referer" TEXT ENCODING DICT,
    "Refresh" TINYINT,
    "RefererCategoryID" SMALLINT,
    "RefererRegionID" INTEGER,
    "URLCategoryID" SMALLINT,
    "URLRegionID" INTEGER,
    "ResolutionWidth" SMALLINT,
    "ResolutionHeight" SMALLINT,
    "ResolutionDepth" TINYINT,
    "FlashMajor" TINYINT,
    "FlashMinor" TINYINT,
    "FlashMinor2" TEXT ENCODING DICT,
    "NetMajor" TINYINT,
    "NetMinor" TINYINT,
    "UserAgentMajor" SMALLINT,
    "UserAgentMinor" TEXT ENCODING DICT(16),
    "CookieEnable" TINYINT,
    "JavascriptEnable" TINYINT,
    "IsMobile" TINYINT,
    "MobilePhone" TINYINT,
    "MobilePhoneModel" TEXT ENCODING DICT,
    "Params" TEXT ENCODING DICT,
    "IPNetworkID" INTEGER,
    "TraficSourceID" TINYINT,
    "SearchEngineID" SMALLINT,
    "SearchPhrase" TEXT ENCODING DICT,
    "AdvEngineID" TINYINT,
    "IsArtifical" TINYINT,
    "WindowClientWidth" SMALLINT,
    "WindowClientHeight" SMALLINT,
    "ClientTimeZone" SMALLINT,
    "ClientEventTime" TIMESTAMP ENCODING FIXED(32),
    "SilverlightVersion1" TINYINT,
    "SilverlightVersion2" TINYINT,
    "SilverlightVersion3" INTEGER,
    "SilverlightVersion4" SMALLINT,
    "PageCharset" TEXT ENCODING DICT,
    "CodeVersion" INTEGER,
    "IsLink" TINYINT,
    "IsDownload" TINYINT,
    "IsNotBounce" TINYINT,
    "FUniqID" BIGINT,
    "OriginalURL" TEXT ENCODING DICT,
    "HID" INTEGER,
    "IsOldCounter" TINYINT,
    "IsEvent" TINYINT,
    "IsParameter" TINYINT,
    "DontCountHits" TINYINT,
    "WithHash" TINYINT,
    "HitColor" TEXT ENCODING DICT(8),
    "LocalEventTime" TIMESTAMP ENCODING FIXED(32),
    "Age" TINYINT,
    "Sex" TINYINT,
    "Income" TINYINT,
    "Interests" SMALLINT,
    "Robotness" TINYINT,
    "RemoteIP" INTEGER,
    "WindowName" INTEGER,
    "OpenerName" INTEGER,
    "HistoryLength" SMALLINT,
    "BrowserLanguage" TEXT ENCODING DICT(16),
    "BrowserCountry" TEXT ENCODING DICT(16),
    "SocialNetwork" TEXT ENCODING DICT,
    "SocialAction" TEXT ENCODING DICT,
    "HTTPError" SMALLINT,
    "SendTiming" INTEGER,
    "DNSTiming" INTEGER,
    "ConnectTiming" INTEGER,
    "ResponseStartTiming" INTEGER,
    "ResponseEndTiming" INTEGER,
    "FetchTiming" INTEGER,
    "SocialSourceNetworkID" TINYINT,
    "SocialSourcePage" TEXT ENCODING DICT,
    "ParamPrice" BIGINT,
    "ParamOrderID" TEXT ENCODING DICT,
    "ParamCurrency" TEXT ENCODING DICT,
    "ParamCurrencyID" SMALLINT,
    "OpenstatServiceName" TEXT ENCODING DICT,
    "OpenstatCampaignID" TEXT ENCODING DICT,
    "OpenstatAdID" TEXT ENCODING DICT,
    "OpenstatSourceID" TEXT ENCODING DICT,
    "UTMSource" TEXT ENCODING DICT,
    "UTMMedium" TEXT ENCODING DICT,
    "UTMCampaign" TEXT ENCODING DICT,
    "UTMContent" TEXT ENCODING DICT,
    "UTMTerm" TEXT ENCODING DICT,
    "FromTag" TEXT ENCODING DICT,
    "HasGCLID" TINYINT,
    "RefererHash" BIGINT,
    "URLHash" BIGINT,
    "CLID" INTEGER
);
```

Convert the dataset, prepare the list of fields for SELECT:

```
clickhouse-client --query "SHOW CREATE TABLE hits_100m" --format TSVRaw |
    tr '`' '"' |
    sed -r -e '
        s/"(\w+)" U?Int([0-9]+)/toInt\2(\1)/;
        s/"(\w+)" (Fixed)?String(\([0-9]+\))?/toValidUTF8(toString(\1))/;
        s/"(\w+)" \w+/\1/'
```

The resulting SELECT query for data preparation:

```
SELECT
    toInt64(WatchID),
    toInt8(JavaEnable),
    toValidUTF8(toString(Title)),
    toInt16(GoodEvent),
    EventTime,
    EventDate,
    toInt32(CounterID),
    toInt32(ClientIP),
    toInt32(RegionID),
    toInt64(UserID),
    toInt8(CounterClass),
    toInt8(OS),
    toInt8(UserAgent),
    toValidUTF8(toString(URL)),
    toValidUTF8(toString(Referer)),
    toInt8(Refresh),
    toInt16(RefererCategoryID),
    toInt32(RefererRegionID),
    toInt16(URLCategoryID),
    toInt32(URLRegionID),
    toInt16(ResolutionWidth),
    toInt16(ResolutionHeight),
    toInt8(ResolutionDepth),
    toInt8(FlashMajor),
    toInt8(FlashMinor),
    toValidUTF8(toString(FlashMinor2)),
    toInt8(NetMajor),
    toInt8(NetMinor),
    toInt16(UserAgentMajor),
    toValidUTF8(toString(UserAgentMinor)),
    toInt8(CookieEnable),
    toInt8(JavascriptEnable),
    toInt8(IsMobile),
    toInt8(MobilePhone),
    toValidUTF8(toString(MobilePhoneModel)),
    toValidUTF8(toString(Params)),
    toInt32(IPNetworkID),
    toInt8(TraficSourceID),
    toInt16(SearchEngineID),
    toValidUTF8(toString(SearchPhrase)),
    toInt8(AdvEngineID),
    toInt8(IsArtifical),
    toInt16(WindowClientWidth),
    toInt16(WindowClientHeight),
    toInt16(ClientTimeZone),
    ClientEventTime,
    toInt8(SilverlightVersion1),
    toInt8(SilverlightVersion2),
    toInt32(SilverlightVersion3),
    toInt16(SilverlightVersion4),
    toValidUTF8(toString(PageCharset)),
    toInt32(CodeVersion),
    toInt8(IsLink),
    toInt8(IsDownload),
    toInt8(IsNotBounce),
    toInt64(FUniqID),
    toValidUTF8(toString(OriginalURL)),
    toInt32(HID),
    toInt8(IsOldCounter),
    toInt8(IsEvent),
    toInt8(IsParameter),
    toInt8(DontCountHits),
    toInt8(WithHash),
    toValidUTF8(toString(HitColor)),
    LocalEventTime,
    toInt8(Age),
    toInt8(Sex),
    toInt8(Income),
    toInt16(Interests),
    toInt8(Robotness),
    toInt32(RemoteIP),
    toInt32(WindowName),
    toInt32(OpenerName),
    toInt16(HistoryLength),
    toValidUTF8(toString(BrowserLanguage)),
    toValidUTF8(toString(BrowserCountry)),
    toValidUTF8(toString(SocialNetwork)),
    toValidUTF8(toString(SocialAction)),
    toInt16(HTTPError),
    toInt32(SendTiming),
    toInt32(DNSTiming),
    toInt32(ConnectTiming),
    toInt32(ResponseStartTiming),
    toInt32(ResponseEndTiming),
    toInt32(FetchTiming),
    toInt8(SocialSourceNetworkID),
    toValidUTF8(toString(SocialSourcePage)),
    toInt64(ParamPrice),
    toValidUTF8(toString(ParamOrderID)),
    toValidUTF8(toString(ParamCurrency)),
    toInt16(ParamCurrencyID),
    toValidUTF8(toString(OpenstatServiceName)),
    toValidUTF8(toString(OpenstatCampaignID)),
    toValidUTF8(toString(OpenstatAdID)),
    toValidUTF8(toString(OpenstatSourceID)),
    toValidUTF8(toString(UTMSource)),
    toValidUTF8(toString(UTMMedium)),
    toValidUTF8(toString(UTMCampaign)),
    toValidUTF8(toString(UTMContent)),
    toValidUTF8(toString(UTMTerm)),
    toValidUTF8(toString(FromTag)),
    toInt8(HasGCLID),
    toInt64(RefererHash),
    toInt64(URLHash),
    toInt32(CLID)
FROM hits_100m_obfuscated
INTO OUTFILE '/home/milovidov/example_datasets/hits_100m_obfuscated.csv'
FORMAT CSV;
```

Upload data to OmniSci:
```
/opt/omnisci/bin/omnisql -t -p HyperInteractive
```
Run CREATE TABLE statement, then run:
```
COPY hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated.csv' WITH (HEADER = 'false');
```

Data loading took
```
336639 ms
```
on a server (Linux Ubuntu, Xeon E5-2560v2, 32 logical CPU, 128 GiB RAM, 8xHDD RAID-5, 40 TB).

Run benchmark:

```
./benchmark.sh
```

Prepare the result to paste into JSON:

```
grep -oP 'Total time: \d+' log.txt |
    grep -oP '\d+' |
    awk '{
        if (i % 3 == 0) { a = $1 }
        else if (i % 3 == 1) { b = $1 }
        else if (i % 3 == 2) { c = $1; print "[" a / 1000 ", " b / 1000 ", " c / 1000 "]," };
        ++i; }'
```

And fill out `[null, null, null]` for missing runs.
