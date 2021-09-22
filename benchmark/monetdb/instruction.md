Go to https://www.monetdb.org/

Dowload now.
Latest binary releases.
Ubuntu & Debian.

https://www.monetdb.org/downloads/deb/

Go to the server where you want to install MonetDB.
```
$ sudo mcedit /etc/apt/sources.list.d/monetdb.list
```
Write:
```
deb https://dev.monetdb.org/downloads/deb/ bionic monetdb
```

```
$ wget --output-document=- https://www.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -

$ sudo apt update
$ sudo apt install monetdb5-sql monetdb-client

$ sudo systemctl enable monetdbd
$ sudo systemctl start monetdbd
$ sudo usermod -a -G monetdb $USER
```

Logout and login back to your server.

Tutorial:
https://www.monetdb.org/Documentation/UserGuide/Tutorial

Creating the database:

```
$ sudo mkdir /opt/monetdb
$ sudo chmod 777 /opt/monetdb
$ monetdbd create /opt/monetdb

$ monetdbd start /opt/monetdb
cannot remove socket files
```

Now you have to stop MonetDB, copy the contents of `/var/monetdb5` to `/opt/monetdb` and replace the `/var/monetdb5` with symlink to `/opt/monetdb`. This is necessary, because I don't have free space in `/var` and creation of database in `/opt` did not succeed.

Start MonetDB again.

```
$ sudo systemctl start monetdbd
```

```
$ monetdb create test
created database in maintenance mode: test

$ monetdb release test
taken database out of maintenance mode: test
```

Run client:
```
$ mclient -u monetdb -d test
```

Type password: monetdb

```
CREATE TABLE hits
(
    "WatchID" BIGINT,
    "JavaEnable" TINYINT,
    "Title" TEXT,
    "GoodEvent" SMALLINT,
    "EventTime" TIMESTAMP,
    "EventDate" Date,
    "CounterID" INTEGER,
    "ClientIP" INTEGER,
    "RegionID" INTEGER,
    "UserID" BIGINT,
    "CounterClass" TINYINT,
    "OS" TINYINT,
    "UserAgent" TINYINT,
    "URL" TEXT,
    "Referer" TEXT,
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
    "FlashMinor2" TEXT,
    "NetMajor" TINYINT,
    "NetMinor" TINYINT,
    "UserAgentMajor" SMALLINT,
    "UserAgentMinor" TEXT,
    "CookieEnable" TINYINT,
    "JavascriptEnable" TINYINT,
    "IsMobile" TINYINT,
    "MobilePhone" TINYINT,
    "MobilePhoneModel" TEXT,
    "Params" TEXT,
    "IPNetworkID" INTEGER,
    "TraficSourceID" TINYINT,
    "SearchEngineID" SMALLINT,
    "SearchPhrase" TEXT,
    "AdvEngineID" TINYINT,
    "IsArtifical" TINYINT,
    "WindowClientWidth" SMALLINT,
    "WindowClientHeight" SMALLINT,
    "ClientTimeZone" SMALLINT,
    "ClientEventTime" TIMESTAMP,
    "SilverlightVersion1" TINYINT,
    "SilverlightVersion2" TINYINT,
    "SilverlightVersion3" INTEGER,
    "SilverlightVersion4" SMALLINT,
    "PageCharset" TEXT,
    "CodeVersion" INTEGER,
    "IsLink" TINYINT,
    "IsDownload" TINYINT,
    "IsNotBounce" TINYINT,
    "FUniqID" BIGINT,
    "OriginalURL" TEXT,
    "HID" INTEGER,
    "IsOldCounter" TINYINT,
    "IsEvent" TINYINT,
    "IsParameter" TINYINT,
    "DontCountHits" TINYINT,
    "WithHash" TINYINT,
    "HitColor" TEXT,
    "LocalEventTime" TIMESTAMP,
    "Age" TINYINT,
    "Sex" TINYINT,
    "Income" TINYINT,
    "Interests" SMALLINT,
    "Robotness" TINYINT,
    "RemoteIP" INTEGER,
    "WindowName" INTEGER,
    "OpenerName" INTEGER,
    "HistoryLength" SMALLINT,
    "BrowserLanguage" TEXT,
    "BrowserCountry" TEXT,
    "SocialNetwork" TEXT,
    "SocialAction" TEXT,
    "HTTPError" SMALLINT,
    "SendTiming" INTEGER,
    "DNSTiming" INTEGER,
    "ConnectTiming" INTEGER,
    "ResponseStartTiming" INTEGER,
    "ResponseEndTiming" INTEGER,
    "FetchTiming" INTEGER,
    "SocialSourceNetworkID" TINYINT,
    "SocialSourcePage" TEXT,
    "ParamPrice" BIGINT,
    "ParamOrderID" TEXT,
    "ParamCurrency" TEXT,
    "ParamCurrencyID" SMALLINT,
    "OpenstatServiceName" TEXT,
    "OpenstatCampaignID" TEXT,
    "OpenstatAdID" TEXT,
    "OpenstatSourceID" TEXT,
    "UTMSource" TEXT,
    "UTMMedium" TEXT,
    "UTMCampaign" TEXT,
    "UTMContent" TEXT,
    "UTMTerm" TEXT,
    "FromTag" TEXT,
    "HasGCLID" TINYINT,
    "RefererHash" BIGINT,
    "URLHash" BIGINT,
    "CLID" INTEGER
);
```

# How to prepare data

Download the 100 million rows dataset from here and insert into ClickHouse:
https://clickhouse.com/docs/en/getting-started/example-datasets/metrica/

Create the dataset from ClickHouse:

```
SELECT
    toInt64(WatchID) = -9223372036854775808 ? -9223372036854775807 : toInt64(WatchID),
    toInt8(JavaEnable) = -128 ? -127 : toInt8(JavaEnable),
    toValidUTF8(toString(Title)),
    toInt16(GoodEvent) = -32768 ? -32767 : toInt16(GoodEvent),
    EventTime,
    EventDate,
    toInt32(CounterID) = -2147483648 ? -2147483647 : toInt32(CounterID),
    toInt32(ClientIP) = -2147483648 ? -2147483647 : toInt32(ClientIP),
    toInt32(RegionID) = -2147483648 ? -2147483647 : toInt32(RegionID),
    toInt64(UserID) = -9223372036854775808 ? -9223372036854775807 : toInt64(UserID),
    toInt8(CounterClass) = -128 ? -127 : toInt8(CounterClass),
    toInt8(OS) = -128 ? -127 : toInt8(OS),
    toInt8(UserAgent) = -128 ? -127 : toInt8(UserAgent),
    toValidUTF8(toString(URL)),
    toValidUTF8(toString(Referer)),
    toInt8(Refresh) = -128 ? -127 : toInt8(Refresh),
    toInt16(RefererCategoryID) = -32768 ? -32767 : toInt16(RefererCategoryID),
    toInt32(RefererRegionID) = -2147483648 ? -2147483647 : toInt32(RefererRegionID),
    toInt16(URLCategoryID) = -32768 ? -32767 : toInt16(URLCategoryID),
    toInt32(URLRegionID) = -2147483648 ? -2147483647 : toInt32(URLRegionID),
    toInt16(ResolutionWidth) = -32768 ? -32767 : toInt16(ResolutionWidth),
    toInt16(ResolutionHeight) = -32768 ? -32767 : toInt16(ResolutionHeight),
    toInt8(ResolutionDepth) = -128 ? -127 : toInt8(ResolutionDepth),
    toInt8(FlashMajor) = -128 ? -127 : toInt8(FlashMajor),
    toInt8(FlashMinor) = -128 ? -127 : toInt8(FlashMinor),
    toValidUTF8(toString(FlashMinor2)),
    toInt8(NetMajor) = -128 ? -127 : toInt8(NetMajor),
    toInt8(NetMinor) = -128 ? -127 : toInt8(NetMinor),
    toInt16(UserAgentMajor) = -32768 ? -32767 : toInt16(UserAgentMajor),
    toValidUTF8(toString(UserAgentMinor)),
    toInt8(CookieEnable) = -128 ? -127 : toInt8(CookieEnable),
    toInt8(JavascriptEnable) = -128 ? -127 : toInt8(JavascriptEnable),
    toInt8(IsMobile) = -128 ? -127 : toInt8(IsMobile),
    toInt8(MobilePhone) = -128 ? -127 : toInt8(MobilePhone),
    toValidUTF8(toString(MobilePhoneModel)),
    toValidUTF8(toString(Params)),
    toInt32(IPNetworkID) = -2147483648 ? -2147483647 : toInt32(IPNetworkID),
    toInt8(TraficSourceID) = -128 ? -127 : toInt8(TraficSourceID),
    toInt16(SearchEngineID) = -32768 ? -32767 : toInt16(SearchEngineID),
    toValidUTF8(toString(SearchPhrase)),
    toInt8(AdvEngineID) = -128 ? -127 : toInt8(AdvEngineID),
    toInt8(IsArtifical) = -128 ? -127 : toInt8(IsArtifical),
    toInt16(WindowClientWidth) = -32768 ? -32767 : toInt16(WindowClientWidth),
    toInt16(WindowClientHeight) = -32768 ? -32767 : toInt16(WindowClientHeight),
    toInt16(ClientTimeZone) = -32768 ? -32767 : toInt16(ClientTimeZone),
    ClientEventTime,
    toInt8(SilverlightVersion1) = -128 ? -127 : toInt8(SilverlightVersion1),
    toInt8(SilverlightVersion2) = -128 ? -127 : toInt8(SilverlightVersion2),
    toInt32(SilverlightVersion3) = -2147483648 ? -2147483647 : toInt32(SilverlightVersion3),
    toInt16(SilverlightVersion4) = -32768 ? -32767 : toInt16(SilverlightVersion4),
    toValidUTF8(toString(PageCharset)),
    toInt32(CodeVersion) = -2147483648 ? -2147483647 : toInt32(CodeVersion),
    toInt8(IsLink) = -128 ? -127 : toInt8(IsLink),
    toInt8(IsDownload) = -128 ? -127 : toInt8(IsDownload),
    toInt8(IsNotBounce) = -128 ? -127 : toInt8(IsNotBounce),
    toInt64(FUniqID) = -9223372036854775808 ? -9223372036854775807 : toInt64(FUniqID),
    toValidUTF8(toString(OriginalURL)),
    toInt32(HID) = -2147483648 ? -2147483647 : toInt32(HID),
    toInt8(IsOldCounter) = -128 ? -127 : toInt8(IsOldCounter),
    toInt8(IsEvent) = -128 ? -127 : toInt8(IsEvent),
    toInt8(IsParameter) = -128 ? -127 : toInt8(IsParameter),
    toInt8(DontCountHits) = -128 ? -127 : toInt8(DontCountHits),
    toInt8(WithHash) = -128 ? -127 : toInt8(WithHash),
    toValidUTF8(toString(HitColor)),
    LocalEventTime,
    toInt8(Age) = -128 ? -127 : toInt8(Age),
    toInt8(Sex) = -128 ? -127 : toInt8(Sex),
    toInt8(Income) = -128 ? -127 : toInt8(Income),
    toInt16(Interests) = -32768 ? -32767 : toInt16(Interests),
    toInt8(Robotness) = -128 ? -127 : toInt8(Robotness),
    toInt32(RemoteIP) = -2147483648 ? -2147483647 : toInt32(RemoteIP),
    toInt32(WindowName) = -2147483648 ? -2147483647 : toInt32(WindowName),
    toInt32(OpenerName) = -2147483648 ? -2147483647 : toInt32(OpenerName),
    toInt16(HistoryLength) = -32768 ? -32767 : toInt16(HistoryLength),
    toValidUTF8(toString(BrowserLanguage)),
    toValidUTF8(toString(BrowserCountry)),
    toValidUTF8(toString(SocialNetwork)),
    toValidUTF8(toString(SocialAction)),
    toInt16(HTTPError) = -32768 ? -32767 : toInt16(HTTPError),
    toInt32(SendTiming) = -2147483648 ? -2147483647 : toInt32(SendTiming),
    toInt32(DNSTiming) = -2147483648 ? -2147483647 : toInt32(DNSTiming),
    toInt32(ConnectTiming) = -2147483648 ? -2147483647 : toInt32(ConnectTiming),
    toInt32(ResponseStartTiming) = -2147483648 ? -2147483647 : toInt32(ResponseStartTiming),
    toInt32(ResponseEndTiming) = -2147483648 ? -2147483647 : toInt32(ResponseEndTiming),
    toInt32(FetchTiming) = -2147483648 ? -2147483647 : toInt32(FetchTiming),
    toInt8(SocialSourceNetworkID) = -128 ? -127 : toInt8(SocialSourceNetworkID),
    toValidUTF8(toString(SocialSourcePage)),
    toInt64(ParamPrice) = -9223372036854775808 ? -9223372036854775807 : toInt64(ParamPrice),
    toValidUTF8(toString(ParamOrderID)),
    toValidUTF8(toString(ParamCurrency)),
    toInt16(ParamCurrencyID) = -32768 ? -32767 : toInt16(ParamCurrencyID),
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
    toInt8(HasGCLID) = -128 ? -127 : toInt8(HasGCLID),
    toInt64(RefererHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(RefererHash),
    toInt64(URLHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(URLHash),
    toInt32(CLID) = -2147483648 ? -2147483647 : toInt32(CLID)
FROM hits_100m_obfuscated
INTO OUTFILE '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv'
FORMAT TSV;
```

Note that MonetDB does not support the most negative numbers like -128. And we have to convert them by adding one.
It makes impossible to store the values of 64bit identifiers in BIGINT.
Maybe it's a trick to optimize NULLs?

Upload the data:

```
$ mclient -u monetdb -d test
```

Type password: monetdb

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t';
```

It takes 28 minutes 02 seconds on a server (Linux Ubuntu, Xeon E5-2560v2, 32 logical CPU, 128 GiB RAM, 8xHDD RAID-5, 40 TB).
It is roughly 60 000 rows per second.

Validate the data:

```
SELECT count(*) FROM hits;
```

Create an index:

```
CREATE INDEX hits_idx ON hits ("CounterID", "EventDate");
```

(it takes 5 seconds)

Run the benchmark:

```
./benchmark.sh | tee log.txt
```

You can find the log in `log.txt` file.

Postprocess data:

```
grep clk log.txt | tr -d '\r' | awk '{ if ($3 == "ms") { print $2 / 1000; } else if ($3 == "sec") { print $2 } else { print } }'
```

Then replace values with "min" (minutes) timing manually and save to `tmp.txt`.
Then process to JSON format:

```
awk '{
    if (i % 3 == 0) { a = $1 }
    else if (i % 3 == 1) { b = $1 }
    else if (i % 3 == 2) { c = $1; print "[" a ", " b ", " c "]," };
    ++i; }' < tmp.txt
```

And paste to `/website/benchmark/dbms/results/005_monetdb.json` in the repository.
