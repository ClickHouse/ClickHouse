Go to https://www.monetdb.org/

The graphical design of the website is a bit old-fashioned but I do not afraid.

Download now.
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

Don't know what it's doing but I hope it's Ok to ignore.

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
$ mclient -u monetdb -d test
password:
Welcome to mclient, the MonetDB/SQL interactive terminal (Jun2020-SP1)
Database: MonetDB v11.37.11 (Jun2020-SP1), 'mapi:monetdb://mtlog-perftest03j:50000/test'
FOLLOW US on https://twitter.com/MonetDB or https://github.com/MonetDB/MonetDB
Type \q to quit, \? for a list of available commands
auto commit mode: on
sql>SELECT 1
more>;
+------+
| %2   |
+======+
|    1 |
+------+
1 tuple
```

Yes, it works.
The only downside is the lack of whitespace after `sql>`.

Upload the dataset.

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
    "UserAgentMinor" TEXT(16),
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
    "HitColor" TEXT(8),
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
    "BrowserLanguage" TEXT(16),
    "BrowserCountry" TEXT(16),
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

operation successful
```

```
sql>SELECT * FROM hits;
+---------+------------+-------+-----------+-----------+-----------+-----------+----------+----------+--------+--------------+----+-----------+-----+---------+---------+-------------------+
| WatchID | JavaEnable | Title | GoodEvent | EventTime | EventDate | CounterID | ClientIP | RegionID | UserID | CounterClass | OS | UserAgent | URL | Referer | Refresh | RefererCategoryID |>
+=========+============+=======+===========+===========+===========+===========+==========+==========+========+==============+====+===========+=====+=========+=========+===================+
+---------+------------+-------+-----------+-----------+-----------+-----------+----------+----------+--------+--------------+----+-----------+-----+---------+---------+-------------------+
0 tuples !88 columns dropped!
note: to disable dropping columns and/or truncating fields use \w-1
```

Perfect.

https://www.monetdb.org/Documentation/Reference/MonetDBClientApplications/mclient - broken link on page https://www.monetdb.org/Documentation/ServerAdministration/QueryTiming


COPY command: https://www.monetdb.org/Documentation/SQLreference/SQLSyntaxOverview#COPY_INTO_FROM

`COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated.csv' USING DELIMITERS ',', '\n', '"';`

```
sql>COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated.csv' USING DELIMITERS ',', '\n', '"';
Failed to import table 'hits', line 55390 field Robotness 'tinyint' expected in '-128'
```

TINYINT - 8 bit signed integer between -127 and 127
The smallest negative number is not supported by any of the types.

It makes impossible to store the values of 64bit identifiers in BIGINT.

Maybe it's a trick to optimize NULLs?

Let's just cheat and add one to all the most negative numbers while exporting dataset from ClickHouse...

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
INTO OUTFILE '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv'
FORMAT CSV;
```

Try №2.

`COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';`


```
sql>COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
Failed to import table 'hits', line 1: column 106: Leftover data '1526320043,139,7122783580357023164,1,44,2,"http://smeshariki.ru/a-albumshowtopic/8940180","http://video.yandex.ru/yandex.ru/site=&airbag=&srt=0&fu=0",0,0,20,0,22,1917,879,37,15,13,"800",0,0,10,"sO",1,1,0,0,"","",3626245,-1,0,"",0,0,746,459,135,"2013-07-21 15:14:16",0,0,0,0,"windows",1,0,0,0,8675577400349020325,"",1034597214,0,0,0,0,0,"5","2013-07-21 11:14:27",31,1,2,3557,5,1782490839,-1,-1,-1,"S0","�
                                                                 "
```

Looks like it does not support newlines inside string literals.

Let's dig into https://www.monetdb.org/Documentation/ServerAdministration/LoadingBulkData/CSVBulkLoads

First, it's better to specify the number of records:
`COPY 100000000 RECORDS INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';`

> Quote characters in quoted fields may be escaped with a backslash.

Ok, then it's TSV, not CSV. Let's create TSV dump...


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

MonetDB client lacks history.

`mclient -u monetdb -d test --timer=clock`

`COPY 100000000 RECORDS INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t', '\n', '';`

```
sql>COPY 100000000 RECORDS INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t', '\n', '';
Failed to import table 'hits',
clk: 1.200 sec
```

Now it gives incomprehensible error...
Looks like it because of 100000000 RECORDS.
Let's try without it.

`COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t', '\n', '';`

Ok, it appeared to work...

`top -d0.5`

`mserver5` consumes about 1 CPU core but with strange pauses.

```
sql>COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t', '\n', '';
Failed to import table 'hits',
clk: 2:31 min
```

It does not work and there is no explanation available.

When I type Ctrl+D in CLI, it does not output a line feed into terminal.

Let's google it.
https://www.monetdb.org/pipermail/users-list/2013-November/007014.html

Probably it because of no quoting for strings.
Let's create a dump with `|` as a separator, `"` as string quote and C-style escaping.
But it's impossible to create dump in that format in ClickHouse.

Let's consider using binary format...

Ok, before we consider binary format, maybe we need to write character literals as E'\t' instead of '\t'?

`mclient` does not have an option to specify password in command line, it's annoying.
PS. I found how to solve it here: https://www.monetdb.org/Documentation/ServerAdministration/ServerSetupAndConfiguration

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS E'\t', E'\n', E'';
```

It does not work either:

```
sql>COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS E'\t', E'\n', E'';
Failed to import table 'hits', Failed to extend the BAT, perhaps disk full
clk: 1:17 min
```

Let's try binary import. But it would not work:

> For variable length strings, the file must have one C-based string value per line, terminated by a newline, and it is processed without escape character conversion. Fixed length strings are handled the same way. MonetDB assumes that all files are aligned, i.e. the i-th value in each file corresponds to the i-th record in the table.

According to the docs, there is no way to import strings with line feed characters.

BTW, the favicon of the MonetDB website makes an impression that the web page is constantly loading (it looks like a spinner).

Let's cheat again and replace all line feeds in strings to whitespaces and all double quotes to single quotes.

```
SELECT
    toInt64(WatchID) = -9223372036854775808 ? -9223372036854775807 : toInt64(WatchID),
    toInt8(JavaEnable) = -128 ? -127 : toInt8(JavaEnable),
    replaceAll(replaceAll(toValidUTF8(toString(Title)), '\n', ' '), '"', '\''),
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
    replaceAll(replaceAll(toValidUTF8(toString(URL)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(Referer)), '\n', ' '), '"', '\''),
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
    replaceAll(replaceAll(toValidUTF8(toString(FlashMinor2)), '\n', ' '), '"', '\''),
    toInt8(NetMajor) = -128 ? -127 : toInt8(NetMajor),
    toInt8(NetMinor) = -128 ? -127 : toInt8(NetMinor),
    toInt16(UserAgentMajor) = -32768 ? -32767 : toInt16(UserAgentMajor),
    replaceAll(replaceAll(toValidUTF8(toString(UserAgentMinor)), '\n', ' '), '"', '\''),
    toInt8(CookieEnable) = -128 ? -127 : toInt8(CookieEnable),
    toInt8(JavascriptEnable) = -128 ? -127 : toInt8(JavascriptEnable),
    toInt8(IsMobile) = -128 ? -127 : toInt8(IsMobile),
    toInt8(MobilePhone) = -128 ? -127 : toInt8(MobilePhone),
    replaceAll(replaceAll(toValidUTF8(toString(MobilePhoneModel)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(Params)), '\n', ' '), '"', '\''),
    toInt32(IPNetworkID) = -2147483648 ? -2147483647 : toInt32(IPNetworkID),
    toInt8(TraficSourceID) = -128 ? -127 : toInt8(TraficSourceID),
    toInt16(SearchEngineID) = -32768 ? -32767 : toInt16(SearchEngineID),
    replaceAll(replaceAll(toValidUTF8(toString(SearchPhrase)), '\n', ' '), '"', '\''),
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
    replaceAll(replaceAll(toValidUTF8(toString(PageCharset)), '\n', ' '), '"', '\''),
    toInt32(CodeVersion) = -2147483648 ? -2147483647 : toInt32(CodeVersion),
    toInt8(IsLink) = -128 ? -127 : toInt8(IsLink),
    toInt8(IsDownload) = -128 ? -127 : toInt8(IsDownload),
    toInt8(IsNotBounce) = -128 ? -127 : toInt8(IsNotBounce),
    toInt64(FUniqID) = -9223372036854775808 ? -9223372036854775807 : toInt64(FUniqID),
    replaceAll(replaceAll(toValidUTF8(toString(OriginalURL)), '\n', ' '), '"', '\''),
    toInt32(HID) = -2147483648 ? -2147483647 : toInt32(HID),
    toInt8(IsOldCounter) = -128 ? -127 : toInt8(IsOldCounter),
    toInt8(IsEvent) = -128 ? -127 : toInt8(IsEvent),
    toInt8(IsParameter) = -128 ? -127 : toInt8(IsParameter),
    toInt8(DontCountHits) = -128 ? -127 : toInt8(DontCountHits),
    toInt8(WithHash) = -128 ? -127 : toInt8(WithHash),
    replaceAll(replaceAll(toValidUTF8(toString(HitColor)), '\n', ' '), '"', '\''),
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
    replaceAll(replaceAll(toValidUTF8(toString(BrowserLanguage)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(BrowserCountry)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(SocialNetwork)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(SocialAction)), '\n', ' '), '"', '\''),
    toInt16(HTTPError) = -32768 ? -32767 : toInt16(HTTPError),
    toInt32(SendTiming) = -2147483648 ? -2147483647 : toInt32(SendTiming),
    toInt32(DNSTiming) = -2147483648 ? -2147483647 : toInt32(DNSTiming),
    toInt32(ConnectTiming) = -2147483648 ? -2147483647 : toInt32(ConnectTiming),
    toInt32(ResponseStartTiming) = -2147483648 ? -2147483647 : toInt32(ResponseStartTiming),
    toInt32(ResponseEndTiming) = -2147483648 ? -2147483647 : toInt32(ResponseEndTiming),
    toInt32(FetchTiming) = -2147483648 ? -2147483647 : toInt32(FetchTiming),
    toInt8(SocialSourceNetworkID) = -128 ? -127 : toInt8(SocialSourceNetworkID),
    replaceAll(replaceAll(toValidUTF8(toString(SocialSourcePage)), '\n', ' '), '"', '\''),
    toInt64(ParamPrice) = -9223372036854775808 ? -9223372036854775807 : toInt64(ParamPrice),
    replaceAll(replaceAll(toValidUTF8(toString(ParamOrderID)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(ParamCurrency)), '\n', ' '), '"', '\''),
    toInt16(ParamCurrencyID) = -32768 ? -32767 : toInt16(ParamCurrencyID),
    replaceAll(replaceAll(toValidUTF8(toString(OpenstatServiceName)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(OpenstatCampaignID)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(OpenstatAdID)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(OpenstatSourceID)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(UTMSource)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(UTMMedium)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(UTMCampaign)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(UTMContent)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(UTMTerm)), '\n', ' '), '"', '\''),
    replaceAll(replaceAll(toValidUTF8(toString(FromTag)), '\n', ' '), '"', '\''),
    toInt8(HasGCLID) = -128 ? -127 : toInt8(HasGCLID),
    toInt64(RefererHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(RefererHash),
    toInt64(URLHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(URLHash),
    toInt32(CLID) = -2147483648 ? -2147483647 : toInt32(CLID)
FROM hits_100m_obfuscated
INTO OUTFILE '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv'
FORMAT CSV;
```

Another try:

```
COPY 100000000 RECORDS INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
```

Does not work.

```
Failed to import table 'hits',
clk: 1.091 sec
```

Another try:

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
```

Does not work.

```
Failed to import table 'hits', line 79128: record too long
clk: 1.194 sec
```

Ok, the error message becomes more meaningful. Looks like MonetDB does not support long TEXT.
Let's continue reading docs...

> CLOB | TEXT | STRING | CHARACTER LARGE OBJECT: UTF-8 character string with unbounded length

It must be unbounded!
But maybe there is global limit on record length...

https://www.monetdb.org/search/node?keys=record+length
https://www.monetdb.org/search/node?keys=record+too+long

The docs search did not give an answer. Let's search in the internet...

https://www.monetdb.org/pipermail/users-list/2017-August/009930.html

It's unclear what is the record numbering scheme - from 1 or from 0.
But when I took at the records with

```
head -n79128 hits_100m_obfuscated_monetdb.csv | tail -n1
head -n79129 hits_100m_obfuscated_monetdb.csv | tail -n1
```

they don't look too long.

Ok, let's try to load data with "best effort" mode that MonetDB offers.

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"' BEST EFFORT;
```

But it loaded just 79127 rows. That's not what I need.

```
79127 affected rows
clk: 1.684 sec
```

The TRUNCATE query works:

```
TRUNCATE TABLE hits;
```

Let's check if the record 79127 is really any longer than other records.

Let's remove all length like `TEXT(16)` from CREATE TABLE statement...

```
DROP TABLE hits;

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

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
```

Unfortunately it did not help.

```
Failed to import table 'hits', line 79128: record too long
clk: 1.224 sec
```

Let's check actual record lengths:

```
$ cat hits_100m_obfuscated_monetdb.csv | awk 'BEGIN { FS = "\n"; max_length = 0 } { ++num; l = length($1); if (l > max_length) { max_length = l; print l, "in line", num } }'
588 in line 1
705 in line 2
786 in line 4
788 in line 5
913 in line 9
917 in line 38
996 in line 56
1007 in line 113
1008 in line 115
1015 in line 183
1147 in line 207
1180 in line 654
1190 in line 656
1191 in line 795
1446 in line 856
1519 in line 1572
1646 in line 1686
1700 in line 3084
1701 in line 3086
2346 in line 4013
2630 in line 8245
3035 in line 8248
3257 in line 8289
3762 in line 8307
5536 in line 8376
5568 in line 71721
6507 in line 92993
6734 in line 163169
7706 in line 473542
8368 in line 2803973
9375 in line 5433559
```

No, there is nothing special in line 79128.

Let's try to load just a single line into MonetDB to figure out what is so special about this line.

```
head -n79128 hits_100m_obfuscated_monetdb.csv | tail -n1 > hits_100m_obfuscated_monetdb.csv1
```

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv1' USING DELIMITERS ',', '\n', '"';
```

`Failed to import table 'hits', line 1: incomplete record at end of file`

Now we have another error.
Ok. I understand that MonetDB is just parsing CSV with C-style escaping rules as TSV.

I will try to stick with TSV.

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t';
```

Nothing good happened, it failed after 2.5 minutes with incomprehensible error:

```
Failed to import table 'hits',
clk: 2:30 min
```

Let's replace all backslashes from CSV.

```
SELECT
    toInt64(WatchID) = -9223372036854775808 ? -9223372036854775807 : toInt64(WatchID),
    toInt8(JavaEnable) = -128 ? -127 : toInt8(JavaEnable),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(Title)), '\n', ' '), '"', '\''), '\\', '/'),
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
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(URL)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(Referer)), '\n', ' '), '"', '\''), '\\', '/'),
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
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(FlashMinor2)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt8(NetMajor) = -128 ? -127 : toInt8(NetMajor),
    toInt8(NetMinor) = -128 ? -127 : toInt8(NetMinor),
    toInt16(UserAgentMajor) = -32768 ? -32767 : toInt16(UserAgentMajor),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UserAgentMinor)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt8(CookieEnable) = -128 ? -127 : toInt8(CookieEnable),
    toInt8(JavascriptEnable) = -128 ? -127 : toInt8(JavascriptEnable),
    toInt8(IsMobile) = -128 ? -127 : toInt8(IsMobile),
    toInt8(MobilePhone) = -128 ? -127 : toInt8(MobilePhone),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(MobilePhoneModel)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(Params)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt32(IPNetworkID) = -2147483648 ? -2147483647 : toInt32(IPNetworkID),
    toInt8(TraficSourceID) = -128 ? -127 : toInt8(TraficSourceID),
    toInt16(SearchEngineID) = -32768 ? -32767 : toInt16(SearchEngineID),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(SearchPhrase)), '\n', ' '), '"', '\''), '\\', '/'),
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
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(PageCharset)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt32(CodeVersion) = -2147483648 ? -2147483647 : toInt32(CodeVersion),
    toInt8(IsLink) = -128 ? -127 : toInt8(IsLink),
    toInt8(IsDownload) = -128 ? -127 : toInt8(IsDownload),
    toInt8(IsNotBounce) = -128 ? -127 : toInt8(IsNotBounce),
    toInt64(FUniqID) = -9223372036854775808 ? -9223372036854775807 : toInt64(FUniqID),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(OriginalURL)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt32(HID) = -2147483648 ? -2147483647 : toInt32(HID),
    toInt8(IsOldCounter) = -128 ? -127 : toInt8(IsOldCounter),
    toInt8(IsEvent) = -128 ? -127 : toInt8(IsEvent),
    toInt8(IsParameter) = -128 ? -127 : toInt8(IsParameter),
    toInt8(DontCountHits) = -128 ? -127 : toInt8(DontCountHits),
    toInt8(WithHash) = -128 ? -127 : toInt8(WithHash),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(HitColor)), '\n', ' '), '"', '\''), '\\', '/'),
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
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(BrowserLanguage)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(BrowserCountry)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(SocialNetwork)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(SocialAction)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt16(HTTPError) = -32768 ? -32767 : toInt16(HTTPError),
    toInt32(SendTiming) = -2147483648 ? -2147483647 : toInt32(SendTiming),
    toInt32(DNSTiming) = -2147483648 ? -2147483647 : toInt32(DNSTiming),
    toInt32(ConnectTiming) = -2147483648 ? -2147483647 : toInt32(ConnectTiming),
    toInt32(ResponseStartTiming) = -2147483648 ? -2147483647 : toInt32(ResponseStartTiming),
    toInt32(ResponseEndTiming) = -2147483648 ? -2147483647 : toInt32(ResponseEndTiming),
    toInt32(FetchTiming) = -2147483648 ? -2147483647 : toInt32(FetchTiming),
    toInt8(SocialSourceNetworkID) = -128 ? -127 : toInt8(SocialSourceNetworkID),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(SocialSourcePage)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt64(ParamPrice) = -9223372036854775808 ? -9223372036854775807 : toInt64(ParamPrice),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(ParamOrderID)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(ParamCurrency)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt16(ParamCurrencyID) = -32768 ? -32767 : toInt16(ParamCurrencyID),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(OpenstatServiceName)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(OpenstatCampaignID)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(OpenstatAdID)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(OpenstatSourceID)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UTMSource)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UTMMedium)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UTMCampaign)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UTMContent)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(UTMTerm)), '\n', ' '), '"', '\''), '\\', '/'),
    replaceAll(replaceAll(replaceAll(toValidUTF8(toString(FromTag)), '\n', ' '), '"', '\''), '\\', '/'),
    toInt8(HasGCLID) = -128 ? -127 : toInt8(HasGCLID),
    toInt64(RefererHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(RefererHash),
    toInt64(URLHash) = -9223372036854775808 ? -9223372036854775807 : toInt64(URLHash),
    toInt32(CLID) = -2147483648 ? -2147483647 : toInt32(CLID)
FROM hits_100m_obfuscated
INTO OUTFILE '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv'
FORMAT CSV;
```

Another try:
```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
```

MonetDB still takes about one CPU core to load the data, while docs promised me parallel load.
And there are strange pauses...

```
sql>COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', '\n', '"';
Failed to import table 'hits',
clk: 2:14 min
```

It still does not work!!!

Let's look at the logs.
Logs are found in

```
/var/log/monetdb$ sudo less merovingian.log
```

And the log is the following:
```
2020-08-12 03:44:03 ERR test[542123]: #wrkr0-hits: GDKextendf: !ERROR: could not extend file: No space left on device
2020-08-12 03:44:03 ERR test[542123]: #wrkr0-hits: MT_mremap: !ERROR: MT_mremap(/var/monetdb5/dbfarm/test/bat/10/1013.theap,0x7f0b2c3b0000,344981504,349175808): GDKextendf() failed
2020-08-12 03:44:03 ERR test[542123]: #wrkr0-hits: GDKmremap: !ERROR: requesting virtual memory failed; memory requested: 349175808, memory in use: 113744056, virtual memory in use: 3124271288
2020-08-12 03:44:03 ERR test[542123]: #wrkr0-hits: HEAPextend: !ERROR: failed to extend to 349175808 for 10/1013.theap: GDKmremap() failed
2020-08-12 03:44:04 ERR test[542123]: #client14: createExceptionInternal: !ERROR: SQLException:importTable:42000!Failed to import table 'hits',
```

So, why it was created my "db farm" inside /var/monetdb5/ instead of /opt/ as I requested?

Let's stop MonetDB and symlink /var/monetdb5 to /opt

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.csv' USING DELIMITERS ',', E'\n', '"';
```

It started to load data... but after ten minutes it looks like stopped processing it, but the query does not finish.

There is no `SHOW PROCESSLIST` command.

I see the following message in `merovingian.log`:
```
2020-08-12 04:03:53 ERR test[682554]: #prod-hits: createExceptionInternal: !ERROR: MALException:sql.copy_from:line 40694471: record too long (EOS found)
```

What does EOS mean? It should not be "end of stream" because we have 100 000 000 records, that's more than just 40 694 471.

Another try with TSV:

```
COPY INTO hits FROM '/home/milovidov/example_datasets/hits_100m_obfuscated_monetdb.tsv' USING DELIMITERS '\t';
```

Ok, it's doing something at least for ten minues...
Ok, it's doing something at least for twenty minues...

```
100000000 affected rows
clk: 28:02 min
```

Finally it has loaded data successfully in 28 minutes. It's not fast - just below 60 000 rows per second.

But the second query from the test does not work:

```
sql>SELECT count(*) FROM hits WHERE AdvEngineID <> 0;
SELECT: identifier 'advengineid' unknown
clk: 0.328 ms
sql>DESC TABLE hits
more>;
syntax error, unexpected DESC in: "desc"
clk: 0.471 ms
sql>DESCRIBE TABLE hits;
syntax error, unexpected IDENT in: "describe"
clk: 0.245 ms
sql>SHOW CREATE TABLE hits;
syntax error, unexpected IDENT in: "show"
clk: 0.246 ms
sql>\d hits;
table sys.hits; does not exist
sql>\d test.hits;
table test.hits; does not exist
sql>\d
TABLE  sys.hits
sql>\t
Current time formatter: clock
sql>\dd
unknown sub-command for \d: d
sql>help
more>;
syntax error, unexpected IDENT in: "help"
clk: 0.494 ms
sql>SELECT count(*) FROM hits;
+-----------+
| %1        |
+===========+
| 100000001 |
+-----------+
1 tuple
clk: 1.949 ms
sql>SELECT * FROM hits LIMIT 1;
```

And the query `SELECT * FROM hits LIMIT 1` does not finish in reasonable time.
It took 3:23 min.

Ok, I has to put all identifiers in quotes in my queries, like this:

```
SELECT count(*) FROM hits WHERE "AdvEngineID" <> 0;
```

There is no approximate count distinct functions. Will use exact count distinct instead.

Run queries:
`./benchmark.sh`

It works rather slowly. It is barely using more than a single CPU core. And there is nothing about performance tuning in:
https://www.monetdb.org/Documentation/ServerAdministration/ServerSetupAndConfiguration


The last 7 queries from the benchmark benefit from index. Let's create it:

`CREATE INDEX hits_idx ON hits ("CounterID", "EventDate");`

```
sql>CREATE INDEX hits_idx ON hits ("CounterID", "EventDate");
operation successful
clk: 5.374 sec
```

Ok. It was created quickly and successful.
Let's check how does it speed up queries...

```
sql>SELECT DATE_TRUNC('minute', "EventTime") AS "Minute", count(*) AS "PageViews" FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-02' AND "Refresh" = 0 AND "DontCountHits" = 0 GROUP BY DATE_TRUNC('minute', "EventTime") ORDER BY DATE_TRUNC('minute', "EventTime");
+--------+-----------+
| Minute | PageViews |
+========+===========+
+--------+-----------+
0 tuples
clk: 4.042 sec
```

There is almost no difference.
And the trivial index lookup query is still slow:

```
sql>SELECT count(*) FROM hits WHERE "CounterID" = 62;
+--------+
| %1     |
+========+
| 738172 |
+--------+
1 tuple
clk: 1.406 sec
```

How to prepare the benchmark report:

`grep clk log.txt | awk '{ if ($3 == "ms") { print $2 / 1000; } else if ($3 == "sec") { print $2 } else { print } }'`

```
awk '{
    if (i % 3 == 0) { a = $1 }
    else if (i % 3 == 1) { b = $1 }
    else if (i % 3 == 2) { c = $1; print "[" a ", " b ", " c "]," };
    ++i; }' < tmp.txt
```

When I run:

```
sudo systemctl stop monetdbd
```

It takes a few minutes to complete.
