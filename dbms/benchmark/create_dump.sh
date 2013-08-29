path=/opt/dump/dump_0.3
db_name=hits_1b
num=1000000000

dump_replaced=$path/dump_"$db_name"_replaced.tsv
dump_meshed=$path/dump_"$db_name"_meshed.tsv

clickhouse-client --query="SET GLOBAL max_block_size=100000"
clickhouse-client --query="SET GLOBAL max_threads=1"

clickhouse-client --query="SELECT toInt64(WatchID), JavaEnable, Title, GoodEvent, (EventTime < toDateTime('1971-01-01 00:00:00') ? toDateTime('1971-01-01 00:00:01') : EventTime), (EventDate < toDate('1971-01-01') ? toDate('1971-01-01') : EventDate), CounterID, ClientIP, RegionID, toInt64(UserID), CounterClass, OS, UserAgent, URL, Referer, Refresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone, (ClientEventTime < toDateTime('1971-01-01 00:00:01') ? toDateTime('1971-01-01 00:00:01') : ClientEventTime), SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4, PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, toInt64(FUniqID), OriginalURL, HID, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor, (LocalEventTime < toDateTime('1971-01-01 00:00:01') ? toDateTime('1971-01-01 00:00:01') : LocalEventTime), Age, Sex, Income, Interests, Robotness, RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, toInt64(RefererHash), toInt64(URLHash), CLID, toInt64(intHash32(UserID)) FROM hits_mt_test_1b LIMIT $num FORMAT TabSeparated" > $dump_replaced

/etc/init.d/clickhouse-server-metrika-yandex-ulimit restart

sudo nsort -format=maximum_size:65535 -k1 -T /opt -o $dump_meshed $dump_replaced