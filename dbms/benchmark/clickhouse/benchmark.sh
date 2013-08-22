#!/bin/bash

ck="clickhouse-client "
test_table="hits_mem_test_10m"
claster="self"

start_date="'2013-07-01'"
early_stop_date="'2013-07-02'"
stop_date="'2013-07-31'"
counter_id=34

function run_ck_server 
{
    sudo sh -c " ulimit  -v 54000000; /etc/init.d/clickhouse-server-metrika-yandex-ulimit restart"
}

# execute queries
function execute()
{
    queries=("${@}")
    queries_count=${#queries[@]}
    
    if [ -z $TIMES ]; then
	TIMES=1
    fi
    
    index=0
    comment_re='\#.*'
    while [ "$index" -lt "$queries_count" ]; do
	query=${queries[$index]}

	if [[ $query =~ $comment_re ]]; then
	    echo "$query"
	    echo
	else	    
	    sync
	    sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
	    #run_ck_server

	    for i in $(seq $TIMES)
	    do
	
		expect -f ./expect.tcl "$query"
		if [ "$?" != "0" ]; then
		    echo "Error: $?"
		    #break
		fi
	    done
	fi

	let "index = $index + 1"
    done
}

init_queries=(
# DB structure with array arguments
#"CREATE TABLE $test_table ( WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, Refresh UInt8, RefererCategoryID UInt16, RefererRegionID UInt32, URLCategoryID UInt16, URLRegionID UInt32, ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, OriginalURL String, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), LocalEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming UInt32, DNSTiming UInt32, ConnectTiming UInt32, ResponseStartTiming UInt32, ResponseEndTiming UInt32, FetchTiming UInt32, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32 ) ENGINE = MergeTree(EventDate, intHash32(UserID), tuple(CounterID, EventDate, intHash32(UserID), EventTime), 8192);"

#DB structure without array arguments
#"CREATE TABLE $test_table ( WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, Refresh UInt8, RefererCategoryID UInt16, RefererRegionID UInt32, URLCategoryID UInt16, URLRegionID UInt32, ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, OriginalURL String, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), LocalEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, RemoteIP UInt32, WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming UInt32, DNSTiming UInt32, ConnectTiming UInt32, ResponseStartTiming UInt32, ResponseEndTiming UInt32, FetchTiming UInt32, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32 ) ENGINE = MergeTree(EventDate, intHash32(UserID), tuple(CounterID, EventDate, intHash32(UserID), EventTime), 8192);"

#modified table without uint
"CREATE TABLE $test_table ( WatchID Int64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, RegionID UInt32, UserID Int64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, Refresh UInt8, RefererCategoryID UInt16, RefererRegionID UInt32, URLCategoryID UInt16, URLRegionID UInt32, ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID Int64, OriginalURL String, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), LocalEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, RemoteIP UInt32, WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming UInt32, DNSTiming UInt32, ConnectTiming UInt32, ResponseStartTiming UInt32, ResponseEndTiming UInt32, FetchTiming UInt32, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash Int64, URLHash Int64, CLID UInt32 ) ENGINE = MergeTree(EventDate, intHash32(UserID), tuple(CounterID, EventDate, intHash32(UserID), EventTime), 8192);"

)

test_queries=(
"SELECT count() FROM $test_table;"
"SELECT count() FROM $test_table WHERE AdvEngineID != 0;"
"SELECT sum(AdvEngineID), count(), avg(ResolutionWidth) FROM $test_table;"
"SELECT sum(UserID) FROM $test_table;"
"SELECT uniq(UserID) FROM $test_table;"
"SELECT uniq(SearchPhrase) FROM $test_table;"
"SELECT min(EventDate), max(EventDate) FROM $test_table;"

"SELECT AdvEngineID, count() FROM $test_table WHERE AdvEngineID != 0 GROUP BY AdvEngineID ORDER BY count() DESC;"
"#- мощная фильтрация. После фильтрации почти ничего не остаётся, но делаем ещё агрегацию.;"

"SELECT RegionID, uniq(UserID) AS u FROM $test_table GROUP BY RegionID ORDER BY u DESC LIMIT 10;"
"#- агрегация, среднее количество ключей.;"

"SELECT RegionID, sum(AdvEngineID), count() AS c, avg(ResolutionWidth), uniq(UserID) FROM $test_table GROUP BY RegionID ORDER BY c DESC LIMIT 10;"
"#- агрегация, среднее количество ключей, несколько агрегатных функций.;"

"SELECT MobilePhoneModel, uniq(UserID) AS u FROM $test_table WHERE MobilePhoneModel != '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;"
"#- мощная фильтрация по строкам, затем агрегация по строкам.;"

"SELECT MobilePhone, MobilePhoneModel, uniq(UserID) AS u FROM $test_table WHERE MobilePhoneModel != '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;"
"#- мощная фильтрация по строкам, затем агрегация по паре из числа и строки.;"

"SELECT SearchPhrase, count() AS c FROM $test_table WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
"#- средняя фильтрация по строкам, затем агрегация по строкам, большое количество ключей.;"

"SELECT SearchPhrase, uniq(UserID) AS u FROM $test_table WHERE SearchPhrase != '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;"
"#- агрегация чуть сложнее.;"

"SELECT SearchEngineID, SearchPhrase, count() AS c FROM $test_table WHERE SearchPhrase != '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;"
"#- агрегация по числу и строке, большое количество ключей.;"

"SELECT UserID, count() FROM $test_table GROUP BY UserID ORDER BY count() DESC LIMIT 10;"
"#- агрегация по очень большому количеству ключей, может не хватить оперативки.;"

"SELECT UserID, SearchPhrase, count() FROM $test_table GROUP BY UserID, SearchPhrase ORDER BY count() DESC LIMIT 10;"
"#- ещё более сложная агрегация.;"

"SELECT UserID, SearchPhrase, count() FROM $test_table GROUP BY UserID, SearchPhrase LIMIT 10;"
"#- то же самое, но без сортировки.;"

"SELECT UserID, toMinute(EventTime) AS m, SearchPhrase, count() FROM $test_table GROUP BY UserID, m, SearchPhrase ORDER BY count() DESC LIMIT 10;"
"#- ещё более сложная агрегация, не стоит выполнять на больших таблицах.;"

"SELECT UserID FROM $test_table WHERE UserID = 12345678901234567890;"
"#- мощная фильтрация по столбцу типа UInt64.;"

"SELECT count() FROM $test_table WHERE URL LIKE '%metrika%';"
"#- фильтрация по поиску подстроки в строке.;"

"SELECT SearchPhrase, any(URL), count() AS c FROM $test_table WHERE URL LIKE '%metrika%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
"#- вынимаем большие столбцы, фильтрация по строке.;"

"SELECT SearchPhrase, any(URL), any(Title), count() AS c, uniq(UserID) FROM $test_table WHERE Title LIKE '%Яндекс%' AND URL NOT LIKE '%.yandex.%' AND SearchPhrase != '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
"#- чуть больше столбцы.;"

"SELECT * FROM $test_table WHERE URL LIKE '%metrika%' ORDER BY EventTime LIMIT 10;"
"#- плохой запрос - вынимаем все столбцы.;"

"SELECT SearchPhrase FROM $test_table WHERE SearchPhrase != '' ORDER BY EventTime LIMIT 10;"
"#- большая сортировка.;"

"SELECT SearchPhrase FROM $test_table WHERE SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10;"
"#- большая сортировка по строкам.;"

"SELECT SearchPhrase FROM $test_table WHERE SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10;"
"#- большая сортировка по кортежу.;"

"SELECT CounterID, avg(length(URL)) AS l, count() AS c FROM $test_table WHERE URL != '' GROUP BY CounterID HAVING c > 100000 ORDER BY l DESC LIMIT 25;"
"#- считаем средние длины URL для крупных счётчиков.;"

"SELECT domainWithoutWWW(Referer) AS key, avg(length(Referer)) AS l, count() AS c, any(Referer) FROM $test_table WHERE Referer != '' GROUP BY key HAVING c > 100000 ORDER BY l DESC LIMIT 25;"
"#- то же самое, но с разбивкой по доменам.;"

"SELECT sum(ResolutionWidth), sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3), sum(ResolutionWidth + 4), sum(ResolutionWidth + 5), sum(ResolutionWidth + 6), sum(ResolutionWidth + 7), sum(ResolutionWidth + 8), sum(ResolutionWidth + 9), sum(ResolutionWidth + 10), sum(ResolutionWidth + 11), sum(ResolutionWidth + 12), sum(ResolutionWidth + 13), sum(ResolutionWidth + 14), sum(ResolutionWidth + 15), sum(ResolutionWidth + 16), sum(ResolutionWidth + 17), sum(ResolutionWidth + 18), sum(ResolutionWidth + 19), sum(ResolutionWidth + 20), sum(ResolutionWidth + 21), sum(ResolutionWidth + 22), sum(ResolutionWidth + 23), sum(ResolutionWidth + 24), sum(ResolutionWidth + 25), sum(ResolutionWidth + 26), sum(ResolutionWidth + 27), sum(ResolutionWidth + 28), sum(ResolutionWidth + 29), sum(ResolutionWidth + 30), sum(ResolutionWidth + 31), sum(ResolutionWidth + 32), sum(ResolutionWidth + 33), sum(ResolutionWidth + 34), sum(ResolutionWidth + 35), sum(ResolutionWidth + 36), sum(ResolutionWidth + 37), sum(ResolutionWidth + 38), sum(ResolutionWidth + 39), sum(ResolutionWidth + 40), sum(ResolutionWidth + 41), sum(ResolutionWidth + 42), sum(ResolutionWidth + 43), sum(ResolutionWidth + 44), sum(ResolutionWidth + 45), sum(ResolutionWidth + 46), sum(ResolutionWidth + 47), sum(ResolutionWidth + 48), sum(ResolutionWidth + 49), sum(ResolutionWidth + 50), sum(ResolutionWidth + 51), sum(ResolutionWidth + 52), sum(ResolutionWidth + 53), sum(ResolutionWidth + 54), sum(ResolutionWidth + 55), sum(ResolutionWidth + 56), sum(ResolutionWidth + 57), sum(ResolutionWidth + 58), sum(ResolutionWidth + 59), sum(ResolutionWidth + 60), sum(ResolutionWidth + 61), sum(ResolutionWidth + 62), sum(ResolutionWidth + 63), sum(ResolutionWidth + 64), sum(ResolutionWidth + 65), sum(ResolutionWidth + 66), sum(ResolutionWidth + 67), sum(ResolutionWidth + 68), sum(ResolutionWidth + 69), sum(ResolutionWidth + 70), sum(ResolutionWidth + 71), sum(ResolutionWidth + 72), sum(ResolutionWidth + 73), sum(ResolutionWidth + 74), sum(ResolutionWidth + 75), sum(ResolutionWidth + 76), sum(ResolutionWidth + 77), sum(ResolutionWidth + 78), sum(ResolutionWidth + 79), sum(ResolutionWidth + 80), sum(ResolutionWidth + 81), sum(ResolutionWidth + 82), sum(ResolutionWidth + 83), sum(ResolutionWidth + 84), sum(ResolutionWidth + 85), sum(ResolutionWidth + 86), sum(ResolutionWidth + 87), sum(ResolutionWidth + 88), sum(ResolutionWidth + 89) FROM $test_table;"
"#- много тупых агрегатных функций.;"

"SELECT SearchEngineID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM $test_table WHERE SearchPhrase != '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;"
"#- сложная агрегация, для больших таблиц может не хватить оперативки.;"

"SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM $test_table WHERE SearchPhrase != '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"
"#- агрегация по двум полям, которая ничего не агрегирует. Для больших таблиц выполнить не получится.;"

"SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM $test_table GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"
"#- то же самое, но ещё и без фильтрации.;"

"SELECT URL, count() AS c FROM $test_table GROUP BY URL ORDER BY c DESC LIMIT 10;"
"#- агрегация по URL.;"

"SELECT 1, URL, count() AS c FROM $test_table GROUP BY 1, URL ORDER BY c DESC LIMIT 10;"
"#- агрегация по URL и числу.;"

"SELECT ClientIP AS x, x - 1, x - 2, x - 3, count() AS c FROM $test_table GROUP BY x, x - 1, x - 2, x - 3 ORDER BY c DESC LIMIT 10;"
 
"SELECT
    URL,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT DontCountHits
    AND NOT Refresh
    AND notEmpty(URL)
GROUP BY URL
ORDER BY PageViews DESC
LIMIT 10;"


"SELECT
    Title,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT DontCountHits
    AND NOT Refresh
    AND notEmpty(Title)
GROUP BY Title
ORDER BY PageViews DESC
LIMIT 10;"

"SELECT
    URL,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT Refresh
    AND IsLink
    AND NOT IsDownload
GROUP BY URL
ORDER BY PageViews DESC
LIMIT 1000;"

"SELECT
    TraficSourceID,
    SearchEngineID,
    AdvEngineID,
    ((SearchEngineID = 0 AND AdvEngineID = 0) ? Referer : '') AS Src,
    URL AS Dst,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT Refresh
GROUP BY
    TraficSourceID,
    SearchEngineID,
    AdvEngineID,
    Src,
    Dst
ORDER BY PageViews DESC
LIMIT 1000;"

"SELECT
    URLHash,
    EventDate,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT Refresh
    AND TraficSourceID IN (-1, 6)
    AND RefererHash = halfMD5('http://example.ru/')
GROUP BY
    URLHash,
    EventDate
ORDER BY PageViews DESC
LIMIT 100000;"


"SELECT
    WindowClientWidth,
    WindowClientHeight,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($stop_date)
    AND NOT Refresh
    AND NOT DontCountHits
    AND URLHash = halfMD5('http://example.ru/')
GROUP BY
    WindowClientWidth,
    WindowClientHeight
ORDER BY PageViews DESC
LIMIT 10000;"

"SELECT
    toStartOfMinute(EventTime) AS Minute,
    count() AS PageViews
FROM $test_table
WHERE
    CounterID = $counter_id
    AND EventDate >= toDate($start_date)
    AND EventDate <= toDate($early_stop_date)
    AND NOT Refresh
    AND NOT DontCountHits
GROUP BY
    Minute
ORDER BY Minute;"

)

function test {
    TIMES=3
    execute "${test_queries[@]}"
}

function init {
    execute "${init_queries[@]}"
}

function debug {
    TIMES=3
    debug_queries=(
"SELECT WatchID, ClientIP, count() AS c, sum(Refresh), avg(ResolutionWidth) FROM $test_table GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"
"SELECT CounterID, avg(length(URL)) AS l, count() AS c FROM $test_table WHERE URL != '' GROUP BY CounterID HAVING c > 100000 ORDER BY l DESC LIMIT 25;"

)
    execute "${debug_queries[@]}"
}

function usage {
    cat <<EOF   
usage: $0 options

This script run benhmark for clickhouse

OPTIONS:
   -h            Show this message
   -d            Run debug queries
   -i            Init database
   -p log_file   Parse log file to columns with result
   -t            Run tests
EOF
}

function parse_log {
   results=$(cat $1 |  grep -P 'Elapsed: \d+.\d+ ' | awk '{print $6}')
  
   index=1
   for res in $results
   do
      echo -n "$res "
      let "index=$index % 3"
      if [ "$index" == "0" ]; then
	  echo 
      fi
      let "index=$index + 1"
   done
}

if [ "$#" == "0" ]; then
    usage
    exit 0
fi

echo "Start date" $(date)

while getopts “hitdp:” OPTION
do
     case $OPTION in
         h)
             usage
             exit 0
             ;;
         i)
             init
             ;;
         t)
             test
             ;;
	 d)
	     debug
	     ;;
	 p)
	     parse_log $OPTARG
	     ;;
         ?)
             usage
             exit 0
             ;;
     esac
done

echo "Stop date" $(date)
