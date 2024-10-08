#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Formats/registerFormats.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <AggregateFunctions/registerAggregateFunctions.h>

using namespace DB;


ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();
    registerFormats();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// The input format is as follows:
        /// - format name on the first line,
        /// - table structure on the second line,
        /// - the data for the rest of the input.

        /** The corpus was generated as follows:

        i=0; find ../../../../tests/queries -name '*.sql' |
            xargs -I{} bash -c "tr '\n' ' ' <{}; echo" |
            rg -o -i 'CREATE TABLE\s+\w+\s+\(.+?\) ENGINE' |
            sed -r -e 's/CREATE TABLE\s+\w+\s+\((.+?)\) ENGINE/\1/i' | sort | uniq |
            while read line; do
                i=$((i+1));
                clickhouse-local --query "SELECT name FROM system.formats ORDER BY rand() LIMIT 1" >> $i;
                echo "$line" >> $i;
                echo $RANDOM >> $i;
                echo $i;
            done
        */

        /** And:

        for format in $(clickhouse-client --query "SELECT name FROM system.formats WHERE is_output"); do
            echo $format;
            echo $format >> $format;
            echo "WatchID Int64, JavaEnable Int16, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID Int32, ClientIP Int32, RegionID Int32, UserID Int64, CounterClass Int16, OS Int16, UserAgent Int16, URL String, Referer String, IsRefresh Int16, RefererCategoryID Int16, RefererRegionID Int32, URLCategoryID Int16, URLRegionID Int32, ResolutionWidth Int16, ResolutionHeight Int16, ResolutionDepth Int16, FlashMajor Int16, FlashMinor Int16, FlashMinor2 String, NetMajor Int16, NetMinor Int16, UserAgentMajor Int16, UserAgentMinor String, CookieEnable Int16, JavascriptEnable Int16, IsMobile Int16, MobilePhone Int16, MobilePhoneModel String, Params String, IPNetworkID Int32, TraficSourceID Int16, SearchEngineID Int16, SearchPhrase String, AdvEngineID Int16, IsArtifical Int16, WindowClientWidth Int16, WindowClientHeight Int16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 Int16, SilverlightVersion2 Int16, SilverlightVersion3 Int32, SilverlightVersion4 Int16, PageCharset String, CodeVersion Int32, IsLink Int16, IsDownload Int16, IsNotBounce Int16, FUniqID Int64, OriginalURL String, HID Int32, IsOldCounter Int16, IsEvent Int16, IsParameter Int16, DontCountHits Int16, WithHash Int16, HitColor String, LocalEventTime DateTime, Age Int16, Sex Int16, Income Int16, Interests Int16, Robotness Int16, RemoteIP Int32, WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage String, BrowserCountry String, SocialNetwork String, SocialAction String, HTTPError Int16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, SocialSourceNetworkID Int16, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency String, ParamCurrencyID Int16, OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID Int16, RefererHash Int64, URLHash Int64, CLID Int32" >> $format;
            clickhouse-client --query "SELECT * FROM hits LIMIT 10 FORMAT $format" >> $format || rm $format;
        done

        */

        /// Compile the code as follows:
        ///   mkdir build_asan_fuzz
        ///   cd build_asan_fuzz
        ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
        ///
        /// The corpus is located here:
        /// https://github.com/ClickHouse/fuzz-corpus/tree/main/format_fuzzer
        ///
        /// The fuzzer can be run as follows:
        ///   ../../../build_asan_fuzz/src/Formats/fuzzers/format_fuzzer corpus -jobs=64 -rss_limit_mb=8192

        DB::ReadBufferFromMemory in(data, size);

        String format;
        readStringUntilNewlineInto(format, in);
        assertChar('\n', in);

        String structure;
        readStringUntilNewlineInto(structure, in);
        assertChar('\n', in);

        ColumnsDescription description = parseColumnsListFromString(structure, context);
        auto columns_info = description.getOrdinary();

        Block header;
        for (const auto & info : columns_info)
        {
            ColumnWithTypeAndName column;
            column.name = info.name;
            column.type = info.type;
            column.column = column.type->createColumn();
            header.insert(std::move(column));
        }

        InputFormatPtr input_format = context->getInputFormat(format, in, header, 13 /* small block size */);

        QueryPipeline pipeline(Pipe(std::move(input_format)));
        PullingPipelineExecutor executor(pipeline);
        Block res;
        while (executor.pull(res))
            ;
    }
    catch (...)
    {
    }

    return 0;
}
