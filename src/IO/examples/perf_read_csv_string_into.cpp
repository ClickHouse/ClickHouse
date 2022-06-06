#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>

using namespace DB;
using ReadFunc =  void (*)(NullOutput &, ReadBuffer &, const FormatSettings::CSV &);

static inline void skipDelimiters(ReadBuffer & in)
{
    while (!in.eof() && (*in.position() == '\x01' || *in.position() == '\r' || *in.position() == '\n'))
        ++in.position();
}

static inline void readCSVFile(ReadBuffer & buf, const FormatSettings::CSV & settings, ReadFunc read_func)
{
    while (!buf.eof())
    {
        NullOutput s;
        read_func(s, buf, settings);
        skipDelimiters(buf);
    }
}

static void benchmark(int retries, int threads, const String & path, ReadFunc read_func, const String & tag = "")
{
    FormatSettings::CSV csv_settings;
    csv_settings.delimiter = '\x01';
    csv_settings.allow_single_quotes = false;
    csv_settings.allow_double_quotes = false;

    UInt64 sum_cost = 0;
    for (int retry = 0; retry < retries; ++retry)
    {
        std::vector<ReadBufferPtr> read_buffers{static_cast<size_t>(threads)};
        for (int i = 0; i < threads; ++i)
            read_buffers[i] = std::make_shared<ReadBufferFromFile>(path);

        Stopwatch watch;
        std::vector<std::thread> group{static_cast<size_t>(threads)};
        for (int i = 0; i < threads; ++i)
            group[i]
                = std::thread([i, read_func, &read_buffers, &csv_settings] { readCSVFile(*(read_buffers[i]), csv_settings, read_func); });

        for (int i = 0; i < threads; ++i)
            group[i].join();
        sum_cost += watch.elapsedMilliseconds();
    }
    std::cout << tag << "\t" << static_cast<double>(sum_cost)/retries << std::endl;
}

int main()
{
#if defined(__AVX512F__) && defined(__AVX512BW__)
    std::cout << "support avx512" << std::endl;
#else
    std::cout << "do not support avx512" << std::endl;
#endif

#if defined(__AVX__) && defined(__AVX2__)
    std::cout << "support avx2" << std::endl;
#else
    std::cout << "do not support avx2" << std::endl;
#endif

#if defined(__SSE2__)
    std::cout << "support sse2" << std::endl;
#else
    std::cout << "do not support sse2" << std::endl;
#endif

    std::map<String, String> paths = {
        /// echo "SELECT randomPrintableASCII(10), randomPrintableASCII(10) , randomPrintableASCII(10)  from system.numbers limit 10000000  FORMAT CSV settings format_csv_delimiter = '\x01'"  | clickhouse-client >  short.csv
        {"short.csv", "short-string"},
        /// echo "SELECT randomPrintableASCII(100), randomPrintableASCII(100) , randomPrintableASCII(100)  from system.numbers limit 1000000  FORMAT CSV settings format_csv_delimiter = '\x01'"  | clickhouse-client >  medium.csv
        {"medium.csv", "medium-string"},
        /// echo "SELECT randomPrintableASCII(1000), randomPrintableASCII(1000) , randomPrintableASCII(1000)  from system.numbers limit 100000  FORMAT CSV settings format_csv_delimiter = '\x01'"  | client-client >  long.csv
        {"long.csv", "long-string"},
    };
    std::map<ReadFunc, String> read_funcs
        = {{readCSVStringInto<NullOutput>, "none"},
           {readCSVStringIntoSSE2<NullOutput>, "sse2"},
           {readCSVStringIntoSSE2Opt<NullOutput>, "sse2-opt"},
        //    {readCSVStringIntoAVX2<NullOutput>, "avx2"},
        //    {readCSVStringIntoAVX512<NullOutput>, "avx512"}
           };

    for (const auto & [path, path_tag] : paths)
        for (const auto & [read_func, func_tag] : read_funcs)
            benchmark(40, 1, path, read_func, path_tag + ":" + func_tag);
    return 0;
}
