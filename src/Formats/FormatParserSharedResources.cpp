#include <Formats/FormatParserSharedResources.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

namespace Setting
{
    extern const SettingsMaxThreads max_download_threads;
    extern const SettingsMaxThreads max_parsing_threads;
}

FormatParserSharedResources::FormatParserSharedResources(const Settings & settings, size_t num_streams_)
    : max_parsing_threads(settings[Setting::max_parsing_threads])
    , max_io_threads(settings[Setting::max_download_threads])
    , num_streams(num_streams_)
{
}

FormatParserSharedResourcesPtr FormatParserSharedResources::singleThreaded(const Settings & settings)
{
    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(settings, 1);
    const_cast<size_t &>(parser_shared_resources->max_parsing_threads) = 1;
    return parser_shared_resources;
}


void FormatParserSharedResources::finishStream()
{
    num_streams.fetch_sub(1, std::memory_order_relaxed);
}

size_t FormatParserSharedResources::getParsingThreadsPerReader() const
{
    size_t n = num_streams.load(std::memory_order_relaxed);
    n = std::max(n, 1ul);
    return (max_parsing_threads + n - 1) / n;
}

size_t FormatParserSharedResources::getIOThreadsPerReader() const
{
    size_t n = num_streams.load(std::memory_order_relaxed);
    n = std::max(n, 1ul);
    return (max_io_threads + n - 1) / n;
}

void FormatParserSharedResources::initOnce(std::function<void()> f)
{
    std::call_once(
        init_flag,
        [&]
        {
            if (init_exception)
                std::rethrow_exception(init_exception);

            try
            {
                f();
            }
            catch (...)
            {
                init_exception = std::current_exception();
                throw;
            }
        });
}

}
