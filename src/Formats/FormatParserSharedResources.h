#pragma once

#include <Common/threadPoolCallbackRunner.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class ActionsDAG;
struct Settings;
class KeyCondition;

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

struct FormatParserSharedResources
{
    const size_t max_parsing_threads = 0;
    const size_t max_io_threads = 0;

    FormatParserSharedResources(const Settings & settings, size_t num_streams_);

    static FormatParserSharedResourcesPtr singleThreaded(const Settings & settings);

    void finishStream();

    size_t getParsingThreadsPerReader() const;
    size_t getIOThreadsPerReader() const;


private:
    std::atomic<size_t> num_streams{0};
    ThreadPoolCallbackRunnerFast parsing_runner;
    ThreadPoolCallbackRunnerFast io_runner;
};
}
