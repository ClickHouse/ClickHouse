#pragma once

#include <vector>

#include <Functions/keyvaluepair/src/impl/state/ExtractorConfiguration.h>

namespace DB
{

/*
 * Did not spend much time here, running against the clock :)
 * */
class NeedleFactory
{
    static constexpr auto NEEDLE_SIZE = 16u;
public:
    static std::vector<char> getWaitNeedles(const ExtractorConfiguration & extractor_configuration);
    static std::vector<char> getReadNeedles(const ExtractorConfiguration & extractor_configuration);
    static std::vector<char> getReadQuotedNeedles(const ExtractorConfiguration & extractor_configuration);
};

class EscapingNeedleFactory
{
public:
    static std::vector<char> getWaitNeedles(const ExtractorConfiguration & extractor_configuration);
    static std::vector<char> getReadNeedles(const ExtractorConfiguration & extractor_configuration);
    static std::vector<char> getReadQuotedNeedles(const ExtractorConfiguration & extractor_configuration);
};

}
