#pragma once

#include <vector>

#include <Functions/keyvaluepair/src/impl/state/Configuration.h>

namespace DB
{

/*
 * Did not spend much time here, running against the clock :)
 * */
class NeedleFactory
{
    static constexpr auto NEEDLE_SIZE = 16u;
public:
    static std::vector<char> getWaitNeedles(const Configuration & extractor_configuration);
    static std::vector<char> getReadNeedles(const Configuration & extractor_configuration);
    static std::vector<char> getReadQuotedNeedles(const Configuration & extractor_configuration);
};

class EscapingNeedleFactory
{
public:
    static std::vector<char> getWaitNeedles(const Configuration & extractor_configuration);
    static std::vector<char> getReadNeedles(const Configuration & extractor_configuration);
    static std::vector<char> getReadQuotedNeedles(const Configuration & extractor_configuration);
};

}
