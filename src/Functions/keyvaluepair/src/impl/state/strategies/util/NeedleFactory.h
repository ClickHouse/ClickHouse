#pragma once

#include <vector>

#include <Functions/keyvaluepair/src/impl/state/Configuration.h>

namespace DB
{

/*
 * Did not spend much time here, running against the clock :)
 *
 * Takes in the Configuration and outputs the characters of interest for each state.
 * Those characters will be later used in string look-ups like `find_first` and `find_first_not`
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
