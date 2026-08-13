#pragma once

#include <Common/re2.h>
#include <Core/Types.h>

#include <list>
#include <string_view>
#include <vector>

namespace DB
{

class JSONBloomPathMatcher
{
public:
    JSONBloomPathMatcher(std::vector<String> paths_, const std::vector<String> & regexps_);

    bool shouldSkip(std::string_view path) const;

private:
    std::vector<String> paths;
    std::list<re2::RE2> regexps;
};

}
