#pragma once

#include <memory>
#include <string>
#include <vector>

#include "config.h"

#if USE_CPPJIEBA

namespace cppjieba
{
class Jieba;
}

namespace DB
{

enum class ChineseTokenizationGranularity
{
    Fine,
    Coarse
};

/// The wrapper around the cppjieba library to tokenize a Chinese text into tokens.
class ChineseTokenizer
{
public:
    static ChineseTokenizer & instance();

    std::vector<std::string> tokenize(const std::string & str, ChineseTokenizationGranularity granularity);

private:
    ChineseTokenizer();

    std::unique_ptr<cppjieba::Jieba> jieba_instance;
};

}

#endif
