#pragma once

#include "config.h"

#if USE_CPPJIEBA

#include <cppjieba/Jieba.hpp>

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

    std::vector<cppjieba::Word> tokenize(std::string_view str, ChineseTokenizationGranularity granularity);

private:
    ChineseTokenizer();

    std::unique_ptr<cppjieba::Jieba> jieba_instance;
};

}

#endif
