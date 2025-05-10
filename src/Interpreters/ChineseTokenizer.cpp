#include "ChineseTokenizer.h"

#if USE_CPPJIEBA

#include <cppjieba/Jieba.hpp>

namespace DB
{

ChineseTokenizer & ChineseTokenizer::instance()
{
    static ChineseTokenizer tokenizer;
    return tokenizer;
}

std::vector<std::string> ChineseTokenizer::tokenize(const std::string & str, ChineseTokenizationGranularity granularity)
{
    std::vector<std::string> tokens;
    switch (granularity)
    {
        case ChineseTokenizationGranularity::Fine:
            jieba_instance->CutForSearch(str, tokens);
            break;
        case ChineseTokenizationGranularity::Coarse:
            jieba_instance->CutAll(str, tokens);
            break;
    }
    return tokens;
}

ChineseTokenizer::ChineseTokenizer()
    : jieba_instance(std::make_unique<cppjieba::Jieba>())
{
}

}

#endif
