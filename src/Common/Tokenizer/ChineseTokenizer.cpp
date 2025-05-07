#include "ChineseTokenizer.h"

#include <cppjieba/Jieba.hpp>

namespace DB
{

ChineseTokenizer & ChineseTokenizer::instance()
{
    static ChineseTokenizer tokenizer;
    return tokenizer;
}

std::vector<std::string> ChineseTokenizer::tokenize(const std::string & str, ChineseGranularMode granular_mode)
{
    std::vector<std::string> tokens;
    if (granular_mode == ChineseGranularMode::Fine)
        jieba_instance->CutForSearch(str, tokens);
    else
        jieba_instance->CutAll(str, tokens);
    return tokens;
}

ChineseTokenizer::ChineseTokenizer()
    : jieba_instance(std::make_unique<cppjieba::Jieba>())
{
}
}
