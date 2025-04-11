#include "ChineseTokenizer.h"

#include <cassert>

#include <base/defines.h>
#include <cppjieba/Jieba.hpp>

namespace DB
{

ChineseTokenizer & ChineseTokenizer::instance()
{
    static ChineseTokenizer tokenizer{};
    return tokenizer;
}

std::vector<std::string> ChineseTokenizer::tokenize(const std::string & str)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    assert(jieba_instance != nullptr);
#endif
    std::vector<std::string> tokens{};
    jieba_instance->CutForSearch(str, tokens);
    return tokens;
}

ChineseTokenizer::ChineseTokenizer()
    : jieba_instance{std::make_unique<cppjieba::Jieba>()}
{
}

}
