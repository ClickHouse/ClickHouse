#include <Interpreters/ChineseTokenizer.h>

#include "config.h"

#if USE_JIEBA

#    include <jieba.h>

namespace DB
{

ChineseTokenizer & ChineseTokenizer::instance()
{
    static ChineseTokenizer tokenizer;
    return tokenizer;
}

std::vector<std::string_view> ChineseTokenizer::tokenize(std::string_view str, ChineseTokenizationGranularity granularity)
{
    std::vector<std::string_view> tokens;
    switch (granularity)
    {
        case ChineseTokenizationGranularity::Fine:
            tokens = jieba->cutAll(str);
            break;
        case ChineseTokenizationGranularity::Coarse:
            tokens = jieba->cut(str);
            break;
    }
    return tokens;
}

ChineseTokenizer::ChineseTokenizer()
    : jieba(std::make_unique<Jieba::Jieba>())
{
}

ChineseTokenizer::~ChineseTokenizer() = default;

}

#endif
