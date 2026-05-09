#include <Interpreters/JiebaSegmenter.h>

#include "config.h"

#if USE_JIEBA

#    include <jieba.h>

namespace DB
{

JiebaSegmenter & JiebaSegmenter::instance()
{
    static JiebaSegmenter segmenter;
    return segmenter;
}

std::vector<std::string_view> JiebaSegmenter::tokenize(std::string_view str, ChineseTokenizationGranularity granularity)
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

JiebaSegmenter::JiebaSegmenter()
    : jieba(std::make_unique<Jieba::Jieba>())
{
}

JiebaSegmenter::~JiebaSegmenter() = default;

}

#endif
