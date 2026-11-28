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

/// Thread-safe: `JiebaSegmenter` is a single process-wide instance, and `Jieba::Jieba`
/// is immutable after construction (the dictionary and HMM model are read-only, and the
/// segmenters keep all per-call state local). Concurrent `tokenize` calls from different
/// queries therefore do not race.
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
