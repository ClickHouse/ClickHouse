#pragma once

#include "config.h"

#if USE_JIEBA

#    include <memory>
#    include <string_view>
#    include <vector>

namespace Jieba
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

/// Singleton wrapping a single instance of the cppjieba segmenter
/// (initializing it is expensive — it loads embedded dictionaries and HMM model).
class JiebaSegmenter
{
public:
    static JiebaSegmenter & instance();
    ~JiebaSegmenter();

    std::vector<std::string_view> tokenize(std::string_view str, ChineseTokenizationGranularity granularity);

private:
    JiebaSegmenter();

    std::unique_ptr<Jieba::Jieba> jieba;
};

}

#endif
