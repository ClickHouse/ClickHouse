#pragma once

#include "config.h"

#if USE_JIEBA

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

class ChineseTokenizer
{
public:
    static ChineseTokenizer & instance();
    ~ChineseTokenizer();

    std::vector<std::string_view> tokenize(std::string_view str, ChineseTokenizationGranularity granularity);
private:
    ChineseTokenizer();

    std::unique_ptr<Jieba::Jieba> jieba;
};

}

#endif
