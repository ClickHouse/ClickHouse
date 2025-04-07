#pragma once

#include <memory>
#include <string>
#include <vector>

namespace cppjieba
{
class Jieba;
}

namespace DB
{

class ChineseTokenizer
{
public:
    static ChineseTokenizer & instance();

    std::vector<std::string> tokenize(const std::string & str);

private:
    ChineseTokenizer();

    std::unique_ptr<cppjieba::Jieba> jieba_instance;
};
}
