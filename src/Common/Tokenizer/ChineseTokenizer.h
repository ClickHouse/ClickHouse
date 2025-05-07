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

/// The wrapper around the cppjieba library to tokenize a Chinese text into tokens.
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
