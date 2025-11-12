#pragma once

#include <jieba_dict.h>

namespace Jieba
{

class Jieba
{
public:
    std::vector<std::string_view> cut(std::string_view sentence);
    std::vector<std::string_view> cutForSearch(std::string_view sentence);
    std::vector<std::string_view> cutAll(std::string_view sentence);

private:
    template <typename Segment>
    std::vector<std::string_view> cutImpl(std::string_view sentence);

    DartsDict dict;
};

}
