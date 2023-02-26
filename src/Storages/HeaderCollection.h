#pragma once
#include <string>

namespace DB
{

struct HttpHeader
{
    std::string name;
    std::string value;

    HttpHeader(const std::string & name_, const std::string & value_) : name(name_), value(value_) {}
    inline bool operator==(const HttpHeader & other) const { return name == other.name && value == other.value; }
};

using HeaderCollection = std::vector<HttpHeader>;

}
