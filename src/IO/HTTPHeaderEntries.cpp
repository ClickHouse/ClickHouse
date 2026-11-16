#include <IO/HTTPHeaderEntries.h>

#include <algorithm>
#include <cctype>

namespace DB
{

void normalizeHeaderNames(HTTPHeaderEntries & headers)
{
    for (auto & header : headers)
        std::transform(header.name.begin(), header.name.end(), header.name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
}

}
