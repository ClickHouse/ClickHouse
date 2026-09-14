#include <IO/HTTPHeaderEntries.h>

#include <algorithm>
#include <cctype>
#include <utility>

namespace DB
{

NormalizedHTTPHeaderEntries::NormalizedHTTPHeaderEntries(const HTTPHeaderEntries & headers)
{
    entries.reserve(headers.size());
    for (const auto & header : headers)
        push_back(header);
}

void NormalizedHTTPHeaderEntries::push_back(HTTPHeaderEntry entry)
{
    std::transform(entry.name.begin(), entry.name.end(), entry.name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    entries.push_back(std::move(entry));
}

}
