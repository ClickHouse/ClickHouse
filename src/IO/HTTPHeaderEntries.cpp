#include <IO/HTTPHeaderEntries.h>

#include <algorithm>
#include <cctype>
#include <utility>

namespace DB
{

namespace
{

void toLowerInPlace(std::string & name)
{
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
}

}

NormalizedHTTPHeaderEntries::NormalizedHTTPHeaderEntries(const HTTPHeaderEntries & headers)
{
    entries.reserve(headers.size());
    for (const auto & header : headers)
        push_back(header);
}

void NormalizedHTTPHeaderEntries::push_back(HTTPHeaderEntry entry)
{
    toLowerInPlace(entry.name);
    entries.push_back(std::move(entry));
}

void NormalizedHTTPHeaderEntries::append(const HTTPHeaderEntries & headers)
{
    entries.reserve(entries.size() + headers.size());
    for (const auto & header : headers)
        push_back(header);
}

void NormalizedHTTPHeaderEntries::append(const NormalizedHTTPHeaderEntries & headers)
{
    entries.insert(entries.end(), headers.entries.begin(), headers.entries.end());
}

void NormalizedHTTPHeaderEntries::eraseByName(std::string_view name)
{
    std::string lower_name(name);
    toLowerInPlace(lower_name);
    std::erase_if(entries, [&](const HTTPHeaderEntry & entry) { return entry.name == lower_name; });
}

}
