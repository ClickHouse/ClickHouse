#include <Common/TLDListsHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int TLD_LIST_NOT_FOUND;
}

///
/// TLDList
///
bool TLDList::insert(const StringRef & host)
{
    StringRefHash hash;
    return tld_container.insert(hash(host)).second;
}
bool TLDList::has(const StringRef & host) const
{
    StringRefHash hash;
    return tld_container.has(hash(host));
}

///
/// TLDListsHolder
///
TLDListsHolder & TLDListsHolder::getInstance()
{
    static TLDListsHolder instance;
    return instance;
}
TLDListsHolder::TLDListsHolder() = default;

void TLDListsHolder::parseConfig(const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys("top_level_domains_lists", config_keys);

    Poco::Logger * log = &Poco::Logger::get("TLDListsHolder");

    for (const auto & key : config_keys)
    {
        auto path = config.getString("top_level_domains_lists." + key);
        LOG_TRACE(log, "{} loading from {}", key, path);
        size_t hosts = parseAndAddTldList(key, path);
        LOG_INFO(log, "{} was added ({} hosts)", key, hosts);
    }
}

size_t TLDListsHolder::parseAndAddTldList(const std::string & name, const std::string & path)
{
    TLDList tld_list;

    ReadBufferFromFile in(path);
    while (!in.eof())
    {
        char * newline = find_first_symbols<'\n'>(in.position(), in.buffer().end());
        if (newline >= in.buffer().end())
            break;

        std::string_view line(in.position(), newline - in.position());
        in.position() = newline + 1;

        /// Skip comments
        if (line.size() > 2 && line[0] == '/' && line[1] == '/')
            continue;
        trim(line);
        /// Skip empty line
        if (line.empty())
            continue;
        tld_list.insert(StringRef{line.data(), line.size()});
    }

    size_t tld_list_size = tld_list.size();
    std::lock_guard<std::mutex> lock(tld_lists_map_mutex);
    tld_lists_map.emplace(name, std::move(tld_list));
    return tld_list_size;
}

const TLDList & TLDListsHolder::getTldList(const std::string & name)
{
    std::lock_guard<std::mutex> lock(tld_lists_map_mutex);
    auto it = tld_lists_map.find(name);
    if (it == tld_lists_map.end())
        throw Exception(ErrorCodes::TLD_LIST_NOT_FOUND, "TLD list {} does not exist", name);
    return it->second;
}

}
