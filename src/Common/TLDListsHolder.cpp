#include <Common/TLDListsHolder.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <string_view>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int TLD_LIST_NOT_FOUND;
    extern const int LOGICAL_ERROR;
}

/// TLDList
TLDList::TLDList(size_t size)
    : tld_container(size)
    , pool(std::make_unique<Arena>(10 << 20))
{}
bool TLDList::insert(StringRef host)
{
    bool inserted;
    tld_container.emplace(DB::ArenaKeyHolder{host, *pool}, inserted);
    return inserted;
}
bool TLDList::has(StringRef host) const
{
    return tld_container.has(host);
}

/// TLDListsHolder
TLDListsHolder & TLDListsHolder::getInstance()
{
    static TLDListsHolder instance;
    return instance;
}
TLDListsHolder::TLDListsHolder() = default;

void TLDListsHolder::parseConfig(const std::string & top_level_domains_path, const Poco::Util::AbstractConfiguration & config)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys("top_level_domains_lists", config_keys);

    Poco::Logger * log = &Poco::Logger::get("TLDListsHolder");

    for (const auto & key : config_keys)
    {
        const std::string & path = top_level_domains_path + config.getString("top_level_domains_lists." + key);
        LOG_TRACE(log, "{} loading from {}", key, path);
        size_t hosts = parseAndAddTldList(key, path);
        LOG_INFO(log, "{} was added ({} hosts)", key, hosts);
    }
}

size_t TLDListsHolder::parseAndAddTldList(const std::string & name, const std::string & path)
{
    std::unordered_set<std::string> tld_list_tmp;

    ReadBufferFromFile in(path);
    String line;
    while (!in.eof())
    {
        readEscapedStringUntilEOL(line, in);
        if (!in.eof())
            ++in.position();
        /// Skip comments
        if (line.size() > 2 && line[0] == '/' && line[1] == '/')
            continue;
        line = trim(line, [](char c) { return std::isspace(c); });
        /// Skip empty line
        if (line.empty())
            continue;
        tld_list_tmp.emplace(line);
    }
    if (!in.eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not all list had been read", name);

    TLDList tld_list(tld_list_tmp.size());
    for (const auto & host : tld_list_tmp)
    {
        StringRef host_ref{host.data(), host.size()};
        tld_list.insert(host_ref);
    }

    size_t tld_list_size = tld_list.size();
    std::lock_guard<std::mutex> lock(tld_lists_map_mutex);
    tld_lists_map.insert(std::make_pair(name, std::move(tld_list)));
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
