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

constexpr size_t StringHashTablePadRequirement = 8;

/// TLDList
TLDList::TLDList(size_t size)
    : tld_container(size)
    , memory_pool(std::make_unique<Arena>())
{
    /// StringHashTable requires padded to 8 bytes key,
    /// and Arena (memory_pool here) does satisfies this,
    /// since it has padding with 15 bytes at the right.
    ///
    /// However, StringHashTable may reference -1 byte of the key,
    /// so left padding is also required:
    memory_pool->alignedAlloc(StringHashTablePadRequirement, StringHashTablePadRequirement);
}
void TLDList::insert(const String & host, TLDType type)
{
    StringRef owned_host{memory_pool->insert(host.data(), host.size()), host.size()};
    tld_container[owned_host] = type;
}
TLDType TLDList::lookup(StringRef host) const
{
    if (auto it = tld_container.find(host); it != nullptr)
        return it->getMapped();
    return TLDType::TLD_NONE;
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
    std::unordered_map<std::string, TLDType> tld_list_tmp;

    ReadBufferFromFile in(path);
    String buffer;
    while (!in.eof())
    {
        readEscapedStringUntilEOL(buffer, in);
        if (!in.eof())
            ++in.position();
        std::string_view line(buffer);
        /// Skip comments
        if (line.starts_with("//"))
            continue;
        line = line.substr(0, line.rend() - std::find_if_not(line.rbegin(), line.rend(), ::isspace));
        /// Skip empty line
        if (line.empty())
            continue;
        /// Validate special symbols.
        if (line.starts_with("*."))
        {
            line = line.substr(2);
            tld_list_tmp.emplace(line, TLDType::TLD_ANY);
        }
        else if (line[0] == '!')
        {
            line = line.substr(1);
            tld_list_tmp.emplace(line, TLDType::TLD_EXCLUDE);
        }
        else
            tld_list_tmp.emplace(line, TLDType::TLD_REGULAR);
    }
    if (!in.eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not all list had been read", name);

    TLDList tld_list(tld_list_tmp.size());
    for (const auto & [host, type] : tld_list_tmp)
    {
        tld_list.insert(host, type);
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
