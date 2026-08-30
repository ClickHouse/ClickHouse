#include <string_view>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/StringUtils.h>
#include <Common/TLDListsHolder.h>
#include <Common/logger_useful.h>

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
    , memory_pool(std::make_unique<Arena>())
{
}

void TLDList::insert(const String & host, TLDType type)
{
    std::string_view owned_host{memory_pool->insert(host.data(), host.size()), host.size()};
    auto hash_fn = [](const char * data, size_t size) { return static_cast<uint32_t>(StringViewHash()(std::string_view(data, size))); };
    tld_container.insertIfNotPresent(PackedStringRef::build(owned_host.data(), owned_host.size(), hash_fn), type);
}

TLDType TLDList::lookup(std::string_view host) const
{
    auto hash_fn = [](const char * data, size_t size) { return static_cast<uint32_t>(StringViewHash()(std::string_view(data, size))); };
    auto key = PackedStringRef::build(host.data(), host.size(), hash_fn);
    if (const auto * it = tld_container.find(key); it != nullptr)
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

    LoggerPtr log = getLogger("TLDListsHolder");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not all list had been read: {}", name);

    TLDList tld_list(tld_list_tmp.size());
    for (const auto & [host, type] : tld_list_tmp)
    {
        tld_list.insert(host, type);
    }

    size_t tld_list_size = tld_list.size();
    std::lock_guard lock(tld_lists_map_mutex);
    tld_lists_map.insert(std::make_pair(name, std::move(tld_list)));
    return tld_list_size;
}

const TLDList & TLDListsHolder::getTldList(const std::string & name)
{
    std::lock_guard lock(tld_lists_map_mutex);
    auto it = tld_lists_map.find(name);
    if (it == tld_lists_map.end())
        throw Exception(ErrorCodes::TLD_LIST_NOT_FOUND, "TLD list {} does not exist", name);
    return it->second;
}

}
