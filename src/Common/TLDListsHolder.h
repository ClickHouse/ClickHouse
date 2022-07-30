#pragma once

#include <base/defines.h>
#include <base/StringRef.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/Arena.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

enum TLDType
{
    /// Does not exist marker
    TLD_NONE,
    /// For regular lines
    TLD_REGULAR,
    /// For asterisk (*)
    TLD_ANY,
    /// For exclamation mark (!)
    TLD_EXCLUDE,
};

/// Custom TLD List
///
/// Unlike tldLookup (which uses gperf) this one uses plain StringHashMap.
class TLDList
{
public:
    using Container = StringHashMap<TLDType>;

    explicit TLDList(size_t size);

    void insert(const String & host, TLDType type);
    TLDType lookup(StringRef host) const;
    size_t size() const { return tld_container.size(); }

private:
    Container tld_container;
    std::unique_ptr<Arena> memory_pool;
};

class TLDListsHolder
{
public:
    using Map = std::unordered_map<std::string, TLDList>;

    static TLDListsHolder & getInstance();

    /// Parse "top_level_domains_lists" section,
    /// And add each found dictionary.
    void parseConfig(const std::string & top_level_domains_path, const Poco::Util::AbstractConfiguration & config);

    /// Parse file and add it as a Set to the list of TLDs
    /// - "//" -- comment,
    /// - empty lines will be ignored.
    ///
    /// Treats the following special symbols:
    /// - "*"
    /// - "!"
    ///
    /// Format : https://github.com/publicsuffix/list/wiki/Format
    /// Example: https://publicsuffix.org/list/public_suffix_list.dat
    ///
    /// Return size of the list.
    size_t parseAndAddTldList(const std::string & name, const std::string & path);
    /// Throws TLD_LIST_NOT_FOUND if list does not exist
    const TLDList & getTldList(const std::string & name);

protected:
    TLDListsHolder();

    std::mutex tld_lists_map_mutex;
    Map tld_lists_map TSA_GUARDED_BY(tld_lists_map_mutex);
};

}
