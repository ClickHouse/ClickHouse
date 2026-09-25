#pragma once

#include <Core/Types_fwd.h>

#include <mutex>
#include <unordered_set>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/** A read-only proxy for a configuration, remembering which keys have been read through it.
  *
  * It answers the question: which elements of the configuration does nothing know about?
  * An element that is present in the configuration but is never read does nothing at all,
  * and it is almost always a typo or an invented name, which is better to report
  * than to silently ignore.
  *
  * Enumerating the keys of a section (`keys`) is not a read of these keys:
  * the code that enumerates a section still has to read the values it is interested in.
  *
  * The proxy shares the ownership of the configuration behind it: a disk keeps the proxy it was
  * created from and reads it later (see `HDFSObjectStorage`), while the configuration it was created
  * from can be gone by then - a disk defined in a query has its own temporary configuration,
  * and the configuration of the server is replaced on reload.
  */
class ConfigurationWithUsageTracking : public Poco::Util::AbstractConfiguration
{
public:
    explicit ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_);

    ~ConfigurationWithUsageTracking() override;

    /// Remember a key as used, for the keys that are read by someone else, not through this object.
    void markAsUsed(const String & key) const;

    /// The keys that have been read through this object or marked as used, in a normalized form.
    /// Only the set of names is returned: it does not touch the configuration behind this object,
    /// which makes it usable when that configuration is already gone (after a configuration reload).
    std::unordered_set<String> getUsedKeys() const;

    /// The leaf keys inside `prefix` that were neither read through this object nor marked as used.
    /// An empty prefix means the whole configuration. The names are returned relative to `prefix`.
    /// Reading a section itself does not make the keys inside it used: `has` of a section is only
    /// a check that the code is going to descend into it, and it still has to read every key it
    /// supports, so a typo inside a section has to be reported as well.
    /// With `skip_used_sections`, the keys inside a section that has been read are not reported:
    /// this gives the keys that are unknown for sure, without knowing what reads a section.
    Strings getUnusedKeys(const String & prefix, bool skip_used_sections = false) const;

protected:
    bool getRaw(const std::string & key, std::string & value) const override;
    void setRaw(const std::string & key, const std::string & value) override;
    void enumerate(const std::string & key, Keys & range) const override;

private:
    /// A reference is held on it (`duplicate` in the constructor, `release` in the destructor).
    const Poco::Util::AbstractConfiguration & config;

    mutable std::mutex mutex;
    mutable std::unordered_set<String> used_keys;

    bool isUsed(const String & key) const;
    void collectUnusedKeys(const String & prefix, const String & relative_key, bool skip_used_sections, Strings & result) const;
};

}
