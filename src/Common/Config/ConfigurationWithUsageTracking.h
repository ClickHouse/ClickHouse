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
  */
class ConfigurationWithUsageTracking : public Poco::Util::AbstractConfiguration
{
public:
    explicit ConfigurationWithUsageTracking(const Poco::Util::AbstractConfiguration & config_);

    ~ConfigurationWithUsageTracking() override;

    /// Remember a key as used, for the keys that are read by someone else, not through this object.
    void markAsUsed(const String & key) const;

    /// The leaf keys inside `prefix` that were neither read through this object nor marked as used.
    /// An empty prefix means the whole configuration. The names are returned relative to `prefix`.
    /// Reading a section itself does not make the keys inside it used: `has` of a section is only
    /// a check that the code is going to descend into it, and it still has to read every key it
    /// supports, so a typo inside a section has to be reported as well.
    Strings getUnusedKeys(const String & prefix) const;

protected:
    bool getRaw(const std::string & key, std::string & value) const override;
    void setRaw(const std::string & key, const std::string & value) override;
    void enumerate(const std::string & key, Keys & range) const override;

private:
    const Poco::Util::AbstractConfiguration & config;

    mutable std::mutex mutex;
    mutable std::unordered_set<String> used_keys;

    bool isUsed(const String & key) const;
    void collectUnusedKeys(const String & prefix, const String & relative_key, Strings & result) const;
};

}
