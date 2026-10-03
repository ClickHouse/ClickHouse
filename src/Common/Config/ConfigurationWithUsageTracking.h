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

    /// What has been done with the configuration through this object, in a normalized form of the keys.
    /// Only the names are kept: it does not touch the configuration behind this object, which makes it
    /// usable when that configuration is already gone (after a configuration reload).
    struct Usage
    {
        /// The keys that have been read or marked as used (including the absent ones).
        std::unordered_set<String> used;
        /// The keys whose children have been enumerated with `keys`.
        std::unordered_set<String> enumerated;
        /// The keys that were present: read and found, or listed by an enumeration.
        std::unordered_set<String> present;
    };

    Usage getUsage() const;

    /// The leaf keys inside `prefix` that were neither read through this object nor marked as used.
    /// An empty prefix means the whole configuration. The names are returned relative to `prefix`.
    /// Reading a section itself does not make the keys inside it used: `has` of a section is only
    /// a check that the code is going to descend into it, and it still has to read every key it
    /// supports, so a typo inside a section has to be reported as well.
    Strings getUnusedKeys(const String & prefix) const;

    /// The leaf keys inside `prefix` that are unknown for sure, judging only by how another configuration
    /// was read before (`previous`, the usage of the configuration a disk was created from). It is done
    /// before the code that reads this configuration runs, so everything that code may read is not reported:
    /// - a leaf inside an enumerated section: it may be read by a pattern of its name (such as `key[1]`);
    /// - the inside of a section that was not present before but was looked at (such as `proxy`) or listed
    ///   in an enumerated section (such as a new location): nothing has read its keys yet.
    /// The keys used through this object (including the ones marked from `previous`) count as read.
    Strings getUnknownKeys(const String & prefix, const Usage & previous) const;

protected:
    bool getRaw(const std::string & key, std::string & value) const override;
    void setRaw(const std::string & key, const std::string & value) override;
    void enumerate(const std::string & key, Keys & range) const override;

private:
    /// A reference is held on it (`duplicate` in the constructor, `release` in the destructor).
    const Poco::Util::AbstractConfiguration & config;

    mutable std::mutex mutex;
    mutable Usage usage;

    bool isUsed(const String & key) const;
    void collectUnusedKeys(const String & prefix, const String & relative_key, const Usage * previous, Strings & result) const;
};

}
