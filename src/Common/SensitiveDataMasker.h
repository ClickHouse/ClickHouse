#pragma once

#include <memory>
#include <mutex>
#include <vector>
#include <base/defines.h>
#include <Common/MultiVersion.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

/// SensitiveDataMasker allows to remove sensitive data from queries using set of regexp-based rules

/// It's used as a singleton via getInstance method

/// Initially it's empty (nullptr). The published instance holds the rules from the server config
/// (set by setInstance) followed by the SQL-defined rules (set by setSQLDefinedRules);
/// each call replaces only its own list.

/// It looks like the singleton is the best option here, as
/// two users of that object (OwnSplitChannel & Interpreters/executeQuery)
/// can't own/share that Masker properly without synchronization & locks,
/// and we can't afford setting global locks for each logged line.

/// I've considered singleton alternatives, but it's unclear who should own the object,
/// and it introduce unnecessary complexity in implementation (passing references back and forward):
///
///  context can't own, as Context is destroyed before logger,
///    and logger lives longer and logging can still happen after Context destruction.
///    resetting masker in the logger at the moment of
///    context destruction can't be done without synchronization / locks in a safe manner.
///
///  logger is Poco derived and i didn't want to brake it's interface,
///    also logger can be dynamically reconfigured without server restart,
///    and it actually recreates OwnSplitChannel when reconfiguration happen,
///    so that makes it's quite tricky. So it a bad candidate for owning masker too.

namespace DB
{
class SensitiveDataMasker
{
private:
    class MaskingRule;
    using MaskingRules = std::vector<std::shared_ptr<const MaskingRule>>;
    MaskingRules all_masking_rules;
    using MaskerMultiVersion = MultiVersion<SensitiveDataMasker>;
    static MaskerMultiVersion sensitive_data_masker;

    static std::mutex rule_sources_mutex;
    static MaskingRules config_rules TSA_GUARDED_BY(rule_sources_mutex);
    static MaskingRules sql_defined_rules TSA_GUARDED_BY(rule_sources_mutex);
    static void publishRules() TSA_REQUIRES(rule_sources_mutex);

public:
    SensitiveDataMasker();
    SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
    ~SensitiveDataMasker();

    /// Returns the number of matched rules.
    size_t wipeSensitiveData(std::string & data) const;
    size_t wipeSensitiveDataThrow(std::string & data) const;

    /// Replaces the rules from the server config.
    static void setInstance(std::unique_ptr<SensitiveDataMasker>&& sensitive_data_masker_);
    /// Replaces the rules defined by CREATE QUERY MASKING RULE.
    static void setSQLDefinedRules(std::unique_ptr<SensitiveDataMasker>&& sql_defined_masker);
    static MaskerMultiVersion::Version getInstance();

    /// Throws if the regexp is empty or cannot be compiled, or the replacement is not a valid rewrite for it.
    /// The error messages never contain any part of the regexp.
    static void validateRule(const std::string & rule_description, const std::string & regexp_string, const std::string & replacement_string);

    void addMaskingRule(
        const std::string & name,
        const std::string & regexp_string,
        const std::string & replacement_string,
        bool throw_on_match
    );

#ifndef NDEBUG
    void printStats();
#endif

    size_t rulesCount() const;
};

/// Wipes sensitive data and cuts to a specified maximum length in one function call.
/// If the maximum length is zero then the function doesn't cut to the maximum length.
std::string wipeSensitiveDataAndCutToLength(std::string str, size_t max_length, bool wipe_sensitive);

}
