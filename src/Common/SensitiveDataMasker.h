#pragma once

#include <memory>
#include <vector>
#include <cstdint>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

/// SensitiveDataMasker allows to remove sensitive data from queries using set of regexp-based rules

/// It's used as a singleton via getInstance method

/// Initially it's empty (nullptr) and after manual initialization
/// (one-time, done by setInstance call) it takes the proper value which
/// is stored in unique_ptr.

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
///    context destruction can't be done w/o synchronization / locks in a safe manner.
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
    std::vector<std::unique_ptr<MaskingRule>> all_masking_rules;
    static std::unique_ptr<SensitiveDataMasker> sensitive_data_masker;

public:
    SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
    ~SensitiveDataMasker();

    /// Returns the number of matched rules.
    size_t wipeSensitiveData(std::string & data) const;

    /// setInstance is not thread-safe and should be called once in single-thread mode.
    /// https://github.com/ClickHouse/ClickHouse/pull/6810#discussion_r321183367
    static void setInstance(std::unique_ptr<SensitiveDataMasker> sensitive_data_masker_);
    static SensitiveDataMasker * getInstance();

    /// Used in tests.
    void addMaskingRule(const std::string & name, const std::string & regexp_string, const std::string & replacement_string);

#ifndef NDEBUG
    void printStats();
#endif

    size_t rulesCount() const;
};

};
