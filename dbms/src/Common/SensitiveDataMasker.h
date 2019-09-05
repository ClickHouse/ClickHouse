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

namespace DB
{
class SensitiveDataMasker
{
private:
    class MaskingRule;
    std::vector<std::unique_ptr<MaskingRule>> all_masking_rules;

public:
    SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
    ~SensitiveDataMasker();

    /// Returns the number of matched rules.
    size_t wipeSensitiveData(std::string & data) const;

    /// Used in tests.
    void addMaskingRule(const std::string & name, const std::string & regexp_string, const std::string & replacement_string);

#ifndef NDEBUG
    void printStats();
#endif

    size_t rulesCount() const;
};

};
