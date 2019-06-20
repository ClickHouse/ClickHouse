#pragma once

#include <memory>
#include <vector>

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
    void addMaskingRule(const std::string & name, const std::string & regexp_string, const std::string & replacement_string);
    int wipeSensitiveData(std::string & data) const;

#ifndef NDEBUG
    void printStats();
#endif

    unsigned long rulesCount() const;
};

};
