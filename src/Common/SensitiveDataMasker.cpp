#include "SensitiveDataMasker.h"

#include <set>
#include <string>
#include <atomic>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <Common/logger_useful.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>

#ifndef NDEBUG
#    include <iostream>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int LOGICAL_ERROR;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

class SensitiveDataMasker::MaskingRule
{
private:
    const std::string name;
    const std::string replacement_string;
    const std::string regexp_string;

    const RE2 regexp;
    const re2::StringPiece replacement;

#ifndef NDEBUG
    mutable std::atomic<std::uint64_t> matches_count = 0;
#endif

public:
    //* TODO: option with hyperscan? https://software.intel.com/en-us/articles/why-and-how-to-replace-pcre-with-hyperscan
    // re2::set should also work quite fast, but it doesn't return the match position, only which regexp was matched

    MaskingRule(const std::string & name_, const std::string & regexp_string_, const std::string & replacement_string_)
        : name(name_)
        , replacement_string(replacement_string_)
        , regexp_string(regexp_string_)
        , regexp(regexp_string, RE2::Quiet)
        , replacement(replacement_string)
    {
        if (!regexp.ok())
            throw DB::Exception(
                "SensitiveDataMasker: cannot compile re2: " + regexp_string_ + ", error: " + regexp.error()
                    + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
                DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }

    uint64_t apply(std::string & data) const
    {
        auto m = RE2::GlobalReplace(&data, regexp, replacement);
#ifndef NDEBUG
        matches_count += m;
#endif
        return m;
    }

    const std::string & getName() const { return name; }
    const std::string & getReplacementString() const { return replacement_string; }
#ifndef NDEBUG
    uint64_t getMatchesCount() const { return matches_count; }
#endif

};

SensitiveDataMasker::~SensitiveDataMasker() = default;

std::unique_ptr<SensitiveDataMasker> SensitiveDataMasker::sensitive_data_masker = nullptr;

void SensitiveDataMasker::setInstance(std::unique_ptr<SensitiveDataMasker> sensitive_data_masker_)
{
    if (!sensitive_data_masker_)
        throw Exception("Logical error: the 'sensitive_data_masker' is not set", ErrorCodes::LOGICAL_ERROR);

    if (sensitive_data_masker_->rulesCount() > 0)
    {
        sensitive_data_masker = std::move(sensitive_data_masker_);
    }
}

SensitiveDataMasker * SensitiveDataMasker::getInstance()
{
    return sensitive_data_masker.get();
}

SensitiveDataMasker::SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    Poco::Logger * logger = &Poco::Logger::get("SensitiveDataMaskerConfigRead");

    std::set<std::string> used_names;

    for (const auto & rule : keys)
    {
        if (startsWith(rule, "rule"))
        {
            auto rule_config_prefix = config_prefix + "." + rule;

            auto rule_name = config.getString(rule_config_prefix + ".name", rule_config_prefix);

            if (!used_names.insert(rule_name).second)
            {
                throw Exception(
                    "query_masking_rules configuration contains more than one rule named '" + rule_name + "'.",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            }

            auto regexp = config.getString(rule_config_prefix + ".regexp", "");

            if (regexp.empty())
            {
                throw Exception(
                    "query_masking_rules configuration, rule '" + rule_name + "' has no <regexp> node or <regexp> is empty.",
                    ErrorCodes::NO_ELEMENTS_IN_CONFIG);
            }

            auto replace = config.getString(rule_config_prefix + ".replace", "******");

            try
            {
                addMaskingRule(rule_name, regexp, replace);
            }
            catch (DB::Exception & e)
            {
                e.addMessage("while adding query masking rule '" + rule_name + "'.");
                throw;
            }
        }
        else
        {
            LOG_WARNING(logger, "Unused param {}.{}", config_prefix, rule);
        }
    }

    auto rules_count = rulesCount();
    if (rules_count > 0)
        LOG_INFO(logger, "{} query masking rules loaded.", rules_count);
}

void SensitiveDataMasker::addMaskingRule(
    const std::string & name, const std::string & regexp_string, const std::string & replacement_string)
{
    all_masking_rules.push_back(std::make_unique<MaskingRule>(name, regexp_string, replacement_string));
}


size_t SensitiveDataMasker::wipeSensitiveData(std::string & data) const
{
    size_t matches = 0;
    for (const auto & rule : all_masking_rules)
        matches += rule->apply(data);
    return matches;
}

#ifndef NDEBUG
void SensitiveDataMasker::printStats()
{
    for (auto & rule : all_masking_rules)
    {
        std::cout << rule->getName() << " (replacement to " << rule->getReplacementString() << ") matched " << rule->getMatchesCount()
                  << " times" << std::endl;
    }
}
#endif

size_t SensitiveDataMasker::rulesCount() const
{
    return all_masking_rules.size();
}

}
