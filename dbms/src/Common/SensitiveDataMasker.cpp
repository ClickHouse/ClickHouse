#include "SensitiveDataMasker.h"

#include <set>
#include <string>
#include <atomic>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>

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

    mutable std::atomic<std::uint64_t> matches_count = 0;

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

    int apply(std::string & data) const
    {
        auto m = RE2::GlobalReplace(&data, regexp, replacement);
        matches_count += m;
        return m;
    }

    const std::string & getName() const { return name; }
    const std::string & getReplacementString() const { return replacement_string; }
    uint64_t getMatchesCount() const { return matches_count; }
};

SensitiveDataMasker::SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    Logger * logger = &Logger::get("SensitiveDataMaskerConfigRead");

    std::set<std::string> used_names;

    for (const auto & rule : keys)
    {
        if (startsWith(rule, "rule"))
        {
            auto rule_config_prefix = config_prefix + "." + rule;

            auto rule_name = config.getString(rule_config_prefix + ".name", rule_config_prefix);

            if (used_names.count(rule_name) == 0)
            {
                used_names.insert(rule_name);
            }
            else
            {
                throw Exception(
                    "query_masking_rules configuration contains more than one rule named '" + rule_name + "'.",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            }

            auto regexp = config.getString(rule_config_prefix + ".regexp", "");

            if (regexp == "")
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
            LOG_WARNING(logger, "Unused param " << config_prefix << '.' << rule);
        }
    }
    auto rules_count = this->rulesCount();
    if (rules_count > 0)
    {
        LOG_INFO(logger, rules_count << " query masking rules loaded.");
    }
}

SensitiveDataMasker::~SensitiveDataMasker() {}

void SensitiveDataMasker::addMaskingRule(
    const std::string & name, const std::string & regexp_string, const std::string & replacement_string)
{
    all_masking_rules.push_back(std::make_unique<MaskingRule>(name, regexp_string, replacement_string));
}


int SensitiveDataMasker::wipeSensitiveData(std::string & data) const
{
    int matches = 0;
    for (auto & rule : all_masking_rules)
    {
        matches += rule->apply(data);
    }
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

unsigned long SensitiveDataMasker::rulesCount() const
{
    return all_masking_rules.size();
}

}
