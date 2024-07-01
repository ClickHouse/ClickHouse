#include "SensitiveDataMasker.h"

#include <set>
#include <string>
#include <atomic>

#include <Poco/Util/AbstractConfiguration.h>

#include <Common/logger_useful.h>
#include <Common/re2.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/ProfileEvents.h>

#ifndef NDEBUG
#    include <iostream>
#endif


namespace ProfileEvents
{
    extern const Event QueryMaskingRulesMatch;
}


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
    const std::string_view replacement;

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
            throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
                "SensitiveDataMasker: cannot compile re2: {}, error: {}. "
                "Look at https://github.com/google/re2/wiki/Syntax for reference.",
                regexp_string_, regexp.error());
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

SensitiveDataMasker::MaskerMultiVersion SensitiveDataMasker::sensitive_data_masker{};

void SensitiveDataMasker::setInstance(std::unique_ptr<SensitiveDataMasker>&& sensitive_data_masker_)
{

    if (!sensitive_data_masker_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The 'sensitive_data_masker' is not set");

    if (sensitive_data_masker_->rulesCount() > 0)
    {
        sensitive_data_masker.set(std::move(sensitive_data_masker_));
    }
    else
    {
        sensitive_data_masker.set(nullptr);
    }
}

SensitiveDataMasker::MaskerMultiVersion::Version SensitiveDataMasker::getInstance()
{
    return sensitive_data_masker.get();
}

SensitiveDataMasker::SensitiveDataMasker(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    LoggerPtr logger = getLogger("SensitiveDataMaskerConfigRead");

    std::set<std::string> used_names;

    for (const auto & rule : keys)
    {
        if (startsWith(rule, "rule"))
        {
            auto rule_config_prefix = config_prefix + "." + rule;

            auto rule_name = config.getString(rule_config_prefix + ".name", rule_config_prefix);

            if (!used_names.insert(rule_name).second)
            {
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                                "query_masking_rules configuration contains more than one rule named '{}'.", rule_name);
            }

            auto regexp = config.getString(rule_config_prefix + ".regexp", "");

            if (regexp.empty())
            {
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                                "query_masking_rules configuration, rule '{}' has no <regexp> node or <regexp> "
                                "is empty.", rule_name);
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

    if (matches)
        ProfileEvents::increment(ProfileEvents::QueryMaskingRulesMatch, matches);

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


std::string wipeSensitiveDataAndCutToLength(const std::string & str, size_t max_length)
{
    std::string res = str;

    if (auto masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(res);

    size_t length = res.length();
    if (max_length && (length > max_length))
    {
        constexpr size_t max_extra_msg_len = sizeof("... (truncated 18446744073709551615 characters)");
        if (max_length < max_extra_msg_len)
            return "(removed " + std::to_string(length) + " characters)";
        max_length -= max_extra_msg_len;
        res.resize(max_length);
        res.append("... (truncated " + std::to_string(length - max_length) +  " characters)");
    }

    return res;
}

}
