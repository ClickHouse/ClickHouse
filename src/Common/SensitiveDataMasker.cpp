#include <Common/SensitiveDataMasker.h>

#include <array>
#include <atomic>
#include <set>
#include <string>
#include <string_view>

#include <Poco/Util/AbstractConfiguration.h>

#include <Common/logger_useful.h>
#include <Common/re2.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>

#ifndef NDEBUG
#include <iostream>
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

    const bool throw_on_match;

    const RE2 regexp;
    const std::string_view replacement;

#ifndef NDEBUG
    mutable std::atomic<std::uint64_t> matches_count = 0;
#endif

public:
    //* TODO: option with hyperscan? https://software.intel.com/en-us/articles/why-and-how-to-replace-pcre-with-hyperscan
    // re2::set should also work quite fast, but it doesn't return the match position, only which regexp was matched

    MaskingRule(
        const std::string & name_, const std::string & regexp_string_, const std::string & replacement_string_, bool throw_on_match_)
        : name(name_)
        , replacement_string(replacement_string_)
        , regexp_string(regexp_string_)
        , throw_on_match(throw_on_match_)
        , regexp(regexp_string, RE2::Quiet)
        , replacement(replacement_string)
    {
        if (!regexp.ok())
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP,
                "SensitiveDataMasker: cannot compile re2: {}, error: {}. "
                "Look at https://github.com/google/re2/wiki/Syntax for reference.",
                regexp_string_,
                regexp.error());
    }

    uint64_t applyThrow(std::string & data) const
    {
        auto m = RE2::GlobalReplace(&data, regexp, replacement);

        if (throw_on_match && m > 0)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The rule {} was triggered on the log line {}", name, data);
        }


#ifndef NDEBUG
        matches_count += m;
#endif
        return m;
    }

    uint64_t applyNoThrow(std::string & data) const
    {
        /// FIXME(nikitamikhylov): There is a misuse of the SensitiveDataMasker class.
        /// It is used in some places where we serialize a query to the storage (Keeper for example).
        /// Effectively breaking it by wiping the crucial information (credentials, etc).
        /// So, rules that may touch a query (those that mask passwords) should be checked in `applyThrow` method only.
        if (throw_on_match)
            return 0;

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

namespace
{

bool isAsciiWordCharacter(char character)
{
    return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9')
        || character == '_';
}

char foldAscii(char character)
{
    if (character >= 'a' && character <= 'z')
        return static_cast<char>(character - ('a' - 'A'));
    return character;
}

bool matchKeyword(std::string_view text, size_t & position, std::string_view keyword)
{
    if (position > 0 && isAsciiWordCharacter(text[position - 1]))
        return false;
    if (keyword.size() > text.size() - position)
        return false;

    for (size_t index = 0; index < keyword.size(); ++index)
        if (foldAscii(text[position + index]) != keyword[index])
            return false;

    const size_t end = position + keyword.size();
    if (end < text.size() && isAsciiWordCharacter(text[end]))
        return false;
    position = end;
    return true;
}

void skipSqlTrivia(std::string_view text, size_t & position)
{
    while (position < text.size())
    {
        const char * whitespace_begin = text.data() + position;
        const char * whitespace_end = skipWhitespacesUTF8(whitespace_begin, text.data() + text.size());
        if (whitespace_end != whitespace_begin)
        {
            position = static_cast<size_t>(whitespace_end - text.data());
            continue;
        }

        if (position + 1 < text.size()
            && ((text[position] == '-' && text[position + 1] == '-') || (text[position] == '/' && text[position + 1] == '/')))
        {
            position += 2;
            while (position < text.size() && text[position] != '\n' && text[position] != '\r')
                ++position;
            continue;
        }

        if (text[position] == '#' && position + 1 < text.size() && (text[position + 1] == ' ' || text[position + 1] == '!'))
        {
            ++position;
            while (position < text.size() && text[position] != '\n' && text[position] != '\r')
                ++position;
            continue;
        }

        if (position + 1 < text.size() && text[position] == '/' && text[position + 1] == '*')
        {
            position += 2;
            size_t nesting_level = 1;
            while (position + 1 < text.size() && nesting_level > 0)
            {
                if (text[position] == '/' && text[position + 1] == '*')
                {
                    position += 2;
                    ++nesting_level;
                }
                else if (text[position] == '*' && text[position + 1] == '/')
                {
                    position += 2;
                    --nesting_level;
                }
                else
                {
                    ++position;
                }
            }
            if (nesting_level > 0)
                position = text.size();
            continue;
        }

        break;
    }
}

struct PhysicalizationApplyTokenRange
{
    size_t begin = 0;
    size_t end = 0;
};

size_t skipLeadingSqlWhitespace(std::string_view text, size_t position)
{
    const char * begin = text.data() + position;
    return static_cast<size_t>(skipWhitespacesUTF8(begin, text.data() + text.size()) - text.data());
}

void skipQuotedSqlToken(std::string_view text, size_t & position)
{
    const char quote = text[position++];
    while (position < text.size())
    {
        if (text[position] == '\\' && position + 1 < text.size())
        {
            position += 2;
            continue;
        }
        if (text[position] != quote)
        {
            ++position;
            continue;
        }
        if (position + 1 < text.size() && text[position + 1] == quote)
        {
            position += 2;
            continue;
        }
        ++position;
        return;
    }
}

bool captureOpaqueTokenValue(std::string_view text, size_t position, PhysicalizationApplyTokenRange & token_range)
{
    const size_t token_trivia_begin = position;
    skipSqlTrivia(text, position);
    if (position >= text.size())
    {
        if (token_trivia_begin == text.size())
            return false;
        token_range = {.begin = skipLeadingSqlWhitespace(text, token_trivia_begin), .end = text.size()};
        return true;
    }

    const size_t token_begin = position;
    if (text[position] != '\'')
    {
        token_range = {.begin = token_begin, .end = text.size()};
        return true;
    }

    ++position;
    bool closed = false;
    while (position < text.size())
    {
        if (text[position] == '\\' && position + 1 < text.size())
        {
            position += 2;
            continue;
        }
        if (text[position] != '\'')
        {
            ++position;
            continue;
        }
        if (position + 1 < text.size() && text[position + 1] == '\'')
        {
            position += 2;
            continue;
        }
        ++position;
        closed = true;
        break;
    }

    token_range = {.begin = token_begin, .end = closed ? position : text.size()};
    return true;
}

bool findTokenAfterMalformedPhysicalizationPrefix(std::string_view text, size_t position, PhysicalizationApplyTokenRange & token_range)
{
    while (position < text.size())
    {
        const size_t trivia_begin = position;
        skipSqlTrivia(text, position);
        if (position >= text.size())
        {
            if (trivia_begin < text.size())
            {
                token_range = {.begin = trivia_begin, .end = text.size()};
                return true;
            }
            return false;
        }

        if (text[position] == ';')
            return false;
        if (text[position] == '\'' || text[position] == '"' || text[position] == '`')
        {
            skipQuotedSqlToken(text, position);
            continue;
        }

        size_t after_keyword = position;
        if (matchKeyword(text, after_keyword, "APPLY"))
        {
            if (after_keyword == text.size())
                return false;

            size_t after_token = after_keyword;
            skipSqlTrivia(text, after_token);
            if (matchKeyword(text, after_token, "TOKEN"))
                return captureOpaqueTokenValue(text, after_token, token_range);

            token_range = {.begin = skipLeadingSqlWhitespace(text, after_keyword), .end = text.size()};
            return true;
        }

        after_keyword = position;
        if (matchKeyword(text, after_keyword, "TOKEN"))
            return captureOpaqueTokenValue(text, after_keyword, token_range);

        if (isAsciiWordCharacter(text[position]))
        {
            do
                ++position;
            while (position < text.size() && isAsciiWordCharacter(text[position]));
        }
        else
        {
            ++position;
        }
    }

    return false;
}

bool findPhysicalizationApplyToken(std::string_view text, size_t command_begin, PhysicalizationApplyTokenRange & token_range)
{
    static constexpr std::array<std::string_view, 5> prefix{"PHYSICALIZE", "TYPE", "REFERENCES", "APPLY", "TOKEN"};

    size_t candidate = command_begin;
    skipSqlTrivia(text, candidate);
    if (candidate >= text.size() || !matchKeyword(text, candidate, prefix.front()))
        return false;

    for (size_t index = 1; index < prefix.size(); ++index)
    {
        const size_t trivia_begin = candidate;
        skipSqlTrivia(text, candidate);
        if (!matchKeyword(text, candidate, prefix[index]))
        {
            /// `APPLY` has no alternate non-token form. Once that keyword is
            /// established, fail closed on malformed trivia/lexemes before
            /// TOKEN because the remaining bytes may contain the opaque value.
            if (index + 1 == prefix.size() && trivia_begin < text.size())
            {
                const size_t sensitive_begin = candidate < text.size() ? candidate : skipLeadingSqlWhitespace(text, trivia_begin);
                token_range = {.begin = sensitive_begin, .end = text.size()};
                return true;
            }

            /// OBJECT/CLOSURE/DATABASE are the three non-secret dry-run
            /// branches. Any other malformed PHYSICALIZE prefix containing a
            /// TOKEN keyword is treated as an APPLY attempt and fails closed.
            if (index == 3)
            {
                size_t scope = candidate;
                if (matchKeyword(text, scope, "OBJECT") || matchKeyword(text, scope, "CLOSURE") || matchKeyword(text, scope, "DATABASE"))
                    return false;
            }
            return findTokenAfterMalformedPhysicalizationPrefix(text, trivia_begin, token_range);
        }
    }

    return captureOpaqueTokenValue(text, candidate, token_range);
}

size_t findNextTopLevelStatement(std::string_view text, size_t position)
{
    while (position < text.size())
    {
        const size_t before_trivia = position;
        skipSqlTrivia(text, position);
        if (position != before_trivia)
            continue;

        if (text[position] == '\'' || text[position] == '"' || text[position] == '`')
        {
            skipQuotedSqlToken(text, position);
            continue;
        }

        if (text[position] == ';')
            return position + 1;

        ++position;
    }

    return std::string_view::npos;
}

/// Successful APPLY ASTs redact themselves. This pre-parse pass additionally
/// protects malformed/truncated statements whose AST was never published to
/// executeQuery's logging path.
template <typename Callback>
void forEachPhysicalizationApplyToken(std::string_view text, Callback && callback)
{
    size_t command_begin = 0;
    while (command_begin < text.size())
    {
        PhysicalizationApplyTokenRange token_range;
        if (findPhysicalizationApplyToken(text, command_begin, token_range))
        {
            callback(token_range);
            command_begin = token_range.end;
        }

        /// Avoid the quote/comment-aware scan for the overwhelmingly common
        /// single-statement case.
        if (text.find(';', command_begin) == std::string::npos)
            return;

        command_begin = findNextTopLevelStatement(text, command_begin);
        if (command_begin == std::string_view::npos)
            return;
    }
}

void maskPhysicalizationApplyTokensImpl(std::string & text)
{
    static constexpr std::string_view replacement = "'[HIDDEN]'";

    size_t output_size = text.size();
    bool found = false;
    bool size_overflow = false;
    forEachPhysicalizationApplyToken(
        text,
        [&](PhysicalizationApplyTokenRange token_range)
        {
            found = true;
            const size_t token_size = token_range.end - token_range.begin;
            if (token_size < replacement.size())
            {
                const size_t growth = replacement.size() - token_size;
                if (growth > text.max_size() - output_size)
                    size_overflow = true;
                else
                    output_size += growth;
            }
            else
            {
                output_size -= token_size - replacement.size();
            }
        });

    if (!found)
        return;
    if (size_overflow)
    {
        text.assign(replacement);
        return;
    }

    std::string masked;
    masked.reserve(output_size);
    size_t copied_until = 0;
    forEachPhysicalizationApplyToken(
        text,
        [&](PhysicalizationApplyTokenRange token_range)
        {
            masked.append(text, copied_until, token_range.begin - copied_until);
            masked.append(replacement);
            copied_until = token_range.end;
        });
    masked.append(text, copied_until, text.size() - copied_until);
    text = std::move(masked);
}

}

void maskPhysicalizationApplyTokens(std::string & text)
{
    maskPhysicalizationApplyTokensImpl(text);
}

bool containsPhysicalizationApplyToken(std::string_view text)
{
    PhysicalizationApplyTokenRange token_range;
    return findPhysicalizationApplyToken(text, 0, token_range);
}

SensitiveDataMasker::~SensitiveDataMasker() = default;

SensitiveDataMasker::MaskerMultiVersion SensitiveDataMasker::sensitive_data_masker{};

void SensitiveDataMasker::setInstance(std::unique_ptr<SensitiveDataMasker> && sensitive_data_masker_)
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
    std::set<std::string> used_rules;

    for (const auto & rule : keys)
    {
        /// Rules names are expected to be unique and be in a form of "rule1", "rule2", etc.
        if (startsWith(rule, "rule"))
        {
            auto rule_config_prefix = config_prefix + "." + rule;

            if (!used_rules.insert(rule).second)
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "There are at least two rules with the same prefix '{}' in the query_masking_rules configuration",
                    rule);
            }

            auto rule_name = config.getString(rule_config_prefix + ".name", rule_config_prefix);
            if (!used_names.insert(rule_name).second)
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "query_masking_rules configuration contains more than one rule named '{}'.",
                    rule_name);
            }

            auto regexp = config.getString(rule_config_prefix + ".regexp", "");

            if (regexp.empty())
            {
                throw Exception(
                    ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                    "query_masking_rules configuration, rule '{}' has no <regexp> node or <regexp> "
                    "is empty.",
                    rule_name);
            }

            auto replace = config.getString(rule_config_prefix + ".replace", "******");
            auto throw_on_match = config.getBool(rule_config_prefix + ".throw_on_match", false);

            try
            {
                addMaskingRule(rule_name, regexp, replace, throw_on_match);
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
    const std::string & name, const std::string & regexp_string, const std::string & replacement_string, bool throw_on_match)
{
    all_masking_rules.push_back(std::make_unique<MaskingRule>(name, regexp_string, replacement_string, throw_on_match));
}


size_t SensitiveDataMasker::wipeSensitiveDataThrow(std::string & data) const
{
    size_t matches = 0;
    for (const auto & rule : all_masking_rules)
        matches += rule->applyThrow(data);
    return matches;
}

size_t SensitiveDataMasker::wipeSensitiveData(std::string & data) const
{
    size_t matches = 0;
    for (const auto & rule : all_masking_rules)
        matches += rule->applyNoThrow(data);

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


std::string wipeSensitiveDataAndCutToLength(std::string str, size_t max_length, bool wipe_sensitive)
{
    std::string res = std::move(str);

    if (wipe_sensitive)
    {
        if (auto masker = SensitiveDataMasker::getInstance())
            masker->wipeSensitiveData(res);
    }

    size_t length = res.length();
    if (max_length && (length > max_length))
    {
        constexpr size_t max_extra_msg_len = sizeof("... (truncated 18446744073709551615 characters)");
        if (max_length < max_extra_msg_len)
            return "(removed " + std::to_string(length) + " characters)";
        max_length -= max_extra_msg_len;
        res.resize(max_length);
        res.append("... (truncated " + std::to_string(length - max_length) + " characters)");
    }

    return res;
}

}
