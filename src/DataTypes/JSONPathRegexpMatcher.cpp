#include <DataTypes/JSONPathRegexpMatcher.h>

#include <Common/Exception.h>
#include <Common/re2.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <re2/set.h>
#pragma clang diagnostic pop

#include <algorithm>
#include <memory>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_COMPILE_REGEXP;
}

namespace
{

constexpr size_t RE2_MAX_MEMORY = 8 * 1024 * 1024;
/// `RE2` doesn't expose the size of a compiled instruction. This estimate is used only by
/// `allocatedBytes`; actual allocations are tracked by the global allocation interceptors.
constexpr size_t ESTIMATED_BYTES_PER_RE2_INSTRUCTION = 32;

re2::RE2::Options makeRE2Options()
{
    re2::RE2::Options options;
    options.set_log_errors(false);
    options.set_never_capture(true);
    options.set_max_mem(RE2_MAX_MEMORY);
    return options;
}

void validateRules(const std::vector<JSONPathRegexpRule> & rules)
{
    if (rules.size() > JSONPathRegexpMatcher::MAX_RULES)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Too many JSON path regular expressions: {}. Maximum: {}",
            rules.size(),
            JSONPathRegexpMatcher::MAX_RULES);

    size_t total_pattern_bytes = 0;
    for (const auto & rule : rules)
    {
        if (rule.mode != JSONPathRegexpMatchMode::Partial && rule.mode != JSONPathRegexpMatchMode::Full)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Unknown JSON path regular expression match mode: {}", static_cast<unsigned int>(rule.mode));

        if (rule.pattern.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "JSON path regular expression cannot be empty");

        if (rule.pattern.size() > JSONPathRegexpMatcher::MAX_PATTERN_BYTES)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "JSON path regular expression is too long: {} bytes. Maximum: {} bytes",
                rule.pattern.size(),
                JSONPathRegexpMatcher::MAX_PATTERN_BYTES);

        if (rule.pattern.size() > JSONPathRegexpMatcher::MAX_TOTAL_PATTERN_BYTES - total_pattern_bytes)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "JSON path regular expressions use too many bytes. Maximum total size: {} bytes",
                JSONPathRegexpMatcher::MAX_TOTAL_PATTERN_BYTES);

        total_pattern_bytes += rule.pattern.size();
    }
}

}

struct JSONPathRegexpMatcher::Impl
{
    explicit Impl(const std::vector<JSONPathRegexpRule> & canonical_rules)
    {
        if (canonical_rules.empty())
            return;

        auto options = makeRE2Options();
        if (canonical_rules.size() == 1)
        {
            single_mode = canonical_rules.front().mode;
            single_regexp = std::make_unique<re2::RE2>(canonical_rules.front().pattern, options);
            if (!single_regexp->ok())
                throw Exception(
                    ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Invalid JSON path regular expression '{}': {}",
                    canonical_rules.front().pattern,
                    single_regexp->error());
            return;
        }

        for (const auto & rule : canonical_rules)
        {
            auto & regexp_set = rule.mode == JSONPathRegexpMatchMode::Partial ? partial_set : full_set;
            if (!regexp_set)
            {
                const auto anchor = rule.mode == JSONPathRegexpMatchMode::Partial ? re2::RE2::UNANCHORED : re2::RE2::ANCHOR_BOTH;
                regexp_set = std::make_unique<re2::RE2::Set>(options, anchor);
            }

            String error;
            if (regexp_set->Add(rule.pattern, &error) < 0)
                throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP, "Invalid JSON path regular expression '{}': {}", rule.pattern, error);
        }

        if (partial_set && !partial_set->Compile())
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile partial JSON path regular expression set within the RE2 memory limit");

        if (full_set && !full_set->Compile())
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP, "Cannot compile full JSON path regular expression set within the RE2 memory limit");
    }

    static bool matchesSet(const re2::RE2::Set & regexp_set, re2::StringPiece path)
    {
        re2::RE2::Set::ErrorInfo error_info{};
        if (regexp_set.Match(path, nullptr, &error_info))
            return true;

        if (error_info.kind != re2::RE2::Set::kNoError)
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP,
                "JSON path regular expression set matching failed with RE2 error kind {}",
                static_cast<int>(error_info.kind));

        return false;
    }

    bool matches(std::string_view path) const
    {
        const re2::StringPiece re2_path(path.data(), path.size());
        if (single_regexp)
        {
            if (single_mode == JSONPathRegexpMatchMode::Partial)
                return re2::RE2::PartialMatch(re2_path, *single_regexp);
            return re2::RE2::FullMatch(re2_path, *single_regexp);
        }

        return (partial_set && matchesSet(*partial_set, re2_path)) || (full_set && matchesSet(*full_set, re2_path));
    }

    size_t allocatedBytes() const
    {
        size_t bytes = sizeof(*this);
        if (single_regexp)
        {
            bytes += sizeof(*single_regexp);
            bytes += static_cast<size_t>(single_regexp->ProgramSize() + single_regexp->ReverseProgramSize())
                * ESTIMATED_BYTES_PER_RE2_INSTRUCTION;
        }
        if (partial_set)
            bytes += sizeof(*partial_set);
        if (full_set)
            bytes += sizeof(*full_set);
        return bytes;
    }

    JSONPathRegexpMatchMode single_mode = JSONPathRegexpMatchMode::Partial;
    std::unique_ptr<re2::RE2> single_regexp;
    std::unique_ptr<re2::RE2::Set> partial_set;
    std::unique_ptr<re2::RE2::Set> full_set;
};

JSONPathRegexpMatcher::Ptr JSONPathRegexpMatcher::create(std::vector<JSONPathRegexpRule> rules)
{
    validateRules(rules);
    std::sort(rules.begin(), rules.end());
    rules.erase(std::unique(rules.begin(), rules.end()), rules.end());
    return Ptr(new JSONPathRegexpMatcher(std::move(rules)));
}

JSONPathRegexpMatcher::JSONPathRegexpMatcher(std::vector<JSONPathRegexpRule> rules_)
    : rules(std::move(rules_))
    , impl(std::make_unique<Impl>(rules))
{
}

JSONPathRegexpMatcher::~JSONPathRegexpMatcher() = default;

bool JSONPathRegexpMatcher::matches(std::string_view path) const
{
    return impl->matches(path);
}

size_t JSONPathRegexpMatcher::allocatedBytes() const
{
    size_t bytes = sizeof(*this) + rules.capacity() * sizeof(JSONPathRegexpRule);
    for (const auto & rule : rules)
        bytes += rule.pattern.capacity();
    bytes += impl->allocatedBytes();
    return bytes;
}

}
