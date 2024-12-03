#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/Algorithms/Graphite.h>
#include <base/find_symbols.h>
#include <base/sort.h>

#include <string_view>
#include <vector>
#include <unordered_map>

#include <fmt/format.h>

#include <Poco/Util/AbstractConfiguration.h>


using namespace std::literals;

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
 }

namespace DB::Graphite
{
static std::unordered_map<RuleType, const String> ruleTypeMap =
{
   { RuleTypeAll, "all" },
   { RuleTypePlain, "plain" },
   { RuleTypeTagged, "tagged"},
   { RuleTypeTagList, "tag_list"}
};

const String & ruleTypeStr(RuleType rule_type)
{
    try
    {
        return ruleTypeMap.at(rule_type);
    }
    catch (...)
    {
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "invalid rule type: {}", std::to_string(rule_type));
    }
}

RuleType ruleType(const String & s)
{
    if (s == "all")
        return RuleTypeAll;
    if (s == "plain")
        return RuleTypePlain;
    if (s == "tagged")
        return RuleTypeTagged;
    if (s == "tag_list")
        return RuleTypeTagList;
    throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "invalid rule type: {}", s);
}

void Pattern::updateHash(SipHash & hash) const
{
    hash.update(rule_type);
    hash.update(regexp_str);
    if (function)
    {
        hash.update(function->getName());
        for (const auto & p : function->getParameters())
            hash.update(toString(p));
    }
    for (const auto & r : retentions)
    {
        hash.update(r.age);
        hash.update(r.precision);
    }
}

static const Graphite::Pattern undef_pattern =
{ /// empty pattern for selectPatternForPath
        .rule_type = RuleTypeAll,
        .regexp = nullptr,
        .regexp_str = "",
        .function = nullptr,
        .retentions = Graphite::Retentions(),
        .type = Graphite::Pattern::TypeUndef,
};

inline static const Patterns & selectPatternsForMetricType(const Graphite::Params & params, std::string_view path)
{
    if (params.patterns_typed)
    {
        std::string_view path_view = path;
        if (path_view.find("?"sv) == std::string::npos)
            return params.patterns_plain;
        return params.patterns_tagged;
    }

    return params.patterns;
}

Graphite::RollupRule selectPatternForPath(
        const Graphite::Params & params,
        std::string_view path)
{
    const Graphite::Pattern * first_match = &undef_pattern;

    const Patterns & patterns_check = selectPatternsForMetricType(params, path);

    for (const auto & pattern : patterns_check)
    {
        if (!pattern.regexp)
        {
            /// Default pattern
            if (first_match->type == Graphite::Pattern::TypeUndef && pattern.type == Graphite::Pattern::TypeAll)
            {
                /// There is only default pattern for both retention and aggregation
                return {&pattern, &pattern};
            }
            if (pattern.type != first_match->type)
            {
                if (first_match->type == Graphite::Pattern::TypeRetention)
                {
                    return {first_match, &pattern};
                }
                if (first_match->type == Graphite::Pattern::TypeAggregation)
                {
                    return {&pattern, first_match};
                }
            }
        }
        else
        {
            if (pattern.regexp->match(path.data(), path.size()))
            {
                /// General pattern with matched path
                if (pattern.type == Graphite::Pattern::TypeAll)
                {
                    /// Only for not default patterns with both function and retention parameters
                    return {&pattern, &pattern};
                }
                if (first_match->type == Graphite::Pattern::TypeUndef)
                {
                    first_match = &pattern;
                    continue;
                }
                if (pattern.type != first_match->type)
                {
                    if (first_match->type == Graphite::Pattern::TypeRetention)
                    {
                        return {first_match, &pattern};
                    }
                    if (first_match->type == Graphite::Pattern::TypeAggregation)
                    {
                        return {&pattern, first_match};
                    }
                }
            }
        }
    }

    return {nullptr, nullptr};
}

/** Is used to order Graphite::Retentions by age and precision descending.
  * Throws exception if not both age and precision are less or greater then another.
  */
static bool compareRetentions(const Retention & a, const Retention & b)
{
    if (a.age > b.age && a.precision > b.precision)
    {
        return true;
    }
    if (a.age < b.age && a.precision < b.precision)
    {
        return false;
    }
    throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Age and precision should only grow up: {}:{} vs {}:{}",
                    a.age, a.precision, b.age, b.precision);
}

bool operator==(const Retention & a, const Retention & b)
{
    return a.age == b.age && a.precision == b.precision;
}

std::ostream & operator<<(std::ostream & stream, const Retentions & a)
{
    stream << "{ ";
    for (size_t i = 0; i < a.size(); i++)
    {
        if (i > 0)
            stream << ",";
        stream << " { age = " << a[i].age << ", precision = " << a[i].precision << " }";
    }
    stream << " }";

    return stream;
}

bool operator==(const Pattern & a, const Pattern & b)
{
    // equal
    // Retentions retentions;    /// Must be ordered by 'age' descending.
    if (a.type != b.type || a.regexp_str != b.regexp_str || a.rule_type != b.rule_type)
        return false;

    if (a.function == nullptr)
    {
        if (b.function != nullptr)
            return false;
    }
    else if (b.function == nullptr)
    {
        return false;
    }
    else if (a.function->getName() != b.function->getName())
    {
        return false;
    }

    return a.retentions == b.retentions;
}

std::ostream & operator<<(std::ostream & stream, const Pattern & a)
{
    stream << "{ rule_type = " << ruleTypeStr(a.rule_type);
    if (!a.regexp_str.empty())
        stream << ", regexp = '" << a.regexp_str << "'";
    if (a.function != nullptr)
        stream << ", function = " << a.function->getName();
    if (!a.retentions.empty())
    {
        stream << ",\n  retentions = {\n";
        for (size_t i = 0; i < a.retentions.size(); i++)
        {
            stream << "    { " << a.retentions[i].age << ", " << a.retentions[i].precision << " }";
            if (i < a.retentions.size() - 1)
                stream << ",";
            stream << "\n";
        }
        stream << "  }\n";
    }
    else
        stream << " ";

    stream << "}";
    return stream;
}

std::string buildTaggedRegex(std::string regexp_str)
{
    /*
    * tags list in format (for name or any value can use regexp, alphabet sorting not needed)
    * spaces are not stiped and used as tag and value part
    * name must be first (if used)
    *
    * tag1=value1; tag2=VALUE2_REGEX;tag3=value3
    * or
    * name;tag1=value1;tag2=VALUE2_REGEX;tag3=value3
    * or for one tag
    * tag1=value1
    *
    * Resulting regex against metric like
    * name?tag1=value1&tag2=value2
    *
    * So,
    *
    * name
    * produce
    * name\?
    *
    * tag2=val2
    * produce
    * [\?&]tag2=val2(&.*)?$
    *
    * nam.* ; tag1=val1 ; tag2=val2
    * produce
    * nam.*\?(.*&)?tag1=val1&(.*&)?tag2=val2(&.*)?$
    */

    std::vector<std::string> tags;

    splitInto<';'>(tags, regexp_str);
    /* remove empty elements */
    using namespace std::string_literals;
    std::erase(tags, ""s);
    if (tags[0].find('=') == tags[0].npos)
    {
        if (tags.size() == 1) /* only name */
            return "^" + tags[0] + "\\?";
        /* start with name value */
        regexp_str = "^" + tags[0] + "\\?(.*&)?";
        tags.erase(std::begin(tags));
    }
    else
        regexp_str = "[\\?&]";

    ::sort(std::begin(tags), std::end(tags)); /* sorted tag keys */
    regexp_str += fmt::format(
        "{}{}",
        fmt::join(tags, "&(.*&)?"),
        "(&.*)?$"  /* close regex */
    );

    return regexp_str;
}

/** Read the settings for Graphite rollup from config.
  * Example
  *
  * <graphite_rollup>
  *     <path_column_name>Path</path_column_name>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>3600</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>60</precision>
  *         </retention>
  *     </pattern>
  *     <default>
  *         <function>max</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>60</precision>
  *         </retention>
  *         <retention>
  *             <age>3600</age>
  *             <precision>300</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>3600</precision>
  *         </retention>
  *     </default>
  * </graphite_rollup>
  */
static const Pattern & appendGraphitePattern(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_element, Patterns & patterns,
    bool default_rule,
    ContextPtr context)
{
    Pattern pattern;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (key == "regexp")
        {
            pattern.regexp_str = config.getString(config_element + ".regexp");
        }
        else if (key == "function")
        {
            String aggregate_function_name_with_params = config.getString(config_element + ".function");
            String aggregate_function_name;
            Array params_row;
            getAggregateFunctionNameAndParametersArray(
                aggregate_function_name_with_params, aggregate_function_name, params_row, "GraphiteMergeTree storage initialization", context);

            /// TODO Not only Float64
            auto action = NullsAction::EMPTY;
            AggregateFunctionProperties properties;
            pattern.function = AggregateFunctionFactory::instance().get(
                aggregate_function_name, action, {std::make_shared<DataTypeFloat64>()}, params_row, properties);
        }
        else if (key == "rule_type")
        {
            String rule_type = config.getString(config_element + ".rule_type");
            pattern.rule_type = ruleType(rule_type);
        }
        else if (startsWith(key, "retention"))
        {
            pattern.retentions.emplace_back(Graphite::Retention{
                .age = config.getUInt(config_element + "." + key + ".age"),
                .precision = config.getUInt(config_element + "." + key + ".precision")});
        }
        else
            throw Exception(DB::ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}", key);
    }

    if (!pattern.regexp_str.empty())
    {
        if (pattern.rule_type == RuleTypeTagList)
        {
            // construct tagged regexp
            pattern.regexp_str = buildTaggedRegex(pattern.regexp_str);
            pattern.rule_type = RuleTypeTagged;
        }
        pattern.regexp = std::make_shared<OptimizedRegularExpression>(pattern.regexp_str);
    }

    if (!pattern.function && pattern.retentions.empty())
        throw Exception(DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "At least one of an aggregate function or retention rules is mandatory for rollup patterns in GraphiteMergeTree");

    if (default_rule && pattern.rule_type != RuleTypeAll)
    {
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "Default must have rule_type all for rollup patterns in GraphiteMergeTree");
    }

    if (!pattern.function)
    {
        pattern.type = Graphite::Pattern::TypeRetention;
    }
    else if (pattern.retentions.empty())
    {
        pattern.type = Graphite::Pattern::TypeAggregation;
    }
    else
    {
        pattern.type = Graphite::Pattern::TypeAll;
    }

    if (pattern.type & Graphite::Pattern::TypeAggregation) /// TypeAggregation or TypeAll
        if (pattern.function->allocatesMemoryInArena())
            throw Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                            "Aggregate function {} isn't supported in GraphiteMergeTree", pattern.function->getName());

    /// retention should be in descending order of age.
    if (pattern.type & Graphite::Pattern::TypeRetention) /// TypeRetention or TypeAll
        ::sort(pattern.retentions.begin(), pattern.retentions.end(), compareRetentions);

    patterns.emplace_back(pattern);
    return patterns.back();
}

void setGraphitePatternsFromConfig(ContextPtr context, const String & config_element, Graphite::Params & params)
{
    const auto & config = context->getConfigRef();

    if (!config.has(config_element))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No '{}' element in configuration file", config_element);

    params.config_name = config_element;
    params.path_column_name = config.getString(config_element + ".path_column_name", "Path");
    params.time_column_name = config.getString(config_element + ".time_column_name", "Time");
    params.value_column_name = config.getString(config_element + ".value_column_name", "Value");
    params.version_column_name = config.getString(config_element + ".version_column_name", "Timestamp");

    params.patterns_typed = false;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_element, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "pattern"))
        {
            if (appendGraphitePattern(config, config_element + "." + key, params.patterns, false, context).rule_type != RuleTypeAll)
                params.patterns_typed = true;
        }
        else if (key == "default")
        {
            /// See below.
        }
        else if (key == "path_column_name" || key == "time_column_name" || key == "value_column_name" || key == "version_column_name")
        {
            /// See above.
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}", key);
    }

    if (config.has(config_element + ".default"))
        appendGraphitePattern(config, config_element + "." + ".default", params.patterns, true, context);

    for (const auto & pattern : params.patterns)
    {
        if (pattern.rule_type == RuleTypeAll)
        {
            if (params.patterns_typed)
            {
                params.patterns_plain.push_back(pattern);
                params.patterns_tagged.push_back(pattern);
            }
        }
        else if (pattern.rule_type == RuleTypePlain)
        {
            params.patterns_plain.push_back(pattern);
        }
        else if (pattern.rule_type == RuleTypeTagged)
        {
            params.patterns_tagged.push_back(pattern);
        }
        else
        {
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unhandled rule_type in config: {}", ruleTypeStr(pattern.rule_type));
        }
    }
}

}
