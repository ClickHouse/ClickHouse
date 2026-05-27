#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Common/OptimizedRegularExpression.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

/// Function timeSeriesSelectorMatchTags(promql_selector, [(tag_name_1, tag_value_1), ...], 'tag_name_2', tag_value_2, ...)
/// returns whether all matchers of the given PromQL instant selector are satisfied by the given set of tags.
class FunctionTimeSeriesSelectorMatchTags final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesSelectorMatchTags";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesSelectorMatchTags>(); }

    String getName() const override { return name; }

    /// First argument is the selector, then a (Map/Array) of tags, then zero or more (name, value) pairs.
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt8>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with at least 2 arguments: "
                "{}(selector, [('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)",
                name, name);
        }
        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForConstString(name, arguments, 0);
        TimeSeriesTagsFunctionHelpers::checkArgumentTypesForTagNamesAndValues(name, arguments, 1);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        String selector_str = TimeSeriesTagsFunctionHelpers::extractConstStringFromArgument(name, arguments, 0);

        PrometheusQueryTree query_tree{selector_str};
        const auto * root = query_tree.getRoot();
        if (!root || (root->node_type != PrometheusQueryTree::NodeType::InstantSelector))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {}: '{}' is not a valid PromQL instant selector",
                name, selector_str);
        }
        const auto & matchers = static_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers;

        /// Precompile any regex matchers once, anchored the same way Prometheus does.
        std::vector<std::optional<OptimizedRegularExpression>> compiled_regexes(matchers.size());
        for (size_t i = 0; i < matchers.size(); ++i)
        {
            const auto & matcher = matchers[i];
            if (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE
                || matcher.matcher_type == PrometheusQueryTree::MatcherType::NRE)
            {
                String pattern = matcher.label_value;
                if (!pattern.starts_with('^'))
                    pattern.insert(0, "^");
                if (!pattern.ends_with('$'))
                    pattern.push_back('$');
                compiled_regexes[i].emplace(pattern);
            }
        }

        /// Per-row, sorted, deduplicated tag sets.
        auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(name, arguments, 1);
        chassert(tags_vector.size() == input_rows_count);

        auto col_res = ColumnUInt8::create(input_rows_count);
        auto & data = col_res->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto & tags = *tags_vector[row];
            bool all_match = true;
            for (size_t mi = 0; mi < matchers.size(); ++mi)
            {
                const auto & matcher = matchers[mi];

                /// `tags` is sorted by name; find the label value by binary search.
                /// A missing label is treated as the empty string (Prometheus semantics).
                std::string_view label_value;
                auto it = std::lower_bound(tags.begin(), tags.end(), matcher.label_name,
                    [](const std::pair<String, String> & lhs, const String & needle) { return lhs.first < needle; });
                if (it != tags.end() && it->first == matcher.label_name)
                    label_value = it->second;

                bool matched = false;
                switch (matcher.matcher_type)
                {
                    case PrometheusQueryTree::MatcherType::EQ:
                        matched = (label_value == matcher.label_value);
                        break;
                    case PrometheusQueryTree::MatcherType::NE:
                        matched = (label_value != matcher.label_value);
                        break;
                    case PrometheusQueryTree::MatcherType::RE:
                        matched = compiled_regexes[mi]->match(label_value.data(), label_value.size());
                        break;
                    case PrometheusQueryTree::MatcherType::NRE:
                        matched = !compiled_regexes[mi]->match(label_value.data(), label_value.size());
                        break;
                }
                if (!matched)
                {
                    all_match = false;
                    break;
                }
            }
            data[row] = all_match ? 1 : 0;
        }

        return col_res;
    }
};

}


REGISTER_FUNCTION(TimeSeriesSelectorMatchTags)
{
    FunctionDocumentation::Description description = R"(
Returns whether all matchers of the given PromQL instant selector are satisfied by the given set of tags.
The tags are supplied as a `Map(String, String)` (or `Array(Tuple(String, String))`),
optionally followed by zero or more (name, value) pairs for additional individual tags.
Empty tag value is treated as a missing tag, matching Prometheus semantics.
)";
    FunctionDocumentation::Syntax syntax = R"(
timeSeriesSelectorMatchTags(promql_selector, [('tag_name_1', 'tag_value_1'), ...], 'tag_name_2', 'tag_value_2', ...)
)";
    FunctionDocumentation::Arguments arguments = {
        {"promql_selector", "A PromQL instant selector, for example `'{__name__=\"up\", job=\"api\"}'`. Must be a constant.", {"String"}},
        {"tags", "A `Map(String, String)` or `Array(Tuple(String, String))` of tag (name, value) pairs.", {"Map(String, String)"}},
        {"tag_name_n / tag_value_n", "Optional individual tag (name, value) pairs. The name must be a constant.", {"String", "String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns 1 if all matchers of the selector are satisfied by the tags, otherwise 0.", {"UInt8"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Example",
            R"(
SELECT timeSeriesSelectorMatchTags(
    '{__name__="up", job="api"}',
    map('job', 'api'),
    '__name__', 'up'
) AS matched;
            )",
            R"(
┌─matched─┐
│       1 │
└─────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesSelectorMatchTags>(documentation);
}

}
