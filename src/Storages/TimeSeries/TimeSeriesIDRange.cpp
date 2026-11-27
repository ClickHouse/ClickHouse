#include <Storages/TimeSeries/TimeSeriesIDRange.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>

#include <algorithm>
#include <limits>
#include <optional>


namespace DB
{

namespace
{
    /// Returns the metric names selected by the matchers if they contain exactly one equality matcher or one regex matcher
    /// of literal alternatives on `__name__`, and all the names are non-empty. Otherwise returns nullopt.
    std::optional<Strings> tryGetMetricNames(const PrometheusQueryTree::MatcherList & matchers)
    {
        /// Find the only matcher selecting metrics by name: `__name__="m"` or `__name__=~"m1|m2"`.
        const PrometheusQueryTree::Matcher * name_matcher = nullptr;
        for (const auto & matcher : matchers)
        {
            if ((matcher.label_name == TimeSeriesTagNames::MetricName)
                && ((matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ) || (matcher.matcher_type == PrometheusQueryTree::MatcherType::RE)))
            {
                if (name_matcher)
                    return {};
                name_matcher = &matcher;
            }
        }
        if (!name_matcher)
            return {};

        /// Take the names: the value of the equality, or the terms of the regex if they are plain literals.
        std::optional<Strings> metric_names;
        if (name_matcher->matcher_type == PrometheusQueryTree::MatcherType::EQ)
            metric_names = Strings{name_matcher->label_value};
        else
            metric_names = tryGetLiteralAlternatives(name_matcher->label_value);

        /// Reject an empty name: a series always has a metric name.
        if (!metric_names || std::ranges::any_of(*metric_names, [](const String & metric_name) { return metric_name.empty(); }))
            return {};

        /// Drop repeated names, as in `__name__=~"m|m"`, so that no range is repeated.
        std::ranges::sort(*metric_names);
        metric_names->erase(std::ranges::unique(*metric_names).begin(), metric_names->end());
        return metric_names;
    }

    /// Constant ASTs for the minimum and the maximum value of one component of a multi-component
    /// series id. The supported types are the ones `TimeSeriesIDGenerator` can generate hashes for.
    std::optional<std::pair<ASTPtr, ASTPtr>> makeMinMaxLiteralsForIDComponent(const IDataType & type)
    {
        /// A LowCardinality component has the value space of its dictionary type: the range bounds
        /// are the dictionary type's bounds (constants have no dictionary encoding of their own).
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(&type))
            return makeMinMaxLiteralsForIDComponent(*low_cardinality_type->getDictionaryType());

        WhichDataType which(type);

        if (which.isUInt64())
            return {{make_intrusive<ASTLiteral>(UInt64{0}), make_intrusive<ASTLiteral>(std::numeric_limits<UInt64>::max())}};

        if (which.isUInt128())
            return {{make_intrusive<ASTLiteral>(UInt128{0}), make_intrusive<ASTLiteral>(std::numeric_limits<UInt128>::max())}};

        if (which.isUUID())
        {
            return {{makeASTFunction("toUUID", make_intrusive<ASTLiteral>("00000000-0000-0000-0000-000000000000")),
                     makeASTFunction("toUUID", make_intrusive<ASTLiteral>("ffffffff-ffff-ffff-ffff-ffffffffffff"))}};
        }

        if (which.isFixedString() && (typeid_cast<const DataTypeFixedString &>(type).getN() == 16))
        {
            auto fixed_string_16 = [](char c)
            {
                return makeASTFunction("CAST",
                    makeASTFunction("unhex", make_intrusive<ASTLiteral>(String(32, c))),
                    make_intrusive<ASTLiteral>("FixedString(16)"));
            };
            return {{fixed_string_16('0'), fixed_string_16('f')}};
        }

        return {};
    }

    /// Replaces references to the `metric_name` column with a string literal, in place.
    void substituteMetricNameInPlace(ASTPtr & node, const String & metric_name_value)
    {
        if (const auto * identifier = node->as<ASTIdentifier>(); identifier && (identifier->name() == TimeSeriesColumnNames::MetricName))
        {
            node = make_intrusive<ASTLiteral>(metric_name_value);
            return;
        }
        for (auto & child : node->children)
            substituteMetricNameInPlace(child, metric_name_value);
    }

    /// Returns the type of the second component of a two-component tuple type, otherwise null.
    DataTypePtr tryGetSecondComponentType(const DataTypePtr & id_type)
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(id_type.get());
        if (!tuple_type || (tuple_type->getElements().size() != 2))
            return nullptr;
        return tuple_type->getElements()[1];
    }

    /// Returns the first argument of a `tuple(...)` expression with two arguments, otherwise null.
    ASTPtr tryGetFirstComponent(const ASTPtr & id_generator)
    {
        const auto * function = id_generator ? id_generator->as<ASTFunction>() : nullptr;
        if (!function || (function->name != "tuple") || !function->arguments || (function->arguments->children.size() != 2))
            return nullptr;
        return function->arguments->children[0];
    }

    /// Whether an expression of the id generator depends on the metric name only, for example `sipHash64(metric_name)`:
    /// then it has the same value for all the time series of one metric (an id generator is deterministic,
    /// otherwise a time series would get a new identifier on every insert).
    bool dependsOnMetricNameOnly(const ASTPtr & expression)
    {
        bool found_metric_name = false;

        /// Checks that a node references no column except `metric_name`.
        auto references_only_metric_name = [&](const auto & self, const IAST & node) -> bool
        {
            if (const auto * identifier = node.as<ASTIdentifier>())
            {
                if (identifier->name() != TimeSeriesColumnNames::MetricName)
                    return false;
                found_metric_name = true;
                return true;
            }
            for (const auto & child : node.children)
            {
                if (child && !self(self, *child))
                    return false;
            }
            return true;
        };

        return references_only_metric_name(references_only_metric_name, *expression) && found_metric_name;
    }
}


std::optional<TimeSeriesIDRanges> tryGetTimeSeriesIDRanges(
    const PrometheusQueryTree::MatcherList & matchers, const ASTPtr & id_generator, const DataTypePtr & id_type)
{
    std::optional<Strings> metric_names = tryGetMetricNames(matchers);
    if (!metric_names)
        return {};

    /// tuple(<expression of the metric name only>, <anything>)
    ASTPtr first_component = tryGetFirstComponent(id_generator);
    if (!first_component || !dependsOnMetricNameOnly(first_component))
        return {};

    /// The identifiers must be two-component tuples: the range spans the whole second component.
    DataTypePtr second_component_type = tryGetSecondComponentType(id_type);
    if (!second_component_type)
        return {};

    /// The bounds of the range: the minimum and the maximum of the type of the second component.
    auto min_max_second_component = makeMinMaxLiteralsForIDComponent(*second_component_type);
    if (!min_max_second_component)
        return {};

    TimeSeriesIDRanges ranges;
    for (String & metric_name : *metric_names)
    {
        TimeSeriesIDRange range;
        /// Substitute the metric name for the `metric_name` column, for example sipHash64(metric_name) -> sipHash64('my_metric').
        range.first_component = first_component->clone();
        substituteMetricNameInPlace(range.first_component, metric_name);
        range.metric_name = std::move(metric_name);
        range.min_second_component = min_max_second_component->first->clone();
        range.max_second_component = min_max_second_component->second->clone();
        ranges.push_back(std::move(range));
    }
    return ranges;
}


ASTPtr makeIDInRangeCondition(const ASTPtr & id, const TimeSeriesIDRange & range)
{
    return makeASTForLogicalAnd(
        {makeASTFunction("greaterOrEquals", id->clone(), makeASTFunction("tuple", range.first_component->clone(), range.min_second_component->clone())),
         makeASTFunction("lessOrEquals", id->clone(), makeASTFunction("tuple", range.first_component->clone(), range.max_second_component->clone()))});
}


ASTPtr makeIDInRangesCondition(const ASTPtr & id, const TimeSeriesIDRanges & ranges)
{
    ASTs conditions;
    for (const auto & range : ranges)
        conditions.push_back(makeIDInRangeCondition(id, range));
    return makeASTForLogicalOr(std::move(conditions));
}


ASTPtr makeIDOutOfRangeCondition(const ASTPtr & id, const TimeSeriesIDRange & range)
{
    return makeASTFunction(
        "notEquals", makeASTFunction("tupleElement", id->clone(), make_intrusive<ASTLiteral>(UInt64{1})), range.first_component->clone());
}


ASTPtr makeIDOutOfRangesCondition(const ASTPtr & id, const TimeSeriesIDRanges & ranges)
{
    if (ranges.size() == 1)
        return makeIDOutOfRangeCondition(id, ranges.front());

    auto first_components = make_intrusive<ASTFunction>();
    first_components->name = "tuple";
    first_components->arguments = make_intrusive<ASTExpressionList>();
    first_components->children.push_back(first_components->arguments);
    for (const auto & range : ranges)
        first_components->arguments->children.push_back(range.first_component->clone());

    return makeASTFunction("notIn", makeASTFunction("tupleElement", id->clone(), make_intrusive<ASTLiteral>(UInt64{1})), std::move(first_components));
}

}
