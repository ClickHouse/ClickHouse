#include <Storages/TimeSeries/TimeSeriesIDRange.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <limits>


namespace DB
{

namespace
{
    /// Returns the metric name selected by the matchers if they contain exactly one equality matcher on `__name__`
    /// with a non-empty value, otherwise nullopt.
    std::optional<String> tryGetMetricName(const PrometheusQueryTree::MatcherList & matchers)
    {
        auto is_metric_name_equality_matcher = [](const PrometheusQueryTree::Matcher & matcher)
        {
            return (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ) && (matcher.label_name == TimeSeriesTagNames::MetricName);
        };

        const PrometheusQueryTree::Matcher * name_matcher = nullptr;
        for (const auto & matcher : matchers)
        {
            if (is_metric_name_equality_matcher(matcher))
            {
                if (name_matcher)
                    return {};
                name_matcher = &matcher;
            }
        }
        if (!name_matcher || name_matcher->label_value.empty())
            return {};
        return name_matcher->label_value;
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


std::optional<TimeSeriesIDRange> tryGetTimeSeriesIDRange(
    const PrometheusQueryTree::MatcherList & matchers, const ASTPtr & id_generator, const DataTypePtr & id_type)
{
    /// tuple(<expression of the metric name only>, <anything>)
    ASTPtr first_component = tryGetFirstComponent(id_generator);
    if (!first_component || !dependsOnMetricNameOnly(first_component))
        return {};

    /// The identifiers must be two-component tuples: the range spans the whole second component.
    DataTypePtr second_component_type = tryGetSecondComponentType(id_type);
    if (!second_component_type)
        return {};

    /// The range covers one metric, so the matchers must select exactly one metric by its name.
    std::optional<String> metric_name = tryGetMetricName(matchers);
    if (!metric_name)
        return {};

    /// The bounds of the range: the minimum and the maximum of the type of the second component.
    auto min_max_second_component = makeMinMaxLiteralsForIDComponent(*second_component_type);
    if (!min_max_second_component)
        return {};

    /// Substitute the metric name for the `metric_name` column, for example sipHash64(metric_name) -> sipHash64('my_metric').
    ASTPtr first_component_value = first_component->clone();
    substituteMetricNameInPlace(first_component_value, *metric_name);

    TimeSeriesIDRange range;
    range.metric_name = std::move(*metric_name);
    range.first_component = std::move(first_component_value);
    range.min_second_component = std::move(min_max_second_component->first);
    range.max_second_component = std::move(min_max_second_component->second);
    return range;
}


ASTPtr TimeSeriesIDRange::makeIDInRangeCondition(const ASTPtr & id) const
{
    return makeASTForLogicalAnd(
        {makeASTFunction("greaterOrEquals", id->clone(), makeASTFunction("tuple", first_component->clone(), min_second_component->clone())),
         makeASTFunction("lessOrEquals", id->clone(), makeASTFunction("tuple", first_component->clone(), max_second_component->clone()))});
}


ASTPtr TimeSeriesIDRange::makeIDOutOfRangeCondition(const ASTPtr & id) const
{
    return makeASTFunction(
        "notEquals", makeASTFunction("tupleElement", id->clone(), make_intrusive<ASTLiteral>(UInt64{1})), first_component->clone());
}

}
