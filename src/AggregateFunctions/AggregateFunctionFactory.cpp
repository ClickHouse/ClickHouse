#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Core/Settings.h>

static constexpr size_t MAX_AGGREGATE_FUNCTION_NAME_LENGTH = 1000;


namespace DB
{
struct Settings;
namespace Setting
{
    extern const SettingsBool log_queries;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

const String & getAggregateFunctionCanonicalNameIfAny(const String & name)
{
    return AggregateFunctionFactory::instance().getCanonicalNameIfAny(name);
}

void AggregateFunctionFactory::registerFunction(const String & name, Value creator_with_properties, Case case_sensitiveness)
{
    if (creator_with_properties.creator == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: "
            "the aggregate function {} has been provided  a null constructor", name);

    if (!aggregate_functions.emplace(name, creator_with_properties).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: the aggregate function name '{}' is not unique",
            name);

    if (case_sensitiveness == Case::Insensitive)
    {
        auto key = Poco::toLower(name);
        if (!case_insensitive_aggregate_functions.emplace(key, creator_with_properties).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory: "
                "the case insensitive aggregate function name '{}' is not unique", name);
        case_insensitive_name_mapping[key] = name;
    }
}

void AggregateFunctionFactory::registerNullsActionTransformation(const String & source_ignores_nulls, const String & target_respect_nulls)
{
    if (!aggregate_functions.contains(source_ignores_nulls))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Source aggregation '{}' not found", source_ignores_nulls);

    if (!aggregate_functions.contains(target_respect_nulls))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Target aggregation '{}' not found", target_respect_nulls);

    if (!respect_nulls.emplace(source_ignores_nulls, target_respect_nulls).second)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Assignment from '{}' is not unique", source_ignores_nulls);

    if (!ignore_nulls.emplace(target_respect_nulls, source_ignores_nulls).second)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "registerNullsActionTransformation: Assignment from '{}' is not unique", target_respect_nulls);
}

static DataTypes convertLowCardinalityTypesToNested(const DataTypes & types)
{
    DataTypes res_types;
    res_types.reserve(types.size());
    for (const auto & type : types)
        res_types.emplace_back(recursiveRemoveLowCardinality(type));

    return res_types;
}

AggregateFunctionPtr AggregateFunctionFactory::get(
    const String & name,
    NullsAction action,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties) const
{
    /// This to prevent costly string manipulation in parsing the aggregate function combinators.
    /// Example: avgArrayArrayArrayArray...(1000 times)...Array
    if (name.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    auto types_without_low_cardinality = convertLowCardinalityTypesToNested(argument_types);

    /// If one of the types is Nullable, we apply aggregate function combinator "Null" if it's not window function.
    /// Window functions are not real aggregate functions. Applying combinators doesn't make sense for them,
    /// they must handle the nullability themselves.
    /// Aggregate functions such as any_value_respect_nulls are considered window functions in that sense
    auto properties = tryGetProperties(name, action);
    bool is_window_function = properties.has_value() && properties->is_window_function;
    if (!is_window_function && std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
        [](const auto & type) { return type->isNullable(); }))
    {
        AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix("Null");
        if (!combinator)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find aggregate function combinator "
                            "to apply a function to Nullable arguments.");

        DataTypes nested_types = combinator->transformArguments(types_without_low_cardinality);
        Array nested_parameters = combinator->transformParameters(parameters);

        bool has_null_arguments = std::any_of(types_without_low_cardinality.begin(), types_without_low_cardinality.end(),
            [](const auto & type) { return type->onlyNull(); });

        AggregateFunctionPtr nested_function = getImpl(name, action, nested_types, nested_parameters, out_properties, has_null_arguments);

        // Pure window functions are not real aggregate functions. Applying
        // combinators doesn't make sense for them, they must handle the
        // nullability themselves. Another special case is functions from Nothing
        // that are rewritten to AggregateFunctionNothing, in this case
        // nested_function is nullptr.
        if (!nested_function || !nested_function->isOnlyWindowFunction())
            return combinator->transformAggregateFunction(nested_function, out_properties, types_without_low_cardinality, parameters);
    }

    auto with_original_arguments = getImpl(name, action, types_without_low_cardinality, parameters, out_properties, false);

    if (!with_original_arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionFactory returned nullptr");
    return with_original_arguments;
}

std::optional<AggregateFunctionWithProperties>
AggregateFunctionFactory::getAssociatedFunctionByNullsAction(const String & name, NullsAction action) const
{
    if (action == NullsAction::RESPECT_NULLS)
    {
        auto it = respect_nulls.find(name);
        if (it == respect_nulls.end())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} does not support RESPECT NULLS", name);
        if (auto associated_it = aggregate_functions.find(it->second); associated_it != aggregate_functions.end())
            return {associated_it->second};
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to find the function {} (equivalent to '{} RESPECT NULLS')", it->second, name);
    }

    if (action == NullsAction::IGNORE_NULLS)
    {
        if (auto it = ignore_nulls.find(name); it != ignore_nulls.end())
        {
            if (auto associated_it = aggregate_functions.find(it->second); associated_it != aggregate_functions.end())
                return {associated_it->second};
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unable to find the function {} (equivalent to '{} IGNORE NULLS')", it->second, name);
        }
        /// We don't throw for IGNORE NULLS of other functions because that's the default in CH
    }

    return {};
}


AggregateFunctionPtr AggregateFunctionFactory::getImpl(
    const String & name_param,
    NullsAction action,
    const DataTypes & argument_types,
    const Array & parameters,
    AggregateFunctionProperties & out_properties,
    bool has_null_arguments) const
{
    String name = getAliasToOrName(name_param);
    String case_insensitive_name;
    bool is_case_insensitive = false;
    Value found;

    /// Find by exact match.
    if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
    {
        found = it->second;
    }

    if (!found.creator)
    {
        case_insensitive_name = Poco::toLower(name);
        if (auto jt = case_insensitive_aggregate_functions.find(case_insensitive_name); jt != case_insensitive_aggregate_functions.end())
        {
            found = jt->second;
            is_case_insensitive = true;
        }
    }

    ContextPtr query_context;
    if (CurrentThread::isInitialized())
        query_context = CurrentThread::get().getQueryContext();

    if (found.creator)
    {
        auto opt = getAssociatedFunctionByNullsAction(is_case_insensitive ? case_insensitive_name : name, action);
        if (opt)
            found = *opt;

        out_properties = found.properties;
        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(
                Context::QueryLogFactories::AggregateFunction, is_case_insensitive ? case_insensitive_name : name);

        /// The case when aggregate function should return NULL on NULL arguments. This case is handled in "get" method.
        if (!out_properties.returns_default_when_only_null && has_null_arguments)
            return nullptr;

        const Settings * settings = query_context ? &query_context->getSettingsRef() : nullptr;
        return found.creator(name, argument_types, parameters, settings);
    }

    /// Combinators of aggregate functions.
    /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
    ///  that can have different number and/or types of arguments, different result type and different behaviour.

    if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        const std::string & combinator_name = combinator->getName();

        if (combinator->isForInternalUsageOnly())
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function combinator '{}' is only for internal usage",
                combinator_name);

        if (query_context && query_context->getSettingsRef()[Setting::log_queries])
            query_context->addQueryFactoriesInfo(Context::QueryLogFactories::AggregateFunctionCombinator, combinator_name);

        String nested_name = name.substr(0, name.size() - combinator_name.size());
        /// Nested identical combinators (i.e. uniqCombinedIfIf) is not
        /// supported (since they don't work -- silently).
        ///
        /// But non-identical is supported and works. For example,
        /// uniqCombinedIfMergeIf is useful in cases when the underlying
        /// storage stores AggregateFunction(uniqCombinedIf) and in SELECT you
        /// need to filter aggregation result based on another column.

        if (!combinator->supportsNesting() && nested_name.ends_with(combinator_name))
        {
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Nested identical combinator '{}' is not supported",
                combinator_name);
        }

        DataTypes nested_types = combinator->transformArguments(argument_types);
        Array nested_parameters = combinator->transformParameters(parameters);

        AggregateFunctionPtr nested_function = get(nested_name, action, nested_types, nested_parameters, out_properties);
        return combinator->transformAggregateFunction(nested_function, out_properties, argument_types, parameters);
    }


    String extra_info;
    if (FunctionFactory::instance().hasNameOrAlias(name))
        extra_info = ". There is an ordinary function with the same name, but aggregate function is expected here";

    auto hints = this->getHints(name);
    if (!hints.empty())
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                        "Unknown aggregate function {}{}. Maybe you meant: {}", name, extra_info, toString(hints));
    throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Unknown aggregate function {}{}", name, extra_info);
}

std::optional<AggregateFunctionProperties> AggregateFunctionFactory::tryGetProperties(String name, NullsAction action) const
{
    if (name.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    while (true)
    {
        name = getAliasToOrName(name);
        Value found;
        String lower_case_name;
        bool is_case_insensitive = false;

        /// Find by exact match.
        if (auto it = aggregate_functions.find(name); it != aggregate_functions.end())
        {
            found = it->second;
        }

        if (!found.creator)
        {
            lower_case_name = Poco::toLower(name);
            if (auto jt = case_insensitive_aggregate_functions.find(lower_case_name); jt != case_insensitive_aggregate_functions.end())
            {
                is_case_insensitive = true;
                found = jt->second;
            }
        }

        if (found.creator)
        {
            auto opt = getAssociatedFunctionByNullsAction(is_case_insensitive ? lower_case_name : name, action);
            if (opt)
                return opt->properties;
            return found.properties;
        }

        /// Combinators of aggregate functions.
        /// For every aggregate function 'agg' and combiner '-Comb' there is a combined aggregate function with the name 'aggComb',
        ///  that can have different number and/or types of arguments, different result type and different behaviour.

        if (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
        {
            if (combinator->isForInternalUsageOnly())
                return {};

            /// NOTE: It's reasonable to also allow to transform properties by combinator.
            name = name.substr(0, name.size() - combinator->getName().size());
        }
        else
            return {};
    }
}


bool AggregateFunctionFactory::isAggregateFunctionName(const String & name_) const
{
    if (name_.size() > MAX_AGGREGATE_FUNCTION_NAME_LENGTH)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long name of aggregate function, maximum: {}", MAX_AGGREGATE_FUNCTION_NAME_LENGTH);

    if (aggregate_functions.contains(name_) || isAlias(name_))
        return true;

    String name_lowercase = Poco::toLower(name_);
    if (case_insensitive_aggregate_functions.contains(name_lowercase) || isAlias(name_lowercase))
        return true;

    String name = name_;
    while (AggregateFunctionCombinatorPtr combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(name))
    {
        name = name.substr(0, name.size() - combinator->getName().size());
        name_lowercase = name_lowercase.substr(0, name_lowercase.size() - combinator->getName().size());

        if (aggregate_functions.contains(name) || isAlias(name) || case_insensitive_aggregate_functions.contains(name_lowercase)
            || isAlias(name_lowercase))
            return true;
    }
    return false;
}

AggregateFunctionFactory & AggregateFunctionFactory::instance()
{
    static AggregateFunctionFactory ret;
    return ret;
}

}
