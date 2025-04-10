#include <Interpreters/JoinInfo.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
#define DECLARE_JOIN_SETTINGS_EXTERN(type, name) \
    extern const Settings##type name; // NOLINT

    APPLY_FOR_JOIN_SETTINGS(DECLARE_JOIN_SETTINGS_EXTERN)
#undef DECLARE_JOIN_SETTINGS_EXTERN
}

JoinSettings JoinSettings::create(const Settings & query_settings)
{
    JoinSettings join_settings;

#define COPY_JOIN_SETTINGS_FROM_QUERY(type, name) \
    join_settings.name = query_settings[Setting::name];

    APPLY_FOR_JOIN_SETTINGS(COPY_JOIN_SETTINGS_FROM_QUERY)
#undef COPY_JOIN_SETTINGS_FROM_QUERY

    return join_settings;
}


std::string_view toString(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Equals: return "=";
        case PredicateOperator::NullSafeEquals: return "<=>";
        case PredicateOperator::Less: return "<";
        case PredicateOperator::LessOrEquals: return "<=";
        case PredicateOperator::Greater: return ">";
        case PredicateOperator::GreaterOrEquals: return ">=";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for PredicateOperator: {}", static_cast<Int32>(op));
}


String toString(const JoinActionRef & predicate)
{
    WriteBufferFromOwnString out;
    out << predicate.column_name;
    if (predicate.column_name != predicate.node->result_name)
        out << " AS " << predicate.node->result_name;
    out << " :: " << predicate.node->result_type->getName();
    if (predicate.node->column)
        out << " CONST " << predicate.node->column->dumpStructure();
    return out.str();
}

String toString(const JoinPredicate & predicate)
{
    return fmt::format("{} {} {}", toString(predicate.left_node), toString(predicate.op), toString(predicate.right_node));
}

String toString(const JoinCondition & condition)
{
    auto format_conditions = [](std::string_view label, const auto & conditions)
    {
        if (conditions.empty())
            return String{};
        return fmt::format("{}: {}", label, fmt::join(conditions | std::views::transform([](auto && x) { return toString(x); }), ", "));
    };
    return fmt::format("{} {} {} {}",
        fmt::join(condition.predicates | std::views::transform([](auto && x) { return toString(x); }), ", "),
        format_conditions("Left filter", condition.left_filter_conditions),
        format_conditions("Right filter", condition.right_filter_conditions),
        format_conditions("Residual filter", condition.residual_conditions)
    );
}


}
