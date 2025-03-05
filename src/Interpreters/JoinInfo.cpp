#include <Interpreters/JoinInfo.h>

#include <Columns/IColumn.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

import fmt;


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


String toString(const JoinActionRef & node)
{
    WriteBufferFromOwnString out;

    const auto & column = node.getColumn();
    out << column.name;
    out << " :: " << column.type->getName();
    if (column. column)
        out << " CONST " << column. column->dumpStructure();
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


static bool checkNodeInOutputs(const ActionsDAG::Node * node, const ActionsDAG * actions_dag)
{
    for (const auto * output : actions_dag->getOutputs())
    {
        if (output == node)
            return true;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} is not in outputs of actions DAG:\n{}", node->result_name, actions_dag->dumpDAG());
}

JoinActionRef::JoinActionRef(const ActionsDAG::Node * node_, const ActionsDAG * actions_dag_)
    : actions_dag(actions_dag_)
    , column_name(node_->result_name)
{
    chassert(checkNodeInOutputs(node_, actions_dag));
}

const ActionsDAG::Node * JoinActionRef::getNode() const
{
    const auto * node = actions_dag ? actions_dag->tryFindInOutputs(column_name) : nullptr;
    if (!node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find column {} in actions DAG:\n{}",
            column_name, actions_dag ? actions_dag->dumpDAG() : "nullptr");
    return node;
}

ColumnWithTypeAndName JoinActionRef::getColumn() const
{
    const auto * node = getNode();
    return {node->column, node->result_type, column_name};
}

const String & JoinActionRef::getColumnName() const
{
    return column_name;
}

DataTypePtr JoinActionRef::getType() const
{
    return getNode()->result_type;
}

}
