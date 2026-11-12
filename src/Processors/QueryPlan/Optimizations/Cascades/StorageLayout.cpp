#include <Processors/QueryPlan/Optimizations/Cascades/StorageLayout.h>

#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>
#include <Storages/KeyDescription.h>
#include <fmt/ranges.h>
#include <algorithm>

namespace DB
{

namespace
{

size_t positionOf(const Names & names, const String & name)
{
    return static_cast<size_t>(std::ranges::find(names, name) - names.begin());
}

/// A `DateTime` with no time zone in its type uses the time zone of the server, so two tables with equal
/// expressions can still compute different values.
bool usesServerTimeZone(const DataTypePtr & type)
{
    const auto inner_type = removeLowCardinalityAndNullable(type);
    if (const auto * date_time = typeid_cast<const DataTypeDateTime *>(inner_type.get()))
        return !date_time->hasExplicitTimeZone();
    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(inner_type.get()))
        return !date_time64->hasExplicitTimeZone();
    return false;
}

/// Walks the function from its output: finds the arguments in a fixed order, checks that every node can
/// be compared between tables, and hashes the structure.
class StructureCollector
{
public:
    StructureCollector(Names & argument_names_, DataTypes & argument_types_, SipHash & hash_)
        : argument_names(argument_names_), argument_types(argument_types_), hash(hash_)
    {
    }

    bool isUsable() const { return usable; }

    void visit(const ActionsDAG::Node & node)
    {
        if (usesServerTimeZone(node.result_type))
            usable = false;

        hash.update(static_cast<UInt8>(node.type));
        hash.update(node.result_type->getName());

        switch (node.type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
                /// `0.` and `-0.` compare as equal, but a hash of them is different.
                if (WhichDataType(removeLowCardinalityAndNullable(node.result_type)).isFloat())
                    usable = false;

                size_t position = positionOf(argument_names, node.result_name);
                if (position == argument_names.size())
                {
                    argument_names.push_back(node.result_name);
                    argument_types.push_back(node.result_type);
                }
                hash.update(position);
                break;
            }
            case ActionsDAG::ActionType::COLUMN:
            {
                if (!node.column)
                {
                    usable = false;
                    break;
                }
                applyVisitor(FieldVisitorHash(hash), node.column->getField());
                break;
            }
            case ActionsDAG::ActionType::ALIAS:
            {
                visit(*node.children.front());
                break;
            }
            case ActionsDAG::ActionType::FUNCTION:
            {
                hash.update(node.function_base->getName());
                hash.update(node.children.size());
                for (const auto * child : node.children)
                    visit(*child);
                break;
            }
            case ActionsDAG::ActionType::ARRAY_JOIN:
            case ActionsDAG::ActionType::PLACEHOLDER:
            {
                usable = false;
                break;
            }
        }
    }

private:
    Names & argument_names;
    DataTypes & argument_types;
    SipHash & hash;
    bool usable = true;
};

bool sameStructure(
    const ActionsDAG::Node & left, const Names & left_arguments, const ActionsDAG::Node & right, const Names & right_arguments)
{
    if (left.type == ActionsDAG::ActionType::ALIAS)
        return sameStructure(*left.children.front(), left_arguments, right, right_arguments);
    if (right.type == ActionsDAG::ActionType::ALIAS)
        return sameStructure(left, left_arguments, *right.children.front(), right_arguments);

    if (left.type != right.type || !left.result_type->equals(*right.result_type))
        return false;

    switch (left.type)
    {
        case ActionsDAG::ActionType::INPUT:
            return positionOf(left_arguments, left.result_name) == positionOf(right_arguments, right.result_name);
        case ActionsDAG::ActionType::COLUMN:
            return left.column && right.column && left.column->getField() == right.column->getField();
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (left.function_base->getName() != right.function_base->getName() || left.children.size() != right.children.size())
                return false;
            for (size_t i = 0; i < left.children.size(); ++i)
                if (!sameStructure(*left.children[i], left_arguments, *right.children[i], right_arguments))
                    return false;
            return true;
        }
        default:
            return false;
    }
}

String dumpNode(const ActionsDAG::Node & node, const Names & argument_names)
{
    switch (node.type)
    {
        case ActionsDAG::ActionType::INPUT:
            return fmt::format("#{}", positionOf(argument_names, node.result_name));
        case ActionsDAG::ActionType::COLUMN:
            return node.column ? applyVisitor(FieldVisitorToString(), node.column->getField()) : "?";
        case ActionsDAG::ActionType::ALIAS:
            return dumpNode(*node.children.front(), argument_names);
        case ActionsDAG::ActionType::FUNCTION:
        {
            Strings arguments;
            for (const auto * child : node.children)
                arguments.push_back(dumpNode(*child, argument_names));
            return fmt::format("{}({})", node.function_base->getName(), fmt::join(arguments, ", "));
        }
        default:
            return "?";
    }
}

}

StorageLayoutFunctionPtr StorageLayoutFunction::tryCreate(const KeyDescription & partition_key, size_t component_index)
{
    if (!partition_key.expression || component_index >= partition_key.column_names.size())
        return nullptr;

    const auto & key_actions = partition_key.expression->getActionsDAG();
    const auto * component = key_actions.tryFindInOutputs(partition_key.column_names[component_index]);
    if (!component)
        return nullptr;

    std::shared_ptr<StorageLayoutFunction> function(new StorageLayoutFunction);
    function->dag = ActionsDAG::cloneSubDAG({component}, /*remove_aliases=*/ true);
    /// The value of a part was computed when the part was written. A function that is deterministic only
    /// within one query (`dictGet`, for example) can give another value later or in another table.
    if (function->dag.hasArrayJoin() || function->dag.hasNonDeterministic() || function->dag.hasStatefulFunctions())
        return nullptr;

    SipHash hash;
    StructureCollector collector(function->argument_names, function->argument_types, hash);
    collector.visit(*function->dag.getOutputs().front());

    /// A component with no arguments is a constant: all rows are in one group.
    if (!collector.isUsable() || function->argument_names.empty())
        return nullptr;

    function->structure_hash = hash.get64();
    return function;
}

bool StorageLayoutFunction::equals(const StorageLayoutFunction & other) const
{
    if (this == &other)
        return true;
    if (structure_hash != other.structure_hash || argument_names.size() != other.argument_names.size())
        return false;
    return sameStructure(*dag.getOutputs().front(), argument_names, *other.dag.getOutputs().front(), other.argument_names);
}

String StorageLayoutFunction::dump() const
{
    return dumpNode(*dag.getOutputs().front(), argument_names);
}

bool sameStorageLayoutFunction(const StorageLayoutFunctionPtr & left, const StorageLayoutFunctionPtr & right)
{
    if (!left || !right)
        return !left && !right;
    return left->equals(*right);
}

StorageLayouts getStorageLayouts(const KeyDescription & partition_key)
{
    StorageLayouts layouts;
    for (size_t component_index = 0; component_index < partition_key.column_names.size(); ++component_index)
    {
        auto function = StorageLayoutFunction::tryCreate(partition_key, component_index);
        if (!function)
            continue;

        StorageLayout layout;
        for (const auto & argument_name : function->getArgumentNames())
            layout.argument_columns.push_back({argument_name});
        layout.function = std::move(function);
        layouts.push_back(std::move(layout));
    }
    return layouts;
}

StorageLayouts remapStorageLayouts(const StorageLayouts & layouts, const ActionsDAG & actions)
{
    const auto input_to_outputs = identityOutputNames(actions);

    StorageLayouts result;
    for (const auto & layout : layouts)
    {
        StorageLayout renamed_layout;
        renamed_layout.function = layout.function;

        bool all_arguments_reach_output = true;
        for (const auto & argument : layout.argument_columns)
        {
            NameSet renamed_argument;
            for (const auto & name : argument)
            {
                auto it = input_to_outputs.find(name);
                if (it != input_to_outputs.end())
                    renamed_argument.insert(it->second.begin(), it->second.end());
            }
            if (renamed_argument.empty())
            {
                all_arguments_reach_output = false;
                break;
            }
            renamed_layout.argument_columns.push_back(std::move(renamed_argument));
        }

        if (all_arguments_reach_output)
            result.push_back(std::move(renamed_layout));
    }
    return result;
}

}
