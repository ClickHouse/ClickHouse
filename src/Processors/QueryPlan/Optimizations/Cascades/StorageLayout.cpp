#include <Processors/QueryPlan/Optimizations/Cascades/StorageLayout.h>

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitors.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/Optimizations/Cascades/DagNameTranslation.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Storages/KeyDescription.h>
#include <fmt/ranges.h>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

size_t positionOf(const Names & names, const String & name)
{
    return static_cast<size_t>(std::ranges::find(names, name) - names.begin());
}

/// The types that a layout function can read and compute. Floating-point types are not here: `0.` and
/// `-0.` compare as equal, but a hash of them is different.
bool isSupportedType(const DataTypePtr & type)
{
    const WhichDataType which(type);
    if (which.isNativeInteger() || which.isString() || which.isFixedString() || which.isDate())
        return true;
    /// A `DateTime` with no time zone in its type uses the time zone of the session.
    const auto * date_time = typeid_cast<const DataTypeDateTime *>(type.get());
    return date_time && date_time->hasExplicitTimeZone();
}

bool isNativeInteger(const ActionsDAG::Node * node)
{
    return WhichDataType(node->result_type).isNativeInteger();
}

/// The functions that a layout function can hold. The result of such a function depends only on the
/// values and the types of its arguments: it reads no setting and no time zone of the session.
bool isSupportedFunction(const ActionsDAG::Node & node)
{
    const auto & name = node.function_base->getName();
    const auto & arguments = node.children;

    if (name == "cityHash64")
        return !arguments.empty();

    if (name == "modulo" || name == "moduloLegacy" || name == "intDiv")
        return arguments.size() == 2 && std::ranges::all_of(arguments, isNativeInteger) && isNativeInteger(&node);

    /// On a number these functions use the time zone of the session. On a `Date`, and on a `DateTime` with
    /// a time zone in its type, they use only the value.
    if (name == "toYYYYMM" || name == "toStartOfMonth" || name == "toDate")
    {
        if (arguments.size() != 1)
            return false;
        const WhichDataType argument_type(arguments.front()->result_type);
        return argument_type.isDate() || argument_type.isDateTime();
    }

    return false;
}

/// Checks that a component of a partition key holds only columns, literals and supported functions over
/// supported types. Each other component can give different values in two tables that look equal: a
/// function can read a setting or the time zone of the session that writes the rows, and the content
/// of an `IN` set or of a lambda is not in the structure that the layout compares.
///
/// The check follows the text of the key and not only the DAG, because the DAG can hold a constant that
/// a function computed when the table was loaded, for example `toDate(1767225600)`. An `INSERT` builds a
/// key with `%` again from its text, with the settings of its session. Without this check two tables
/// with equal constants in the DAG could hold different partition values.
bool isSupportedExpression(const IAST & ast, const ActionsDAG::Node & node)
{
    if (node.type == ActionsDAG::ActionType::ALIAS)
        return isSupportedExpression(ast, *node.children.front());

    if (!isSupportedType(node.result_type))
        return false;

    if (ast.as<ASTIdentifier>())
        return node.type == ActionsDAG::ActionType::INPUT;
    if (ast.as<ASTLiteral>())
        return node.type == ActionsDAG::ActionType::COLUMN && node.column;

    const auto * function = ast.as<ASTFunction>();
    if (!function || !function->arguments || function->parameters || node.type != ActionsDAG::ActionType::FUNCTION)
        return false;

    const auto & ast_arguments = function->arguments->children;
    if (function->name != node.function_base->getName() || ast_arguments.size() != node.children.size()
        || !isSupportedFunction(node))
        return false;

    for (size_t i = 0; i < node.children.size(); ++i)
        if (!isSupportedExpression(*ast_arguments[i], *node.children[i]))
            return false;
    return true;
}

/// Walks a supported function from its output: finds the arguments in a fixed order and hashes the structure.
class StructureCollector
{
public:
    StructureCollector(Names & argument_names_, DataTypes & argument_types_, SipHash & hash_)
        : argument_names(argument_names_), argument_types(argument_types_), hash(hash_)
    {
    }

    void visit(const ActionsDAG::Node & node)
    {
        hash.update(static_cast<UInt8>(node.type));
        hash.update(node.result_type->getName());

        switch (node.type)
        {
            case ActionsDAG::ActionType::INPUT:
            {
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node {} in a storage layout function", node.result_name);
        }
    }

private:
    Names & argument_names;
    DataTypes & argument_types;
    SipHash & hash;
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
    /// The text of the key has one expression for each column name of the key, in the same order.
    if (!partition_key.expression || !partition_key.expression_list_ast
        || component_index >= partition_key.column_names.size()
        || component_index >= partition_key.expression_list_ast->children.size())
        return nullptr;

    const auto & key_actions = partition_key.expression->getActionsDAG();
    const auto * component = key_actions.tryFindInOutputs(partition_key.column_names[component_index]);
    if (!component || !isSupportedExpression(*partition_key.expression_list_ast->children[component_index], *component))
        return nullptr;

    std::shared_ptr<StorageLayoutFunction> function(new StorageLayoutFunction);
    auto & expression = function->form.emplace<PartitionExpression>();
    expression.dag = ActionsDAG::cloneSubDAG({component}, /*remove_aliases=*/ true);

    SipHash hash;
    StructureCollector collector(expression.argument_names, function->argument_types, hash);
    collector.visit(*expression.dag.getOutputs().front());

    /// A component with no arguments is a constant: all rows are in one group.
    if (expression.argument_names.empty())
        return nullptr;

    function->structure_hash = hash.get64();
    return function;
}

StorageLayoutFunctionPtr StorageLayoutFunction::createKeyRanges(
    DataTypes key_types, bool in_reverse_order, VectorWithMemoryTracking<VectorWithMemoryTracking<Field>> borders)
{
    SipHash hash;
    hash.update(key_types.size());
    for (const auto & type : key_types)
        hash.update(type->getName());
    hash.update(in_reverse_order);
    hash.update(borders.size());
    for (const auto & border : borders)
    {
        hash.update(border.size());
        for (const auto & value : border)
            applyVisitor(FieldVisitorHash(hash), value);
    }

    std::shared_ptr<StorageLayoutFunction> function(new StorageLayoutFunction);
    function->form = KeyRanges{.in_reverse_order = in_reverse_order, .borders = std::move(borders)};
    function->argument_types = std::move(key_types);
    function->structure_hash = hash.get64();
    return function;
}

bool StorageLayoutFunction::equals(const StorageLayoutFunction & other) const
{
    if (this == &other)
        return true;
    if (structure_hash != other.structure_hash || form.index() != other.form.index()
        || argument_types.size() != other.argument_types.size())
        return false;

    if (const auto * expression = tryGetPartitionExpression())
    {
        const auto & other_expression = std::get<PartitionExpression>(other.form);
        return sameStructure(
            *expression->dag.getOutputs().front(), expression->argument_names,
            *other_expression.dag.getOutputs().front(), other_expression.argument_names);
    }

    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (!argument_types[i]->equals(*other.argument_types[i]))
            return false;
    }
    const auto & ranges = std::get<KeyRanges>(form);
    const auto & other_ranges = std::get<KeyRanges>(other.form);
    return ranges.in_reverse_order == other_ranges.in_reverse_order && ranges.borders == other_ranges.borders;
}

String StorageLayoutFunction::dump() const
{
    if (const auto * expression = tryGetPartitionExpression())
        return dumpNode(*expression->dag.getOutputs().front(), expression->argument_names);

    /// The border values are not here: they can be long, and they change with the data.
    Strings arguments;
    for (size_t i = 0; i < argument_types.size(); ++i)
        arguments.push_back(fmt::format("#{}", i));
    return fmt::format("keyRanges({}; {} borders)", fmt::join(arguments, ", "), std::get<KeyRanges>(form).borders.size());
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
        for (const auto & argument_name : function->tryGetPartitionExpression()->argument_names)
            layout.argument_columns.push_back({argument_name});
        layout.function = std::move(function);
        layouts.push_back(std::move(layout));
    }
    return layouts;
}

/// The output names that carry one of the input columns `names` unchanged.
static NameSet renameColumns(const NameSet & names, const UnorderedMapWithMemoryTracking<String, Names> & input_to_outputs)
{
    NameSet renamed;
    for (const auto & name : names)
    {
        auto it = input_to_outputs.find(name);
        if (it != input_to_outputs.end())
            renamed.insert(it->second.begin(), it->second.end());
    }
    return renamed;
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
            NameSet renamed_argument = renameColumns(argument, input_to_outputs);
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

void KeyRangeLayoutCandidate::resize(size_t size)
{
    key_columns.resize(size);
    key_types.resize(size);
}

std::optional<KeyRangeLayoutCandidate> getKeyRangeLayoutCandidate(
    const KeyDescription & primary_key, const KeyDescription & sorting_key, const Block & output_header, size_t read_group)
{
    if (!primary_key.expression)
        return {};

    const auto & key_actions = primary_key.expression->getActionsDAG();
    const auto & reverse_flags = sorting_key.reverse_flags;

    KeyRangeLayoutCandidate candidate;
    candidate.in_reverse_order = !reverse_flags.empty() && reverse_flags.front();
    candidate.read_groups.push_back(read_group);

    for (size_t i = 0; i < primary_key.column_names.size(); ++i)
    {
        const bool is_reverse = i < reverse_flags.size() && reverse_flags[i];
        if (is_reverse != candidate.in_reverse_order || !isSafePrimaryDataKeyType(*primary_key.data_types[i]))
            break;

        /// A key column that is an expression is not used: the query computes the expression again, and its
        /// settings can give other values than the ones that the table stores in the primary index.
        const auto * key_column = key_actions.tryFindInOutputs(primary_key.column_names[i]);
        while (key_column && key_column->type == ActionsDAG::ActionType::ALIAS)
            key_column = key_column->children.front();
        if (!key_column || key_column->type != ActionsDAG::ActionType::INPUT || !output_header.has(key_column->result_name))
            break;

        candidate.key_columns.push_back({key_column->result_name});
        candidate.key_types.push_back(primary_key.data_types[i]);
    }

    if (candidate.key_columns.empty())
        return {};
    return candidate;
}

KeyRangeLayoutCandidates remapKeyRangeLayoutCandidates(const KeyRangeLayoutCandidates & candidates, const ActionsDAG & actions)
{
    const auto input_to_outputs = identityOutputNames(actions);

    KeyRangeLayoutCandidates result;
    for (const auto & candidate : candidates)
    {
        KeyRangeLayoutCandidate renamed_candidate = candidate;
        size_t size = 0;
        for (; size < candidate.key_columns.size(); ++size)
        {
            NameSet renamed_column = renameColumns(candidate.key_columns[size], input_to_outputs);
            if (renamed_column.empty())
                break;
            renamed_candidate.key_columns[size] = std::move(renamed_column);
        }

        if (size == 0)
            continue;
        renamed_candidate.resize(size);
        result.push_back(std::move(renamed_candidate));
    }
    return result;
}

}
