#include <Analyzer/Passes/getSubcolumnOfFunction.h>
#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Core/Names.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/StorageSnapshot.h>

#include <fmt/format.h>

#include <array>
#include <numeric>
#include <optional>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{
namespace
{

/// The optimization applies only to `String` / `FixedString`. Although `length`, `empty` and
/// `notEmpty` also accept `Array` and `Map`, a composite path can share its payload with other
/// expressions under a different nested-column name. The optimizer cannot prove that removing one
/// such path removes the payload from a sort or filter, so it must not push those functions down.
/// `empty` / `notEmpty` are additionally defined for fixed-size types (`UUID`, `IPv4`, `IPv6`,
/// `QBit`), but replacing a fixed-size column with another fixed-size column reduces no volume.
bool isSupportedArgumentType(const String & function_name, const DataTypePtr & type)
{
    if (function_name == "lengthUTF8")
    {
        if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(type.get()))
            return fixed_string->getN() > sizeof(UInt64);

        return isStringOrFixedString(type);
    }

    if (function_name == "length" || function_name == "empty" || function_name == "notEmpty")
        return isStringOrFixedString(type);

    return false;
}

/// A `Filter` evaluates the pushed function for every input row, whereas the original plan
/// evaluates it only for rows which pass. Restrict this path to operations whose cost does not
/// depend on the payload size. `lengthUTF8` scans every byte, and `empty` / `notEmpty` scan a
/// `FixedString` to find a non-zero byte, so a selective predicate that also reads the argument
/// can make either rewrite slower.
bool isCheapToEvaluateBeforeFilter(const IFunctionBase & function, const DataTypePtr & type)
{
    const auto & function_name = function.getName();
    return function_name == "length"
        || ((function_name == "empty" || function_name == "notEmpty") && typeid_cast<const DataTypeString *>(type.get()));
}

/// Follow a chain of renames back to the column it started from.
const ActionsDAG::Node * resolveAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
        node = node->children.front();

    return node;
}

/// The `ActionsDAG` owned by the step, or `nullptr` for steps that own none (`Sorting`, `Limit`).
ActionsDAG * tryGetStepActions(IQueryPlanStep * step)
{
    if (auto * expression = typeid_cast<ExpressionStep *>(step))
        return &expression->getExpression();

    if (auto * filter = typeid_cast<FilterStep *>(step))
        return &filter->getExpression();

    return nullptr;
}

/// Find actions below a chain of one-input, header-preserving steps. `SortingStep` itself owns no
/// actions, and steps such as `LimitStep` can sit between it and the `ExpressionStep` that created
/// aliases of the same wide input.
ActionsDAG * findActionsBelowHeaderPreservingSteps(QueryPlan::Node * node)
{
    while (node && node->children.size() == 1)
    {
        if (auto * actions = tryGetStepActions(node->step.get()))
            return actions;

        const auto & input_headers = node->step->getInputHeaders();
        if (input_headers.size() != 1 || !blocksHaveEqualStructure(*input_headers.front(), *node->step->getOutputHeader()))
            return nullptr;

        node = node->children.front();
    }

    return nullptr;
}

/// Names of the input columns some output of `actions` depends on. An input nothing depends on is
/// tolerated by `ActionsDAG::updateHeader` and by `ExpressionActions::execute` when the column is
/// missing from the block, so it does not keep the column alive.
NameSet collectInputsNeededByNode(const ActionsDAG::Node * root)
{
    NameSet names;
    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> visited;
    ActionsDAG::NodeRawConstPtrs stack{root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;

        if (node->type == ActionsDAG::ActionType::INPUT)
            names.insert(node->result_name);

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }

    return names;
}

NameSet collectInputsNeededByOutputs(const ActionsDAG & actions)
{
    NameSet names;
    for (const auto * output : actions.getOutputs())
    {
        auto output_inputs = collectInputsNeededByNode(output);
        names.insert(output_inputs.begin(), output_inputs.end());
    }

    return names;
}

/// Whether the wide argument column is worth taking out of the step, and which of its input columns
/// the step reads on its own regardless of what the parent needs (those can never stop being
/// produced below the step).
///
/// Only steps that really move the column around are worth pushing below: `Sorting` buffers and
/// merges every column of every row, `Filter` copies the surviving rows of every column. Other 1:1
/// steps gain nothing and would fight the optimizations that move them in the opposite direction
/// (`tryMergeExpressions` merges an `Expression` back with the pushed step, `tryPushDownLimit`
/// moves a `Limit` below it).
bool canPushBelowStep(IQueryPlanStep * step, NameSet & columns_pinned_by_step)
{
    if (const auto * sorting = typeid_cast<const SortingStep *>(step))
    {
        /// `PARTITION BY` reads columns which are not part of the sort description.
        if (sorting->hasPartitions())
            return false;

        for (const auto & column : sorting->getSortDescription())
            columns_pinned_by_step.insert(column.column_name);

        return true;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        columns_pinned_by_step.insert(filter->getFilterColumnName());
        return true;
    }

    return false;
}

bool hasDuplicatedNames(const Block & header)
{
    UnorderedSetWithMemoryTracking<std::string_view> seen;
    for (const auto & column : header)
        if (!seen.insert(column.name).second)
            return true;

    return false;
}

bool hasDuplicatedInputNames(const ActionsDAG & actions)
{
    UnorderedSetWithMemoryTracking<std::string_view> seen;
    for (const auto * input : actions.getInputs())
        if (!seen.insert(input->result_name).second)
            return true;

    return false;
}

/// One function to recompute below the child step. Captured from the parent's node before the
/// rewrite, because the parent's `ActionsDAG` is replaced by the second part of the split.
struct PushedFunction
{
    FunctionBasePtr function_base;
    /// Exactly the name of the parent's node, so that the second part of the split finds it.
    String result_name;
    /// Name of the argument column in the child step's *input* header, which may differ from the
    /// name the parent sees when the child renames the column.
    String argument_name;
};

}

namespace
{

/// The functions of `actions` that `is_candidate` accepts, grouped by the column their first argument is (an input, or
/// a chain of renames of one), restricted to columns nothing else in the DAG reads. Their other arguments are constants.
template <typename IsCandidate>
UnorderedMapWithMemoryTracking<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs>
collectFunctionsReplacingTheirArgument(const ActionsDAG & actions, IsCandidate && is_candidate)
{
    /// The column a node stands for, for inputs and for chains of renames of an input.
    UnorderedMapWithMemoryTracking<const ActionsDAG::Node *, const ActionsDAG::Node *> column_of_node;
    for (const auto & node : actions.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::INPUT)
            column_of_node.emplace(&node, &node);
        else if (node.type == ActionsDAG::ActionType::ALIAS)
            if (const auto * source = resolveAliases(&node); source->type == ActionsDAG::ActionType::INPUT)
                column_of_node.emplace(&node, source);
    }

    UnorderedMapWithMemoryTracking<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs> result;
    for (const auto & node : actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || node.children.empty())
            continue;

        auto it = column_of_node.find(node.children.front());
        if (it == column_of_node.end())
            continue;

        if (!std::all_of(node.children.begin() + 1, node.children.end(),
                [](const auto * child) { return child->type == ActionsDAG::ActionType::COLUMN; }))
            continue;

        if (!is_candidate(node, *it->second))
            continue;

        result[it->second].push_back(&node);
    }

    if (result.empty())
        return result;

    /// A column is replaced only if nothing but those functions reads it: otherwise the wide column
    /// is needed anyway and computing the functions early only adds to the data being carried.
    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> has_other_readers;
    for (const auto & node : actions.getNodes())
    {
        const auto * reader_column = column_of_node.contains(&node) ? column_of_node.at(&node) : nullptr;
        for (const auto * child : node.children)
        {
            auto it = column_of_node.find(child);
            if (it == column_of_node.end() || it->second == reader_column)
                continue;

            auto functions = result.find(it->second);
            if (functions == result.end())
                continue;

            if (std::find(functions->second.begin(), functions->second.end(), &node) == functions->second.end())
                has_other_readers.insert(it->second);
        }
    }

    for (const auto * output : actions.getOutputs())
        if (auto it = column_of_node.find(output); it != column_of_node.end())
            has_other_readers.insert(it->second);

    for (auto it = result.begin(); it != result.end();)
    {
        /// Two functions producing the same name would be indistinguishable in a header.
        NameSet names;
        bool has_duplicated_names = false;
        for (const auto * function : it->second)
            has_duplicated_names |= !names.insert(function->result_name).second;

        if (has_other_readers.contains(it->first) || has_duplicated_names)
            it = result.erase(it);
        else
            ++it;
    }

    return result;
}

}

std::unordered_map<const ActionsDAG::Node *, ActionsDAG::NodeRawConstPtrs>
collectVolumeReducingFunctionsReplacingTheirArgument(const ActionsDAG & actions)
{
    return collectFunctionsReplacingTheirArgument(
        actions,
        [](const ActionsDAG::Node & node, const ActionsDAG::Node & source)
        {
            /// The function is going to be evaluated on a different set of rows.
            return node.function_base->isVolumeReducing() && node.function_base->isDeterministic()
                && node.function_base->isDeterministicInScopeOfQuery() && node.children.size() == 1
                && isSupportedArgumentType(node.function_base->getName(), source.result_type);
        });
}

UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> collectVolumeReducingFunctionsToKeepBelow(
    const ActionsDAG & actions, const ActionsDAG::Node * low_part_root)
{
    /// A column the DAG surfaces crosses the step no matter where the function is computed, so the
    /// function may be lifted as before.
    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> surfaced;
    for (const auto * output : actions.getOutputs())
        surfaced.insert(resolveAliases(output));

    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> split_nodes;
    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> low_part_nodes;
    if (low_part_root)
    {
        ActionsDAG::NodeRawConstPtrs stack{low_part_root};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (!low_part_nodes.insert(node).second)
                continue;

            stack.insert(stack.end(), node->children.begin(), node->children.end());
        }
    }

    for (const auto & node : actions.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || !node.function_base->isVolumeReducing())
            continue;

        if (!node.function_base->isDeterministic() || !node.function_base->isDeterministicInScopeOfQuery())
            continue;

        if (node.children.size() != 1)
            continue;

        /// Unlike in `collectVolumeReducingFunctionsReplacingTheirArgument`, the argument does not
        /// have to be an `INPUT` with no other readers: `tryMergeExpressions` can merge the pushed
        /// step with the step below it (the argument becomes a computed column) or with a `Filter`
        /// whose condition reads the argument. Lifting the function in those cases would make the
        /// wide argument cross the step again, undoing `tryPushDownVolumeReducingFunction` and
        /// re-triggering it forever.
        const auto * argument = resolveAliases(node.children.front());
        if (!isSupportedArgumentType(node.function_base->getName(), argument->result_type))
            continue;

        /// `trySplitFilter` also visits expressions which were never rewritten by
        /// `tryPushDownVolumeReducingFunction`, for example after an outer filter is merged into
        /// a subquery projection. Keep a function below a filter only when the dedicated rewrite
        /// could have moved it there: the predicate must read its argument, the function must be
        /// cheap on every input row, and the predicate must not already calculate the same scalar.
        if (low_part_root)
        {
            if (!low_part_nodes.contains(argument)
                || !isCheapToEvaluateBeforeFilter(*node.function_base, argument->result_type))
                continue;

            bool predicate_computes_function = false;
            for (const auto * low_part_node : low_part_nodes)
            {
                if (low_part_node == &node
                    || low_part_node->type != ActionsDAG::ActionType::FUNCTION
                    || !low_part_node->function_base
                    || low_part_node->children.size() != 1
                    || low_part_node->function_base->getName() != node.function_base->getName())
                    continue;

                if (resolveAliases(low_part_node->children.front()) == argument)
                {
                    predicate_computes_function = true;
                    break;
                }
            }

            if (predicate_computes_function)
                continue;
        }

        if (surfaced.contains(argument))
            continue;

        /// Keeping a volume-reducing function below the barrier only pays off if no calculation
        /// that is still lifted also needs its argument. Otherwise the wide argument crosses the
        /// barrier anyway and evaluating the function before a row-reducing step is a regression.
        bool argument_is_needed_by_lifted_node = false;
        for (const auto & reader : actions.getNodes())
        {
            if (low_part_nodes.contains(&reader))
                continue;

            if (reader.type == ActionsDAG::ActionType::ALIAS)
                continue;

            for (const auto * child : reader.children)
            {
                if (resolveAliases(child) != argument)
                    continue;

                const bool is_another_supported_volume_reducing_function = reader.type == ActionsDAG::ActionType::FUNCTION
                    && reader.function_base && reader.function_base->isVolumeReducing() && reader.function_base->isDeterministic()
                    && reader.function_base->isDeterministicInScopeOfQuery() && reader.children.size() == 1
                    && isSupportedArgumentType(reader.function_base->getName(), argument->result_type);

                /// A sibling function is harmless only if it can stay below the filter too.
                /// Otherwise its argument still crosses the filter, and evaluating this function
                /// for rejected rows is a regression. For example, `length` is cheap but
                /// `lengthUTF8` scans the payload, so neither can stay below a filter that
                /// reads their shared argument.
                const bool can_keep_sibling_below = is_another_supported_volume_reducing_function
                    && (!low_part_root || isCheapToEvaluateBeforeFilter(*reader.function_base, argument->result_type));

                if (&reader != &node && !can_keep_sibling_below)
                {
                    argument_is_needed_by_lifted_node = true;
                    break;
                }
            }

            if (argument_is_needed_by_lifted_node)
                break;
        }

        if (argument_is_needed_by_lifted_node)
            continue;

        split_nodes.insert(&node);
    }

    if (split_nodes.empty())
        return split_nodes;

    /// Prefer splitting at the output itself, so that an alias of a kept function does not end up
    /// alone in the lifted part.
    for (const auto * output : actions.getOutputs())
        if (split_nodes.contains(resolveAliases(output)))
            split_nodes.insert(output);

    return split_nodes;
}

namespace
{

using SubcolumnPredicate = std::function<bool(const ISerialization::SubstreamPath &)>;

/// A constant argument of a function of a subcolumn.
struct ResidualArgument
{
    ColumnConstPtr column;
    DataTypePtr type;
    String name;
};

/// A function of a column that is a fixed expression of one of its subcolumns: `getSubcolumn(x, 'p')` itself, or one of
/// the functions `FunctionToSubcolumnsPass` rewrites to subcolumns for a column read directly from a table.
struct SubcolumnRead
{
    const ActionsDAG::Node * node = nullptr;
    String subcolumn;
    DataTypePtr subcolumn_type;
    /// What the subcolumn has to be in storage. Empty for `getSubcolumn`, which means the subcolumn of that name.
    SubcolumnPredicate is_expected_subcolumn;
    /// The function of the subcolumn and of `residual_arguments` that gives the value. Empty: the subcolumn is the value.
    String residual;
    std::vector<ResidualArgument> residual_arguments;
    bool is_function_form = false;
    bool is_map_key = false;
};

std::optional<Field> tryGetConstant(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
        return {};

    return node->column->getField();
}

ResidualArgument makeConstantArgument(const ActionsDAG::Node * node)
{
    return {node->column, node->result_type, node->result_name};
}

ResidualArgument makeZeroArgument()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return {ColumnConst::create(type->createColumnConst(1, static_cast<UInt64>(0))->convertToFullColumnIfConst(), 0), type, "0_UInt64"};
}

std::optional<SubcolumnRead> tryMatchSubcolumnReadImpl(const ActionsDAG::Node & node, const DataTypePtr & source_type, bool allow_function_forms)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base || node.children.empty())
        return {};

    if (!node.function_base->isDeterministic() || !node.function_base->isDeterministicInScopeOfQuery())
        return {};

    const auto & name = node.function_base->getName();
    const auto & result_type = node.result_type;
    const size_t arguments = node.children.size();

    SubcolumnRead read;
    read.node = &node;

    if (name == "getSubcolumn")
    {
        auto subcolumn = arguments == 2 ? tryGetConstant(node.children[1]) : std::nullopt;
        if (!subcolumn || subcolumn->getType() != Field::Types::String)
            return {};

        read.subcolumn = subcolumn->safeGet<String>();
        read.subcolumn_type = result_type;
        return read;
    }

    if (!allow_function_forms)
        return {};

    read.is_function_form = true;
    const auto source_type_id = source_type->getTypeId();
    const auto nested_type = removeNullable(source_type);

    auto set_subcolumn = [&](String subcolumn, DataTypePtr subcolumn_type, SubcolumnPredicate predicate)
    {
        read.subcolumn = std::move(subcolumn);
        read.subcolumn_type = std::move(subcolumn_type);
        read.is_expected_subcolumn = std::move(predicate);
    };

    if (name == "tupleElement" && arguments == 2 && isTuple(nested_type))
    {
        /// `tupleElement` fills defaults for outer NULLs when the element cannot represent NULL, while the subcolumn keeps
        /// the values beneath the parent null map.
        if (source_type->isNullable() && !canContainNull(*result_type))
            return {};

        auto value = tryGetConstant(node.children[1]);
        if (!value)
            return {};

        const auto & tuple = assert_cast<const DataTypeTuple &>(*nested_type);
        auto element = getElementSubcolumnName(*value, tuple);
        if (!element || tupleElementNameIsAmbiguousWhenFlattened(tuple, *element))
            return {};

        set_subcolumn(*element, result_type, [element_name = *element](const auto & path) { return SerializationTuple::isElementSubcolumn(path, element_name); });
        return read;
    }

    if (name == "variantElement" && arguments == 2 && source_type_id == TypeIndex::Variant)
    {
        auto value = tryGetConstant(node.children[1]);
        if (!value)
            return {};

        auto element = getElementSubcolumnName(*value, assert_cast<const DataTypeVariant &>(*source_type));
        if (!element)
            return {};

        set_subcolumn(*element, result_type, [element_name = *element](const auto & path) { return SerializationVariant::isElementSubcolumn(path, element_name); });
        return read;
    }

    if (name == "tupleElement" && arguments == 2 && source_type_id == TypeIndex::QBit)
    {
        auto value = tryGetConstant(node.children[1]);
        if (!value)
            return {};

        auto element = getElementSubcolumnName(*value, assert_cast<const DataTypeQBit &>(*source_type));
        if (!element)
            return {};

        set_subcolumn(*element, result_type, [element_name = *element](const auto & path) { return SerializationTuple::isElementSubcolumn(path, element_name); });
        return read;
    }

    if (source_type_id == TypeIndex::Map)
    {
        const auto & map = assert_cast<const DataTypeMap &>(*source_type);
        auto keys_type = std::make_shared<DataTypeArray>(map.getKeyType());
        auto values_type = std::make_shared<DataTypeArray>(map.getValueType());

        if ((name == "mapKeys" || name == "mapValues") && arguments == 1)
        {
            if (name == "mapKeys")
                set_subcolumn("keys", keys_type, SerializationMap::isKeysSubcolumn);
            else
                set_subcolumn("values", values_type, SerializationMap::isValuesSubcolumn);
            return read;
        }

        if ((name == "mapContainsKey" || name == "has" || name == "notHas" || name == "mapContainsValue") && arguments == 2)
        {
            /// The Map implementation removes LowCardinality before comparing keys, the keys subcolumn would not.
            if (name != "mapContainsKey" && name != "mapContainsValue" && WhichDataType(map.getKeyType()).isLowCardinality())
                return {};

            if (name == "mapContainsValue")
                set_subcolumn("values", values_type, SerializationMap::isValuesSubcolumn);
            else
                set_subcolumn("keys", keys_type, SerializationMap::isKeysSubcolumn);

            read.residual = name == "notHas" ? "notHas" : "has";
            read.residual_arguments = {makeConstantArgument(node.children[1])};
            return read;
        }

        if (name == "arrayElement" && arguments == 2)
        {
            auto key = tryGetConstant(node.children[1]);
            if (!key)
                return {};

            /// A shorter constant is padded, so that a missing key would match a key with trailing zero bytes.
            const auto key_type = removeLowCardinality(map.getKeyType());
            if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(key_type.get());
                fixed_string && (key->getType() != Field::Types::String || key->safeGet<String>().size() != fixed_string->getN()))
                return {};

            auto subcolumn = tryGetMapKeySubcolumnName(map, *key);
            if (!subcolumn)
                return {};

            set_subcolumn(*subcolumn, map.getValueType(), SerializationMap::isKeyValueSubcolumn);
            read.is_map_key = true;
            return read;
        }
    }

    const bool is_array_or_map = source_type_id == TypeIndex::Array || source_type_id == TypeIndex::Map;
    const bool is_string = isString(nested_type) && (source_type_id == TypeIndex::String || source_type_id == TypeIndex::Nullable);
    if ((name == "length" || name == "empty" || name == "notEmpty") && arguments == 1 && (is_array_or_map || is_string))
    {
        if (is_array_or_map)
            set_subcolumn("size0", std::make_shared<DataTypeUInt64>(), SerializationArray::isArraySizesSubcolumn);
        else
            set_subcolumn("size", getStringSizesSubcolumnType(source_type), SerializationString::isStringSizesSubcolumn);

        if (name != "length")
        {
            read.residual = name == "empty" ? "equals" : "notEquals";
            read.residual_arguments = {makeZeroArgument()};
        }
        return read;
    }

    if ((name == "isNull" || name == "isNotNull") && arguments == 1 && source_type_id == TypeIndex::Nullable)
    {
        set_subcolumn("null", std::make_shared<DataTypeUInt8>(), SerializationNullable::isNullMapSubcolumn);
        /// A null map byte only has to be non-zero to mean NULL.
        read.residual = name == "isNull" ? "notEquals" : "not";
        if (name == "isNull")
            read.residual_arguments = {makeZeroArgument()};
        return read;
    }

    return {};
}

std::optional<SubcolumnRead> tryMatchSubcolumnRead(const ActionsDAG::Node & node, const DataTypePtr & source_type, bool allow_function_forms)
{
    auto read = tryMatchSubcolumnReadImpl(node, source_type, allow_function_forms);
    /// The subcolumn stands for the node, so it has the node's type.
    if (read && read->residual.empty() && !read->subcolumn_type->equals(*node.result_type))
        return {};

    return read;
}

/// Subcolumn reads of `actions` grouped by the input they read, for inputs nothing else in the DAG reads.
std::unordered_map<const ActionsDAG::Node *, std::vector<SubcolumnRead>>
collectSubcolumnReads(const ActionsDAG & actions, bool allow_function_forms)
{
    auto nodes_by_input = collectFunctionsReplacingTheirArgument(
        actions,
        [&](const ActionsDAG::Node & node, const ActionsDAG::Node & source)
        { return tryMatchSubcolumnRead(node, source.result_type, allow_function_forms).has_value(); });

    std::unordered_map<const ActionsDAG::Node *, std::vector<SubcolumnRead>> result;
    for (const auto & [input, nodes] : nodes_by_input)
        for (const auto * node : nodes)
            result[input].push_back(*tryMatchSubcolumnRead(*node, input->result_type, allow_function_forms));

    return result;
}

/// One step of the path from the top step down to the read, with what the rewrite does to it.
struct SubcolumnsPathStep
{
    QueryPlan::Node * node = nullptr;
    /// For `Expression` / `Filter`: a copy of the step's actions without the outputs that carry the columns further up.
    std::optional<ActionsDAG> actions;
    /// Names of the inputs of `actions` that are the columns.
    NameSet inputs;
    std::vector<SubcolumnRead> reads;
    /// The candidate column each of `reads` reads.
    std::vector<size_t> read_candidates;
    std::unordered_map<const ActionsDAG::Node *, FunctionBasePtr> residuals;
    std::optional<ActionsDAG> new_actions;
    /// For a join: the side the columns come from, the positions of their outputs and of the columns in the side's input.
    std::optional<JoinTableSide> join_side;
    std::vector<size_t> join_output_positions;
    std::vector<size_t> join_input_positions;
};

/// A column crossing the path instead of a read column.
struct CarriedColumn
{
    String name;
    String subcolumn;
    DataTypePtr type;
    size_t candidate = 0;
};

/// The columns of its sides that `join` outputs unchanged and reads for nothing else.
class JoinPassThroughColumns
{
public:
    explicit JoinPassThroughColumns(const JoinStepLogical & join_)
        : join(join_)
    {
        if (join.hasCorrelatedExpressions() || !join.canRemoveUnusedColumns() || hasDuplicatedNames(*join.getOutputHeader())
            || hasDuplicatedNames(*join.getInputHeaders().at(0)) || hasDuplicatedNames(*join.getInputHeaders().at(1)))
            return;

        const auto & actions = join.getActionsDAG();
        for (const auto * output : actions.getOutputs())
            outputs.emplace(output->result_name, output);
        for (const auto * input : actions.getInputs())
            inputs.emplace(input->result_name, input);
        for (const auto & node : actions.getNodes())
            read_nodes.insert(node.children.begin(), node.children.end());
        for (const auto & action : join.getJoinOperator().expression)
            read_nodes.insert(action.getNode());
        for (const auto & action : join.getJoinOperator().residual_filter)
            read_nodes.insert(action.getNode());
    }

    bool returns(const String & name) const { return outputs.contains(name); }

    bool reads(const String & name) const
    {
        auto it = inputs.find(name);
        return it != inputs.end() && read_nodes.contains(it->second);
    }

    /// The side of column `name`, if the join never fills that side with defaults.
    std::optional<JoinTableSide> getSide(const String & name) const
    {
        auto it = outputs.find(name);
        if (it == outputs.end() || it->second->type != ActionsDAG::ActionType::INPUT || read_nodes.contains(it->second))
            return {};

        const JoinActionRef ref(it->second, join.getExpressionActions());
        const auto kind = join.getJoinOperator().kind;
        const bool both_sides_kept = kind == JoinKind::Inner || kind == JoinKind::Cross || kind == JoinKind::Comma;
        if (ref.fromLeft() && !ref.fromRight() && (both_sides_kept || kind == JoinKind::Left))
            return JoinTableSide::Left;
        if (ref.fromRight() && !ref.fromLeft() && (both_sides_kept || kind == JoinKind::Right))
            return JoinTableSide::Right;

        return {};
    }

private:
    const JoinStepLogical & join;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs;
    std::unordered_set<const ActionsDAG::Node *> read_nodes;
};

/// Makes `join` return `carried`, read from `side`, instead of the columns at `input_positions` of that side, which it
/// returns at `output_positions` and reads for nothing else. The carried columns go first, so that the join always keeps
/// an output of its own. Returns whether exactly those outputs went away.
bool replaceJoinPassThroughColumns(
    JoinStepLogical & join, JoinTableSide side, const std::vector<size_t> & input_positions, const std::vector<size_t> & output_positions,
    const ColumnsWithTypeAndName & carried)
{
    const size_t index = JoinStepLogical::getInputIndex(side);
    for (const auto & column : carried)
        join.addPassThroughColumn(column, side);

    SharedHeaders headers = join.getInputHeaders();
    Block header_with_carried = *headers.at(index);
    for (const auto & column : carried)
        header_with_carried.insert(column);
    headers.at(index) = std::make_shared<const Block>(std::move(header_with_carried));
    join.updateInputHeaders(headers);

    std::vector<IQueryPlanStep::PrunedInput> pruned;
    for (size_t i = 0; i < headers.size(); ++i)
    {
        if (i != index)
        {
            pruned.push_back(IQueryPlanStep::PrunedInput::unchanged(headers[i]));
            continue;
        }

        Block header;
        for (size_t position = 0; position < headers[i]->columns(); ++position)
            if (!std::ranges::binary_search(input_positions, position))
                header.insert(headers[i]->getByPosition(position));
        pruned.push_back({input_positions, std::make_shared<const Block>(std::move(header))});
    }

    return join.removeUnusedColumns(output_positions, pruned).dropped_output_positions == output_positions;
}

Block transformStepHeader(const IQueryPlanStep * step, const Block & input_header, const ActionsDAG & actions)
{
    if (const auto * filter = typeid_cast<const FilterStep *>(step))
        return FilterTransform::transformHeader(input_header, &actions, filter->getFilterColumnName(), filter->removesFilterColumn());

    return ExpressionTransform::transformHeader(input_header, actions);
}

/// Whether storage reads the subcolumn `column.subcolumn` with the values `read` means of `column` that `read_step` reads.
bool storageResolvesSubcolumn(const ReadFromMergeTree & read_step, const String & column, const SubcolumnRead & read)
{
    const auto & storage_snapshot = read_step.getStorageSnapshot();
    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();
    auto source = storage_snapshot->tryGetColumn(options, column);
    auto resolved = storage_snapshot->tryGetColumn(options, column + "." + read.subcolumn);
    if (!source || !resolved || !resolved->isSubcolumn() || resolved->getNameInStorage() != source->getNameInStorage()
        || !resolved->type->equals(*read.subcolumn_type))
        return false;

    auto resolved_info = resolved->getTypeInStorage()->tryGetSubcolumnInfo(resolved->getSubcolumnName());
    if (!resolved_info)
        return false;

    /// Subcolumn names are flat: `c.a.b` may name a sibling element called `a.b` instead of `b` of `a`.
    if (source->isSubcolumn())
    {
        auto source_info = source->getTypeInStorage()->tryGetSubcolumnInfo(source->getSubcolumnName());
        auto inner_info = source->type->tryGetSubcolumnInfo(read.subcolumn);
        if (!source_info || !inner_info)
            return false;

        ISerialization::SubstreamPath expected_path = source_info->substreams_path;
        expected_path.insert(expected_path.end(), inner_info->substreams_path.begin(), inner_info->substreams_path.end());
        if (expected_path.size() != resolved_info->substreams_path.size()
            || !std::equal(expected_path.begin(), expected_path.end(), resolved_info->substreams_path.begin(), isSameSubstream))
            return false;
    }
    else if (resolved->getSubcolumnName() != read.subcolumn)
        return false;

    return !read.is_expected_subcolumn || read.is_expected_subcolumn(resolved_info->substreams_path);
}

/// The name the subcolumn of `read` crosses the path under.
String getCarriedColumnName(const SubcolumnRead & read)
{
    if (read.residual.empty())
        return read.node->result_name;

    return resolveAliases(read.node->children.front())->result_name + "." + read.subcolumn;
}

/// Drops the inputs of the read columns, which no longer reach the step. A step must not declare more inputs than its
/// input header has, and the other inputs stay so that the columns they match do not pass the step unmatched.
void removeInputsOfReadColumn(ActionsDAG & actions, const NameSet & column_names)
{
    std::unordered_set<const ActionsDAG::Node *> kept_inputs;
    for (const auto * input : actions.getInputs())
        if (!column_names.contains(input->result_name))
            kept_inputs.insert(input);

    actions.removeUnusedActions(kept_inputs, /*allow_constant_folding=*/false);
}

/// `actions` with every read replaced: a plain read by an input of its name, a read with a residual function by that
/// function of the carried subcolumn, computed in the same step.
std::optional<ActionsDAG> replaceSubcolumnReads(
    const ActionsDAG & actions, const std::vector<SubcolumnRead> & reads, const std::unordered_map<const ActionsDAG::Node *, FunctionBasePtr> & residuals)
{
    std::unordered_set<const ActionsDAG::Node *> split_nodes;
    for (const auto & read : reads)
        split_nodes.insert(read.node);

    auto second = std::move(actions.split(split_nodes).second);
    if (residuals.empty())
        return second;

    ActionsDAG prefix;
    std::unordered_map<String, const ActionsDAG::Node *> carried_inputs;
    for (const auto & read : reads)
    {
        if (read.residual.empty())
            continue;

        const auto carried_name = getCarriedColumnName(read);
        auto [it, inserted] = carried_inputs.emplace(carried_name, nullptr);
        if (inserted)
            it->second = &prefix.addInput(carried_name, read.subcolumn_type);

        ActionsDAG::NodeRawConstPtrs arguments{it->second};
        for (const auto & argument : read.residual_arguments)
            arguments.push_back(&prefix.addColumn(argument.column, argument.type, argument.name));

        prefix.getOutputs().push_back(&prefix.addFunction(residuals.at(read.node), std::move(arguments), read.node->result_name));
    }

    auto merged = ActionsDAG::merge(std::move(prefix), std::move(second));
    if (hasDuplicatedInputNames(merged))
        return {};
    return merged;
}

/// The step the push-down starts from, and its subcolumn reads.
struct SubcolumnsTop
{
    QueryPlan::Node * node = nullptr;
    /// A copy of the step's actions without unused actions.
    ActionsDAG actions;
    std::unordered_map<const ActionsDAG::Node *, std::vector<SubcolumnRead>> reads;
    std::unordered_map<std::string_view, const ActionsDAG::Node *> input_by_name;
    /// Copies: the rewrite replaces the headers of the step.
    Block input_header;
    Block output_header;
    bool allow_function_forms = false;
};

/// A column of the top step's input that only subcolumn reads read.
struct SubcolumnsCandidate
{
    String top_name;
    /// Its name in the output of the read.
    String column;
};

/// Candidates that come from the same read through the same steps.
struct SubcolumnsGroup
{
    QueryPlan::Node * read_node = nullptr;
    /// Each step from the top down, with the index of the child the columns come from.
    std::vector<std::pair<QueryPlan::Node *, size_t>> path;
    std::vector<SubcolumnsCandidate> candidates;
};

/// Follows the candidates down through the steps that pass them unchanged, to the reads that return them. The ones that
/// get to no read are dropped.
std::vector<SubcolumnsGroup> followCandidatesDownToReads(QueryPlan::Node * top_node, const Names & top_names)
{
    struct Branch
    {
        QueryPlan::Node * node = nullptr;
        SubcolumnsGroup group;
    };

    std::vector<Branch> branches(1);
    branches.front().node = top_node->children.front();
    for (const auto & name : top_names)
        branches.front().group.candidates.push_back({name, name});

    std::vector<SubcolumnsGroup> groups;
    while (!branches.empty())
    {
        auto branch = std::move(branches.back());
        branches.pop_back();
        auto & candidates = branch.group.candidates;

        while (!candidates.empty())
        {
            QueryPlan::Node * node = branch.node;
            auto * step = node->step.get();
            if (typeid_cast<ReadFromMergeTree *>(step))
            {
                const auto & header = *step->getOutputHeader();
                NameSet columns;
                std::erase_if(candidates, [&](const auto & candidate)
                    { return !header.has(candidate.column) || !columns.insert(candidate.column).second; });
                branch.group.read_node = node;
                if (!candidates.empty())
                    groups.push_back(std::move(branch.group));
                break;
            }

            if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
            {
                if (node->children.size() != 2)
                    break;

                const JoinPassThroughColumns pass_through(*join);
                std::array<Branch, 2> sides;
                for (auto & candidate : candidates)
                {
                    const auto side = pass_through.getSide(candidate.column);
                    if (!side)
                        continue;

                    const size_t index = JoinStepLogical::getInputIndex(*side);
                    if (join->getInputHeaders().at(index)->has(candidate.column))
                        sides[index].group.candidates.push_back(std::move(candidate));
                }

                for (size_t index = 0; index < sides.size(); ++index)
                {
                    if (sides[index].group.candidates.empty())
                        continue;

                    sides[index].node = node->children.at(index);
                    sides[index].group.path = branch.group.path;
                    sides[index].group.path.emplace_back(node, index);
                    branches.push_back(std::move(sides[index]));
                }
                break;
            }

            if (node->children.size() != 1 || hasDuplicatedNames(*step->getInputHeaders().front())
                || hasDuplicatedNames(*step->getOutputHeader()))
                break;

            if (auto * actions = tryGetStepActions(step))
            {
                if (actions->hasArrayJoin() || hasDuplicatedInputNames(*actions))
                    break;

                std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
                for (const auto * output : actions->getOutputs())
                    outputs.emplace(output->result_name, output);
                std::unordered_set<std::string_view> inputs;
                for (const auto * input : actions->getInputs())
                    inputs.insert(input->result_name);

                const auto & input_header = *step->getInputHeaders().front();
                std::erase_if(candidates, [&](auto & candidate)
                {
                    if (auto it = outputs.find(candidate.column); it != outputs.end())
                    {
                        const auto * source = resolveAliases(it->second);
                        if (source->type != ActionsDAG::ActionType::INPUT || !source->result_type->equals(*it->second->result_type))
                            return true;
                        candidate.column = source->result_name;
                        return false;
                    }

                    /// The column passes the step unmatched.
                    return inputs.contains(candidate.column) || !input_header.has(candidate.column);
                });
            }
            else if (const auto * sorting = typeid_cast<const SortingStep *>(step))
            {
                if (sorting->hasPartitions())
                    break;
            }
            else if (const auto * limit = typeid_cast<const LimitStep *>(step))
            {
                if (limit->withTies())
                    break;
            }
            else if (!typeid_cast<const OffsetStep *>(step))
                break;

            branch.group.path.emplace_back(node, 0);
            branch.node = node->children.front();
        }
    }

    return groups;
}

/// A rewrite of candidates of a group, worked out before the plan changes.
struct SubcolumnsRewrite
{
    /// Bottom-up.
    std::vector<SubcolumnsPathStep> steps;
    SubcolumnsPathStep top;
    std::vector<CarriedColumn> carried;
    /// For each of `steps`, the carried columns the steps above it consume.
    std::vector<NameSet> carried_above_step;
    Names columns;
    Names subcolumns;
};

/// The candidates a check fails for, or else the rewrite if the steps take the candidates together.
struct SubcolumnsRewriteAttempt
{
    std::vector<size_t> failed;
    std::optional<SubcolumnsRewrite> rewrite;
};

SubcolumnsRewriteAttempt prepareSubcolumnsRewrite(const SubcolumnsTop & top, const SubcolumnsGroup & group, const std::vector<size_t> & members)
{
    const auto & read_step = typeid_cast<const ReadFromMergeTree &>(*group.read_node->step);
    const size_t candidates_count = group.candidates.size();

    /// Candidates out of the attempt, from the start or after a failed check.
    std::vector<char> skipped(candidates_count, 1);
    for (size_t member : members)
        skipped[member] = 0;

    SubcolumnsRewriteAttempt attempt;
    auto fail = [&](size_t candidate)
    {
        if (!skipped[candidate])
        {
            skipped[candidate] = 1;
            attempt.failed.push_back(candidate);
        }
    };

    auto fail_absent = [&](const std::unordered_map<String, size_t> & names)
    {
        std::vector<char> present(candidates_count, 0);
        for (const auto & [_, candidate] : names)
            present[candidate] = 1;
        for (size_t member : members)
            if (!present[member])
                fail(member);
    };

    auto add_read = [](SubcolumnsPathStep & path_step, SubcolumnRead read, size_t candidate)
    {
        path_step.reads.push_back(std::move(read));
        path_step.read_candidates.push_back(candidate);
    };

    SubcolumnsRewrite rewrite;

    /// Up: every name the columns have on the way, and their uses.
    std::unordered_map<String, size_t> names;
    for (size_t member : members)
        names.emplace(group.candidates[member].column, member);

    for (auto it = group.path.rbegin(); it != group.path.rend(); ++it)
    {
        fail_absent(names);

        auto * step = it->first->step.get();
        SubcolumnsPathStep path_step;
        path_step.node = it->first;
        std::unordered_map<String, size_t> names_above;

        if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
        {
            const size_t index = it->second;
            const JoinPassThroughColumns pass_through(*join);
            const auto & input_header = *join->getInputHeaders().at(index);
            for (const auto & [name, candidate] : names)
            {
                if (skipped[candidate])
                    continue;

                path_step.join_input_positions.push_back(input_header.getPositionByName(name));
                if (!pass_through.returns(name))
                {
                    /// The join does not return it: nothing may read it.
                    if (pass_through.reads(name))
                        fail(candidate);
                    continue;
                }

                const auto side = pass_through.getSide(name);
                if (!side || JoinStepLogical::getInputIndex(*side) != index)
                    fail(candidate);
                names_above.emplace(name, candidate);
            }

            const auto & outputs = join->getActionsDAG().getOutputs();
            for (size_t position = 0; position < outputs.size(); ++position)
                if (names_above.contains(outputs[position]->result_name))
                    path_step.join_output_positions.push_back(position);

            std::ranges::sort(path_step.join_input_positions);
            path_step.join_side = index == JoinStepLogical::getInputIndex(JoinTableSide::Left) ? JoinTableSide::Left : JoinTableSide::Right;
        }
        else if (const auto * actions = tryGetStepActions(step))
        {
            auto copy = actions->clone();
            copy.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/false);

            std::unordered_map<const ActionsDAG::Node *, size_t> input_candidates;
            for (const auto * input : copy.getInputs())
            {
                auto name_it = names.find(input->result_name);
                if (name_it == names.end() || skipped[name_it->second])
                    continue;

                input_candidates.emplace(input, name_it->second);
                path_step.inputs.insert(input->result_name);
            }

            for (const auto & [name, candidate] : names)
                if (!skipped[candidate] && !path_step.inputs.contains(name))
                    names_above.emplace(name, candidate);

            const auto * filter = typeid_cast<const FilterStep *>(step);
            NameSet outputs_to_remove;
            for (const auto * output : copy.getOutputs())
            {
                auto input_it = input_candidates.find(resolveAliases(output));
                if (input_it == input_candidates.end())
                    continue;

                if (filter && output->result_name == filter->getFilterColumnName())
                    fail(input_it->second);
                names_above.emplace(output->result_name, input_it->second);
                outputs_to_remove.insert(output->result_name);
            }

            copy.removeFromOutputs(outputs_to_remove);
            copy.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/false);

            auto reads = collectSubcolumnReads(copy, top.allow_function_forms);
            std::unordered_set<const ActionsDAG::Node *> read_nodes;
            for (const auto & copy_node : copy.getNodes())
                for (const auto * child : copy_node.children)
                    read_nodes.insert(resolveAliases(child));

            for (const auto * input : copy.getInputs())
            {
                auto input_it = input_candidates.find(input);
                if (input_it == input_candidates.end())
                    continue;

                if (auto reads_it = reads.find(input); reads_it != reads.end())
                {
                    for (auto & read : reads_it->second)
                        add_read(path_step, std::move(read), input_it->second);
                }
                /// Without subcolumn reads, nothing may read the column.
                else if (read_nodes.contains(input))
                    fail(input_it->second);
            }

            path_step.actions = std::move(copy);
        }
        else
        {
            names_above = names;
            if (const auto * sorting = typeid_cast<const SortingStep *>(step))
                for (const auto & sort_column : sorting->getSortDescription())
                    if (auto name_it = names.find(sort_column.column_name); name_it != names.end())
                        fail(name_it->second);
        }

        names = std::move(names_above);
        rewrite.steps.push_back(std::move(path_step));
    }

    /// At the top, every name of a column is an input read only by subcolumn reads.
    fail_absent(names);
    rewrite.top.node = top.node;
    for (const auto & [name, candidate] : names)
    {
        if (skipped[candidate])
            continue;

        auto input_it = top.input_by_name.find(name);
        auto reads_it = input_it != top.input_by_name.end() ? top.reads.find(input_it->second) : top.reads.end();
        if (reads_it == top.reads.end())
        {
            fail(candidate);
            continue;
        }

        rewrite.top.inputs.insert(name);
        for (const auto & read : reads_it->second)
            add_read(rewrite.top, read, candidate);
    }

    for (size_t member : members)
        if (auto name_it = names.find(group.candidates[member].top_name); name_it == names.end() || name_it->second != member)
            fail(member);

    /// The read checks and the carried columns, top first.
    std::vector<char> function_forms(candidates_count, 0);
    std::unordered_map<String, size_t> carried_position;
    auto check_reads = [&](const SubcolumnsPathStep & path_step)
    {
        for (size_t i = 0; i < path_step.reads.size(); ++i)
        {
            const auto & read = path_step.reads[i];
            const size_t candidate = path_step.read_candidates[i];
            if (skipped[candidate])
                continue;

            function_forms[candidate] |= read.is_function_form;

            CarriedColumn carried_column{getCarriedColumnName(read), read.subcolumn, read.subcolumn_type, candidate};
            auto [position, inserted] = carried_position.emplace(carried_column.name, rewrite.carried.size());
            if (inserted)
                rewrite.carried.push_back(std::move(carried_column));
            else if (const auto & existing = rewrite.carried[position->second]; existing.candidate != candidate
                || existing.subcolumn != carried_column.subcolumn || !existing.type->equals(*carried_column.type))
                fail(candidate);

            if (!storageResolvesSubcolumn(read_step, group.candidates[candidate].column, read))
                fail(candidate);

            /// Without buckets the key subcolumn `m.key_<key>` reads the whole Map.
            if (read.is_map_key && !read_step.getMergeTreeData().hasBucketedMapSerialization())
                fail(candidate);
        }
    };

    check_reads(rewrite.top);
    for (auto it = rewrite.steps.rbegin(); it != rewrite.steps.rend(); ++it)
        check_reads(*it);

    std::vector<size_t> read_checked;
    std::vector<std::pair<String, bool>> read_checked_columns;
    for (size_t member : members)
    {
        if (skipped[member])
            continue;
        read_checked.push_back(member);
        read_checked_columns.emplace_back(group.candidates[member].column, function_forms[member]);
    }

    const auto can_read = read_step.canReadSubcolumnsOfColumns(read_checked_columns);
    for (size_t i = 0; i < read_checked.size(); ++i)
        if (!can_read[i])
            fail(read_checked[i]);

    /// A carried column must not shadow a column that exists on the way, other than the result of a read it replaces.
    auto check_shadows = [&](const Block & header, const SubcolumnsPathStep * own_step)
    {
        std::set<std::pair<std::string_view, size_t>> own_reads;
        if (own_step)
            for (size_t i = 0; i < own_step->reads.size(); ++i)
                own_reads.emplace(own_step->reads[i].node->result_name, own_step->read_candidates[i]);

        for (const auto & carried_column : rewrite.carried)
            if (!skipped[carried_column.candidate] && header.has(carried_column.name)
                && !own_reads.contains({carried_column.name, carried_column.candidate}))
                fail(carried_column.candidate);
    };

    check_shadows(*read_step.getOutputHeader(), nullptr);
    check_shadows(top.input_header, nullptr);
    check_shadows(top.output_header, &rewrite.top);
    for (const auto & path_step : rewrite.steps)
    {
        for (const auto & input_header : path_step.node->step->getInputHeaders())
            check_shadows(*input_header, nullptr);
        check_shadows(*path_step.node->step->getOutputHeader(), &path_step);
    }

    auto build_residuals = [&](SubcolumnsPathStep & path_step)
    {
        for (size_t i = 0; i < path_step.reads.size(); ++i)
        {
            const auto & read = path_step.reads[i];
            if (read.residual.empty() || skipped[path_step.read_candidates[i]])
                continue;

            ColumnsWithTypeAndName arguments{{nullptr, read.subcolumn_type, getCarriedColumnName(read)}};
            for (const auto & argument : read.residual_arguments)
                arguments.push_back({ColumnConst::create(argument.column->getDataColumnPtr(), 1), argument.type, argument.name});
            auto function = FunctionFactory::instance().get(read.residual, read_step.getContext())->build(arguments);
            if (!function->getResultType()->equals(*read.node->result_type))
                fail(path_step.read_candidates[i]);
            path_step.residuals.emplace(read.node, std::move(function));
        }
    };

    build_residuals(rewrite.top);
    for (auto & path_step : rewrite.steps)
        build_residuals(path_step);

    /// Subcolumns of each column, in the order of the candidates.
    std::ranges::stable_sort(rewrite.carried, {}, &CarriedColumn::candidate);
    std::unordered_map<String, size_t> column_candidates;
    for (size_t member : members)
        column_candidates.emplace(group.candidates[member].column, member);

    NameSet read_columns(read_step.getAllColumnNames().begin(), read_step.getAllColumnNames().end());
    NameSet subcolumns;
    for (const auto & carried_column : rewrite.carried)
    {
        if (skipped[carried_column.candidate])
            continue;

        auto subcolumn = group.candidates[carried_column.candidate].column + "." + carried_column.subcolumn;
        /// A subcolumn the read already reads only for PREWHERE or the row policy is not returned by it.
        if (read_columns.contains(subcolumn) && !read_step.getOutputHeader()->has(subcolumn))
            fail(carried_column.candidate);

        /// A column that is the subcolumn of another candidate stays read.
        if (auto it = column_candidates.find(subcolumn); it != column_candidates.end())
            fail(it->second);

        if (subcolumns.insert(subcolumn).second)
            rewrite.subcolumns.push_back(std::move(subcolumn));
    }

    /// The carried columns the steps consume, from the bottom.
    std::unordered_map<String, size_t> carried_above;
    auto add_carried_above = [&](const SubcolumnsPathStep & path_step)
    {
        for (size_t i = 0; i < path_step.reads.size(); ++i)
            if (!skipped[path_step.read_candidates[i]])
                carried_above.emplace(getCarriedColumnName(path_step.reads[i]), path_step.read_candidates[i]);
    };

    add_carried_above(rewrite.top);
    rewrite.carried_above_step.resize(rewrite.steps.size());
    for (size_t i = rewrite.steps.size(); i > 0; --i)
    {
        /// A filter that removes its filter column cannot pass it on as a carried column.
        if (const auto * filter = typeid_cast<const FilterStep *>(rewrite.steps[i - 1].node->step.get()); filter && filter->removesFilterColumn())
            if (auto it = carried_above.find(filter->getFilterColumnName()); it != carried_above.end())
                fail(it->second);

        for (const auto & [name, _] : carried_above)
            rewrite.carried_above_step[i - 1].insert(name);
        add_carried_above(rewrite.steps[i - 1]);
    }

    if (!attempt.failed.empty())
        return attempt;

    auto carried_columns_above = [&](size_t i)
    {
        ColumnsWithTypeAndName columns;
        for (const auto & carried_column : rewrite.carried)
            if (rewrite.carried_above_step[i].contains(carried_column.name))
                columns.push_back({carried_column.type->createColumn(), carried_column.type, carried_column.name});
        return columns;
    };

    /// A join is changed in place, so try it on a copy first.
    for (size_t i = 0; i < rewrite.steps.size(); ++i)
    {
        const auto & path_step = rewrite.steps[i];
        if (!path_step.join_side)
            continue;

        auto copy = path_step.node->step->clone();
        if (!replaceJoinPassThroughColumns(
                typeid_cast<JoinStepLogical &>(*copy), *path_step.join_side, path_step.join_input_positions,
                path_step.join_output_positions, carried_columns_above(i)))
            return attempt;
    }

    for (size_t i = 0; i < rewrite.steps.size(); ++i)
    {
        auto & path_step = rewrite.steps[i];
        if (!path_step.actions)
            continue;

        path_step.new_actions = replaceSubcolumnReads(*path_step.actions, path_step.reads, path_step.residuals);
        if (!path_step.new_actions)
            return attempt;

        removeInputsOfReadColumn(*path_step.new_actions, path_step.inputs);
        NameSet outputs;
        for (const auto * output : path_step.new_actions->getOutputs())
            outputs.insert(output->result_name);
        for (const auto * input : path_step.new_actions->getInputs())
            if (rewrite.carried_above_step[i].contains(input->result_name) && outputs.insert(input->result_name).second)
                path_step.new_actions->getOutputs().push_back(input);
    }

    rewrite.top.new_actions = replaceSubcolumnReads(top.actions, rewrite.top.reads, rewrite.top.residuals);
    if (!rewrite.top.new_actions)
        return attempt;
    removeInputsOfReadColumn(*rewrite.top.new_actions, rewrite.top.inputs);

    NameSet top_new_inputs;
    for (const auto * input : rewrite.top.new_actions->getInputs())
        top_new_inputs.insert(input->result_name);

    Block new_top_input_header;
    for (const auto & column_with_type : top.input_header)
        if (!rewrite.top.inputs.contains(column_with_type.name))
            new_top_input_header.insert(column_with_type);
    for (const auto & carried_column : rewrite.carried)
        if (top_new_inputs.contains(carried_column.name))
            new_top_input_header.insert({carried_column.type->createColumn(), carried_column.type, carried_column.name});

    if (!blocksHaveEqualStructure(transformStepHeader(top.node->step.get(), new_top_input_header, *rewrite.top.new_actions), top.output_header))
        return attempt;

    for (size_t member : members)
        rewrite.columns.push_back(group.candidates[member].column);

    attempt.rewrite = std::move(rewrite);
    return attempt;
}

size_t applySubcolumnsRewrite(
    const SubcolumnsTop & top, const SubcolumnsGroup & group, SubcolumnsRewrite & rewrite, QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * read_node = group.read_node;
    auto & read_step = typeid_cast<ReadFromMergeTree &>(*read_node->step);
    const Block old_read_header = *read_step.getOutputHeader();
    read_step.readSubcolumnsInsteadOfColumns(rewrite.columns, rewrite.subcolumns);

    ActionsDAG read_subcolumns_actions;
    std::unordered_map<String, const ActionsDAG::Node *> read_input_by_name;
    for (const auto & column_with_type : *read_step.getOutputHeader())
    {
        const auto & input = read_subcolumns_actions.addInput(column_with_type);
        read_input_by_name.emplace(input.result_name, &input);
        if (old_read_header.has(column_with_type.name))
            read_subcolumns_actions.getOutputs().push_back(&input);
    }
    for (const auto & carried_column : rewrite.carried)
    {
        const auto * input = read_input_by_name.at(group.candidates[carried_column.candidate].column + "." + carried_column.subcolumn);
        read_subcolumns_actions.getOutputs().push_back(
            input->result_name == carried_column.name ? input : &read_subcolumns_actions.addAlias(*input, carried_column.name));
    }

    const auto & path = group.path;
    QueryPlan::Node * read_parent = path.empty() ? top.node : path.back().first;
    const size_t read_child_index = path.empty() ? 0 : path.back().second;
    auto & read_subcolumns_node = nodes.emplace_back();
    read_subcolumns_node.children = {read_node};
    read_subcolumns_node.step = std::make_unique<ExpressionStep>(read_step.getOutputHeader(), std::move(read_subcolumns_actions));
    read_subcolumns_node.step->setStepDescription("Read subcolumns", settings.max_step_description_length);
    read_parent->children.at(read_child_index) = &read_subcolumns_node;

    auto carried_columns_above = [&](size_t i)
    {
        ColumnsWithTypeAndName columns;
        for (const auto & carried_column : rewrite.carried)
            if (rewrite.carried_above_step[i].contains(carried_column.name))
                columns.push_back({carried_column.type->createColumn(), carried_column.type, carried_column.name});
        return columns;
    };

    for (size_t i = 0; i < rewrite.steps.size(); ++i)
    {
        auto & path_step = rewrite.steps[i];
        if (auto * join = typeid_cast<JoinStepLogical *>(path_step.node->step.get()))
        {
            if (!replaceJoinPassThroughColumns(
                    *join, *path_step.join_side, path_step.join_input_positions, path_step.join_output_positions, carried_columns_above(i)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Join did not stop returning the columns whose subcolumns are read instead");

            join->updateInputHeaders({path_step.node->children.at(0)->step->getOutputHeader(), path_step.node->children.at(1)->step->getOutputHeader()});
            continue;
        }

        if (path_step.new_actions)
            *tryGetStepActions(path_step.node->step.get()) = std::move(*path_step.new_actions);
        path_step.node->step->updateInputHeader(path_step.node->children.front()->step->getOutputHeader());
    }

    auto * top_step = top.node->step.get();
    *tryGetStepActions(top_step) = std::move(*rewrite.top.new_actions);
    top_step->updateInputHeader(top.node->children.front()->step->getOutputHeader());
    chassert(blocksHaveEqualStructure(*top_step->getOutputHeader(), top.output_header));

    return path.size() + 3;
}

/// Reads subcolumns of columns that `ReadFromMergeTree` reads instead of the whole columns, when a column reaches the top
/// step only through steps that pass it unchanged and every use of it on the way is a subcolumn read:
///
///   Expression [getSubcolumn(tup, 'a')]          Expression [INPUT getSubcolumn(tup, 'a')]
///     └── Sorting                                  └── Sorting        [carries tup.a, not tup]
///           └── ReadFromMergeTree [tup]   →              └── Expression [tup.a AS getSubcolumn(tup, 'a')]
///                                                              └── ReadFromMergeTree [tup.a]
///
/// The columns of one read are rewritten together, and the whole path at once or not at all. Only subcolumn values
/// move: a function of the subcolumn (`equals(arr.size0, 0)` for `empty(arr)`) stays in the step where the replaced
/// function was.
size_t tryPushDownSubcolumnsToReading(QueryPlan::Node * top_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    /// The checks below read the parts of this server, while a receiver builds the read from its own.
    if (settings.make_distributed_plan || settings.serialize_query_plan || settings.enable_parallel_replicas)
        return 0;

    const bool allow_function_forms = settings.optimize_functions_to_subcolumns;
    const bool has_subcolumn_read = std::ranges::any_of(
        tryGetStepActions(top_node->step.get())->getNodes(),
        [&](const ActionsDAG::Node & node)
        {
            if (node.type != ActionsDAG::ActionType::FUNCTION || node.children.empty())
                return false;
            const auto * source = resolveAliases(node.children.front());
            return source->type == ActionsDAG::ActionType::INPUT
                && tryMatchSubcolumnRead(node, source->result_type, allow_function_forms).has_value();
        });
    if (!has_subcolumn_read)
        return 0;

    auto * top_step = top_node->step.get();
    SubcolumnsTop top;
    top.node = top_node;
    top.allow_function_forms = allow_function_forms;
    top.input_header = *top_step->getInputHeaders().front();
    top.output_header = *top_step->getOutputHeader();
    if (hasDuplicatedNames(top.input_header) || hasDuplicatedNames(top.output_header))
        return 0;

    top.actions = tryGetStepActions(top_step)->clone();
    top.actions.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/false);

    top.reads = collectSubcolumnReads(top.actions, allow_function_forms);
    if (top.reads.empty())
        return 0;

    Names candidate_names;
    for (const auto * input : top.actions.getInputs())
    {
        top.input_by_name.emplace(input->result_name, input);
        if (top.reads.contains(input))
            candidate_names.push_back(input->result_name);
    }

    for (const auto & group : followCandidatesDownToReads(top_node, candidate_names))
    {
        std::vector<size_t> members(group.candidates.size());
        std::iota(members.begin(), members.end(), 0);
        while (!members.empty())
        {
            auto attempt = prepareSubcolumnsRewrite(top, group, members);
            if (attempt.rewrite)
                return applySubcolumnsRewrite(top, group, *attempt.rewrite, nodes, settings);
            if (attempt.failed.empty())
                break;

            std::ranges::sort(attempt.failed);
            std::erase_if(members, [&](size_t member) { return std::ranges::binary_search(attempt.failed, member); });
        }

        /// The steps take each column, but not all of them together.
        if (members.size() > 1)
            for (size_t member : members)
                if (auto attempt = prepareSubcolumnsRewrite(top, group, {member}); attempt.rewrite)
                    return applySubcolumnsRewrite(top, group, *attempt.rewrite, nodes, settings);
    }

    return 0;
}

}

/// Pushes volume-reducing functions (`length`, `lengthUTF8`, `empty`, `notEmpty`) from an
/// `Expression` / `Filter` step below its child step, so that the wide `String` / `FixedString` /
/// `Array` / `Map` argument is replaced by the fixed-size result:
///
///   Expression  [lengthUTF8(s)]        Expression  [INPUT lengthUTF8(s)]
///     └── Sorting                        └── Sorting        [carries a UInt64, not a String]
///           └── X              →               └── Expression  [pushed: lengthUTF8(s)]
///                                                    └── X
///
/// The parent is rewritten with `ActionsDAG::split`: the function nodes form the first part, which
/// is discarded, and the second part becomes the parent's `ActionsDAG` and reads the results as
/// inputs. The step below the child is built separately because the child may rename the column
/// (`Change column names to column identifiers` is usually merged into the `Filter`), so the pushed
/// functions have to read the argument under its name in the child's input header while producing
/// the result under the name the parent expects.
///
/// The rewrite is only applied when the wide argument column really stops flowing through the child
/// step: it is removed from the child's output and — unless the child itself reads it, like a
/// `Filter` whose condition mentions it — it is not even produced below the child anymore. Without
/// that the only effect would be computing the function earlier, on more rows.
size_t tryPushDownVolumeReducingFunction(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    if (parent_node->children.size() != 1)
        return 0;

    auto * parent_actions = tryGetStepActions(parent_node->step.get());
    if (!parent_actions)
        return 0;

    /// `ARRAY JOIN` changes the number of rows, so the two split parts are not interchangeable.
    /// Stateful and non-deterministic functions stay in the second part and keep seeing the same
    /// rows in the same order, so they do not prevent the rewrite.
    if (parent_actions->hasArrayJoin())
        return 0;

    /// The rewrite resolves columns by name, which is ambiguous with duplicated names.
    if (hasDuplicatedInputNames(*parent_actions))
        return 0;

    if (size_t changed = tryPushDownSubcolumnsToReading(parent_node, nodes, settings))
        return changed;

    auto functions_by_argument = collectVolumeReducingFunctionsReplacingTheirArgument(*parent_actions);
    if (functions_by_argument.empty())
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();
    if (child_node->children.size() != 1)
        return 0;

    NameSet columns_pinned_by_child;
    if (!canPushBelowStep(child_node->step.get(), columns_pinned_by_child))
        return 0;

    const Block & child_input_header = *child_node->children.front()->step->getOutputHeader();
    const Block & child_output_header = *child_node->step->getOutputHeader();
    if (hasDuplicatedNames(child_input_header) || hasDuplicatedNames(child_output_header))
        return 0;

    ActionsDAG * child_actions = tryGetStepActions(child_node->step.get());
    const NameSet child_inputs_in_use = child_actions ? collectInputsNeededByOutputs(*child_actions) : NameSet{};
    const auto * child_filter = typeid_cast<const FilterStep *>(child_node->step.get());
    NameSet child_filter_inputs;
    if (child_filter)
    {
        const auto * filter_column = child_actions->tryFindInOutputs(child_filter->getFilterColumnName());
        if (!filter_column)
            return 0;
        child_filter_inputs = collectInputsNeededByNode(filter_column);
    }

    UnorderedSetWithMemoryTracking<const ActionsDAG::Node *> functions_to_split;
    VectorWithMemoryTracking<PushedFunction> pushed_functions;
    /// Names of the columns the child stops surfacing, on the parent's and on the child's side.
    NameSet columns_to_stop_surfacing;
    NameSet columns_to_stop_producing;
    for (const auto & [argument, functions] : functions_by_argument)
    {
        /// Name of the column as the parent receives it, and as the child receives it. They differ
        /// when the child renames the column on its way up.
        const String & name_above = argument->result_name;
        String name_below = name_above;

        if (child_actions)
        {
            const ActionsDAG::Node * source_below = nullptr;
            if (const auto * surfaced = child_actions->tryFindInOutputs(name_above))
            {
                source_below = resolveAliases(surfaced);
                if (source_below->type != ActionsDAG::ActionType::INPUT)
                    continue;
                name_below = source_below->result_name;
            }
            else if (child_inputs_in_use.contains(name_above))
            {
                /// A column the child consumes but does not output cannot reach the parent at all.
                continue;
            }

            /// Removing only the parent-visible alias is not enough when the child also surfaces
            /// the same wide input under another name. That sibling would still cross the barrier,
            /// so pushing the function down would add work without reducing the carried volume.
            bool has_surfaced_sibling = false;
            if (source_below)
                for (const auto * output : child_actions->getOutputs())
                    if (output->result_name != name_above && resolveAliases(output) == source_below)
                    {
                        has_surfaced_sibling = true;
                        break;
                    }

            if (has_surfaced_sibling)
                continue;

            /// A `FilterStep` can pass an unmatched input column through even when its actions do
            /// not surface that column. Inspect the expression below the filter as well: it knows
            /// whether another name in the filter input header is an alias of the same source.
            /// Replacing only one of those names would leave the wide payload in the filter.
            if (child_filter)
            {
                const auto * filter_input_actions = findActionsBelowHeaderPreservingSteps(child_node->children.front());
                if (filter_input_actions)
                {
                    const auto * filter_input_source = filter_input_actions->tryFindInOutputs(name_below);
                    if (filter_input_source)
                    {
                        filter_input_source = resolveAliases(filter_input_source);
                        if (filter_input_source->type != ActionsDAG::ActionType::INPUT)
                            continue;

                        bool has_passthrough_sibling = false;
                        for (const auto * output : filter_input_actions->getOutputs())
                            if (output->result_name != name_below && resolveAliases(output) == filter_input_source)
                            {
                                has_passthrough_sibling = true;
                                break;
                            }

                        if (has_passthrough_sibling)
                            continue;
                    }
                }
            }
        }
        else if (const auto * sorting_input_actions = findActionsBelowHeaderPreservingSteps(child_node->children.front()))
        {
            /// A `SortingStep` has no `ActionsDAG` of its own. Its input can nevertheless expose
            /// the same wide source under multiple aliases, possibly through header-preserving
            /// steps such as `LimitStep`, so inspect the expression below them before deciding
            /// that removing one name reduces the sort payload.
            const auto * source_below = sorting_input_actions->tryFindInOutputs(name_below);
            if (!source_below)
                continue;

            source_below = resolveAliases(source_below);
            if (source_below->type != ActionsDAG::ActionType::INPUT)
                continue;

            bool has_surfaced_sibling = false;
            for (const auto * output : sorting_input_actions->getOutputs())
                if (output->result_name != name_below && resolveAliases(output) == source_below)
                {
                    has_surfaced_sibling = true;
                    break;
                }

            if (has_surfaced_sibling)
                continue;
        }

        if (columns_pinned_by_child.contains(name_below))
            continue;

        /// A filter must itself read the argument. Otherwise the function is evaluated before a
        /// potentially selective filter, adding CPU work while only saving the copied values of
        /// the wide column from rows that survive the filter.
        if (child_filter && !child_filter_inputs.contains(name_below))
            continue;

        /// The filter-specific rewrite changes the number of rows on which the function is
        /// evaluated. Do not use it for functions that scan the payload: saving copies of the
        /// surviving strings cannot in general repay scanning every input string.
        if (child_filter)
        {
            bool all_functions_are_cheap = true;
            for (const auto * function : functions)
                if (!isCheapToEvaluateBeforeFilter(*function->function_base, argument->result_type))
                {
                    all_functions_are_cheap = false;
                    break;
                }

            if (!all_functions_are_cheap)
                continue;
        }

        /// Do not compute a scalar before a filter if its predicate already computes the same
        /// scalar from the same argument. The filter is kept intact by this rewrite, so pushing
        /// the parent's calculation below it would evaluate the scalar twice for rejected rows.
        if (child_filter)
        {
            bool filter_computes_pushed_function = false;
            for (const auto & filter_node : child_actions->getNodes())
            {
                if (filter_node.type != ActionsDAG::ActionType::FUNCTION || !filter_node.function_base || filter_node.children.size() != 1)
                    continue;

                if (resolveAliases(filter_node.children.front())->result_name != name_below)
                    continue;

                for (const auto * function : functions)
                    if (filter_node.function_base->getName() == function->function_base->getName())
                    {
                        filter_computes_pushed_function = true;
                        break;
                    }

                if (filter_computes_pushed_function)
                    break;
            }

            if (filter_computes_pushed_function)
                continue;
        }

        const auto * column_below = child_input_header.findByName(name_below);
        if (!column_below || !column_below->type->equals(*argument->result_type))
            continue;

        for (const auto * function : functions)
            pushed_functions.push_back({function->function_base, function->result_name, name_below});
        functions_to_split.insert(functions.begin(), functions.end());
        columns_to_stop_surfacing.insert(name_above);
        columns_to_stop_producing.insert(name_below);
    }

    if (pushed_functions.empty())
        return 0;

    /// A result must not shadow a column that already exists around the child step, otherwise the
    /// parent's new input could bind to that column instead of the computed one.
    NameSet pushed_result_names;
    for (const auto & pushed_function : pushed_functions)
        if (child_input_header.has(pushed_function.result_name) || child_output_header.has(pushed_function.result_name)
            || !pushed_result_names.insert(pushed_function.result_name).second)
            return 0;

    /// Stop the child from surfacing the replaced columns. Input declarations are kept on purpose:
    /// dropping one would turn the column into an unmatched passthrough and put it right back into
    /// the child's output. A column that stays in use (a `Filter` reading it in its condition) is
    /// still produced below the child, it just does not leave the child anymore.
    std::optional<ActionsDAG> pruned_child_actions;
    if (child_actions)
    {
        auto pruned = child_actions->clone();
        pruned.removeFromOutputs(columns_to_stop_surfacing);
        pruned.removeUnusedActions(/*allow_remove_inputs=*/false, /*allow_constant_folding=*/false);

        for (const auto & name : collectInputsNeededByOutputs(pruned))
            columns_to_stop_producing.erase(name);

        pruned_child_actions = std::move(pruned);
    }

    /// The step below the child: the child's input columns minus the replaced ones, plus one node
    /// per pushed function named exactly like the parent's node, so that the second part of the
    /// split below finds it as an input after the child passes it through.
    ActionsDAG pushed_actions;
    UnorderedMapWithMemoryTracking<std::string_view, const ActionsDAG::Node *> input_by_name;
    ActionsDAG::NodeRawConstPtrs pushed_outputs;
    pushed_outputs.reserve(child_input_header.columns() + pushed_functions.size());
    for (const auto & column : child_input_header)
    {
        const auto & input = pushed_actions.addInput(column);
        input_by_name.emplace(input.result_name, &input);
        if (!columns_to_stop_producing.contains(column.name))
            pushed_outputs.push_back(&input);
    }

    for (const auto & pushed_function : pushed_functions)
    {
        const auto & function = pushed_actions.addFunction(
            pushed_function.function_base, {input_by_name.at(pushed_function.argument_name)}, pushed_function.result_name);
        pushed_outputs.push_back(&function);
    }
    pushed_actions.getOutputs() = std::move(pushed_outputs);

    /// The second part of the split reads the results as inputs of the same name.
    auto split_result = parent_actions->split(functions_to_split);

    /// All the bail-outs are behind us, the plan may be modified now.
    *parent_actions = std::move(split_result.second);
    if (pruned_child_actions)
        *child_actions = std::move(*pruned_child_actions);

    auto & pushed_node = nodes.emplace_back();
    pushed_node.children.swap(child_node->children);
    child_node->children = {&pushed_node};
    pushed_node.step = std::make_unique<ExpressionStep>(pushed_node.children.front()->step->getOutputHeader(), std::move(pushed_actions));

    /// The pass can fire again on a step whose description already carries the marker (the pushed
    /// step gets merged into the child and the result is pushed further down). Without the guard
    /// the marker would be appended once per round.
    auto base_description = parent_node->step->getStepDescription();
    constexpr std::string_view marker = " [volume-reducing functions]";
    auto description = base_description.contains(marker)
        ? std::string{base_description}
        : fmt::format("{}{}", base_description, marker);
    pushed_node.step->setStepDescription(std::move(description), settings.max_step_description_length);

    child_node->step->updateInputHeader(pushed_node.step->getOutputHeader());
    parent_node->step->updateInputHeader(child_node->step->getOutputHeader());

    return 3;
}

}
