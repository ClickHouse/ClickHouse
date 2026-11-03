#include <Storages/MergeTree/MergeTreeIndexHypothesis.h>

#include <Columns/IColumn.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

void MergeTreeIndexGranuleHypothesis::serializeBinary(WriteBuffer & ostr) const
{
    DataTypeUInt8().getDefaultSerialization()->serializeBinary(static_cast<UInt8>(met), ostr, {});
}

void MergeTreeIndexGranuleHypothesis::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version)
{
    if (version != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);

    Field field_met;
    DataTypeUInt8().getDefaultSerialization()->deserializeBinary(field_met, istr, {});
    met = field_met.safeGet<UInt8>();
    is_empty = false;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorHypothesis::getGranuleAndReset()
{
    auto granule = std::make_shared<MergeTreeIndexGranuleHypothesis>(met);
    met = true;
    is_empty = true;
    return granule;
}

void MergeTreeIndexAggregatorHypothesis::update(const Block & block, size_t * pos, size_t limit)
{
    size_t rows_read = std::min(limit, block.rows() - *pos);
    if (rows_read == 0)
        return;

    const auto & column = *block.getByName(column_name).column;
    for (size_t i = *pos; met && i < *pos + rows_read; ++i)
        met = column.getBool(i);

    is_empty = false;
    *pos += rows_read;
}

namespace
{

/// The comparisons of a conjunction. `NOT` was already pushed into the comparisons by `ActionsDAGWithInversionPushDown`.
void collectComparisons(const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & out)
{
    node = node->getWithoutAlias();
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return;

    if (node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            collectComparisons(child, out);
        return;
    }

    if (node->children.size() == 2
        && ComparisonGraph<const ActionsDAG::Node *>::atomToCompareResult({.node = node}) != ComparisonGraphCompareResult::UNKNOWN)
        out.push_back(node);
}

}

MergeTreeIndexConditionHypothesis::MergeTreeIndexConditionHypothesis(
    const IndexDescription & index,
    const StorageMetadataPtr & metadata_snapshot,
    const ActionsDAG::Node * predicate,
    ContextPtr context)
    : description(index.sample_block.getNames().front())
{
    ActionsDAG::NodeRawConstPtrs constraint_atoms;
    const auto columns = metadata_snapshot->getColumns().get(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
    for (const auto & constraint : metadata_snapshot->getConstraints().getConstraints())
    {
        ASTPtr expression = constraint->as<ASTConstraintDeclaration &>().expr->clone();
        auto syntax_result = TreeRewriter(context).analyze(expression, columns);
        auto dag = ExpressionAnalyzer(expression, syntax_result, context).getActionsDAG(false, true);
        /// Changes the number of rows, so it says nothing about a row. See `ConstraintsDescription::filterConstraintsForOptimization`.
        if (dag.hasArrayJoin())
            continue;
        collectComparisons(owned_dags.emplace_back(&dag.findInOutputs(expression->getColumnName()), context, true).predicate, constraint_atoms);
    }

    auto hypothesis_atoms = constraint_atoms;
    const auto & hypothesis = index.expression->getActionsDAG().findInOutputs(index.sample_block.getNames().front());
    collectComparisons(owned_dags.emplace_back(&hypothesis, context, true).predicate, hypothesis_atoms);

    graph_if_not_met = Graph(constraint_atoms);
    graph_if_met = Graph(hypothesis_atoms);

    const auto & filter = owned_dags.emplace_back(predicate, context, true);
    RPNBuilder<RPNElement>::ExtractAtomFromTreeFunction extract_atom = [](const RPNBuilderTreeNode & node, RPNElement & out)
    {
        auto function = node.toFunctionNodeOrNull();
        if (!function || function->getArgumentsSize() != 2)
            return false;

        auto expected = Graph::atomToCompareResult({.node = function->getDAGNode()});
        if (expected == ComparisonGraphCompareResult::UNKNOWN)
            return false;

        out.function = RPNElement::FUNCTION_COMPARE;
        out.expected = expected;
        out.left = function->getArgumentAt(0).getDAGNode();
        out.right = function->getArgumentAt(1).getDAGNode();
        return true;
    };
    rpn = RPNBuilder<RPNElement>(filter.predicate, context, extract_atom).extractRPN();
}

/// Useful when the graph with the index expression has a verdict on some comparison of the filter.
bool MergeTreeIndexConditionHypothesis::alwaysUnknownOrTrue() const
{
    for (const auto & element : rpn)
        if (element.function == RPNElement::FUNCTION_COMPARE
            && graph_if_met.compare(element.left, element.right) != ComparisonGraphCompareResult::UNKNOWN)
            return false;
    return true;
}

bool MergeTreeIndexConditionHypothesis::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn &) const
{
    const auto & granule = assert_cast<const MergeTreeIndexGranuleHypothesis &>(*idx_granule);
    if (granule.empty())
        return true;
    return evaluate(granule.met ? graph_if_met : graph_if_not_met).can_be_true;
}

BoolMask MergeTreeIndexConditionHypothesis::evaluate(const Graph & graph) const
{
    std::vector<BoolMask> stack;
    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_UNKNOWN:
                stack.emplace_back(true, true);
                break;
            case RPNElement::ALWAYS_TRUE:
                stack.emplace_back(true, false);
                break;
            case RPNElement::ALWAYS_FALSE:
                stack.emplace_back(false, true);
                break;
            case RPNElement::FUNCTION_COMPARE:
                stack.emplace_back(
                    graph.isPossibleCompare(element.expected, element.left, element.right),
                    !graph.isAlwaysCompare(element.expected, element.left, element.right));
                break;
            case RPNElement::FUNCTION_NOT:
                stack.back() = !stack.back();
                break;
            case RPNElement::FUNCTION_AND:
            {
                auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() & right;
                break;
            }
            case RPNElement::FUNCTION_OR:
            {
                auto right = stack.back();
                stack.pop_back();
                stack.back() = stack.back() | right;
                break;
            }
        }
    }

    if (stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in MergeTreeIndexConditionHypothesis::evaluate");
    return stack.front();
}

MergeTreeIndexGranulePtr MergeTreeIndexHypothesis::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleHypothesis>();
}

MergeTreeIndexAggregatorPtr MergeTreeIndexHypothesis::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorHypothesis>(index.sample_block.getNames().front());
}

MergeTreeIndexConditionPtr MergeTreeIndexHypothesis::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionHypothesis>(index, metadata_snapshot, predicate, context);
}

MergeTreeIndexPtr hypothesisIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & /*settings*/)
{
    return std::make_shared<MergeTreeIndexHypothesis>(std::move(metadata_snapshot), index);
}

void hypothesisIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & /*settings*/)
{
    if (index.expression_list_ast->children.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Hypothesis index needs exactly one expression");

    if (attach)
        return;

    /// The granule stores whether the expression is true for every row, so it is read with `getBool`.
    if (!WhichDataType(removeNullable(index.data_types.front())).isNativeInteger())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Hypothesis index expression must be of an integer type (for example Bool or UInt8), got {}", index.data_types.front()->getName());

    /// A hypothesis that contradicts itself, e.g. `a < b AND b < a`, would fail every query at the graph.
    ActionsDAG::NodeRawConstPtrs atoms;
    collectComparisons(&index.expression->getActionsDAG().findInOutputs(index.sample_block.getNames().front()), atoms);
    try
    {
        ComparisonGraph<const ActionsDAG::Node *> graph(atoms);
    }
    catch (Exception & e)
    {
        e.addMessage("in the expression of hypothesis index {}", backQuote(index.name));
        throw;
    }
}

}
