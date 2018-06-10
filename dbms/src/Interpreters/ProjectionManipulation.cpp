#include <memory>
#include <string>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ProjectionManipulation.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>

namespace DB
{
ProjectionManipulatorBase::~ProjectionManipulatorBase() {}

DefaultProjectionManipulator::DefaultProjectionManipulator(ScopeStack & scopes) : scopes(scopes) {}

bool DefaultProjectionManipulator::tryToGetFromUpperProjection(const std::string & column_name)
{
    return scopes.getSampleBlock().has(column_name);
}

std::string DefaultProjectionManipulator::getColumnName(const std::string & column_name) const
{
    return column_name;
}

std::string DefaultProjectionManipulator::getProjectionExpression()
{
    return "";
}

std::string DefaultProjectionManipulator::getProjectionSourceColumn() const
{
    return "";
}

ConditionalTree::Node::Node() : projection_expression_string(), parent_node(0), is_root(false) {}

size_t ConditionalTree::Node::getParentNode() const
{
    if (is_root)
    {
        throw Exception(
            "Failed to get parent projection node of node " + projection_expression_string, ErrorCodes::CONDITIONAL_TREE_PARENT_NOT_FOUND);
    }
    else
    {
        return parent_node;
    }
}

std::string ConditionalTree::getColumnNameByIndex(const std::string & col_name, const size_t node) const
{
    std::string projection_name = nodes[node].projection_expression_string;
    if (projection_name.empty())
    {
        return col_name;
    }
    else
    {
        return col_name + '<' + projection_name + '>';
    }
}

std::string ConditionalTree::getColumnName(const std::string & col_name) const
{
    return getColumnNameByIndex(col_name, current_node);
}

std::string ConditionalTree::getProjectionColumnName(
    const std::string & first_projection_expr, const std::string & second_projection_expr) const
{
    return std::string("P<") + first_projection_expr + "><" + second_projection_expr + ">";
}

std::string ConditionalTree::getProjectionColumnName(const size_t first_index, const size_t second_index) const
{
    return getProjectionColumnName(nodes[first_index].projection_expression_string, nodes[second_index].projection_expression_string);
}

void ConditionalTree::buildProjectionCompositionRecursive(
    const std::vector<size_t> & path, const size_t child_index, const size_t parent_index)
{
    std::string projection_name = getProjectionColumnName(path[parent_index], path[child_index]);
    if (parent_index - child_index >= 2 && !scopes.getSampleBlock().has(projection_name))
    {
        size_t middle_index = (child_index + parent_index) / 2;
        buildProjectionCompositionRecursive(path, child_index, middle_index);
        buildProjectionCompositionRecursive(path, middle_index, parent_index);
        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get("__inner_build_projection_composition__", context);
        scopes.addAction(ExpressionAction::applyFunction(function_builder,
            {getProjectionColumnName(path[parent_index], path[middle_index]),
                getProjectionColumnName(path[middle_index], path[child_index])},
            projection_name,
            getProjectionSourceColumn()));
    }
}

void ConditionalTree::buildProjectionComposition(const size_t child_node, const size_t parent_node)
{
    std::vector<size_t> path;
    size_t node = child_node;
    while (true)
    {
        path.push_back(node);
        if (node == parent_node)
        {
            break;
        }
        node = nodes[node].getParentNode();
    }
    buildProjectionCompositionRecursive(path, 0, path.size() - 1);
}

std::string ConditionalTree::getProjectionSourceColumn(size_t node) const
{
    if (nodes[node].is_root)
    {
        return "";
    }
    else
    {
        return ConditionalTree::getProjectionColumnName(nodes[node].getParentNode(), node);
    }
}

ConditionalTree::ConditionalTree(ScopeStack & scopes, const Context & context)
    : current_node(0), nodes(1), scopes(scopes), context(context), projection_expression_index()
{
    nodes[0].is_root = true;
}

void ConditionalTree::goToProjection(const std::string & field_name)
{
    std::string current_projection_name = nodes[current_node].projection_expression_string;
    std::string new_projection_name = current_projection_name.empty() ? field_name : current_projection_name + ";" + field_name;
    std::string projection_column_name = getProjectionColumnName(current_projection_name, new_projection_name);
    if (!scopes.getSampleBlock().has(projection_column_name))
    {
        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get("one_or_zero", context);
        scopes.addAction(ExpressionAction::applyFunction(
            function_builder, {getColumnName(field_name)}, projection_column_name, getProjectionSourceColumn()));
        nodes.emplace_back(Node());
        nodes.back().projection_expression_string = new_projection_name;
        nodes.back().parent_node = current_node;
        current_node = nodes.size() - 1;
        projection_expression_index[projection_column_name] = current_node;
    }
    else
    {
        current_node = projection_expression_index[projection_column_name];
    }
}

std::string ConditionalTree::buildRestoreProjectionAndGetName(const size_t levels_up)
{
    size_t target_node = current_node;
    for (size_t i = 0; i < levels_up; ++i)
    {
        target_node = nodes[target_node].getParentNode();
    }
    buildProjectionComposition(current_node, target_node);
    return getProjectionColumnName(target_node, current_node);
}

void ConditionalTree::restoreColumn(
    const std::string & default_values_name, const std::string & new_values_name, const size_t levels_up, const std::string & result_name)
{
    size_t target_node = current_node;
    for (size_t i = 0; i < levels_up; ++i)
    {
        target_node = nodes[target_node].getParentNode();
    }
    const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get("__inner_restore_projection__", context);
    scopes.addAction(ExpressionAction::applyFunction(function_builder,
        {getProjectionColumnName(target_node, current_node),
            getColumnNameByIndex(default_values_name, current_node),
            getColumnNameByIndex(new_values_name, current_node)},
        getColumnNameByIndex(result_name, target_node),
        getProjectionSourceColumn()));
}

void ConditionalTree::goUp(const size_t levels_up)
{
    for (size_t i = 0; i < levels_up; ++i)
    {
        current_node = nodes[current_node].getParentNode();
    }
}

bool ConditionalTree::tryToGetFromUpperProjection(const std::string & column_name)
{
    size_t node = current_node;
    while (true)
    {
        if (scopes.getSampleBlock().has(getColumnNameByIndex(column_name, node)))
        {
            if (node != current_node)
            {
                buildProjectionComposition(current_node, node);
                const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get("__inner_project__", context);
                scopes.addAction(ExpressionAction::applyFunction(function_builder,
                    {getColumnNameByIndex(column_name, node), getProjectionColumnName(node, current_node)},
                    getColumnName(column_name),
                    getProjectionSourceColumn(node)));
            }
            return true;
        }
        if (nodes[node].is_root)
        {
            break;
        }
        node = nodes[node].getParentNode();
    }
    return false;
}

std::string ConditionalTree::getProjectionExpression()
{
    return nodes[current_node].projection_expression_string;
}

std::string ConditionalTree::getProjectionSourceColumn() const
{
    return getProjectionSourceColumn(current_node);
}

void DefaultProjectionAction::preArgumentAction() {}

void DefaultProjectionAction::postArgumentAction(const std::string & /*argument_name*/) {}

void DefaultProjectionAction::preCalculation() {}

bool DefaultProjectionAction::isCalculationRequired()
{
    return true;
}

AndOperatorProjectionAction::AndOperatorProjectionAction(
    ScopeStack & scopes, ProjectionManipulatorPtr projection_manipulator, const std::string & expression_name, const Context & context)
    : scopes(scopes)
    , projection_manipulator(projection_manipulator)
    , previous_argument_name()
    , projection_levels_count(0)
    , expression_name(expression_name)
    , context(context)
{
}

std::string AndOperatorProjectionAction::getZerosColumnName()
{
    return "__inner_zeroes_column__" + expression_name;
}

std::string AndOperatorProjectionAction::getFinalColumnName()
{
    return "__inner_final_column__" + expression_name;
}

void AndOperatorProjectionAction::createZerosColumn(const std::string & restore_projection_name)
{
    auto zeros_column_name = projection_manipulator->getColumnName(getZerosColumnName());
    if (!scopes.getSampleBlock().has(zeros_column_name))
    {
        scopes.addAction(ExpressionAction::addColumn(
            ColumnWithTypeAndName(ColumnUInt8::create(0, 1), std::make_shared<DataTypeUInt8>(), zeros_column_name),
            restore_projection_name,
            true));
    }
}

void AndOperatorProjectionAction::preArgumentAction()
{
    if (!previous_argument_name.empty())
    {
        // Before processing arguments starting from second to last
        if (auto * conditional_tree = typeid_cast<ConditionalTree *>(projection_manipulator.get()))
        {
            conditional_tree->goToProjection(previous_argument_name);
        }
        else
        {
            throw Exception(
                "Illegal projection manipulator used in AndOperatorProjectionAction", ErrorCodes::ILLEGAL_PROJECTION_MANIPULATOR);
        }
        ++projection_levels_count;
    }
}

void AndOperatorProjectionAction::postArgumentAction(const std::string & argument_name)
{
    previous_argument_name = argument_name;
}

void AndOperatorProjectionAction::preCalculation()
{
    if (auto * conditional_tree = typeid_cast<ConditionalTree *>(projection_manipulator.get()))
    {
        auto final_column = getFinalColumnName();
        const FunctionBuilderPtr & function_builder = FunctionFactory::instance().get("one_or_zero", context);
        scopes.addAction(ExpressionAction::applyFunction(function_builder,
            {projection_manipulator->getColumnName(previous_argument_name)},
            projection_manipulator->getColumnName(final_column),
            projection_manipulator->getProjectionSourceColumn()));
        std::string restore_projection_name = conditional_tree->buildRestoreProjectionAndGetName(projection_levels_count);
        createZerosColumn(restore_projection_name);
        conditional_tree->restoreColumn(getZerosColumnName(), final_column, projection_levels_count, expression_name);
        conditional_tree->goUp(projection_levels_count);
    }
    else
    {
        throw Exception("Illegal projection manipulator used in AndOperatorProjectionAction", ErrorCodes::ILLEGAL_PROJECTION_MANIPULATOR);
    }
}

bool AndOperatorProjectionAction::isCalculationRequired()
{
    return false;
}

ProjectionActionBase::~ProjectionActionBase() {}

ProjectionActionPtr getProjectionAction(const std::string & node_name,
    ScopeStack & scopes,
    ProjectionManipulatorPtr projection_manipulator,
    const std::string & expression_name,
    const Context & context)
{
    if (typeid_cast<ConditionalTree *>(projection_manipulator.get()) && node_name == "and")
    {
        return std::make_shared<AndOperatorProjectionAction>(scopes, projection_manipulator, expression_name, context);
    }
    else
    {
        return std::make_shared<DefaultProjectionAction>();
    }
}

}
