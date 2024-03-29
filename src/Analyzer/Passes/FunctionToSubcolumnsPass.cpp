#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>

#include <Storages/IStorage.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>

namespace DB
{

namespace
{

class FunctionToSubcolumnsVisitor : public InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FunctionToSubcolumnsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node) const
    {
        if (!getSettings().optimize_functions_to_subcolumns)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        auto & function_arguments_nodes = function_node->getArguments().getNodes();
        size_t function_arguments_nodes_size = function_arguments_nodes.size();

        if (function_arguments_nodes.empty() || function_arguments_nodes_size > 2)
            return;

        auto * first_argument_column_node = function_arguments_nodes.front()->as<ColumnNode>();

        if (!first_argument_column_node)
            return;

        if (first_argument_column_node->getColumnName() == "__grouping_set")
            return;

        auto column_source = first_argument_column_node->getColumnSource();
        auto * table_node = column_source->as<TableNode>();

        if (!table_node)
            return;

        const auto & storage = table_node->getStorage();
        if (!storage->supportsSubcolumns())
            return;

        auto column = first_argument_column_node->getColumn();
        WhichDataType column_type(column.type);

        const auto & function_name = function_node->getFunctionName();

        if (function_arguments_nodes_size == 1)
        {
            if (column_type.isArray())
            {
                if (function_name == "length")
                {
                    /// Replace `length(array_argument)` with `array_argument.size0`
                    column.name += ".size0";
                    column.type = std::make_shared<DataTypeUInt64>();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "empty")
                {
                    /// Replace `empty(array_argument)` with `equals(array_argument.size0, 0)`
                    column.name += ".size0";
                    column.type = std::make_shared<DataTypeUInt64>();

                    function_arguments_nodes.clear();
                    function_arguments_nodes.push_back(std::make_shared<ColumnNode>(column, column_source));
                    function_arguments_nodes.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

                    resolveOrdinaryFunctionNode(*function_node, "equals");
                }
                else if (function_name == "notEmpty")
                {
                    /// Replace `notEmpty(array_argument)` with `notEquals(array_argument.size0, 0)`
                    column.name += ".size0";
                    column.type = std::make_shared<DataTypeUInt64>();

                    function_arguments_nodes.clear();
                    function_arguments_nodes.push_back(std::make_shared<ColumnNode>(column, column_source));
                    function_arguments_nodes.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

                    resolveOrdinaryFunctionNode(*function_node, "notEquals");
                }
            }
            else if (column_type.isNullable())
            {
                if (function_name == "isNull")
                {
                    /// Replace `isNull(nullable_argument)` with `nullable_argument.null`
                    column.name += ".null";
                    column.type = std::make_shared<DataTypeUInt8>();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "isNotNull")
                {
                    /// Replace `isNotNull(nullable_argument)` with `not(nullable_argument.null)`
                    column.name += ".null";
                    column.type = std::make_shared<DataTypeUInt8>();

                    function_arguments_nodes = {std::make_shared<ColumnNode>(column, column_source)};

                    resolveOrdinaryFunctionNode(*function_node, "not");
                }
            }
            else if (column_type.isMap())
            {
                if (function_name == "mapKeys")
                {
                    /// Replace `mapKeys(map_argument)` with `map_argument.keys`
                    column.name += ".keys";
                    column.type = function_node->getResultType();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "mapValues")
                {
                    /// Replace `mapValues(map_argument)` with `map_argument.values`
                    column.name += ".values";
                    column.type = function_node->getResultType();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
            }
        }
        else
        {
            const auto * second_argument_constant_node = function_arguments_nodes[1]->as<ConstantNode>();

            if (function_name == "tupleElement" && column_type.isTuple() && second_argument_constant_node)
            {
                /** Replace `tupleElement(tuple_argument, string_literal)`, `tupleElement(tuple_argument, integer_literal)`
                  * with `tuple_argument.column_name`.
                  */
                const auto & tuple_element_constant_value = second_argument_constant_node->getValue();
                const auto & tuple_element_constant_value_type = tuple_element_constant_value.getType();

                const auto & data_type_tuple = assert_cast<const DataTypeTuple &>(*column.type);

                String subcolumn_name;

                if (tuple_element_constant_value_type == Field::Types::String)
                {
                    subcolumn_name = tuple_element_constant_value.get<const String &>();
                }
                else if (tuple_element_constant_value_type == Field::Types::UInt64)
                {
                    auto tuple_column_index = tuple_element_constant_value.get<UInt64>();
                    subcolumn_name = data_type_tuple.getNameByPosition(tuple_column_index);
                }
                else
                {
                    return;
                }

                column.name += '.';
                column.name += subcolumn_name;
                column.type = function_node->getResultType();

                node = std::make_shared<ColumnNode>(column, column_source);
            }
            else if (function_name == "variantElement" && isVariant(column_type) && second_argument_constant_node)
            {
                /// Replace `variantElement(variant_argument, type_name)` with `variant_argument.type_name`.
                const auto & variant_element_constant_value = second_argument_constant_node->getValue();
                String subcolumn_name;

                if (variant_element_constant_value.getType() != Field::Types::String)
                    return;

                subcolumn_name = variant_element_constant_value.get<const String &>();

                column.name += '.';
                column.name += subcolumn_name;
                column.type = function_node->getResultType();

                node = std::make_shared<ColumnNode>(column, column_source);
            }
            else if (function_name == "mapContains" && column_type.isMap())
            {
                const auto & data_type_map = assert_cast<const DataTypeMap &>(*column.type);

                /// Replace `mapContains(map_argument, argument)` with `has(map_argument.keys, argument)`
                column.name += ".keys";
                column.type = std::make_shared<DataTypeArray>(data_type_map.getKeyType());

                auto has_function_argument = std::make_shared<ColumnNode>(column, column_source);
                function_arguments_nodes[0] = std::move(has_function_argument);

                resolveOrdinaryFunctionNode(*function_node, "has");
            }
        }
    }

private:
    inline void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function = FunctionFactory::instance().get(function_name, getContext());
        function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
    }
};

}

void FunctionToSubcolumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FunctionToSubcolumnsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
