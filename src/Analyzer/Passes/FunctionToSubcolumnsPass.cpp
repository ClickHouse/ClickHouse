#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>

#include <Storages/IStorage.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace
{

class FunctionToSubcolumnsVisitor : public InDepthQueryTreeVisitor<FunctionToSubcolumnsVisitor>
{
public:
    explicit FunctionToSubcolumnsVisitor(ContextPtr & context_)
        : context(context_)
    {}

    void visitImpl(QueryTreeNodePtr & node) const
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        auto function_arguments_nodes = function_node->getArguments().getNodes();
        size_t function_arguments_nodes_size = function_arguments_nodes.size();

        if (function_arguments_nodes.empty() || function_arguments_nodes_size > 2)
            return;

        auto * first_argument_column_node = function_arguments_nodes.front()->as<ColumnNode>();

        if (!first_argument_column_node)
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
                    /// Replace `length(array_argument)` with `array_argument.length`
                    column.name += ".size0";

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "empty")
                {
                    /// Replace `empty(array_argument)` with `equals(array_argument.size0, 0)`
                    column.name += ".size0";
                    column.type = std::make_shared<DataTypeUInt64>();

                    auto equals_function = std::make_shared<FunctionNode>("equals");
                    resolveOrdinaryFunctionNode(*equals_function, "equals");

                    auto & equals_function_arguments = equals_function->getArguments().getNodes();
                    equals_function_arguments.reserve(2);
                    equals_function_arguments.push_back(std::make_shared<ColumnNode>(column, column_source));
                    equals_function_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

                    node = std::move(equals_function);
                }
                else if (function_name == "notEmpty")
                {
                    /// Replace `notEmpty(array_argument)` with `notEquals(array_argument.size0, 0)`
                    column.name += ".size0";
                    column.type = std::make_shared<DataTypeUInt64>();

                    auto not_equals_function = std::make_shared<FunctionNode>("notEquals");
                    resolveOrdinaryFunctionNode(*not_equals_function, "notEquals");

                    auto & not_equals_function_arguments = not_equals_function->getArguments().getNodes();
                    not_equals_function_arguments.reserve(2);
                    not_equals_function_arguments.push_back(std::make_shared<ColumnNode>(column, column_source));
                    not_equals_function_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

                    node = std::move(not_equals_function);
                }
            }
            else if (column_type.isNullable())
            {
                if (function_name == "isNull")
                {
                    /// Replace `isNull(nullable_argument)` with `nullable_argument.null`
                    column.name += ".null";

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "isNotNull")
                {
                    /// Replace `isNotNull(nullable_argument)` with `not(nullable_argument.null)`
                    column.name += ".null";
                    column.type = std::make_shared<DataTypeUInt8>();

                    auto not_function = std::make_shared<FunctionNode>("not");
                    resolveOrdinaryFunctionNode(*not_function, "not");

                    auto & not_function_arguments = not_function->getArguments().getNodes();
                    not_function_arguments.reserve(2);
                    not_function_arguments.push_back(std::make_shared<ColumnNode>(column, column_source));
                    not_function_arguments.push_back(std::make_shared<ConstantNode>(static_cast<UInt64>(0)));

                    node = std::move(not_function);
                }
            }
            else if (column_type.isMap())
            {
                const auto & data_type_map = assert_cast<const DataTypeMap &>(*column.type);

                if (function_name == "mapKeys")
                {
                    /// Replace `mapKeys(map_argument)` with `map_argument.keys`
                    column.name += ".keys";
                    column.type = data_type_map.getKeyType();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
                else if (function_name == "mapValues")
                {
                    /// Replace `mapValues(map_argument)` with `map_argument.values`
                    column.name += ".values";
                    column.type = data_type_map.getValueType();

                    node = std::make_shared<ColumnNode>(column, column_source);
                }
            }
        }
        else
        {
            auto second_argument_constant_value = function_arguments_nodes[1]->getConstantValueOrNull();

            if (function_name == "tupleElement" && column_type.isTuple() && second_argument_constant_value)
            {
                /** Replace `tupleElement(tuple_argument, string_literal)`, `tupleElement(tuple_argument, integer_literal)`
                  * with `tuple_argument.column_name`.
                  */
                const auto & tuple_element_constant_value = second_argument_constant_value->getValue();
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

                column.name += ".";
                column.name += subcolumn_name;

                size_t subcolumn_position = data_type_tuple.getPositionByName(subcolumn_name);
                column.type = data_type_tuple.getElement(subcolumn_position);

                node = std::make_shared<ColumnNode>(column, column_source);
            }
            else if (function_name == "mapContains" && column_type.isMap())
            {
                const auto & data_type_map = assert_cast<const DataTypeMap &>(*column.type);

                /// Replace `mapContains(map_argument, argument)` with `has(map_argument.keys, argument)`
                column.name += ".keys";
                column.type = data_type_map.getKeyType();

                auto has_function_argument = std::make_shared<ColumnNode>(column, column_source);
                auto has_function = std::make_shared<FunctionNode>("has");
                resolveOrdinaryFunctionNode(*has_function, "has");

                auto & has_function_arguments = has_function->getArguments().getNodes();
                has_function_arguments.reserve(2);
                has_function_arguments.push_back(std::move(has_function_argument));
                has_function_arguments.push_back(std::move(function_arguments_nodes[1]));

                node = std::move(has_function);
            }
        }
    }

    inline void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function_result_type = function_node.getResultType();
        auto function = FunctionFactory::instance().get(function_name, context);
        function_node.resolveAsFunction(function, std::move(function_result_type));
    }

private:
    ContextPtr & context;
};

}

void FunctionToSubcolumnsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FunctionToSubcolumnsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
