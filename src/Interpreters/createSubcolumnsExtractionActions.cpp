#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

ActionsDAG createSubcolumnsExtractionActions(const Block & available_columns, const Names & required_columns, const ContextPtr & context)
{
    /// Create a new ActionsDAG that only extracts all subcolumns that are not presented in the available_columns.
    ActionsDAG extract_subcolumns_dag;

    /// Find all subcolumns in the required_columns that are not presented
    /// in the available_columns and extract them using getSubcolumn function.
    std::unordered_map<String, const ActionsDAG::Node *> input_nodes;
    for (const auto & required_column : required_columns)
    {
        if (available_columns.has(required_column))
            continue;

        for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(required_column))
        {
            const auto * column = available_columns.findByName(column_name);
            if (!column || !column->type->tryGetSubcolumnType(subcolumn_name))
                continue;

            const ActionsDAG::Node * column_input_node;
            /// Check if we don't have input with this column yet.
            if (auto it = input_nodes.find(column->name); it == input_nodes.end())
            {
                const auto * node = &extract_subcolumns_dag.addInput(available_columns.getByName(column->name));
                extract_subcolumns_dag.getOutputs().push_back(node);
                input_nodes[column->name] = node;
            }
            column_input_node = input_nodes[column->name];

            /// Create the second argument of getSubcolumn function with string
            /// containing subcolumn name and add it to the ActionsDAG.
            ColumnWithTypeAndName subcolumn_name_arg;
            subcolumn_name_arg.name = subcolumn_name;
            subcolumn_name_arg.type = std::make_shared<DataTypeString>();
            subcolumn_name_arg.column = subcolumn_name_arg.type->createColumnConst(1, subcolumn_name);
            const auto & subcolumn_name_arg_node = extract_subcolumns_dag.addColumn(std::move(subcolumn_name_arg));

            /// Create and add getSubcolumn function
            auto get_subcolumn_function = FunctionFactory::instance().get("getSubcolumn", context);
            ActionsDAG::NodeRawConstPtrs arguments = {column_input_node, &subcolumn_name_arg_node};
            const auto & function_node = extract_subcolumns_dag.addFunction(get_subcolumn_function, std::move(arguments), {});

            /// Create an alias for getSubcolumn function so it has the name of the subcolumn.
            const auto & alias_node = extract_subcolumns_dag.addAlias(function_node, required_column);
            extract_subcolumns_dag.getOutputs().push_back(&alias_node);

            break;
        }
    }

    return extract_subcolumns_dag;
}

}
