#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/addSubcolumnsExtraction.h>

namespace DB
{

ActionsDAG addSubcolumnsExtraction(ActionsDAG dag, const Block & header, const ContextPtr & context)
{
    /// Create a new ActionsDAG that only extracts all subcolumns that are not presented in the header.
    ActionsDAG extract_subcolumns_dag;

    /// First, add inputs that are presented in the header.
    std::unordered_map<String, const ActionsDAG::Node *> input_nodes;
    for (const auto & input : dag.getInputs())
    {
        if (header.has(input->result_name))
        {
            const auto & input_node = extract_subcolumns_dag.addInput(input->result_name, input->result_type);
            extract_subcolumns_dag.addOrReplaceInOutputs(input_node);
            input_nodes[input->result_name] = &input_node;
        }
    }

    /// Second, find all subcolumns in the inputs that are not presented
    /// in the header and extract them using getSubcolumn function.
    for (const auto & input : dag.getInputs())
    {
        auto subcolumn = header.findSubcolumnByName(input->result_name);
        if (!header.has(input->result_name) && subcolumn)
        {
            auto [column_name, subcolumn_name] = Nested::splitName(input->result_name);
            const ActionsDAG::Node * column_input_node;
            /// Check if we already have input with this column.
            if (auto it = input_nodes.find(column_name); it != input_nodes.end())
                column_input_node = input_nodes[column_name];
            else
                column_input_node = &extract_subcolumns_dag.addInput(header.getByName(column_name));

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
            const auto & alias_node = extract_subcolumns_dag.addAlias(function_node, input->result_name);
            extract_subcolumns_dag.addOrReplaceInOutputs(alias_node);
        }
    }

    return ActionsDAG::merge(std::move(extract_subcolumns_dag), std::move(dag));
}

}
