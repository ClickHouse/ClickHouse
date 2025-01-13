#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/NestedUtils.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/addSubcolumnsExtraction.h>
#include <iostream>

namespace DB
{

ActionsDAG addSubcolumnsExtraction(ActionsDAG dag, const Block & header, const ContextPtr & context)
{
    /// First check if we need to extract anything or not.
    bool need_to_extract_subcolumns = false;
    for (const auto & input : dag.getInputs())
    {
        if (!header.has(input->result_name) && header.findSubcolumnByName(input->result_name).has_value())
        {
            need_to_extract_subcolumns = true;
            break;
        }
    }

    if (!need_to_extract_subcolumns)
        return dag;

    /// Create a new ActionsDAG that only extracts all subcolumns that are not presented in the header.
    ActionsDAG extract_subcolumns_dag;

    /// Add inputs that are presented in the header.
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

    /// Find all subcolumns in the inputs that are not presented
    /// in the header and extract them using getSubcolumn function.
    for (const auto & input : dag.getInputs())
    {
        auto subcolumn = header.findSubcolumnByName(input->result_name);
        if (!header.has(input->result_name) && subcolumn)
        {
            auto [column_name, subcolumn_name] = Nested::splitName(input->result_name);
            const ActionsDAG::Node * column_input_node;
            /// Check if we don't have input with this column yet.
            if (auto it = input_nodes.find(column_name); it == input_nodes.end())
            {
                const auto & node = extract_subcolumns_dag.addInput(header.getByName(column_name));
                extract_subcolumns_dag.addOrReplaceInOutputs(node);
                input_nodes[column_name] = &node;
            }
            column_input_node = input_nodes[column_name];

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
