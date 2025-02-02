#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include "Common/Exception.h"

namespace DB
{

ColumnPtr ExecutableReorderingEvolutionFunction::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const auto& column = arguments[0].column;
    auto result_column = ColumnTuple::create(0);

    std::vector<size_t> index_path;
    auto current_types = types;
    for (auto path : operation.root)
    {
        size_t result_index = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (current_types[i]->getName() == path)
            {
                result_index = i;
                break;
            }
        }
        if (result_index == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find path {} in data", path);  
                
        index_path.push_back(result_index);
        current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
    }

    std::vector<size_t> permutation(operation.final_order.size());
    std::unordered_map<std::string, size_t> initial_name_to_index;
    for (size_t i = 0; i < operation.initial_order.size(); ++i)
        initial_name_to_index[operation.initial_order[i]] = i;

    for (size_t i = 0; i < operation.final_order.size(); ++i)
        permutation[i] = initial_name_to_index[operation.final_order[i]];

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        Tuple current_node;
        Field field;
        column->get(i, field);
        field.tryGet(current_node);
        Tuple result_node = current_node;

        std::vector<Tuple> nodes_in_path;
        for (auto subfield_index : index_path)
        {
            nodes_in_path.push_back(current_node);
            current_node[subfield_index].tryGet(current_node);
        }
        Tuple shuffled_node;
        for (auto index : permutation) 
            shuffled_node.push_back(current_node[index]);

        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }
        result_column->insert(shuffled_node);
    }
    return result_column->convertToFullColumnIfSparse();
}

ColumnPtr ExecutableDeletingEvolutionFunction::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const auto& column = arguments[0].column;
    auto result_column = ColumnTuple::create(0);

    std::vector<size_t> index_path;
    auto current_types = types;
    for (auto path : operation.root)
    {
        size_t result_index = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (current_types[i]->getName() == path)
            {
                result_index = i;
                break;
            }
        }
        if (result_index == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find path {} in data", path);  
                
        index_path.push_back(result_index);
        current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
    }

    size_t index_to_delete = current_types.size();
    for (size_t i = 0; i < current_types.size(); ++i)
        if (current_types[i]->getName() == operation.field_name)
            index_to_delete = i;
    
    if (index_to_delete == current_types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find field {} in data", operation.field_name);  

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        Tuple current_node;
        Field field;
        column->get(i, field);
        field.tryGet(current_node);
        Tuple result_node = current_node;

        std::vector<Tuple> nodes_in_path;
        for (auto subfield_index : index_path)
        {
            nodes_in_path.push_back(current_node);
            current_node[subfield_index].tryGet(current_node);
        }
        Tuple shuffled_node = current_node;
        shuffled_node.erase(shuffled_node.begin() + index_to_delete);

        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }
        result_column->insert(shuffled_node);
    }
    return result_column->convertToFullColumnIfSparse();
}

}
