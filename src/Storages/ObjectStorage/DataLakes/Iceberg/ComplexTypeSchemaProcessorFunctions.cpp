#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include "Common/Exception.h"
#include "DataTypes/DataTypeTuple.h"

namespace DB
{

ColumnPtr ExecutableDeletingEvolutionFunction::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    std::cerr << "FIELD IN DELETE " << operation.field_name << '\n';
    for (const auto& elem : operation.root) {
        std::cerr << "PATH IN DELETE " << elem << '\n';
    }
    const auto& column = arguments[0].column;
    auto result_column = types[0]->createColumn();

    std::vector<size_t> index_path;
    auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_types[0])->getElements();
    auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_types[0])->getElementNames();
    for (auto path : operation.root)
    {
        size_t result_index = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            std::cerr << "Check element name " << element_names[i] << '\n';
            if (element_names[i] == path)
            {
                result_index = i;
                break;
            }
        }
        if (result_index == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find path {} in data", path);  

        index_path.push_back(result_index);
        auto old_current_types = current_types;
        current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
        element_names = std::static_pointer_cast<const DataTypeTuple>(old_current_types[result_index])->getElementNames();
    }

    size_t index_to_delete = current_types.size();
    std::cerr << "types size " << current_types.size() << '\n';
    for (size_t i = 0; i < current_types.size(); ++i)
    {
        std::cerr << "iterating over names " << element_names[i] << '\n';
        if (element_names[i] == operation.field_name)
            index_to_delete = i;
    }
    if (index_to_delete == current_types.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find field {} in data", operation.field_name);  

    std::cerr << "input_rows_count " << input_rows_count << ' ' << column->size() << ' ' << index_to_delete << '\n';
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
        std::cerr << "count field after deleting " << shuffled_node.size() << '\n';
        for (const auto & pf : shuffled_node) {
            std::cerr << "result field after deleting " << pf.dump() << '\n';
        }
        result_column->insert(shuffled_node);
    }
    return result_column->convertToFullColumnIfSparse();
}

ColumnPtr ExecutableAddingEvolutionFunction::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const auto& column = arguments[0].column;
    auto result_column = types[0]->createColumn();

    std::vector<size_t> index_path;
    auto current_types = std::static_pointer_cast<const DataTypeTuple>(types[0])->getElements();
    auto element_names = std::static_pointer_cast<const DataTypeTuple>(types[0])->getElementNames();
    for (auto path : operation.root)
    {
        size_t result_index = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (element_names[i] == path)
            {
                result_index = i;
                break;
            }
        }
        if (result_index == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find path {} in data", path);  
                
        index_path.push_back(result_index);
        auto old_current_types = current_types;
        current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
        element_names = std::static_pointer_cast<const DataTypeTuple>(old_current_types[result_index])->getElementNames();
    }

    size_t index_to_insert = current_types.size();
    for (size_t i = 0; i < current_types.size(); ++i)
    {
        if (element_names[i] == operation.field_name)
            index_to_insert = i;
    }
    if (index_to_insert == current_types.size())
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
        
        std::cerr << "index_to_insert " << shuffled_node.size() << ' ' << shuffled_node[0].dump() << ' ' << index_to_insert << ' ' << element_names[index_to_insert] << '\n';
        Field inserted_field = current_types[index_to_insert]->getDefault();
        shuffled_node.insert(shuffled_node.begin() + index_to_insert, inserted_field);
        std::cerr << "result child node " << shuffled_node.size() << ' ' << shuffled_node[0].dump() << '\n';
        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }
        std::cerr << "INSERT: count field after deleting " << shuffled_node.size() << '\n';
        for (const auto & pf : shuffled_node) {
            std::cerr << "INSERT: result field after deleting " << pf.dump() << '\n';
        }

        result_column->insert(shuffled_node);
    }
    return result_column->convertToFullColumnIfSparse();
}


std::string DumpJSONIntoStr(Poco::SharedPtr<Poco::JSON::Object> obj)
{
    std::stringstream os;
    Poco::JSON::Stringifier::stringify(obj, os);
    return os.str();
}

}
