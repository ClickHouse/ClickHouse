#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include "Common/Exception.h"
#include "Columns/IColumn.h"
#include "DataTypes/DataTypeTuple.h"

namespace DB
{

class DeletingTransform : public IIcebergSchemaTransform
{
public:
    explicit DeletingTransform(const IcebergDeletingOperation & operation_, DataTypePtr old_type) : operation(operation_)
    {
        auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();
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

        index_to_delete = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (element_names[i] == operation.field_name)
                index_to_delete = i;
        }
        if (index_to_delete == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find field {} in data", operation.field_name);
    }

    void transform(Tuple & initial_node) override
    {
        auto current_node = initial_node;
        Tuple result_node = current_node;

        std::vector<Tuple> nodes_in_path;
        for (auto subfield_index : index_path)
        {
            nodes_in_path.push_back(current_node);
            auto tmp_node = current_node;
            current_node[subfield_index].tryGet(tmp_node);
            current_node = tmp_node;
        }
        Tuple shuffled_node = current_node;
        shuffled_node.erase(shuffled_node.begin() + index_to_delete);

        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }
        initial_node = shuffled_node;
    }

private:
    IcebergDeletingOperation operation;
    std::vector<size_t> index_path;
    size_t index_to_delete;
};

class AddingTransform : public IIcebergSchemaTransform
{
public:
    explicit AddingTransform(const IcebergAddingOperation & operation_, DataTypePtr type) : operation(operation_)
    {
        current_types = std::static_pointer_cast<const DataTypeTuple>(type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(type)->getElementNames();
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

        index_to_insert = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (element_names[i] == operation.field_name)
                index_to_insert = i;
        }
        if (index_to_insert == current_types.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find field {} in data", operation.field_name);
    }

    void transform(Tuple & initial_node) override
    {
        auto current_node = initial_node;
        Tuple result_node = current_node;

        std::vector<Tuple> nodes_in_path;
        for (auto subfield_index : index_path)
        {
            nodes_in_path.push_back(current_node);
            auto tmp_node = current_node;
            current_node[subfield_index].tryGet(tmp_node);
            current_node = tmp_node;
        }
        Tuple shuffled_node = current_node;
        Field inserted_field = current_types[index_to_insert]->getDefault();
        shuffled_node.insert(shuffled_node.begin() + index_to_insert, inserted_field);
        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }

        initial_node = shuffled_node;
    }

private:
    IcebergAddingOperation operation;
    std::vector<size_t> index_path;
    size_t index_to_insert;
    DataTypes current_types;
};

ColumnPtr ExecutableIdentityEvolutionFunction::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    if (!initialized)
    {
        lazyInitialize();
        initialized = true;
    }

    const auto & column = arguments[0].column;
    auto result_column = type->createColumn();

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        Tuple current_node;
        Field column_field;
        column->get(i, column_field);
        column_field.tryGet(current_node);
        for (const auto & transform : transforms)
            transform->transform(current_node);
        Tuple result_node = current_node;
        result_column->insert(result_node);
    }

    return result_column;
}

void ExecutableIdentityEvolutionFunction::lazyInitialize() const
{
    std::stack<std::tuple<Poco::JSON::Array::Ptr, Poco::JSON::Array::Ptr, std::vector<std::string>>> walk_stack;
    {
        auto subfields = field->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
        auto old_subfields = old_json->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();

        walk_stack.push({old_subfields, subfields, {}});
    }

    int num_iterations = 0;

    while (!walk_stack.empty())
    {
        auto [old_subfields, subfields, current_path] = walk_stack.top();
        walk_stack.pop();
        num_iterations++;
        if (num_iterations > 5)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stack overflow");

        std::vector<std::string> initial_field_names;
        std::vector<std::string> result_field_names;

        std::unordered_map<size_t, Poco::SharedPtr<Poco::JSON::Object>> id_to_type;
        std::unordered_set<size_t> old_types;
        for (size_t j = 0; j < subfields->size(); ++j)
        {
            auto subfield = subfields->getObject(static_cast<UInt32>(j));
            result_field_names.push_back(subfield->getValue<String>("name"));

            id_to_type[subfield->getValue<size_t>("id")] = subfield;
        }

        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            size_t id_old_subfield = old_subfield->getValue<size_t>("id");
            old_types.insert(id_old_subfield);
        }

        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            size_t id_old_subfield = old_subfield->getValue<size_t>("id");
            old_types.insert(id_old_subfield);
            if (!id_to_type.contains(id_old_subfield))
            {
                auto operation = IcebergDeletingOperation(current_path, old_subfield->getValue<String>("name"));
                auto transform = std::make_shared<DeletingTransform>(operation, old_type);
                transforms.push_back(transform);
                continue;
            }
        }

        for (size_t j = 0; j < subfields->size(); ++j)
        {
            auto subfield = subfields->getObject(static_cast<UInt32>(j));
            size_t id_subfield = subfield->getValue<size_t>("id");
            if (!old_types.contains(id_subfield))
            {
                auto operation = IcebergAddingOperation(current_path, subfield->getValue<String>("name"));
                auto transform = std::make_shared<AddingTransform>(operation, type);
                transforms.push_back(transform);
            }
        }

        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            size_t id_old_subfield = old_subfield->getValue<size_t>("id");
            old_types.insert(id_old_subfield);
            if (!id_to_type.contains(id_old_subfield))
            {
                continue;
            }

            auto subfield = id_to_type[id_old_subfield];

            if (subfield->isObject("type"))
            {
                auto child_subfields = subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                auto child_old_subfields = old_subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                current_path.push_back(old_subfield->getValue<std::string>("name"));
                walk_stack.push({child_old_subfields, child_subfields, current_path});
                current_path.pop_back();
            }
        }
    }
}

std::string DumpJSONIntoStr(Poco::SharedPtr<Poco::JSON::Object> obj)
{
    std::stringstream os;
    Poco::JSON::Stringifier::stringify(obj, os);
    return os.str();
}

}
