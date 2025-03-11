#include <memory>
#include <stack>
#include <variant>

#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include <Poco/JSON/Object.h>
#include <Poco/SharedPtr.h>
#include "Common/Exception.h"
#include "Columns/IColumn.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeTuple.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

IIcebergSchemaTransform::IIcebergSchemaTransform(std::vector<IcebergChangeSchemaOperation::Edge> path_) : path(path_)
{
}

std::vector<std::vector<size_t>> IIcebergSchemaTransform::traverseAllPathes(const ComplexNode & initial_node)
{
    std::vector<std::vector<size_t>> result;

    struct State
    {
        std::vector<size_t> current_path;
        ComplexNode current_value;
    };
    std::stack<State> walk_stack;
    walk_stack.push(State{{}, initial_node});

    while (!walk_stack.empty())
    {
        auto [current_path, current_value] = walk_stack.top();
        walk_stack.pop();

        size_t height = current_path.size();
        if (path[height].child_type == TransformType::STRUCT)
        {
            current_path.push_back(index_path[height]);

            if (current_path.size() == index_path.size())
            {
                result.push_back(current_path);
                continue;
            }
            auto next_val = std::get<Tuple>(current_value)[index_path[height]];
            Tuple tmp_node_tuple;
            Array tmp_node_array;
            if (next_val.tryGet(tmp_node_tuple))
            {
                current_value = std::move(tmp_node_tuple);
            }
            else if (next_val.tryGet(tmp_node_array))
            {
                current_value = std::move(tmp_node_array);
            }
            walk_stack.push({current_path, current_value});
        }
        else
        {
            for (size_t i = 0; i < std::get<Array>(current_value).size(); ++i)
            {
                current_path.push_back(i);

                if (current_path.size() == index_path.size())
                {
                    result.push_back(current_path);
                    current_path.pop_back();
                    continue;
                }
                auto next_val = std::get<Array>(current_value)[i];
                Tuple tmp_node_tuple;
                Array tmp_node_array;
                ComplexNode result_node;
                if (next_val.tryGet(tmp_node_tuple))
                {
                    result_node = std::move(tmp_node_tuple);
                }
                else if (next_val.tryGet(tmp_node_array))
                {
                    result_node = std::move(tmp_node_array);
                }
                walk_stack.push({current_path, result_node});
                current_path.pop_back();
            }
        }
    }
    return result;
}

void IIcebergSchemaTransform::transform(ComplexNode & initial_node)
{
    auto current_node = initial_node;
    ComplexNode result_node = current_node;

    std::vector<ComplexNode> nodes_in_path;

    auto all_pathes_to_transform = traverseAllPathes(initial_node);
    for (const auto & path_to_transform : all_pathes_to_transform)
    {
        for (size_t i = 0; i < path_to_transform.size(); ++i)
        {
            auto subfield_index = path_to_transform[i];
            auto edge_type = path[i].child_type;
            nodes_in_path.push_back(current_node);
            if (edge_type == TransformType::STRUCT)
            {
                auto current_tuple = std::get<Tuple>(current_node);
                Tuple tmp_node_tuple;
                Array tmp_node_array;
                if (current_tuple[subfield_index].tryGet(tmp_node_tuple))
                {
                    current_node = std::move(tmp_node_tuple);
                }
                else if (current_tuple[subfield_index].tryGet(tmp_node_array))
                {
                    current_node = std::move(tmp_node_array);
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect type in iceberg complex schema transform");
            }
            else
            {
                auto current_tuple = std::get<Array>(current_node);
                Tuple tmp_node_tuple;
                Array tmp_node_array;
                if (current_tuple[subfield_index].tryGet(tmp_node_tuple))
                {
                    current_node = std::move(tmp_node_tuple);
                }
                else if (current_tuple[subfield_index].tryGet(tmp_node_array))
                {
                    current_node = std::move(tmp_node_array);
                }
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect type in iceberg complex schema transform");
            }
        }

        transformChildNode(std::get<Tuple>(current_node));
        ComplexNode fixed_node = std::move(current_node);
        for (int j = static_cast<int>(path_to_transform.size()) - 1; j >= 0; --j)
        {
            if (path[j].child_type == TransformType::STRUCT)
            {
                if (std::holds_alternative<Tuple>(fixed_node))
                    std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Tuple>(fixed_node);
                else
                    std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Array>(fixed_node);
                fixed_node = nodes_in_path[j];
            }
            else
            {
                if (std::holds_alternative<Tuple>(fixed_node))
                    std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Tuple>(fixed_node);
                else
                    std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Array>(fixed_node);
                fixed_node = nodes_in_path[j];
            }
        }
        initial_node = fixed_node;
    }
}

class DeletingTransform : public IIcebergSchemaTransform
{
public:
    explicit DeletingTransform(
        const IcebergDeletingOperation & operation_, DataTypePtr old_type, const std::vector<std::vector<size_t>> & permutations)
        : IIcebergSchemaTransform(operation_.getPath()), operation(operation_)
    {
        for (size_t height = 0; height < operation_.getPath().size(); ++height)
        {
            auto path = operation_.getPath()[height];
            if (path.child_type == TransformType::STRUCT)
            {
                auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
                auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();

                const auto & permutation = permutations[height];
                size_t result_index = current_types.size();
                for (size_t i = 0; i < current_types.size(); ++i)
                {
                    if (element_names[permutation[i]] == path.path)
                    {
                        result_index = i;
                        break;
                    }
                }
                if (result_index == current_types.size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path.path);

                index_path.push_back(static_cast<Int32>(result_index));
                old_type = current_types[result_index];
            }
            else
            {
                index_path.push_back(-1);
                old_type = std::static_pointer_cast<const DataTypeArray>(old_type)->getNestedType();
            }
        }

        auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();

        index_to_delete = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            const auto & permutation = permutations[operation_.getPath().size()];
            if (element_names[permutation[i]] == operation.field_name)
                index_to_delete = i;
        }
        if (index_to_delete == current_types.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find field {} in data", operation.field_name);
    }

    void transformChildNode(Tuple & initial_node) override { initial_node.erase(initial_node.begin() + index_to_delete); }

private:
    IcebergDeletingOperation operation;
    size_t index_to_delete;
};

class AddingTransform : public IIcebergSchemaTransform
{
public:
    explicit AddingTransform(const IcebergAddingOperation & operation_, DataTypePtr type)
        : IIcebergSchemaTransform(operation_.getPath()), operation(operation_)
    {
        for (const auto & path : operation_.getPath())
        {
            if (path.child_type == TransformType::STRUCT)
            {
                current_types = std::static_pointer_cast<const DataTypeTuple>(type)->getElements();
                auto element_names = std::static_pointer_cast<const DataTypeTuple>(type)->getElementNames();

                size_t result_index = current_types.size();
                for (size_t i = 0; i < current_types.size(); ++i)
                {
                    if (element_names[i] == path.path)
                    {
                        result_index = i;
                        break;
                    }
                }
                if (result_index == current_types.size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path.path);

                index_path.push_back(static_cast<Int32>(result_index));
                type = current_types[result_index];
            }
            else
            {
                index_path.push_back(-1);
                type = std::static_pointer_cast<const DataTypeArray>(type)->getNestedType();
            }
        }

        current_types = std::static_pointer_cast<const DataTypeTuple>(type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(type)->getElementNames();

        index_to_insert = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (element_names[i] == operation.field_name)
                index_to_insert = i;
        }
        if (index_to_insert == current_types.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find field {} in data", operation.field_name);
    }

    void transformChildNode(Tuple & initial_node) override
    {
        Field inserted_field = current_types[index_to_insert]->getDefault();
        initial_node.insert(initial_node.begin() + index_to_insert, inserted_field);
    }

private:
    IcebergAddingOperation operation;
    size_t index_to_insert;
    DataTypes current_types;
};

class ReorderingTransform : public IIcebergSchemaTransform
{
public:
    explicit ReorderingTransform(const IcebergReorderingOperation & operation_, DataTypePtr type)
        : IIcebergSchemaTransform(operation_.getPath()), operation(operation_)
    {
        for (const auto & path : operation_.getPath())
        {
            if (path.child_type == TransformType::STRUCT)
            {
                current_types = std::static_pointer_cast<const DataTypeTuple>(type)->getElements();
                auto element_names = std::static_pointer_cast<const DataTypeTuple>(type)->getElementNames();

                size_t result_index = current_types.size();
                for (size_t i = 0; i < current_types.size(); ++i)
                {
                    if (element_names[i] == path.path)
                    {
                        result_index = i;
                        break;
                    }
                }
                if (result_index == current_types.size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path.path);

                index_path.push_back(static_cast<Int32>(result_index));
                type = current_types[result_index];
            }
            else
            {
                index_path.push_back(-1);
                type = std::static_pointer_cast<const DataTypeArray>(type)->getNestedType();
            }
        }
    }

    void transformChildNode(Tuple & initial_node) override
    {
        Tuple permuted_node;
        for (auto index : operation.permutation)
            permuted_node.push_back(initial_node[index]);
        initial_node = std::move(permuted_node);
    }

private:
    IcebergReorderingOperation operation;
    DataTypes current_types;
};

ColumnPtr ExecutableEvolutionFunction::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    {
        std::lock_guard lock(mutex);
        if (!initialized)
        {
            lazyInitialize();
            initialized = true;
        }
    }

    const auto & column = arguments[0].column;
    auto result_column = type->createColumn();

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        Tuple current_tuple_node;
        Array current_array_node;
        Field column_field;
        column->get(i, column_field);
        if (column_field.tryGet(current_tuple_node))
        {
            ComplexNode tuple_node = current_tuple_node;
            for (const auto & transform : transforms)
                transform->transform(tuple_node);
            Tuple result_node = std::get<Tuple>(tuple_node);
            result_column->insert(result_node);
        }
        else if (column_field.tryGet(current_array_node))
        {
            ComplexNode tuple_node = current_array_node;
            for (const auto & transform : transforms)
                transform->transform(tuple_node);
            Array result_node = std::get<Array>(tuple_node);
            result_column->insert(result_node);
        }
    }

    return result_column;
}

void ExecutableEvolutionFunction::fillMissingElementsInPermutation(std::vector<size_t> & permutation, size_t target_size)
{
    std::unordered_set<size_t> existing_elements;
    for (auto elem : permutation)
        existing_elements.insert(elem);

    for (size_t i = 0; i < target_size; ++i)
    {
        if (!existing_elements.contains(i))
            permutation.push_back(i);
    }
}

Poco::JSON::Array::Ptr ExecutableEvolutionFunction::makeArrayFromObject(Poco::JSON::Object::Ptr object)
{
    Poco::JSON::Array::Ptr json_array(new Poco::JSON::Array);
    json_array->add(object);
    return json_array;
}

void ExecutableEvolutionFunction::lazyInitialize() const
{
    /// To process schema evolution in case when we have tree-based tuple we use DFS
    /// and process 3-stage pipeline, which was described above.
    /// Struct below is state of out DFS algorithm.
    struct TraverseItem
    {
        Poco::JSON::Array::Ptr old_subfields = nullptr;
        Poco::JSON::Array::Ptr fields = nullptr;
        std::vector<IcebergChangeSchemaOperation::Edge> current_path = {};
        std::vector<std::vector<size_t>> permutations = {};
        bool is_struct = false;
    };

    std::stack<TraverseItem> walk_stack;
    if (old_json->isObject("type") && old_json->getObject("type")->getValue<std::string>("type") == "struct")
    {
        auto subfields = field->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
        auto old_subfields = old_json->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();

        walk_stack.push(TraverseItem{old_subfields, subfields, {}, {}, true});
    }
    else if (old_json->isObject("type") && old_json->getObject("type")->getValue<std::string>("type") == "list")
    {
        if (field->getObject("type")->isObject("element"))
        {
            auto subfields = field->getObject("type")->getObject("element");
            auto old_subfields = old_json->getObject("type")->getObject("element");

            walk_stack.push({makeArrayFromObject(old_subfields), makeArrayFromObject(subfields), {}, {}, false});
        }
    }

    while (!walk_stack.empty())
    {
        auto [old_subfields, subfields, current_path, parent_permutations, is_struct] = walk_stack.top();
        walk_stack.pop();

        if (!is_struct)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(0));
            auto subfield = subfields->getObject(static_cast<UInt32>(0));
            parent_permutations.push_back({});

            current_path.push_back({"", TransformType::ARRAY});
            if (old_subfield->has("type") && old_subfield->getValue<std::string>("type") == "struct")
            {
                walk_stack.push(
                    {old_subfield->get("fields").extract<Poco::JSON::Array::Ptr>(),
                     subfield->get("fields").extract<Poco::JSON::Array::Ptr>(),
                     current_path,
                     parent_permutations,
                     true});
            }
            if (old_subfield->has("type") && old_subfield->getValue<std::string>("type") == "list")
            {
                walk_stack.push(
                    {makeArrayFromObject(old_subfield->getObject("element")),
                     makeArrayFromObject(subfield->getObject("element")),
                     current_path,
                     parent_permutations,
                     true});
            }
            current_path.pop_back();

            continue;
        }

        std::vector<std::string> initial_field_names;
        std::vector<std::string> result_field_names;

        std::unordered_map<size_t, Poco::SharedPtr<Poco::JSON::Object>> id_to_type;
        std::unordered_map<size_t, size_t> old_types;

        /// Stage 1: collect new permutation to reorder current fields.
        std::vector<size_t> initial_permutation;
        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            old_types[old_subfield->getValue<size_t>("id")] = j;
        }

        for (size_t j = 0; j < subfields->size(); ++j)
        {
            auto subfield = subfields->getObject(static_cast<UInt32>(j));
            result_field_names.push_back(subfield->getValue<String>("name"));
            auto subfield_id = subfield->getValue<size_t>("id");
            id_to_type[subfield_id] = subfield;
            if (old_types.contains(subfield_id))
            {
                initial_permutation.push_back(old_types[subfield_id]);
            }
        }

        fillMissingElementsInPermutation(initial_permutation, old_subfields->size());
        parent_permutations.push_back(initial_permutation);
        {
            auto operation = IcebergReorderingOperation(current_path, initial_permutation);
            auto transform = std::make_shared<ReorderingTransform>(operation, old_type);
            transforms.push_back(transform);
        }
        /// Stage 2: Deleting extra fields in current schema.
        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            auto id_old_subfield = old_subfield->getValue<size_t>("id");
            if (!id_to_type.contains(id_old_subfield))
            {
                auto operation = IcebergDeletingOperation(current_path, old_subfield->getValue<String>("name"));
                auto transform = std::make_shared<DeletingTransform>(operation, old_type, parent_permutations);
                transforms.push_back(transform);
                continue;
            }
        }

        /// Stage 3: Adding new fields from new object.
        for (size_t j = 0; j < subfields->size(); ++j)
        {
            auto subfield = subfields->getObject(static_cast<UInt32>(j));
            auto id_subfield = subfield->getValue<size_t>("id");
            if (!old_types.contains(id_subfield))
            {
                auto operation = IcebergAddingOperation(current_path, subfield->getValue<String>("name"));
                auto transform = std::make_shared<AddingTransform>(operation, type);
                transforms.push_back(transform);
            }
        }

        /// Transition to child structures, if any.
        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            auto id_old_subfield = old_subfield->getValue<size_t>("id");
            if (!id_to_type.contains(id_old_subfield))
            {
                continue;
            }
            auto subfield = id_to_type[id_old_subfield];

            if (subfield->isObject("type") && subfield->getObject("type")->getValue<std::string>("type") == "struct")
            {
                auto child_subfields = subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                auto child_old_subfields = old_subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                current_path.push_back({old_subfield->getValue<std::string>("name"), TransformType::STRUCT});
                walk_stack.push({child_old_subfields, child_subfields, current_path, parent_permutations, true});
                current_path.pop_back();
            }
            else if (subfield->isObject("type") && subfield->getObject("type")->getValue<std::string>("type") == "list")
            {
                auto child_subfield = subfield->getObject("type")->getObject("element");
                auto child_old_subfield = old_subfield->getObject("type")->getObject("element");
                current_path.push_back({"", TransformType::ARRAY});
                walk_stack.push(
                    {makeArrayFromObject(child_old_subfield),
                     makeArrayFromObject(child_subfield),
                     current_path,
                     parent_permutations,
                     false});
                current_path.pop_back();
            }
        }
    }
}

}
