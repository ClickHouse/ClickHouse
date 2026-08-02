#include <memory>
#include <stack>
#include <variant>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include <base/defines.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/SharedPtr.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Iceberg
{

class IcebergDeletingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergDeletingOperation(const std::vector<Edge> & root_, const std::string & field_name_)
        : IcebergChangeSchemaOperation(ChangeType::DELETING, root_)
        , field_name(field_name_)
    {
    }

    std::string field_name;
};

class IcebergAddingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergAddingOperation(const std::vector<Edge> & root_, const std::string & field_name_)
        : IcebergChangeSchemaOperation(ChangeType::ADDING, root_)
        , field_name(field_name_)
    {
    }

    std::string field_name;
};

class IcebergReorderingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergReorderingOperation(const std::vector<Edge> & root_, const std::vector<size_t> & permutation_)
        : IcebergChangeSchemaOperation(ChangeType::REORDERING, root_)
        , permutation(permutation_)
    {
    }

    std::vector<size_t> permutation;
};

IIcebergSchemaTransform::IIcebergSchemaTransform(std::vector<IcebergChangeSchemaOperation::Edge> path_)
    : path(path_)
{
}

std::vector<std::vector<size_t>> IIcebergSchemaTransform::traverseAllPaths(const ComplexNode & initial_node)
{
    if (path.empty())
        return {{}};

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
        switch (path[height].parent_type)
        {
            case TransformType::STRUCT:
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
                Map tmp_node_map;

                if (next_val.tryGet(tmp_node_tuple))
                {
                    current_value = std::move(tmp_node_tuple);
                }
                else if (next_val.tryGet(tmp_node_array))
                {
                    current_value = std::move(tmp_node_array);
                }
                else if (next_val.tryGet(tmp_node_map))
                {
                    current_value = std::move(tmp_node_map);
                }

                walk_stack.push({current_path, current_value});
                break;
            }
            case TransformType::ARRAY:
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
                    Map tmp_node_map;

                    ComplexNode result_node;
                    if (next_val.tryGet(tmp_node_tuple))
                    {
                        result_node = std::move(tmp_node_tuple);
                    }
                    else if (next_val.tryGet(tmp_node_array))
                    {
                        result_node = std::move(tmp_node_array);
                    }
                    else if (next_val.tryGet(tmp_node_map))
                    {
                        result_node = std::move(tmp_node_map);
                    }

                    walk_stack.push({current_path, result_node});
                    current_path.pop_back();
                }
                break;
            }
            case TransformType::MAP:
            {
                for (size_t i = 0; i < std::get<Map>(current_value).size(); ++i)
                {
                    current_path.push_back(i);

                    if (current_path.size() == index_path.size())
                    {
                        result.push_back(current_path);
                        current_path.pop_back();
                        continue;
                    }
                    auto next_val = std::get<Map>(current_value)[i];

                    Tuple tmp_node_tuple;
                    Array tmp_node_array;
                    Map tmp_node_map;

                    ComplexNode result_node;
                    if (next_val.tryGet(tmp_node_tuple))
                    {
                        result_node = std::move(tmp_node_tuple);
                    }
                    else if (next_val.tryGet(tmp_node_array))
                    {
                        result_node = std::move(tmp_node_array);
                    }
                    else if (next_val.tryGet(tmp_node_map))
                    {
                        result_node = std::move(tmp_node_map);
                    }
                    walk_stack.push({current_path, result_node});
                    current_path.pop_back();
                }
                break;
            }
        }
    }
    return result;
}

void IIcebergSchemaTransform::transform(ComplexNode & initial_node)
{
    auto all_paths_to_transform = traverseAllPaths(initial_node);
    for (const auto & path_to_transform : all_paths_to_transform)
    {
        auto current_node = initial_node;

        std::vector<ComplexNode> nodes_in_path;
        for (size_t i = 0; i < path_to_transform.size(); ++i)
        {
            auto subfield_index = path_to_transform[i];
            auto edge_type = path[i].parent_type;
            nodes_in_path.push_back(current_node);

            switch (edge_type)
            {
                case TransformType::STRUCT:
                {
                    auto current_tuple = std::get<Tuple>(current_node);
                    Tuple tmp_node_tuple;
                    Array tmp_node_array;
                    Map tmp_node_map;

                    if (current_tuple[subfield_index].tryGet(tmp_node_tuple))
                    {
                        current_node = std::move(tmp_node_tuple);
                    }
                    else if (current_tuple[subfield_index].tryGet(tmp_node_array))
                    {
                        current_node = std::move(tmp_node_array);
                    }
                    else if (current_tuple[subfield_index].tryGet(tmp_node_map))
                    {
                        current_node = std::move(tmp_node_map);
                    }

                    else
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Incorrect type in iceberg complex schema transform {}",
                            current_tuple[subfield_index].getTypeName());
                    break;
                }
                case TransformType::ARRAY:
                {
                    auto current_tuple = std::get<Array>(current_node);
                    Tuple tmp_node_tuple;
                    Array tmp_node_array;
                    Map tmp_node_map;

                    if (current_tuple[subfield_index].tryGet(tmp_node_tuple))
                    {
                        current_node = std::move(tmp_node_tuple);
                    }
                    else if (current_tuple[subfield_index].tryGet(tmp_node_array))
                    {
                        current_node = std::move(tmp_node_array);
                    }
                    else if (current_tuple[subfield_index].tryGet(tmp_node_map))
                    {
                        current_node = std::move(tmp_node_array);
                    }

                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect type in iceberg complex schema transform");
                    break;
                }
                case TransformType::MAP:
                {
                    auto current_tuple = std::get<Map>(current_node);

                    Tuple tmp_node_tuple;
                    Array tmp_node_array;
                    Map tmp_node_map;

                    if (current_tuple[subfield_index].safeGet<Tuple>()[1].tryGet(tmp_node_tuple))
                    {
                        current_node = std::move(tmp_node_tuple);
                    }
                    else if (current_tuple[subfield_index].safeGet<Tuple>()[1].tryGet(tmp_node_array))
                    {
                        current_node = std::move(tmp_node_array);
                    }
                    else if (current_tuple[subfield_index].safeGet<Tuple>()[1].tryGet(tmp_node_map))
                    {
                        current_node = std::move(tmp_node_map);
                    }
                    else
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect type in iceberg complex schema transform");
                    break;
                }
            }
        }

        transformChildNode(std::get<Tuple>(current_node));
        for (int j = static_cast<int>(path_to_transform.size()) - 1; j >= 0; --j)
        {
            switch (path[j].parent_type)
            {
                case TransformType::STRUCT:
                {
                    if (std::holds_alternative<Tuple>(current_node))
                        std::get<Tuple>(nodes_in_path[j])[path_to_transform[j]] = std::get<Tuple>(current_node);
                    else if (std::holds_alternative<Array>(current_node))
                        std::get<Tuple>(nodes_in_path[j])[path_to_transform[j]] = std::get<Array>(current_node);
                    else if (std::holds_alternative<Map>(current_node))
                        std::get<Tuple>(nodes_in_path[j])[path_to_transform[j]] = std::get<Map>(current_node);

                    current_node = std::move(nodes_in_path[j]);
                    break;
                }
                case TransformType::ARRAY:
                {
                    if (std::holds_alternative<Tuple>(current_node))
                        std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Tuple>(current_node);
                    else if (std::holds_alternative<Array>(current_node))
                        std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Array>(current_node);
                    else if (std::holds_alternative<Map>(current_node))
                        std::get<Array>(nodes_in_path[j])[path_to_transform[j]] = std::get<Map>(current_node);

                    current_node = std::move(nodes_in_path[j]);
                    break;
                }
                case TransformType::MAP:
                {
                    if (std::holds_alternative<Tuple>(current_node))
                        std::get<Map>(nodes_in_path[j])[path_to_transform[j]].safeGet<Tuple>()[1] = std::get<Tuple>(current_node);
                    else if (std::holds_alternative<Array>(current_node))
                        std::get<Map>(nodes_in_path[j])[path_to_transform[j]].safeGet<Tuple>()[1] = std::get<Array>(current_node);
                    else if (std::holds_alternative<Map>(current_node))
                        std::get<Map>(nodes_in_path[j])[path_to_transform[j]].safeGet<Tuple>()[1] = std::get<Map>(current_node);

                    current_node = std::move(nodes_in_path[j]);
                    break;
                }
            }
        }
        initial_node = std::move(current_node);
    }
}

class DeletingTransform : public IIcebergSchemaTransform
{
public:
    explicit DeletingTransform(const IcebergDeletingOperation & operation_, DataTypePtr old_type, const std::vector<size_t> & permutation)
        : IIcebergSchemaTransform(operation_.getPath())
        , operation(operation_)
    {
        for (const auto & path : operation_.getPath())
        {
            switch (path.parent_type)
            {
                case TransformType::STRUCT:
                {
                    auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
                    auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();

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
                    old_type = current_types[result_index];
                    break;
                }
                case TransformType::ARRAY:
                {
                    index_path.push_back(-1);
                    old_type = std::static_pointer_cast<const DataTypeArray>(old_type)->getNestedType();
                    break;
                }
                case TransformType::MAP:
                {
                    index_path.push_back(-1);
                    old_type = std::static_pointer_cast<const DataTypeTuple>(
                                   std::static_pointer_cast<const DataTypeArray>(
                                       std::static_pointer_cast<const DataTypeMap>(old_type)->getNestedType())
                                       ->getNestedType())
                                   ->getElement(1);
                    break;
                }
            }
        }

        auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();

        index_to_delete = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            if (element_names[permutation[i]] == operation.field_name)
                index_to_delete = i;
        }
        if (index_to_delete == current_types.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find field {} in data", operation.field_name);
    }

    void transformChildNode(Tuple & initial_node) override { initial_node.erase(initial_node.begin() + index_to_delete); }

    size_t getDeleteIndex() const { return index_to_delete; }

private:
    IcebergDeletingOperation operation;
    size_t index_to_delete;
};

class AddingTransform : public IIcebergSchemaTransform
{
public:
    explicit AddingTransform(const IcebergAddingOperation & operation_, DataTypePtr type, DataTypePtr old_type)
        : IIcebergSchemaTransform(operation_.getPath())
        , operation(operation_)
    {
        for (const auto & path : operation_.getPath())
        {
            switch (path.parent_type)
            {
                case TransformType::STRUCT:
                {
                    auto old_current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
                    auto old_element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();

                    size_t old_result_index = old_current_types.size();
                    for (size_t i = 0; i < old_current_types.size(); ++i)
                    {
                        if (old_element_names[i] == path.path)
                        {
                            old_result_index = i;
                            break;
                        }
                    }
                    if (old_result_index == old_current_types.size())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in old data", path.path);

                    index_path.push_back(static_cast<Int32>(old_result_index));
                    old_type = old_current_types[old_result_index];

                    current_types = std::static_pointer_cast<const DataTypeTuple>(type)->getElements();
                    auto element_names = std::static_pointer_cast<const DataTypeTuple>(type)->getElementNames();

                    size_t result_index = current_types.size();
                    for (size_t i = 0; i < current_types.size(); ++i)
                    {
                        if (element_names[i] == path.new_path)
                        {
                            result_index = i;
                            break;
                        }
                    }
                    if (result_index == current_types.size())
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in new data", path.new_path);

                    type = current_types[result_index];
                    break;
                }
                case TransformType::ARRAY:
                {
                    index_path.push_back(-1);
                    old_type = std::static_pointer_cast<const DataTypeArray>(old_type)->getNestedType();
                    type = std::static_pointer_cast<const DataTypeArray>(type)->getNestedType();
                    break;
                }
                case TransformType::MAP:
                {
                    index_path.push_back(-1);
                    old_type = std::static_pointer_cast<const DataTypeTuple>(
                                   std::static_pointer_cast<const DataTypeArray>(
                                       std::static_pointer_cast<const DataTypeMap>(old_type)->getNestedType())
                                       ->getNestedType())
                                   ->getElement(1);
                    type = std::static_pointer_cast<const DataTypeTuple>(
                               std::static_pointer_cast<const DataTypeArray>(
                                   std::static_pointer_cast<const DataTypeMap>(type)->getNestedType())
                                   ->getNestedType())
                               ->getElement(1);
                    break;
                }
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
        : IIcebergSchemaTransform(operation_.getPath())
        , operation(operation_)
    {
        for (const auto & path : operation_.getPath())
        {
            switch (path.parent_type)
            {
                case TransformType::STRUCT:
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
                    break;
                }
                case TransformType::ARRAY:
                {
                    index_path.push_back(-1);
                    type = std::static_pointer_cast<const DataTypeArray>(type)->getNestedType();
                    break;
                }
                case TransformType::MAP:
                {
                    index_path.push_back(-1);
                    type = std::static_pointer_cast<const DataTypeTuple>(
                               std::static_pointer_cast<const DataTypeArray>(
                                   std::static_pointer_cast<const DataTypeMap>(type)->getNestedType())
                                   ->getNestedType())
                               ->getElement(1);
                    break;
                }
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
        Map current_map_node;

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
        else if (column_field.tryGet(current_map_node))
        {
            ComplexNode tuple_node = current_map_node;
            for (const auto & transform : transforms)
                transform->transform(tuple_node);
            Map result_node = std::get<Map>(tuple_node);
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

void ExecutableEvolutionFunction::pushNewNode(
    std::stack<ExecutableEvolutionFunction::TraverseItem> & walk_stack,
    Poco::JSON::Object::Ptr old_node,
    Poco::JSON::Object::Ptr new_node,
    std::vector<IcebergChangeSchemaOperation::Edge> * current_path)
{
    if (!old_node->isObject("type"))
        return;

    if (old_node->getObject("type")->getValue<std::string>("type") == "struct")
    {
        Poco::JSON::Array::Ptr subfields = new_node->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
        Poco::JSON::Array::Ptr old_subfields = old_node->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();

        if (current_path)
            current_path->push_back(
                {old_node->getValue<std::string>("name"), new_node->getValue<std::string>("name"), TransformType::STRUCT});

        walk_stack.push(TraverseItem{
            old_subfields,
            subfields,
            current_path ? *current_path : std::vector<IcebergChangeSchemaOperation::Edge>{},
            TransformType::STRUCT});
        if (current_path)
            current_path->pop_back();
    }
    else if (old_node->getObject("type")->getValue<std::string>("type") == "list" && old_node->getObject("type")->isObject("element"))
    {
        auto subfields = new_node->getObject("type")->getObject("element");
        auto old_subfields = old_node->getObject("type")->getObject("element");

        if (current_path)
            current_path->push_back({"", "", TransformType::ARRAY});

        walk_stack.push(
            {makeArrayFromObject(old_subfields),
             makeArrayFromObject(subfields),
             current_path ? *current_path : std::vector<IcebergChangeSchemaOperation::Edge>{},
             TransformType::ARRAY});
        if (current_path)
            current_path->pop_back();
    }
    else if (old_node->getObject("type")->getValue<std::string>("type") == "map" && old_node->getObject("type")->isObject("value"))
    {
        auto subfields = new_node->getObject("type")->getObject("value");
        auto old_subfields = old_node->getObject("type")->getObject("value");
        if (current_path)
            current_path->push_back({"", "", TransformType::MAP});

        walk_stack.push(
            {makeArrayFromObject(old_subfields),
             makeArrayFromObject(subfields),
             current_path ? *current_path : std::vector<IcebergChangeSchemaOperation::Edge>{},
             TransformType::MAP});
        if (current_path)
            current_path->pop_back();
    }
}

std::shared_ptr<ReorderingTransform> makeReorderingTransform(
    const std::vector<IcebergChangeSchemaOperation::Edge> & current_path,
    const std::vector<size_t> & initial_permutation,
    DataTypePtr old_type)
{
    auto operation = IcebergReorderingOperation(current_path, initial_permutation);
    auto transform = std::make_shared<ReorderingTransform>(operation, old_type);
    return transform;
}

std::shared_ptr<DeletingTransform> makeDeletingTransform(
    const std::vector<IcebergChangeSchemaOperation::Edge> & current_path,
    const std::vector<size_t> & initial_permutation,
    DataTypePtr old_type,
    Poco::JSON::Object::Ptr old_subfield)
{
    auto operation = IcebergDeletingOperation(current_path, old_subfield->getValue<String>("name"));
    auto transform = std::make_shared<DeletingTransform>(operation, old_type, initial_permutation);
    return transform;
}


std::shared_ptr<AddingTransform> makeAddingTransform(
    const std::vector<IcebergChangeSchemaOperation::Edge> & current_path,
    DataTypePtr type,
    DataTypePtr old_type,
    Poco::JSON::Object::Ptr subfield)
{
    auto operation = IcebergAddingOperation(current_path, subfield->getValue<String>("name"));
    auto transform = std::make_shared<AddingTransform>(operation, type, old_type);
    return transform;
}

void ExecutableEvolutionFunction::lazyInitialize() const
{
    std::stack<TraverseItem> walk_stack;
    chassert(old_json->isObject("type"));
    pushNewNode(walk_stack, old_json, field, nullptr);
    std::map<size_t, std::vector<std::shared_ptr<IIcebergSchemaTransform>>> per_layer_transforms;

    while (!walk_stack.empty())
    {
        auto [old_subfields, subfields, current_path, transform_type] = walk_stack.top();
        walk_stack.pop();

        if (transform_type != TransformType::STRUCT)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(0));
            auto subfield = subfields->getObject(static_cast<UInt32>(0));

            current_path.push_back({"", "", transform_type});

            if (old_subfield->has("type") && old_subfield->getValue<std::string>("type") == "struct")
            {
                walk_stack.push(
                    {old_subfield->get("fields").extract<Poco::JSON::Array::Ptr>(),
                     subfield->get("fields").extract<Poco::JSON::Array::Ptr>(),
                     current_path,
                     TransformType::STRUCT});
            }
            else if (
                old_subfield->has("type") && old_subfield->getValue<std::string>("type") == "list" && old_subfield->isObject("element"))
            {
                walk_stack.push(
                    {makeArrayFromObject(old_subfield->getObject("element")),
                     makeArrayFromObject(subfield->getObject("element")),
                     current_path,
                     TransformType::ARRAY});
            }
            else if (old_subfield->has("type") && old_subfield->getValue<std::string>("type") == "map" && old_subfield->isObject("value"))
            {
                walk_stack.push(
                    {makeArrayFromObject(old_subfield->getObject("value")),
                     makeArrayFromObject(subfield->getObject("value")),
                     current_path,
                     TransformType::MAP});
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
        per_layer_transforms[current_path.size()].push_back(makeReorderingTransform(current_path, initial_permutation, old_type));
        /// Stage 2: Deleting extra fields in current schema.
        /// All transforms should be ordered by indices of deletion in decreasing order.
        std::vector<std::pair<size_t, std::shared_ptr<DeletingTransform>>> delete_transforms;
        for (size_t j = 0; j < old_subfields->size(); ++j)
        {
            auto old_subfield = old_subfields->getObject(static_cast<UInt32>(j));
            auto id_old_subfield = old_subfield->getValue<size_t>("id");
            if (!id_to_type.contains(id_old_subfield))
            {
                auto transform = makeDeletingTransform(current_path, initial_permutation, old_type, old_subfield);
                delete_transforms.push_back({transform->getDeleteIndex(), transform});
                continue;
            }
        }
        std::sort(
            delete_transforms.begin(), delete_transforms.end(), [](const auto & lhs, const auto & rhs) { return lhs.first > rhs.first; });
        for (const auto & [_, transform] : delete_transforms)
            per_layer_transforms[current_path.size()].push_back(transform);

        /// Stage 3: Adding new fields from new object.
        for (size_t j = 0; j < subfields->size(); ++j)
        {
            auto subfield = subfields->getObject(static_cast<UInt32>(j));
            auto id_subfield = subfield->getValue<size_t>("id");
            if (!old_types.contains(id_subfield))
            {
                auto transform = makeAddingTransform(current_path, type, old_type, subfield);
                per_layer_transforms[current_path.size()].push_back(transform);
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

            pushNewNode(walk_stack, old_subfield, subfield, &current_path);
        }
    }

    for (auto it = per_layer_transforms.rbegin(); it != per_layer_transforms.rend(); ++it)
        for (const auto & transform : it->second)
            transforms.push_back(transform);
}

}
}
