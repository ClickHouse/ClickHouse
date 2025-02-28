#include <sstream>
#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ComplexTypeSchemaProcessorFunctions.h>
#include <Poco/SharedPtr.h>
#include "Common/Exception.h"
#include "Columns/IColumn.h"
#include "DataTypes/DataTypeTuple.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

class DeletingTransform : public IIcebergSchemaTransform
{
public:
    explicit DeletingTransform(
        const IcebergDeletingOperation & operation_, DataTypePtr old_type, const std::vector<std::vector<size_t>> & permutations)
        : operation(operation_)
    {
        auto current_types = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElements();
        auto element_names = std::static_pointer_cast<const DataTypeTuple>(old_type)->getElementNames();
        for (size_t height = 0; height < operation_.root.size(); ++height)
        {
            auto path = operation_.root[height];
            const auto & permutation = permutations[height];
            size_t result_index = current_types.size();
            for (size_t i = 0; i < current_types.size(); ++i)
            {
                if (element_names[permutation[i]] == path)
                {
                    result_index = i;
                    break;
                }
            }
            if (result_index == current_types.size())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path);

            index_path.push_back(result_index);
            auto old_current_types = current_types;
            current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
            element_names = std::static_pointer_cast<const DataTypeTuple>(old_current_types[result_index])->getElementNames();
        }

        index_to_delete = current_types.size();
        for (size_t i = 0; i < current_types.size(); ++i)
        {
            const auto & permutation = permutations[operation_.root.size()];
            if (element_names[permutation[i]] == operation.field_name)
                index_to_delete = i;
        }
        if (index_to_delete == current_types.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find field {} in data", operation.field_name);
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path);

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find field {} in data", operation.field_name);
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

class ReorderingTransform : public IIcebergSchemaTransform
{
public:
    explicit ReorderingTransform(const IcebergReorderingOperation & operation_, DataTypePtr type) : operation(operation_)
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not find path {} in data", path);

            index_path.push_back(result_index);
            auto old_current_types = current_types;
            current_types = std::static_pointer_cast<const DataTypeTuple>(current_types[result_index])->getElements();
            element_names = std::static_pointer_cast<const DataTypeTuple>(old_current_types[result_index])->getElementNames();
        }
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
        Tuple permuted_node;
        for (auto index : operation.permutation)
            permuted_node.push_back(shuffled_node[index]);
        shuffled_node = std::move(permuted_node);
        for (int j = static_cast<int>(index_path.size()) - 1; j >= 0; --j)
        {
            nodes_in_path[j][index_path[j]] = shuffled_node;
            shuffled_node = nodes_in_path[j];
        }

        initial_node = shuffled_node;
    }

private:
    IcebergReorderingOperation operation;
    std::vector<size_t> index_path;
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

static void fillMissingElementsInPermutation(std::vector<size_t> & permutation, size_t target_size)
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

/// To process schema evolution in case when we have tree-based tuple we use DFS
/// and process 3-stage pipeline, which was described above.
/// Struct below is state of out DFS algorithm.
struct TraverseItem
{
    Poco::JSON::Array::Ptr old_subfields;
    Poco::JSON::Array::Ptr fields;
    std::vector<String> current_path;
    std::vector<std::vector<size_t>> permutations;
};

void ExecutableIdentityEvolutionFunction::lazyInitialize() const
{
    std::stack<TraverseItem> walk_stack;
    {
        auto subfields = field->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
        auto old_subfields = old_json->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();

        walk_stack.push(TraverseItem{old_subfields, subfields, {}, {}});
    }

    while (!walk_stack.empty())
    {
        auto [old_subfields, subfields, current_path, parent_permutations] = walk_stack.top();
        walk_stack.pop();

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

            if (subfield->isObject("type"))
            {
                auto child_subfields = subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                auto child_old_subfields = old_subfield->getObject("type")->get("fields").extract<Poco::JSON::Array::Ptr>();
                current_path.push_back(old_subfield->getValue<std::string>("name"));
                walk_stack.push({child_old_subfields, child_subfields, current_path, parent_permutations});
                current_path.pop_back();
            }
        }
    }
}

}
