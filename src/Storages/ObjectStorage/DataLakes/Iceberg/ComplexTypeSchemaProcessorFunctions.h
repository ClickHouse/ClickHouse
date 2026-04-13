#pragma once

#include <memory>
#include <variant>
#include <vector>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/SharedPtr.h>

namespace DB::Iceberg
{

/// Complex type schema evolution consists of multiple stages.
/// First stage is reordering current elements.
/// Second stage is deleting extra fields in tuple.
/// Third stage is adding new fields into tuple.
enum class ChangeType : uint8_t
{
    DELETING,
    ADDING,
    REORDERING,
};

enum class TransformType : uint8_t
{
    STRUCT,
    ARRAY,
    MAP
};

class IcebergChangeSchemaOperation
{
public:
    struct Edge
    {
        String path;
        String new_path;
        TransformType parent_type;
    };

    explicit IcebergChangeSchemaOperation(ChangeType change_type_, const std::vector<Edge> & root_)
        : change_type(change_type_)
        , root(root_)
    {
    }

    ChangeType getType() const { return change_type; }

    const std::vector<Edge> & getPath() const { return root; }

protected:
    ChangeType change_type;
    std::vector<Edge> root;
};

using ComplexNode = std::variant<Tuple, Array, Map>;

class IIcebergSchemaTransform
{
public:
    explicit IIcebergSchemaTransform(std::vector<IcebergChangeSchemaOperation::Edge> path_);

    virtual void transformChildNode(Tuple & initial_node) = 0;

    void transform(ComplexNode & initial_node);

    virtual ~IIcebergSchemaTransform() = default;

protected:
    /// Get real paths from template
    std::vector<std::vector<size_t>> traverseAllPaths(const ComplexNode & initial_node);

    /// index_path is a template for real paths
    /// If current node is ARRAY, then index in path will be equals to -1
    /// In `traverseAllPaths` function this -1 will be replaced to values 0,1,...length_array
    std::vector<int> index_path;
    std::vector<IcebergChangeSchemaOperation::Edge> path;
};

class ExecutableEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableEvolutionFunction(
        DataTypePtr type_, DataTypePtr old_type_, Poco::SharedPtr<Poco::JSON::Object> old_json_, Poco::SharedPtr<Poco::JSON::Object> field_)
        : type(type_)
        , old_type(old_type_)
        , old_json(old_json_)
        , field(field_)
    {
    }

    String getName() const override { return "ExecutableEvolutionFunction"; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    /// To process schema evolution in case when we have tree-based tuple we use DFS
    /// and process 3-stage pipeline, which was described above.
    /// Struct below is state of out DFS algorithm.
    struct TraverseItem
    {
        Poco::JSON::Array::Ptr old_subfields = nullptr;
        Poco::JSON::Array::Ptr fields = nullptr;
        std::vector<IcebergChangeSchemaOperation::Edge> current_path = {};
        TransformType type;
    };

    void lazyInitialize() const;

    static void pushNewNode(
        std::stack<TraverseItem> & walk_stack,
        Poco::JSON::Object::Ptr old_node,
        Poco::JSON::Object::Ptr new_node,
        std::vector<IcebergChangeSchemaOperation::Edge> * current_path);

    static void fillMissingElementsInPermutation(std::vector<size_t> & permutation, size_t target_size);
    static Poco::JSON::Array::Ptr makeArrayFromObject(Poco::JSON::Object::Ptr object);

    DataTypePtr type;
    DataTypePtr old_type;
    Poco::SharedPtr<Poco::JSON::Object> old_json;
    Poco::SharedPtr<Poco::JSON::Object> field;

    mutable std::vector<std::shared_ptr<IIcebergSchemaTransform>> transforms;
    mutable bool initialized = false;
    mutable std::mutex mutex;
};

class EvolutionFunctionStruct : public IFunctionBase
{
public:
    explicit EvolutionFunctionStruct(
        const DataTypes & types_,
        const DataTypes & old_types_,
        Poco::SharedPtr<Poco::JSON::Object> old_json_,
        Poco::SharedPtr<Poco::JSON::Object> field_)
        : types(types_)
        , old_types(old_types_)
        , old_json(old_json_)
        , field(field_)
    {
    }

    String getName() const override { return "EvolutionFunctionStruct"; }

    const DataTypes & getArgumentTypes() const override { return old_types; }

    const DataTypePtr & getResultType() const override { return types[0]; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableEvolutionFunction>(types[0], old_types[0], old_json, field);
    }

    bool isDeterministic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

private:
    DataTypes types;
    DataTypes old_types;
    Poco::SharedPtr<Poco::JSON::Object> old_json;
    Poco::SharedPtr<Poco::JSON::Object> field;
};

}
