#pragma once

#include <memory>
#include <sstream>
#include <vector>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>

#include <Functions/IFunction.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/SharedPtr.h>
#include "DataTypes/IDataType.h"

namespace DB
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

class IcebergChangeSchemaOperation
{
public:
    explicit IcebergChangeSchemaOperation(ChangeType change_type_) : change_type(change_type_) { }

    ChangeType getType() const { return change_type; }

protected:
    ChangeType change_type;
};

class IcebergDeletingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergDeletingOperation(const std::vector<std::string> & root_, const std::string & field_name_)
        : IcebergChangeSchemaOperation(ChangeType::DELETING), root(root_), field_name(field_name_)
    {
    }

    std::vector<std::string> root;
    std::string field_name;
};

class IcebergAddingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergAddingOperation(const std::vector<std::string> & root_, const std::string & field_name_)
        : IcebergChangeSchemaOperation(ChangeType::ADDING), root(root_), field_name(field_name_)
    {
    }

    std::vector<std::string> root;
    std::string field_name;
};

class IcebergReorderingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergReorderingOperation(const std::vector<std::string> & root_, const std::vector<size_t> & permutation_)
        : IcebergChangeSchemaOperation(ChangeType::REORDERING), root(root_), permutation(permutation_)
    {
    }

    std::vector<std::string> root;
    std::vector<size_t> permutation;
};

struct IIcebergSchemaTransform
{
    virtual void transform(Tuple & initial_node) = 0;

    virtual ~IIcebergSchemaTransform() = default;
};

class ExecutableEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableEvolutionFunction(
        DataTypePtr type_, DataTypePtr old_type_, Poco::SharedPtr<Poco::JSON::Object> old_json_, Poco::SharedPtr<Poco::JSON::Object> field_)
        : type(type_), old_type(old_type_), old_json(old_json_), field(field_)
    {
    }

    String getName() const override { return "ExecutableEvolutionFunction"; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    void lazyInitialize() const;
    static void fillMissingElementsInPermutation(std::vector<size_t> & permutation, size_t target_size);

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
        : types(types_), old_types(old_types_), old_json(old_json_), field(field_)
    {
    }

    String getName() const override { return "IdentityFunctionStruct"; }

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
