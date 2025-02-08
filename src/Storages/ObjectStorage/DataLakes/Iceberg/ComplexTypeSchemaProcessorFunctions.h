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

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

enum class ChangeType : uint8_t
{
    REORDERING,
    DELETING,
    ADDING,

    /// Type below is used for casts and renamings
    IDENTITY,
};

class IcebergChangeSchemaOperation
{
public:
    explicit IcebergChangeSchemaOperation(ChangeType change_type_) : change_type(change_type_) { }

    ChangeType getType() const { return change_type; }

protected:
    ChangeType change_type;
};

class IcebergReorderingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergReorderingOperation(
        const std::vector<std::string> & root_,
        const std::vector<std::string> & initial_order_,
        const std::vector<std::string> & final_order_)
        : IcebergChangeSchemaOperation(ChangeType::REORDERING), root(root_), initial_order(initial_order_), final_order(final_order_)
    {
    }

    std::vector<std::string> root;
    std::vector<std::string> initial_order;
    std::vector<std::string> final_order;
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

class IcebergIdentityOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergIdentityOperation() : IcebergChangeSchemaOperation(ChangeType::IDENTITY) { }
};

struct IIcebergSchemaTransform
{
    virtual void transform(Tuple & initial_node) = 0;

    virtual ~IIcebergSchemaTransform() = default;
};

class ExecutableIdentityEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableIdentityEvolutionFunction(
        DataTypePtr type_, DataTypePtr old_type_, Poco::SharedPtr<Poco::JSON::Object> old_json_, Poco::SharedPtr<Poco::JSON::Object> field_)
        : type(type_), old_type(old_type_), old_json(old_json_), field(field_)
    {
    }

    String getName() const override { return "ExecutableIdentityEvolutionFunction"; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    void lazyInitialize() const;

    DataTypePtr type;
    DataTypePtr old_type;
    Poco::SharedPtr<Poco::JSON::Object> old_json;
    Poco::SharedPtr<Poco::JSON::Object> field;

    mutable std::vector<std::shared_ptr<IIcebergSchemaTransform>> transforms;
    mutable bool initialized = false;
};

class IdentityFunctionStruct : public IFunctionBase
{
public:
    explicit IdentityFunctionStruct(
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
        return std::make_unique<ExecutableIdentityEvolutionFunction>(types[0], old_types[0], old_json, field);
    }

    bool isDeterministic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

private:
    IcebergIdentityOperation operation;
    DataTypes types;
    DataTypes old_types;
    Poco::SharedPtr<Poco::JSON::Object> old_json;
    Poco::SharedPtr<Poco::JSON::Object> field;
};

std::string DumpJSONIntoStr(Poco::SharedPtr<Poco::JSON::Object> obj);

}
