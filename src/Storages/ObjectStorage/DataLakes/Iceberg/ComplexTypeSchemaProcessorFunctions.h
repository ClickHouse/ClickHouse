#pragma once

#include <vector>
#include <memory>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>

#include <Functions/IFunction.h>
#include "Common/Exception.h"
#include "Common/typeid_cast.h"
#include "Columns/ColumnTuple.h"
#include "DataTypes/DataTypeTuple.h"
#include "DataTypes/IDataType.h"

namespace DB
{

enum class ChangeType : uint8_t 
{
    REORDERING,
    DELETING,

    /// Type below is used for casts and renamings
    IDENTITY,
};

class IcebergChangeSchemaOperation
{
public:
    explicit IcebergChangeSchemaOperation(ChangeType change_type_)
    : change_type(change_type_)
    {
    }

    ChangeType getType() const
    {
        return change_type;
    }

protected:
    ChangeType change_type;
};

class IcebergReorderingOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergReorderingOperation(const std::vector<std::string> & root_,
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

class IcebergIdentityOperation : public IcebergChangeSchemaOperation
{
public:
    IcebergIdentityOperation()
        : IcebergChangeSchemaOperation(ChangeType::IDENTITY)
    {
    }
};

class ExecutableIdentityEvolutionFunction : public IExecutableFunction
{
public:
    String getName() const override { return "ExecutableIdentityEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const auto& column = arguments[0].column;
        return column;
    }

private:
    Field time_value;
};

class IdentityFunctionStruct : public IFunctionBase
{
public:
    explicit IdentityFunctionStruct(const IcebergIdentityOperation & operation_, const DataTypes & types_, const DataTypes & old_types_)
        : operation(operation_)
        , types(types_)
        , old_types(old_types_)
    {
    }

    String getName() const override { return "IdentityFunctionStruct"; }

    const DataTypes & getArgumentTypes() const override
    {
        return types;
    }

    const DataTypePtr & getResultType() const override
    {
        return types[0];
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableIdentityEvolutionFunction>();
    }

    bool isDeterministic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

private:
    IcebergIdentityOperation operation;
    DataTypes types;
    DataTypes old_types;
};

class ExecutableReorderingEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableReorderingEvolutionFunction(const IcebergReorderingOperation & operation_, const DataTypes & types_) : operation(operation_), types(types_) {}

    String getName() const override { return "ExecutableReorderingEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    IcebergReorderingOperation operation;
    DataTypes types;
};

class ReorderingFunctionStruct : public IFunctionBase
{
public:
    explicit ReorderingFunctionStruct(const IcebergReorderingOperation & operation_, const DataTypes & types_, const DataTypes & old_types_)
        : operation(operation_)
        , types(types_)
        , old_types(old_types_)
    {
    }

    String getName() const override { return "ReorderingFunctionStruct"; }

    const DataTypes & getArgumentTypes() const override
    {
        return old_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return types[0];
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableReorderingEvolutionFunction>(operation, types);
    }

    bool isDeterministic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

private:
    IcebergReorderingOperation operation;
    DataTypes types;
    DataTypes old_types;
};

class ExecutableDeletingEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableDeletingEvolutionFunction(const IcebergDeletingOperation & operation_, const DataTypes & types_) : operation(operation_), types(types_) {}

    String getName() const override { return "ExecutableReorderingEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    IcebergDeletingOperation operation;
    DataTypes types;
};

class DeleteFunctionStruct : public IFunctionBase
{
public:
    explicit DeleteFunctionStruct(const IcebergDeletingOperation & operation_, const DataTypes & types_, const DataTypes & old_types_)
        : operation(operation_)
        , types(types_)
        , old_types(old_types_)
    {
    }

    String getName() const override { return "ReorderingFunctionStruct"; }

    const DataTypes & getArgumentTypes() const override
    {
        return old_types;
    }

    const DataTypePtr & getResultType() const override
    {
        return types[0];
    }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableDeletingEvolutionFunction>(operation, types);
    }

    bool isDeterministic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

private:
    IcebergDeletingOperation operation;
    DataTypes types;
    DataTypes old_types;
};


}
