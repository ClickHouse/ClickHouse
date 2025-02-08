#pragma once

#include <sstream>
#include <vector>
#include <memory>

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <Interpreters/ActionsDAG.h>

#include <Functions/IFunction.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/SharedPtr.h>
#include "Common/Exception.h"
#include "Common/typeid_cast.h"
#include "Columns/ColumnTuple.h"
#include "DataTypes/DataTypeTuple.h"
#include "DataTypes/IDataType.h"

#include <iostream>

namespace DB
{

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
    IcebergIdentityOperation()
        : IcebergChangeSchemaOperation(ChangeType::IDENTITY)
    {
    }
};

class ExecutableIdentityEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableIdentityEvolutionFunction(DataTypePtr type_) : type(type_)
    {
    }

    String getName() const override { return "ExecutableIdentityEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto& column = arguments[0].column;
        auto result_column = type->createColumn();
        std::cerr << "column type " << type->getName() << ' ' << input_rows_count << '\n';

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Tuple current_node;
            Field field;
            column->get(i, field);
            field.tryGet(current_node);
            std::cerr <<"add " << field.dump() << '\n';
            Tuple result_node = current_node;
            result_column->insert(result_node);
        }

        return result_column;
    }

private:
    DataTypePtr type;
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
        return std::make_unique<ExecutableIdentityEvolutionFunction>(types[0]);
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

class ExecutableAddingEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableAddingEvolutionFunction(const IcebergAddingOperation & operation_, const DataTypes & types_, const DataTypes & old_types_)
        : operation(operation_)
        , types(types_)
        , old_types(old_types_)
    {
    }

    String getName() const override { return "ExecutableAddingEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    IcebergAddingOperation operation;
    DataTypes types;
    DataTypes old_types;
};

class AddingFunctionStruct : public IFunctionBase
{
public:
    explicit AddingFunctionStruct(const IcebergAddingOperation & operation_, const DataTypes & types_, const DataTypes & old_types_)
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
        return std::make_unique<ExecutableAddingEvolutionFunction>(operation, types, old_types);
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
    IcebergAddingOperation operation;
    DataTypes types;
    DataTypes old_types;
};

class ExecutableDeletingEvolutionFunction : public IExecutableFunction
{
public:
    explicit ExecutableDeletingEvolutionFunction(const IcebergDeletingOperation & operation_, const DataTypes & types_, const DataTypes & old_types_) : operation(operation_), types(types_), old_types(old_types_) {}

    String getName() const override { return "ExecutableReorderingEvolutionFunction"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override;

private:
    IcebergDeletingOperation operation;
    DataTypes types;
    DataTypes old_types;
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
        return std::make_unique<ExecutableDeletingEvolutionFunction>(operation, types, old_types);
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

std::string DumpJSONIntoStr(Poco::SharedPtr<Poco::JSON::Object> obj);

}
