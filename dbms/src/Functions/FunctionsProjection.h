#pragma once

#include <Functions/IFunction.h>
#include "FunctionsConversion.h"

namespace DB {

class FunctionOneOrZero final : public IFunction {
public:
    static constexpr auto name = "one_or_zero";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

class FunctionProject final : public IFunction {
public:
    static constexpr auto name = "__inner_project__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

class FunctionBuildProjectionComposition final : public IFunction {
public:
    static constexpr auto name = "__inner_build_projection_composition__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

class FunctionRestoreProjection final : public IFunction {
public:
    static constexpr auto name = "__inner_restore_projection__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

}
