#pragma once

#include <Functions/IFunction.h>
#include "FunctionsConversion.h"

namespace DB
{
/*
 * This function accepts one column and converts it to UInt8, replacing values, which evaluate to true, with 1, and values,
 * which evaluate to false with 0
 */
class FunctionOneOrZero final : public IFunction
{
public:
    static constexpr auto name = "one_or_zero";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

/*
 * FunctionProject accepts two columns: data column and projection column.
 * Projection column is a column of UInt8 values 0 and 1, which indicate the binary mask of rows, where to project.
 * This function builds a column of a smaller, which contains values of the data column at the positions where
 * the projection column contained 1. The size of result column equals the count of ones in the projection column.
 */
class FunctionProject final : public IFunction
{
public:
    static constexpr auto name = "__inner_project__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

/*
 * FunctionBuildProjectionComposition constructs the composition of two projection columns. The size of
 * second projection column should equal the count of ones in the first input projection column.
 */
class FunctionBuildProjectionComposition final : public IFunction
{
public:
    static constexpr auto name = "__inner_build_projection_composition__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

/*
 * Accepts mapping column with values from range [0, N) and N more columns as arguments.
 * Forms a column by taking value from column, which number is in the mapping column.
 */
class FunctionRestoreProjection final : public IFunction
{
public:
    static constexpr auto name = "__inner_restore_projection__";
    static FunctionPtr create(const Context &);
    String getName() const override;
    bool isVariadic() const override;
    size_t getNumberOfArguments() const override;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};

}
