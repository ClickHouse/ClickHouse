#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/IDataType.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class Block;
class Context;

/// Implements the function isNull which returns true if a value
/// is null, false otherwise.
class FunctionIsNull : public IFunction
{
public:
    static constexpr auto name = "isNull";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction
{
public:
    static constexpr auto name = "isNotNull";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

/// Implements the function coalesce which takes a set of arguments and
/// returns the value of the leftmost non-null argument. If no such value is
/// found, coalesce() returns NULL.
class FunctionCoalesce : public IFunction
{
public:
    static constexpr auto name = "coalesce";
    static FunctionPtr create(const Context & context);
    FunctionCoalesce(const Context & context) : context(context) {}

    std::string getName() const override;
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;

private:
    const Context & context;
};

/// Implements the function ifNull which takes 2 arguments and returns
/// the value of the 1st argument if it is not null. Otherwise it returns
/// the value of the 2nd argument.
class FunctionIfNull : public IFunction
{
public:
    static constexpr auto name = "ifNull";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

/// Implements the function nullIf which takes 2 arguments and returns
/// NULL if both arguments have the same value. Otherwise it returns the
/// value of the first argument.
class FunctionNullIf : public IFunction
{
public:
    static constexpr auto name = "nullIf";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

/// Implements the function assumeNotNull which takes 1 argument and works as follows:
/// - if the argument is a nullable column, return its embedded column;
/// - otherwise return the original argument.
/// NOTE: assumeNotNull may not be called with the NULL value.
class FunctionAssumeNotNull : public IFunction
{
public:
    static constexpr auto name = "assumeNotNull";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

/// If value is not Nullable or NULL, wraps it to Nullable.
class FunctionToNullable : public IFunction
{
public:
    static constexpr auto name = "toNullable";
    static FunctionPtr create(const Context & context);

    std::string getName() const override;
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

}
