#pragma once

#include <DB/DataTypes/IDataType.h>

namespace DB
{

namespace Conditional
{

/// Determine the return type of the function multiIf when all the
/// branches (then, else) have numeric types.
DataTypePtr getReturnTypeForArithmeticArgs(const DataTypes & args);
/// Returns true if all the branches (then, else) have numeric types.
bool hasArithmeticBranches(const DataTypes & args);
/// Returns true if all the branches (then, else) are arrays.
bool hasArrayBranches(const DataTypes & args);
/// Returns true if all the branches (then, else) have the same type name.
bool hasIdenticalTypes(const DataTypes & args);
/// Returns true if all the branches (then, else) are fixed strings.
bool hasFixedStrings(const DataTypes & args);
/// Returns true if all the branches (then, else) are fixed strings of equal length.
bool hasFixedStringsOfIdenticalLength(const DataTypes & args);
/// Returns true if all the branches (then, else) are strings.
bool hasStrings(const DataTypes & args);

}

}
