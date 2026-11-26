#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeCustom.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

/** The type SimpleAggregateFunction(fct, type) is meant to be used in an AggregatingMergeTree. It behaves like a standard
 * data type but when rows are merged, an aggregation function is applied.
 *
 * The aggregation function is limited to simple functions whose merge state is the final result:
 * any, anyLast, min, max, sum
 *
 * Examples:
 *
 * SimpleAggregateFunction(sum, Nullable(Float64))
 * SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String)))
 * SimpleAggregateFunction(anyLast, IPv4)
 *
 * Technically, a standard IDataType is instantiated and customized with IDataTypeCustomName and DataTypeCustomDesc.
  */

class DataTypeCustomSimpleAggregateFunction : public IDataTypeCustomName
{
private:
    const AggregateFunctionPtr function;
    const DataTypes argument_types;
    const Array parameters;

public:
    DataTypeCustomSimpleAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
            : function(function_), argument_types(argument_types_), parameters(parameters_) {}

    [[nodiscard]] AggregateFunctionPtr getFunction() const { return function; }
    [[nodiscard]] String getFunctionName() const;
    [[nodiscard]] const DataTypes & getArgumentsDataTypes() const { return argument_types; }
    [[nodiscard]] const Array & getParameters() const { return parameters; }
    [[nodiscard]] String getName() const override;
    [[nodiscard]] DataTypeCustomDescPtr rederiveFor(const DataTypePtr & rebuilt, const RewriteNestedFn & rewrite_nested) const override;
    static void checkSupportedFunctions(const AggregateFunctionPtr & function);
};

DataTypePtr createSimpleAggregateFunctionType(const AggregateFunctionPtr & function, const DataTypes & argument_types, const Array & parameters);

/// `SimpleAggregateFunction(f, T)` -> `T`, nested layers too, with the same columns and text serialization; any other type
/// is returned unchanged.
DataTypePtr getSimpleAggregateFunctionStorageType(const DataTypePtr & type);

/// `SimpleAggregateFunction(f, T)` -> `T`, nested layers too, but `UInt8` instead of `Bool` unless `isSimpleAggregateFunctionOfBool`
/// holds for the outermost layer or `from_type`, the type converted to `type`, is text: such a type converts numbers like
/// `UInt8` but parses text like `Bool`. Any other type is returned unchanged.
DataTypePtr removeSimpleAggregateFunction(const DataTypePtr & type, const DataTypePtr & from_type = nullptr);

/// True for `SimpleAggregateFunction(f, Bool)` whose `f` returns `Bool`; such a type keeps `Bool` values under its own name.
bool isSimpleAggregateFunctionOfBool(const IDataType & type);

/// The type of a stored definition (table metadata, `columns.txt` of a part, ZooKeeper, a kept mutation, a `Native` header).
/// `Nullable(SimpleAggregateFunction(f, T))`, which older versions allowed, is read as `SimpleAggregateFunction(f, Nullable(T))`,
/// with the same columns, and likewise for `LowCardinality`.
DataTypePtr getStoredDataType(const ASTPtr & ast);
DataTypePtr getStoredDataType(const String & name);
/// The AST of a stored type as `getStoredDataType` reads it: `ast` itself, or a rewritten copy.
ASTPtr getStoredDataTypeAST(const ASTPtr & ast);

}
