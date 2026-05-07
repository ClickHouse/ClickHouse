#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr std::array<const char *, 2> names = {"generate_series", "generateSeries"};

struct StepWithSign
{
    UInt64 abs_value;
    bool negative;
};

template <size_t alias_num>
class TableFunctionGenerateSeries : public ITableFunction
{
public:
    static_assert(alias_num < names.size());
    static constexpr auto name = names[alias_num];
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageEngineName() const override
    {
        /// No underlying storage engine
        return "";
    }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;
    StepWithSign parseStep(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
};

template <size_t alias_num>
ColumnsDescription TableFunctionGenerateSeries<alias_num>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    /// NOTE: https://bugs.llvm.org/show_bug.cgi?id=47418
    return ColumnsDescription{{{"generate_series", std::make_shared<DataTypeUInt64>()}}};
}

template <size_t alias_num>
StoragePtr TableFunctionGenerateSeries<alias_num>::executeImpl(
    const ASTPtr & ast_function,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool /*is_insert_query*/) const
{
    if (const auto * function = ast_function->as<ASTFunction>())
    {
        auto arguments = function->arguments->children;

        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'length' or 'offset, length'.", getName());

        UInt64 start = evaluateArgument(context, arguments[0]);
        UInt64 stop = evaluateArgument(context, arguments[1]);
        StepWithSign step = (arguments.size() == 3) ? parseStep(context, arguments[2]) : StepWithSign{1, false};

        StorageID storage_id(getDatabaseName(), table_name);

        /// Check whether the series direction matches the step sign.
        /// Otherwise, nothing to generate.
        bool empty = step.negative ? (start < stop) : (start > stop);

        /// Domain window size: the number of consecutive integers between start and stop (inclusive).
        /// Stored as UInt128 so that `generate_series(0, UInt64_MAX)` (domain size = 2^64) is representable.
        UInt128 domain_size = empty ? UInt128(0) : (step.negative ? UInt128(start - stop) + 1 : UInt128(stop - start) + 1);

        /// Guard against generating more values than can be represented in UInt64.
        if (!empty)
        {
            UInt128 count = (domain_size + step.abs_value - 1) / step.abs_value;
            if (count > std::numeric_limits<UInt64>::max())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Table function '{}' would generate more values than can be represented in UInt64",
                    getName());
        }

        auto res = std::make_shared<StorageSystemNumbers>(
            storage_id, false, std::string{"generate_series"}, domain_size, start, step.abs_value, step.negative);
        res->startup();
        return res;
    }
    throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'limit' or 'offset, limit'.", getName());
}

template <size_t alias_num>
UInt64 TableFunctionGenerateSeries<alias_num>::evaluateArgument(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The value {} is not representable as UInt64",
            applyVisitor(FieldVisitorToString(), field));

    return converted.safeGet<UInt64>();
}

/// Parse the step argument, detecting whether it's negative.
/// Try Int64 first: if it succeeds and the value is negative, we have a descending series.
/// If the value doesn't fit into Int64 (e.g. a large UInt64), fall back to UInt64.
template <size_t alias_num>
StepWithSign TableFunctionGenerateSeries<alias_num>::parseStep(ContextPtr context, ASTPtr & argument) const
{
    const auto & [field, type] = evaluateConstantExpression(argument, context);

    if (!isNativeNumber(type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} expression, must be numeric type", type->getName());

    /// Try converting to Int64 first to detect negative values.
    Field as_signed = convertFieldToType(field, DataTypeInt64());
    if (!as_signed.isNull())
    {
        Int64 step_val = as_signed.safeGet<Int64>();
        if (step_val == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires step to be a non-zero number", getName());

        if (step_val < 0)
        {
            /// Avoid signed overflow during conversion (e.g. -INT64_MIN overflows Int64).
            /// Instead, cast to UInt64 first (preserving the bit pattern), then negate in
            /// unsigned arithmetic where overflow is well-defined.
            return {UInt64(0) - static_cast<UInt64>(step_val), true};
        }
        return {static_cast<UInt64>(step_val), false};
    }

    /// Value too large for Int64 — must be a large positive UInt64.
    Field as_unsigned = convertFieldToType(field, DataTypeUInt64());
    if (as_unsigned.isNull())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The value {} is not representable as UInt64",
            applyVisitor(FieldVisitorToString(), field));

    UInt64 abs_step = as_unsigned.safeGet<UInt64>();
    if (abs_step == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires step to be a non-zero number", getName());

    return {abs_step, false};
}

}

void registerTableFunctionGenerateSeries(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateSeries<0>>({}, {.allow_readonly = true});
    factory.registerFunction<TableFunctionGenerateSeries<1>>({}, {.allow_readonly = true});
}

}
