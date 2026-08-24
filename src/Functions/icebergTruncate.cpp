#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// This function specification https://iceberg.apache.org/spec/#truncate-transform-details
class FunctionIcebergTruncate : public IFunction
{

public:
    static inline const char * name = "icebergTruncate";

    explicit FunctionIcebergTruncate(ContextPtr)
    {
    }

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionIcebergTruncate>(context_);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// You may ask, why use global context and not the context provided
        /// in create/Constructor? Two reasons:
        /// 1. We need context only to access global functions factory, that is why global context is the most suitable
        /// 2. It's terribly unsafe to store ContextPtr inside function because function object is so low-level
        /// that it can be stored in multiple other objects which itself stored in global context.
        /// Very common example ContextPtr->Storage->KeyDescription->Expressions->Function->ContextPtr oops
        /// here we have a loop and memory leak.
        auto context = Context::getGlobalContextInstance();

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments: expected 2 arguments");

        const auto & truncate_number = arguments[0];
        if (!WhichDataType(truncate_number).isNativeUInt())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument should be UInt data type");

        const auto & truncate_type = arguments[1];
        WhichDataType which_truncate(truncate_type);
        if (!which_truncate.isDecimal64() && !which_truncate.isDecimal32() && !which_truncate.isStringOrFixedString() && !which_truncate.isNativeInteger())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument must be of native integer type, String/FixedString, Decimal");

        if (which_truncate.isStringOrFixedString())
        {
            return std::make_shared<DataTypeString>();
        }
        else
        {
            auto get_column_const = [] (const DataTypePtr data_type)
            {
                return ColumnWithTypeAndName(data_type->createColumnConst(1, data_type->getDefault()), data_type, "");
            };

            ColumnsWithTypeAndName modulo_arguments;
            if (which_truncate.isNativeInteger())
            {
                modulo_arguments = {get_column_const(arguments[1]), get_column_const(arguments[0])};
            }
            else
            {
                auto decimal_scaled = arguments[1]->createColumnConst(1, arguments[1]->getDefault());
                ColumnWithTypeAndName decimal_scaled_with_type(decimal_scaled, arguments[1], "");
                modulo_arguments = {get_column_const(arguments[1]), decimal_scaled_with_type};
            }

            auto modulo_func = FunctionFactory::instance().get("positiveModulo", context)->build(modulo_arguments);
            auto modulo_result_type = modulo_func->getResultType();
            auto minus_arguments = {get_column_const(arguments[1]), get_column_const(modulo_result_type)};
            auto minus_func = FunctionFactory::instance().get("minus", context)->build(minus_arguments);
            auto minus_result_type = minus_func->getResultType();

            return minus_result_type;
        }

    }

    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override { return { .is_monotonic = true, .is_always_monotonic = true }; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto value = (*arguments[0].column)[0].safeGet<Int64>();
        if (value <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function icebergTruncate accepts only positive width");

        auto context = Context::getGlobalContextInstance();
        WhichDataType which_truncate(arguments[1].type);
        if (which_truncate.isStringOrFixedString())
        {
            auto string_arguments = {arguments[1], arguments[0]};
            if (which_truncate.isFixedString())
            {
                auto substr_func = FunctionFactory::instance().get("left", context)->build(string_arguments);
                return substr_func->execute(string_arguments, std::make_shared<DataTypeString>(), input_rows_count, false);
            }
            else
            {
                auto substr_func = FunctionFactory::instance().get("leftUTF8", context)->build(string_arguments);
                return substr_func->execute(string_arguments, std::make_shared<DataTypeString>(), input_rows_count, false);
            }
        }
        else if (which_truncate.isNativeInteger() || which_truncate.isDecimal())
        {
            ColumnsWithTypeAndName modulo_arguments;
            if (which_truncate.isNativeInteger())
            {
                modulo_arguments = {arguments[1], arguments[0]};
            }
            else
            {
                ColumnPtr decimal_scaled;
                if (const auto * decimal_type = checkDecimal<Decimal32>(*arguments[1].type))
                    decimal_scaled = arguments[1].type->createColumnConst(input_rows_count, DecimalField<Decimal32>(value, decimal_type->getScale()));
                if (const auto * decimal_type = checkDecimal<Decimal64>(*arguments[1].type))
                    decimal_scaled = arguments[1].type->createColumnConst(input_rows_count, DecimalField<Decimal64>(value, decimal_type->getScale()));

                if (!decimal_scaled)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected decimal data type");

                ColumnWithTypeAndName decimal_scaled_with_type(decimal_scaled, arguments[1].type, "");
                modulo_arguments = {arguments[1], decimal_scaled_with_type};
            }

            auto modulo_func = FunctionFactory::instance().get("positiveModulo", context)->build(modulo_arguments);
            auto modulo_result_type = modulo_func->getResultType();
            auto modulo_result = modulo_func->execute(modulo_arguments, modulo_result_type, input_rows_count, false);
            ColumnWithTypeAndName modulo_result_with_type(modulo_result, modulo_result_type, "");
            auto minus_arguments = {arguments[1], modulo_result_with_type};
            auto minus_func = FunctionFactory::instance().get("minus", context)->build(minus_arguments);
            auto minus_result_type = minus_func->getResultType();
            return minus_func->execute(minus_arguments, minus_result_type, input_rows_count, false);
        }

        std::unreachable();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
};

REGISTER_FUNCTION(IcebergTruncate)
{
    FunctionDocumentation::Description description = R"(Implements logic of iceberg truncate transform: https://iceberg.apache.org/spec/#truncate-transform-details.)";
    FunctionDocumentation::Syntax syntax = "icebergTruncate(N, value)";
    FunctionDocumentation::Arguments arguments = {{"value", "The value to transform.", {"String", "(U)Int*", "Decimal"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The same type as the argument"};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT icebergTruncate(3, 'iceberg')", "ice"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;

    factory.registerFunction<FunctionIcebergTruncate>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}

}

}
