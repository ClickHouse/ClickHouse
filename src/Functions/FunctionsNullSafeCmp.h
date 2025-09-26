#pragma once
#include <cstdint>
#include <Functions/IFunction.h>
#include <Functions/FunctionsComparison.h>
#include <Columns/ColumnNullable.h>
#include <Common/quoteString.h>
namespace DB
{

enum class NullSafeCmpMode : uint8_t
{
    NullSafeEqual,
    NullSafeNotEqual
};

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <
    typename Name,                                              // Function Name
    NullSafeCmpMode cmp_mode,                                   // Null-safe mode (Equal or NotEqual)
    template <typename, typename > class CompareOp,             // EqualsOp / NotEqualsOp
    typename CompareName>                                       // NameEquals / NameNotEquals
class FunctionsNullSafeCmp : public IFunction
{
private:
    const ComparisonParams params;
public:
    explicit FunctionsNullSafeCmp(ComparisonParams params_) : params(std::move(params_)) {}

    static constexpr auto name = Name::name;
    static constexpr bool is_equal_mode = (cmp_mode == NullSafeCmpMode::NullSafeEqual);


    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionsNullSafeCmp>(context ? ComparisonParams(context) : ComparisonParams());
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} expects exactly 2 arguments, got {}",
                            backQuote(name),
                            arguments.size());

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr left_col = arguments[0].column;
        ColumnPtr right_col = arguments[1].column;
        if (!left_col || !right_col)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} received null column: left_col={} right_col={}. "
                            "Please check the input columns.",
                            backQuote(name),
                            left_col ? "NOT NULL" : "NULL",
                            right_col ? "NOT NULL" : "NULL");
        }

        // get common type for null-safe comparsion
        DataTypePtr common_type = getLeastSupertype(DataTypes{arguments[0].type, arguments[1].type});

        ColumnPtr c0_converted = castColumn(arguments[0], common_type);
        ColumnPtr c1_converted = castColumn(arguments[1], common_type);

        if (c0_converted->isNullable() && c1_converted->isNullable())
        {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container & vec_res = c_res->getData();
            vec_res.resize(arguments[0].column->size());
            c0_converted = c0_converted->convertToFullColumnIfConst();
            c1_converted = c1_converted->convertToFullColumnIfConst();

            for (size_t i = 0; i < input_rows_count ; i++)
                vec_res[i] = c0_converted->compareAt(i, i, *c1_converted, 1) == 0 ? is_equal_mode : !is_equal_mode;

            return c_res;
        }

        // address normal
        ColumnPtr res;

        FunctionOverloadResolverPtr comparator
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionComparison<CompareOp, CompareName>>(params));

        auto executable_func = comparator->build(arguments);
        auto data_type = executable_func->getResultType();
        res = executable_func->execute(arguments, data_type, input_rows_count, /* dry_run = */ false);

        return res;
    }
};
}
