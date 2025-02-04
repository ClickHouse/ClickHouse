#include <cstddef>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>
#include "Common/FunctionDocumentation.h"
#include "Columns/IColumn.h"
#include "Functions/IFunction.h"
#include "Interpreters/castColumn.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct TrimModeLeft
{
    static constexpr auto name = "trimLeft";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "trimRight";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "trimBoth";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};

template <typename Mode>
class FunctionTrim : public IFunction
{
public:
    // TODO(manish): find out if this function is suitable for short-circuit execution
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    static constexpr auto name = Mode::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTrim<Mode>>(); }

    String getName() const override { return name; }

    // trim() is variadic, but we only support 1 or 2 arguments
    bool isVariadic() const override { return true; }

    // 0 => function is variadic
    size_t getNumberOfArguments() const override { return 0; } // 1 or 2 arguments are supported

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 1 || arguments.size() > 2)
        {
            throw Exception{ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Invalid number of arguments for function {}: passed {}, required 1 or 2",
                name,
                arguments.size()};
        }

        if (!isString(arguments[0].type))
        {
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[0].type->getName(),
                getName()};
        }

        if (arguments.size() == 2 && !isString(arguments[1].type))
        {
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[1].type->getName(),
                getName()};
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr& result_type, size_t) const override
    {
        size_t arg_size = arguments.size();
        Columns converted_columns(arg_size);
        for (size_t arg = 0; arg < arg_size; ++arg)
        {
            converted_columns[arg] = castColumn(arguments[arg], result_type)->convertToFullColumnIfConst();
        }

        const ColumnString* str_column = checkAndGetColumn<ColumnString>(converted_columns[0].get());
        const ColumnString* chars_column = (arg_size == 2) ? checkAndGetColumn<ColumnString>(converted_columns[1].get()) : nullptr;

        if (!str_column)
        {
            throw Exception{ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName()};
        }

        auto result_column = ColumnString::create();

        for (size_t i = 0; i < str_column->size(); ++i) {
            StringRef str = str_column->getDataAt(i);
            size_t start = 0;
            size_t end = str.size;

            // Trim hardcoded whitespace char (same as 1-argument trim())
            if (!chars_column) {
                // Compile-time check for whitespace
                if constexpr (Mode::trim_left) {
                    start = find_first_not_symbols<' '>(str.data, str.data + str.size) - str.data;
                }
                if constexpr (Mode::trim_right) {
                    const char * right_pos = find_last_not_symbols_or_null<' '>(str.data, str.data + str.size);
                    end = right_pos ? (right_pos - str.data + 1) : 0;
                }
            }
            // Custom trim characters (dynamic trim with the second argument)
            else
            {
                StringRef chars = chars_column->getDataAt(i);
                SearchSymbols trim_chars(std::string(chars.data, chars.size));

                // Trim left at runtime
                if constexpr (Mode::trim_left) {
                    start = find_first_not_symbols(str.data, str.data + str.size, trim_chars) - str.data;
                }

                // Trim right at runtime
                if constexpr (Mode::trim_right) {
                    const char * right_pos = find_last_not_symbols_or_null(str.data, str.data + str.size, trim_chars);
                    end = right_pos ? (right_pos - str.data + 1) : 0;
                }
            }
            start = std::min(start, end);
            result_column->insertData(str.data + start, end - start);
        }
        return result_column;
    }
};

using FunctionTrimLeft = FunctionTrim<TrimModeLeft>;
using FunctionTrimRight = FunctionTrim<TrimModeRight>;
using FunctionTrimBoth = FunctionTrim<TrimModeBoth>;

}

REGISTER_FUNCTION(Trim)
{
    factory.registerFunction<FunctionTrimLeft>(
        FunctionDocumentation{.description="..."},
        FunctionFactory::Case::Insensitive
    );
    factory.registerFunction<FunctionTrimRight>(
        FunctionDocumentation{.description="..."},
        FunctionFactory::Case::Insensitive
    );
    factory.registerFunction<FunctionTrimBoth>(
        FunctionDocumentation{.description="..."},
        FunctionFactory::Case::Insensitive
    );
    factory.registerAlias("ltrim", FunctionTrimLeft::name);
    factory.registerAlias("rtrim", FunctionTrimRight::name);
    factory.registerAlias("trim", FunctionTrimBoth::name);
}
}
