#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GregorianDate.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }

    template <typename Name, typename ToDataType, bool nullOnErrors>
    class ExecutableFunctionToModifiedJulianDay : public IExecutableFunction
    {
    public:
        String getName() const override
        {
            return Name::name;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const IColumn * col_from = arguments[0].column.get();
            const ColumnString * col_from_string = checkAndGetColumn<ColumnString>(col_from);
            const ColumnFixedString * col_from_fixed_string = checkAndGetColumn<ColumnFixedString>(col_from);

            const ColumnString::Chars * chars = nullptr;
            const IColumn::Offsets * offsets = nullptr;
            size_t fixed_string_size = 0;

            if (col_from_string)
            {
                chars = &col_from_string->getChars();
                offsets = &col_from_string->getOffsets();
            }
            else if (col_from_fixed_string)
            {
                chars = &col_from_fixed_string->getChars();
                fixed_string_size = col_from_fixed_string->getN();
            }
            else
            {
                 throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                                 col_from->getName(), Name::name);
            }

            using ColVecTo = typename ToDataType::ColumnType;
            typename ColVecTo::MutablePtr col_to = ColVecTo::create(input_rows_count);
            typename ColVecTo::Container & vec_to = col_to->getData();

            ColumnUInt8::MutablePtr col_null_map_to;
            UInt8 * vec_null_map_to [[maybe_unused]] = nullptr;
            if constexpr (nullOnErrors)
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count);
                vec_null_map_to = col_null_map_to->getData().data();
            }

            size_t current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const size_t next_offset = offsets ? (*offsets)[i] : current_offset + fixed_string_size;
                const size_t string_size = offsets ? next_offset - current_offset : fixed_string_size;
                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);
                current_offset = next_offset;

                if constexpr (nullOnErrors)
                {
                    GregorianDate date;

                    int64_t res = 0;
                    bool success = date.tryInit(read_buffer) && date.tryToModifiedJulianDay(res);

                    vec_to[i] = static_cast<typename ToDataType::FieldType>(res);
                    vec_null_map_to[i] = !success;
                }
                else
                {
                    const GregorianDate date(read_buffer);
                    vec_to[i] = static_cast<typename ToDataType::FieldType>(date.toModifiedJulianDay());
                }
            }

            if constexpr (nullOnErrors)
                return ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                return col_to;
        }

        bool useDefaultImplementationForConstants() const override
        {
            return true;
        }
    };

    template <typename Name, typename ToDataType, bool nullOnErrors>
    class FunctionBaseToModifiedJulianDay : public IFunctionBase
    {
    public:
        explicit FunctionBaseToModifiedJulianDay(DataTypes argument_types_, DataTypePtr return_type_)
            : argument_types(std::move(argument_types_))
            , return_type(std::move(return_type_)) {}

        String getName() const override
        {
            return Name::name;
        }

        const DataTypes & getArgumentTypes() const override
        {
            return argument_types;
        }

        const DataTypePtr & getResultType() const override
        {
            return return_type;
        }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
        {
            return std::make_unique<ExecutableFunctionToModifiedJulianDay<Name, ToDataType, nullOnErrors>>();
        }

        bool isInjective(const ColumnsWithTypeAndName &) const override
        {
            return true;
        }

        bool hasInformationAboutMonotonicity() const override
        {
            return true;
        }

        Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
        {
            return { .is_monotonic = true, .is_always_monotonic = true, .is_strict = true };
        }

    private:
        DataTypes argument_types;
        DataTypePtr return_type;
    };

    template <typename Name, typename ToDataType, bool nullOnErrors>
    class ToModifiedJulianDayOverloadResolver : public IFunctionOverloadResolver
    {
    public:
        static constexpr auto name = Name::name;

        static FunctionOverloadResolverPtr create(ContextPtr)
        {
            return std::make_unique<ToModifiedJulianDayOverloadResolver<Name, ToDataType, nullOnErrors>>();
        }

        String getName() const override
        {
            return Name::name;
        }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            DataTypes argument_types = { arguments[0].type };

            return std::make_unique<FunctionBaseToModifiedJulianDay<Name, ToDataType, nullOnErrors>>(argument_types, return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isStringOrFixedString(arguments[0]))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must be String or FixedString",
                    getName());
            }

            DataTypePtr base_type = std::make_shared<ToDataType>();
            if constexpr (nullOnErrors)
            {
                return std::make_shared<DataTypeNullable>(base_type);
            }
            else
            {
                return base_type;
            }
        }

        size_t getNumberOfArguments() const override
        {
            return 1;
        }

        bool isInjective(const ColumnsWithTypeAndName &) const override
        {
            return true;
        }
    };

    struct NameToModifiedJulianDay
    {
        static constexpr auto name = "toModifiedJulianDay";
    };

    struct NameToModifiedJulianDayOrNull
    {
        static constexpr auto name = "toModifiedJulianDayOrNull";
    };

    REGISTER_FUNCTION(ToModifiedJulianDay)
    {
        FunctionDocumentation::Description description_toModifiedJulianDay = R"(
Converts a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD` to a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number in `Int32`. This function supports date from `0000-01-01` to `9999-12-31`. It raises an exception if the argument cannot be parsed as a date, or the date is invalid.
    )";
        FunctionDocumentation::Syntax syntax_toModifiedJulianDay = R"(
toModifiedJulianDay(date)
    )";
        FunctionDocumentation::Arguments arguments_toModifiedJulianDay =
        {
            {"date", "The date in String form.", {"String", "FixedString"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_toModifiedJulianDay = {"Returns Modified Julian Day number.", {"Int32"}};
        FunctionDocumentation::Examples examples_toModifiedJulianDay =
        {
            {"Convert date to Modified Julian Day", R"(
SELECT toModifiedJulianDay('2020-01-01')
        )",
            R"(
┌─toModifiedJulianDay('2020-01-01')─┐
│                             58849 │
└───────────────────────────────────┘
        )"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_toModifiedJulianDay = {21, 1};
        FunctionDocumentation::Category category_toModifiedJulianDay = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation documentation_toModifiedJulianDay = {description_toModifiedJulianDay, syntax_toModifiedJulianDay, arguments_toModifiedJulianDay, returned_value_toModifiedJulianDay, examples_toModifiedJulianDay, introduced_in_toModifiedJulianDay, category_toModifiedJulianDay};

        factory.registerFunction<ToModifiedJulianDayOverloadResolver<NameToModifiedJulianDay, DataTypeInt32, false>>(documentation_toModifiedJulianDay);

        FunctionDocumentation::Description description_toModifiedJulianDayOrNull = R"(
Similar to [`toModifiedJulianDay()`](#toModifiedJulianDay), but instead of raising exceptions it returns `NULL`.
    )";
        FunctionDocumentation::Syntax syntax_toModifiedJulianDayOrNull = R"(
toModifiedJulianDayOrNull(date)
    )";
        FunctionDocumentation::Arguments arguments_toModifiedJulianDayOrNull =
        {
            {"date", "Date in text form.", {"String", "FixedString"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_toModifiedJulianDayOrNull = {"Returns the modified Julian day number for valid `date`, otherwise `null`.", {"Nullable(Int32)"}};
        FunctionDocumentation::Examples examples_toModifiedJulianDayOrNull =
        {
            {"Convert date to Modified Julian Day with null handling", R"(
SELECT toModifiedJulianDayOrNull('2020-01-01');
SELECT toModifiedJulianDayOrNull('0000-00-00'); -- invalid date, returns NULL
        )",
            R"(
┌─toModifiedJu⋯020-01-01')─┐
│                    58849 │
└──────────────────────────┘
┌─toModifiedJu⋯000-00-00')─┐
│                     ᴺᵁᴸᴸ │
└──────────────────────────┘
        )"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_toModifiedJulianDayOrNull = {21, 1};
        FunctionDocumentation::Category category_toModifiedJulianDayOrNull = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation documentation_toModifiedJulianDayOrNull =
        {description_toModifiedJulianDayOrNull, syntax_toModifiedJulianDayOrNull, arguments_toModifiedJulianDayOrNull, returned_value_toModifiedJulianDayOrNull, examples_toModifiedJulianDayOrNull, introduced_in_toModifiedJulianDayOrNull, category_toModifiedJulianDayOrNull};

        factory.registerFunction<ToModifiedJulianDayOverloadResolver<NameToModifiedJulianDayOrNull, DataTypeInt32, true>>(documentation_toModifiedJulianDayOrNull);
    }
}
