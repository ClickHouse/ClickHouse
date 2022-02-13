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
        extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
        extern const int CANNOT_PARSE_DATE;
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
                 throw Exception("Illegal column " + col_from->getName()
                                 + " of first argument of function " + Name::name,
                                 ErrorCodes::ILLEGAL_COLUMN);
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
                const size_t string_size = offsets ? next_offset - current_offset - 1 : fixed_string_size;
                ReadBufferFromMemory read_buffer(&(*chars)[current_offset], string_size);
                current_offset = next_offset;

                if constexpr (nullOnErrors)
                {
                    try
                    {
                        const GregorianDate<> date(read_buffer);
                        vec_to[i] = date.toModifiedJulianDay<typename ToDataType::FieldType>();
                        vec_null_map_to[i] = false;
                    }
                    catch (const Exception & e)
                    {
                        if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED || e.code() == ErrorCodes::CANNOT_PARSE_DATE)
                        {
                            vec_to[i] = static_cast<Int32>(0);
                            vec_null_map_to[i] = true;
                        }
                        else
                            throw;
                    }
                }
                else
                {
                    const GregorianDate<> date(read_buffer);
                    vec_to[i] = date.toModifiedJulianDay<typename ToDataType::FieldType>();
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
            return { .is_monotonic = true, .is_always_monotonic = true };
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
                throw Exception(
                    "The argument of function " + getName() + " must be String or FixedString", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

    void registerFunctionToModifiedJulianDay(FunctionFactory & factory)
    {
        factory.registerFunction<ToModifiedJulianDayOverloadResolver<NameToModifiedJulianDay, DataTypeInt32, false>>();
        factory.registerFunction<ToModifiedJulianDayOverloadResolver<NameToModifiedJulianDayOrNull, DataTypeInt32, true>>();
    }
}
