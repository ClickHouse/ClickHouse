#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

namespace DB
{
	namespace ErrorCodes
	{
		extern const int ILLEGAL_COLUMN;
		extern const int ILLEGAL_TYPE_OF_ARGUMENT;
	}

	struct RepeatImpl
	{
        static void vectorNonConstStr(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            const UInt64 repeatTime)
        {
            UInt64 data_size = 0;
            res_offsets.assign(offsets);
            for (UInt64 i = 0; i < offsets.size(); ++i)
			{
                data_size += (offsets[i] - offsets[i - 1] - 1) * repeatTime + 1;
                res_offsets[i] = data_size;
            }
            res_data.resize(data_size);
            for (UInt64 i = 0; i < res_offsets.size(); ++i)
            {
                array(data.data() + offsets[i - 1], res_data.data() + res_offsets[i - 1], offsets[i] - offsets[i - 1], repeatTime);
            }
        }

        static void
        vectorConst(const String & copy_str, const UInt64 repeatTime, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
        {
            UInt64 data_size = copy_str.size() * repeatTime + 1;
            res_data.resize(data_size);
            res_offsets.resize_fill(1, data_size);
            array((UInt8 *)copy_str.data(), res_data.data(), copy_str.size() + 1, repeatTime);
        }

        template <typename T>
        static void vectorNonConst(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            const PaddedPODArray<T> & col_num)
        {
            UInt64 data_size = 0;
            res_offsets.assign(offsets);
            for (UInt64 i = 0; i < col_num.size(); ++i)
			{
                data_size += (offsets[i] - offsets[i - 1] - 1) * col_num[i] + 1;
                res_offsets[i] = data_size;
            }
            res_data.resize(data_size);
            for (UInt64 i = 0; i < col_num.size(); ++i)
			{
                array(data.data() + offsets[i - 1], res_data.data() + res_offsets[i - 1], offsets[i] - offsets[i - 1], col_num[i]);
            }
        }


        template <typename T>
        static void vectorNonConstInteger(
            const String & copy_str, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, const PaddedPODArray<T> & col_num)
        {
            UInt64 data_size = 0;
            res_offsets.resize(col_num.size());
            UInt64 str_size = copy_str.size();
            for (UInt64 i = 0; i < col_num.size(); ++i)
			{
                data_size += str_size * col_num[i] + 1;
                res_offsets[i] = data_size;
            }
            res_data.resize(data_size);
            for (UInt64 i = 0; i < col_num.size(); ++i)
			{
                array((UInt8 *)copy_str.data(), res_data.data() + res_offsets[i - 1], str_size + 1, col_num[i]);
            }
        }

    private:
        template <typename T>
        static void array(const UInt8 * src, UInt8 * dst, const UInt64 size, T repeatTime)
        {
            UInt64 i = 0;
            do
            {
                memcpy(dst, src, size - 1);
                dst += size - 1;
                ++i;
            } while (i < repeatTime);
            *dst = 0;
        }
    };


    template <typename Impl>
    class FunctionRepeatImpl : public IFunction
        {
            template <typename F>
            static bool castType(const IDataType * type, F && f)
            {
                return castTypeToEither<
                    DataTypeUInt8,
                    DataTypeUInt16,
                    DataTypeUInt32,
                    DataTypeUInt64>(type, std::forward<F>(f));
            }

        public:
			static constexpr auto name = "repeat";
            static FunctionPtr create(const Context &) { return std::make_shared<FunctionRepeatImpl>(); }

            String getName() const override { return name; }

			size_t getNumberOfArguments() const override { return 2; }

			DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
			{
                if (!isString(arguments[0]))
                    throw Exception(
                        "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                if (!isUnsignedInteger(arguments[1]))
                    throw Exception(
                        "Illegal type " + arguments[1]->getName() + " of argument of function 1" + getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                return arguments[0];
            }

            bool useDefaultImplementationForConstants() const override { return true; }

            void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
			{
                const ColumnPtr strcolumn = block.getByPosition(arguments[0]).column;
                const ColumnPtr numcolumn = block.getByPosition(arguments[1]).column;

                if (const ColumnString * col = checkAndGetColumn<ColumnString>(strcolumn.get()))
                {
                    if (const ColumnConst * scale_column_num = checkAndGetColumn<ColumnConst>(numcolumn.get()))
                    {
                        Field scale_field_num = scale_column_num->getField();
                        UInt64 repeat_time = scale_field_num.get<UInt64>();
                        auto col_res = ColumnString::create();
                        Impl::vectorNonConstStr(
                            col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), repeat_time);
                        block.getByPosition(result).column = std::move(col_res);
                    }
                    else if (!castType(
                                 block.getByPosition(arguments[1]).type.get(), [&](const auto & type) {
                                     using DataType = std::decay_t<decltype(type)>;
                                     using T0 = typename DataType::FieldType;
                                     const ColumnVector<T0> * colnum = checkAndGetColumn<ColumnVector<T0>>(numcolumn.get());
                                     if (col->size() > 1 && colnum->size() > 1 && col->size() != colnum->size())
                                         throw Exception(
                                             "Column size doesn't match of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
                                     auto col_res = ColumnString::create();
                                     if (colnum->size() == 1 && col->size() >= 1)
                                     {
                                         UInt64 repeat_time = colnum->get64(0);
                                         Impl::vectorNonConstStr(
                                             col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), repeat_time);
                                     }
                                     else
                                     {
                                         Impl::vectorNonConst(
                                             col->getChars(),
                                             col->getOffsets(),
                                             col_res->getChars(),
                                             col_res->getOffsets(),
                                             colnum->getData());
                                     }
                                     block.getByPosition(result).column = std::move(col_res);
                                     return 0;
                                 }))
                        ;
                    else
                        throw Exception(
                            "Illegal column " + block.getByPosition(arguments[1]).column->getName() + " of argument of function2 "
                                + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
                }
                else if (const ColumnConst * scale_column_str = checkAndGetColumn<ColumnConst>(strcolumn.get()))
                {
                    Field scale_field_str = scale_column_str->getField();
                    String copy_str = scale_field_str.get<String>();
                    if (const ColumnConst * scale_column_num = checkAndGetColumn<ColumnConst>(numcolumn.get()))
                    {
                        Field scale_field_num = scale_column_num->getField();
                        UInt64 repeat_time = scale_field_num.get<UInt64>();
                        auto col_res = ColumnString::create();
                        Impl::vectorConst(copy_str, repeat_time, col_res->getChars(), col_res->getOffsets());
                        block.getByPosition(result).column = std::move(col_res);
                    }
                    else if (!castType(block.getByPosition(arguments[1]).type.get(), [&](const auto & type) {
                                 using DataType = std::decay_t<decltype(type)>;
                                 using T0 = typename DataType::FieldType;
                                 const ColumnVector<T0> * colnum = checkAndGetColumn<ColumnVector<T0>>(numcolumn.get());
                                 auto col_res = ColumnString::create();
                                 Impl::vectorNonConstInteger(copy_str, col_res->getChars(), col_res->getOffsets(), colnum->getData());
                                 block.getByPosition(result).column = std::move(col_res);
                                 return 0;
                             }))
                        ;
                    else
                        throw Exception(
                            "Illegal column " + block.getByPosition(arguments[1]).column->getName() + " of argument of function2 "
                                + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
                }
                else
                    throw Exception(
                        "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
            }
        };

    using FunctionRepeat = FunctionRepeatImpl<RepeatImpl>;

    void registerFunctionRepeat(FunctionFactory & factory)
	{
        factory.registerFunction<FunctionRepeat>();
    }
}
