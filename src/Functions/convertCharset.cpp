#include "config.h"

#if USE_ICU
#    include <Columns/ColumnConst.h>
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <Functions/FunctionFactory.h>
#    include <Functions/FunctionHelpers.h>
#    include <Functions/IFunction.h>
#    include <IO/WriteHelpers.h>
#    include <Common/ObjectPool.h>
#    include <Common/typeid_cast.h>
#    include <base/range.h>

#    include <memory>
#    include <string>
#    include <unicode/ucnv.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int CANNOT_CREATE_CHARSET_CONVERTER;
    extern const int CANNOT_CONVERT_CHARSET;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** convertCharset(s, from, to)
  *
  * Assuming string 's' contains bytes in charset 'from',
  *  returns another string with bytes, representing same content in charset 'to'.
  * from and to must be constants.
  *
  * When bytes are illegal in 'from' charset or are not representable in 'to' charset,
  *  behavior is implementation specific.
  */
class FunctionConvertCharset : public IFunction
{
private:
    struct Converter : private boost::noncopyable
    {
        UConverter * impl;

        explicit Converter(const String & charset)
        {
            UErrorCode status = U_ZERO_ERROR;
            impl = ucnv_open(charset.data(), &status);

            if (U_SUCCESS(status))
                ucnv_setToUCallBack(impl,
                    UCNV_TO_U_CALLBACK_SUBSTITUTE,
                    nullptr,
                    nullptr, nullptr,
                    &status);

            if (U_SUCCESS(status))
                ucnv_setFromUCallBack(impl,
                    UCNV_FROM_U_CALLBACK_SUBSTITUTE,
                    nullptr,
                    nullptr, nullptr,
                    &status);

            if (!U_SUCCESS(status))
                throw Exception(ErrorCodes::CANNOT_CREATE_CHARSET_CONVERTER, "Cannot create UConverter with charset {}, error: {}",
                    charset, String(u_errorName(status)));
        }

        ~Converter()
        {
            ucnv_close(impl);
        }
    };

    /// Separate converter is created for each thread.
    using Pool = ObjectPoolMap<Converter, String>;

    static Pool::Pointer getConverter(const String & charset)
    {
        static Pool pool;
        return pool.get(charset, [&charset] { return new Converter(charset); });
    }

    static void convert(const String & from_charset, const String & to_charset,
        const ColumnString::Chars & from_chars, const ColumnString::Offsets & from_offsets,
        ColumnString::Chars & to_chars, ColumnString::Offsets & to_offsets,
        size_t input_rows_count)
    {
        auto converter_from = getConverter(from_charset);
        auto converter_to = getConverter(to_charset);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        to_offsets.resize(input_rows_count);

        PODArray<UChar> uchars;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t from_string_size = from_offsets[i] - current_from_offset - 1;

            /// We assume that empty string is empty in every charset.
            if (0 != from_string_size)
            {
                /// reset state of converter
                ucnv_reset(converter_from->impl);
                ucnv_reset(converter_to->impl);

                /// maximum number of code points is number of bytes in input string plus one for terminating zero
                uchars.resize(from_string_size + 1);

                UErrorCode status = U_ZERO_ERROR;
                int32_t res = ucnv_toUChars(
                    converter_from->impl,
                    uchars.data(), uchars.size(),
                    reinterpret_cast<const char *>(&from_chars[current_from_offset]), from_string_size,
                    &status);

                if (!U_SUCCESS(status))
                    throw Exception(ErrorCodes::CANNOT_CONVERT_CHARSET, "Cannot convert from charset {}, error: {}",
                        from_charset, String(u_errorName(status)));

                auto max_to_char_size = ucnv_getMaxCharSize(converter_to->impl);
                auto max_to_size = UCNV_GET_MAX_BYTES_FOR_STRING(res, max_to_char_size);

                to_chars.resize(current_to_offset + max_to_size);

                res = ucnv_fromUChars(
                    converter_to->impl,
                    reinterpret_cast<char *>(&to_chars[current_to_offset]), max_to_size,
                    uchars.data(), res,
                    &status);

                if (!U_SUCCESS(status))
                    throw Exception(ErrorCodes::CANNOT_CONVERT_CHARSET, "Cannot convert to charset {}, error: {}",
                        to_charset, String(u_errorName(status)));

                current_to_offset += res;
            }

            if (to_chars.size() < current_to_offset + 1)
                to_chars.resize(current_to_offset + 1);

            to_chars[current_to_offset] = 0;

            ++current_to_offset;
            to_offsets[i] = current_to_offset;

            current_from_offset = from_offsets[i];
        }

        to_chars.resize(current_to_offset);
    }

public:
    static constexpr auto name = "convertCharset";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConvertCharset>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i : collections::range(0, 3))
            if (!isString(arguments[i]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}, must be String",
                    arguments[i]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & arg_from = arguments[0];
        const ColumnWithTypeAndName & arg_charset_from = arguments[1];
        const ColumnWithTypeAndName & arg_charset_to = arguments[2];

        const ColumnConst * col_charset_from = checkAndGetColumnConstStringOrFixedString(arg_charset_from.column.get());
        const ColumnConst * col_charset_to = checkAndGetColumnConstStringOrFixedString(arg_charset_to.column.get());

        if (!col_charset_from || !col_charset_to)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "2nd and 3rd arguments of function {} (source charset and destination charset) must "
                            "be constant strings.", getName());

        String charset_from = col_charset_from->getValue<String>();
        String charset_to = col_charset_to->getValue<String>();

        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(arg_from.column.get()))
        {
            auto col_to = ColumnString::create();
            convert(charset_from, charset_to, col_from->getChars(), col_from->getOffsets(), col_to->getChars(), col_to->getOffsets(), input_rows_count);
            return col_to;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column passed as first argument of function {} (must be ColumnString).", getName());
    }
};

}

REGISTER_FUNCTION(ConvertCharset)
{
    factory.registerFunction<FunctionConvertCharset>();
}

}

#endif
