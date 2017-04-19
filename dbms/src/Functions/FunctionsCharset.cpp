#include <Common/config.h>
#if USE_ICU

#include <Functions/IFunction.h>
#include <Functions/ObjectPool.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <ext/range.hpp>

#include <unicode/ucnv.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CREATE_CHARSET_CONVERTER;
    extern const int CANNOT_CONVERT_CHARSET;
}


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

        Converter(const String & charset)
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
                throw Exception("Cannot create UConverter with charset " + charset + ", error: " + String(u_errorName(status)),
                    ErrorCodes::CANNOT_CREATE_CHARSET_CONVERTER);
        }

        ~Converter()
        {
            ucnv_close(impl);
        }
    };

    /// Separate converter is created for each thread.
    using Pool = ObjectPoolMap<Converter, String>;

    Pool::Pointer getConverter(const String & charset)
    {
        static Pool pool;
        return pool.get(charset, [&charset] { return new Converter(charset); });
    }

    void convert(const String & from_charset, const String & to_charset,
        const ColumnString::Chars_t & from_chars, const ColumnString::Offsets_t & from_offsets,
        ColumnString::Chars_t & to_chars, ColumnString::Offsets_t & to_offsets)
    {
        auto converter_from = getConverter(from_charset);
        auto converter_to = getConverter(to_charset);

        ColumnString::Offset_t current_from_offset = 0;
        ColumnString::Offset_t current_to_offset = 0;

        size_t size = from_offsets.size();
        to_offsets.resize(size);

        PODArray<UChar> uchars;

        for (size_t i = 0; i < size; ++i)
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
                    throw Exception("Cannot convert from charset " + from_charset + ", error: " + String(u_errorName(status)),
                        ErrorCodes::CANNOT_CONVERT_CHARSET);

                auto max_to_char_size = ucnv_getMaxCharSize(converter_to->impl);
                auto max_to_size = UCNV_GET_MAX_BYTES_FOR_STRING(res, max_to_char_size);

                to_chars.resize(current_to_offset + max_to_size);

                res = ucnv_fromUChars(
                    converter_to->impl,
                    reinterpret_cast<char *>(&to_chars[current_to_offset]), max_to_size,
                    uchars.data(), res,
                    &status);

                if (!U_SUCCESS(status))
                    throw Exception("Cannot convert to charset " + to_charset + ", error: " + String(u_errorName(status)),
                        ErrorCodes::CANNOT_CONVERT_CHARSET);

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
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionConvertCharset>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (size_t i : ext::range(0, 3))
            if (!typeid_cast<const DataTypeString *>(&*arguments[i]))
                throw Exception("Illegal type " + arguments[i]->getName() + " of argument of function " + getName()
                    + ", must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        const ColumnWithTypeAndName & arg_from = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg_charset_from = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg_charset_to = block.getByPosition(arguments[2]);
        ColumnWithTypeAndName & res = block.getByPosition(result);

        const ColumnConstString * col_charset_from = typeid_cast<const ColumnConstString *>(arg_charset_from.column.get());
        const ColumnConstString * col_charset_to = typeid_cast<const ColumnConstString *>(arg_charset_to.column.get());

        if (!col_charset_from || !col_charset_to)
            throw Exception("2nd and 3rd arguments of function " + getName() + " (source charset and destination charset) must be constant strings.",
                ErrorCodes::ILLEGAL_COLUMN);

        String charset_from = col_charset_from->getData();
        String charset_to = col_charset_to->getData();

        if (const ColumnString * col_from = typeid_cast<const ColumnString *>(arg_from.column.get()))
        {
            auto col_to = std::make_shared<ColumnString>();
            convert(charset_from, charset_to, col_from->getChars(), col_from->getOffsets(), col_to->getChars(), col_to->getOffsets());
            res.column = col_to;
        }
        else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(arg_from.column.get()))
        {
            auto full_column_holder = col_from->cloneResized(1)->convertToFullColumnIfConst();
            const ColumnString * col_from_full = static_cast<const ColumnString *>(full_column_holder.get());

            auto col_to_full = std::make_shared<ColumnString>();
            convert(charset_from, charset_to, col_from_full->getChars(), col_from_full->getOffsets(), col_to_full->getChars(), col_to_full->getOffsets());

            res.column = std::make_shared<ColumnConstString>(col_from->size(), (*col_to_full)[0].get<String>(), res.type);
        }
        else
            throw Exception("Illegal column passed as first argument of function " + getName() + " (must be ColumnString).",
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


void registerFunctionsCharset(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConvertCharset>();
}

}

#else

namespace DB
{
    class FunctionFactory;
    void registerFunctionsCharset(FunctionFactory & factory) {}
}


#endif
