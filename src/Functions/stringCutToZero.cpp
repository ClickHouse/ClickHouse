#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

class FunctionToStringCutToZero : public IFunction
{
public:
    static constexpr auto name = "toStringCutToZero";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToStringCutToZero>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                            arguments[0]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    static bool tryExecuteString(const IColumn * col, ColumnPtr & col_res, size_t input_rows_count)
    {
        const ColumnString * col_str_in = checkAndGetColumn<ColumnString>(col);

        if (col_str_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_str_in->getChars();
            const ColumnString::Offsets & in_offsets = col_str_in->getOffsets();

            out_offsets.resize(input_rows_count);
            out_vec.resize(in_vec.size());

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;

            ColumnString::Offset current_in_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const char * pos_in = reinterpret_cast<const char *>(&in_vec[current_in_offset]);
                size_t current_size = strlen(pos_in);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                ++pos;
                out_offsets[i] = pos - begin;
                current_in_offset = in_offsets[i];
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column size mismatch (internal logical error)");

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    static bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res, size_t input_rows_count)
    {
        const ColumnFixedString * col_fstr_in = checkAndGetColumn<ColumnFixedString>(col);

        if (col_fstr_in)
        {
            auto col_str = ColumnString::create();
            ColumnString::Chars & out_vec = col_str->getChars();
            ColumnString::Offsets & out_offsets = col_str->getOffsets();

            const ColumnString::Chars & in_vec = col_fstr_in->getChars();

            out_offsets.resize(input_rows_count);
            out_vec.resize(in_vec.size() + input_rows_count);

            char * begin = reinterpret_cast<char *>(out_vec.data());
            char * pos = begin;
            const char * pos_in = reinterpret_cast<const char *>(in_vec.data());

            size_t n = col_fstr_in->getN();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t current_size = strnlen(pos_in, n);
                memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
                pos += current_size;
                *pos = '\0';
                out_offsets[i] = ++pos - begin;
                pos_in += n;
            }
            out_vec.resize(pos - begin);

            if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column size mismatch (internal logical error)");

            col_res = std::move(col_str);
            return true;
        }

        return false;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * column = arguments[0].column.get();
        ColumnPtr res_column;

        if (tryExecuteFixedString(column, res_column, input_rows_count) || tryExecuteString(column, res_column, input_rows_count))
            return res_column;

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                        arguments[0].column->getName(), getName());
    }
};


REGISTER_FUNCTION(ToStringCutToZero)
{
    factory.registerFunction<FunctionToStringCutToZero>();
}

}
