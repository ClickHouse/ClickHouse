#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Parsers/queryNormalization.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <bool keep_names>
struct Impl
{
    static constexpr auto name = keep_names ? "normalizeQueryKeepNames" : "normalizeQuery";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_offsets.resize(input_rows_count);
        res_data.reserve(data.size());

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset curr_src_offset = offsets[i];

            normalizeQueryToPODArray(
                reinterpret_cast<const char *>(&data[prev_src_offset]),
                reinterpret_cast<const char *>(&data[curr_src_offset - 1]),
                res_data, keep_names);

            prev_src_offset = offsets[i];
            res_offsets[i] = res_data.size();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot apply function normalizeQuery to fixed string.");
    }
};

}

REGISTER_FUNCTION(NormalizeQuery)
{
    factory.registerFunction<FunctionStringToString<Impl<true>, Impl<true>>>();
    factory.registerFunction<FunctionStringToString<Impl<false>, Impl<false>>>();
}

}
