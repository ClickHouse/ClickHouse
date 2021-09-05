#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <unicode/normalizer2.h>
#include <unicode/rep.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>
#include "common/logger_useful.h"
#include "Columns/ColumnString.h"
#include "Parsers/IAST_fwd.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_NORMALIZE_STRING;
}

namespace
{

struct NormalizeUTF8Impl
{

    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        UErrorCode err = U_ZERO_ERROR;

        const UNormalizer2 *normalizer = unorm2_getNFCInstance(&err);
        if (U_FAILURE(err)) {
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed: {}", u_errorName(err));
        }

        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        icu::UnicodeString to_string;

        PODArray<UChar> from_uchars;
        PODArray<UChar> to_uchars;

        for (size_t i = 0; i < size; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset - 1;

            from_uchars.resize(from_size + 1);
            int32_t from_code_points;
            u_strFromUTF8(
                from_uchars.data(),
                from_uchars.size(),
                &from_code_points,
                reinterpret_cast<const char*>(&data[current_from_offset]),
                from_size,
                &err);
            if (U_FAILURE(err)) {
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed: {}", u_errorName(err));
            }

            // NFC should produce no more than 3x code points
            // https://unicode.org/faq/normalization.html#12
            to_uchars.resize(from_code_points * 3 + 1);

            int32_t to_code_points = unorm2_normalize(
                normalizer,
                from_uchars.data(),
                from_code_points,
                to_uchars.data(),
                to_uchars.size(),
                &err);
            if (U_FAILURE(err)) {
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed: {}", u_errorName(err));
            }

            size_t max_to_size = current_to_offset + 2 * to_code_points + 1;
            if (res_data.size() < max_to_size) {
                res_data.resize(max_to_size);
            }

            int32_t to_size;
            u_strToUTF8(
                reinterpret_cast<char*>(&res_data[current_to_offset]),
                res_data.size() - current_to_offset,
                &to_size,
                to_uchars.data(),
                to_code_points,
                &err);
            if (U_FAILURE(err)) {
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed: {}", u_errorName(err));
            }

            current_to_offset += to_size;
            res_data[current_to_offset] = 0;
            ++current_to_offset;
            res_offsets[i] = current_to_offset;

            current_from_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Cannot apply function normalizeUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct NameNormalizeUTF8
{
    static constexpr auto name = "normalizeUTF8";
};

using FunctionNormalizeUTF8 = FunctionStringToString<NormalizeUTF8Impl, NameNormalizeUTF8>;
}

void registerFunctionNormalizeUTF8(FunctionFactory & factory) {
    factory.registerFunction<FunctionNormalizeUTF8>();
}

}
