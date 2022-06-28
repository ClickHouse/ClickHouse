#include "config_core.h"

#if USE_ICU
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <unicode/rep.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnString.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_NORMALIZE_STRING;
}

namespace
{

// Expansion factors are specified for UTF-32, since icu uses UTF-32 for normalization
// Maximum expansion factors for different normalization forms
// https://unicode.org/faq/normalization.html#12

struct NormalizeNFCImpl
{
    static constexpr auto name = "normalizeUTF8NFC";

    static constexpr auto expansionFactor = 3;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFCInstance(err);
    }
};

struct NormalizeNFDImpl
{
    static constexpr auto name = "normalizeUTF8NFD";

    static constexpr auto expansionFactor = 4;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFDInstance(err);
    }
};

struct NormalizeNFKCImpl
{
    static constexpr auto name = "normalizeUTF8NFKC";

    static constexpr auto expansionFactor = 18;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFKCInstance(err);
    }
};


struct NormalizeNFKDImpl
{
    static constexpr auto name = "normalizeUTF8NFKD";

    static constexpr auto expansionFactor = 18;

    static const UNormalizer2 *getNormalizer(UErrorCode *err)
    {
        return unorm2_getNFKDInstance(err);
    }
};

template<typename NormalizeImpl>
struct NormalizeUTF8Impl
{

    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        UErrorCode err = U_ZERO_ERROR;

        const UNormalizer2 *normalizer = NormalizeImpl::getNormalizer(&err);
        if (U_FAILURE(err))
            throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (getNormalizer): {}", u_errorName(err));

        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset current_from_offset = 0;
        ColumnString::Offset current_to_offset = 0;

        PODArray<UChar> from_uchars;
        PODArray<UChar> to_uchars;

        for (size_t i = 0; i < size; ++i)
        {
            size_t from_size = offsets[i] - current_from_offset - 1;

            from_uchars.resize(from_size + 1);
            int32_t from_code_points = 0;
            u_strFromUTF8(
                from_uchars.data(),
                from_uchars.size(),
                &from_code_points,
                reinterpret_cast<const char*>(&data[current_from_offset]),
                from_size,
                &err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (strFromUTF8): {}", u_errorName(err));

            to_uchars.resize(from_code_points * NormalizeImpl::expansionFactor + 1);

            int32_t to_code_points = unorm2_normalize(
                normalizer,
                from_uchars.data(),
                from_code_points,
                to_uchars.data(),
                to_uchars.size(),
                &err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (normalize): {}", u_errorName(err));

            size_t max_to_size = current_to_offset + 4 * to_code_points + 1;
            if (res_data.size() < max_to_size)
                res_data.resize(max_to_size);

            int32_t to_size = 0;
            u_strToUTF8(
                reinterpret_cast<char*>(&res_data[current_to_offset]),
                res_data.size() - current_to_offset,
                &to_size,
                to_uchars.data(),
                to_code_points,
                &err);
            if (U_FAILURE(err))
                throw Exception(ErrorCodes::CANNOT_NORMALIZE_STRING, "Normalization failed (strToUTF8): {}", u_errorName(err));

            current_to_offset += to_size;
            res_data[current_to_offset] = 0;
            ++current_to_offset;
            res_offsets[i] = current_to_offset;

            current_from_offset = offsets[i];
        }

        res_data.resize(current_to_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Cannot apply function normalizeUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

using FunctionNormalizeUTF8NFC = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFCImpl>, NormalizeNFCImpl>;
using FunctionNormalizeUTF8NFD = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFDImpl>, NormalizeNFDImpl>;
using FunctionNormalizeUTF8NFKC = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFKCImpl>, NormalizeNFKCImpl>;
using FunctionNormalizeUTF8NFKD = FunctionStringToString<NormalizeUTF8Impl<NormalizeNFKDImpl>, NormalizeNFKDImpl>;
}

void registerFunctionNormalizeUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNormalizeUTF8NFC>();
    factory.registerFunction<FunctionNormalizeUTF8NFD>();
    factory.registerFunction<FunctionNormalizeUTF8NFKC>();
    factory.registerFunction<FunctionNormalizeUTF8NFKD>();
}

}

#endif
