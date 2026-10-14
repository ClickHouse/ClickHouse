#pragma once
#include <Columns/ColumnString.h>
#include <Common/TargetSpecific.h>

namespace DB
{

template <char not_case_lower_bound, char not_case_upper_bound>
struct LowerUpperImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t /*input_rows_count*/)
    {
        res_data.resize_exact(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t /*n*/, ColumnString::Chars & res_data, size_t /*input_rows_count*/)
    {
        res_data.resize_exact(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorRaw(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        array(src, src_end, dst);
    }

private:
    MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(static void NO_INLINE), arrayImpl, MULTITARGET_FUNCTION_BODY((const UInt8 * __restrict src, const UInt8 * src_end, UInt8 * __restrict dst) /// NOLINT
    {
        constexpr UInt8 flip_case_mask = 'A' ^ 'a';

        /// Selecting the mask rather than the result keeps the vectorizer on `vpand`, not the 2-uop `vpblendvb`.
        for (; src < src_end; ++src, ++dst)
        {
            const UInt8 c = *src;
            *dst = c ^ ((c >= not_case_lower_bound && c <= not_case_upper_bound) ? flip_case_mask : UInt8(0));
        }
    }))

    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
        {
            arrayImpl_x86_64_v4(src, src_end, dst);
            return;
        }
#endif
        arrayImpl(src, src_end, dst);
    }
};

}
