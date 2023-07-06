#pragma once
namespace DB
{
namespace
{
template <typename Name, typename Impl>
struct HasSubsequenceImpl
{
    using ResultType = UInt8;

    static constexpr bool use_default_implementation_for_constants = false;
    static constexpr bool supports_start_pos = false;
    static constexpr auto name = Name::name;

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {};}

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & needle,
        const ColumnPtr & /*start_pos*/,
        PaddedPODArray<UInt8> & res,
        [[maybe_unused]] ColumnUInt8 * /*res_null*/)
    {
        if (needle.empty())
        {
            for (auto & r : res)
                r = 1;
            return;
        }

        ColumnString::Offset prev_haystack_offset = 0;
        for (size_t i = 0; i < haystack_offsets.size(); ++i)
        {
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;
            const char * haystack = reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]);
            res[i] = hasSubsequence(haystack, haystack_size, needle.c_str(), needle.size());
            prev_haystack_offset = haystack_offsets[i];
        }
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & /*start_pos*/,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * /*res_null*/)
    {
        ColumnString::Offset prev_haystack_offset = 0;
        ColumnString::Offset prev_needle_offset = 0;

        size_t size = haystack_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;
            size_t haystack_size = haystack_offsets[i] - prev_haystack_offset - 1;

            if (0 == needle_size)
            {
                res[i] = 1;
            }
            else
            {
                const char * needle = reinterpret_cast<const char *>(&needle_data[prev_needle_offset]);
                const char * haystack = reinterpret_cast<const char *>(&haystack_data[prev_haystack_offset]);
                res[i] = hasSubsequence(haystack, haystack_size, needle, needle_size);
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offsets[i];
        }
    }

    static void constantVector(
        const String & haystack,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offsets,
        const ColumnPtr & /*start_pos*/,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * /*res_null*/)
    {
        ColumnString::Offset prev_needle_offset = 0;

        size_t size = needle_offsets.size();

        for (size_t i = 0; i < size; ++i)
        {
            size_t needle_size = needle_offsets[i] - prev_needle_offset - 1;

            if (0 == needle_size)
            {
                res[i] = 1;
            }
            else
            {
                const char * needle = reinterpret_cast<const char *>(&needle_data[prev_needle_offset]);
                res[i] = hasSubsequence(haystack.c_str(), haystack.size(), needle, needle_size);
            }
            prev_needle_offset = needle_offsets[i];
        }
    }

    static void constantConstant(
        String haystack,
        String needle,
        const ColumnPtr & /*start_pos*/,
        PaddedPODArray<UInt8> & res,
        ColumnUInt8 * /*res_null*/)
    {
        size_t size = res.size();
        Impl::toLowerIfNeed(haystack);
        Impl::toLowerIfNeed(needle);

        UInt8 result = hasSubsequence(haystack.c_str(), haystack.size(), needle.c_str(), needle.size());

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = result;
        }
    }

    static UInt8 hasSubsequence(const char * haystack, size_t haystack_size, const char * needle, size_t needle_size)
    {
        size_t j = 0;
        for (size_t i = 0; (i < haystack_size) && (j < needle_size); i++)
            if (needle[j] == haystack[i])
                ++j;
        return j == needle_size;
    }

    template <typename... Args>
    static void vectorFixedConstant(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }

    template <typename... Args>
    static void vectorFixedVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support FixedString haystack argument", name);
    }
};

}

}
