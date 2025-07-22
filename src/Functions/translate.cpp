#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Common/HashTable/HashMap.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/iota.h>

#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

struct TranslateImpl
{
    using Map = std::array<UInt8, 128>;

    static void fillMapWithValues(
        Map & map,
        const std::string & map_from,
        const std::string & map_to)
    {
        iota(map.data(), map.size(), UInt8(0));

        size_t min_size = std::min(map_from.size(), map_to.size());

        // Map characters from map_from to map_to for the overlapping range
        for (size_t i = 0; i < min_size; ++i)
        {
            if (!isASCII(map_from[i]) || !isASCII(map_to[i]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second and third arguments must be ASCII strings");
            map[static_cast<unsigned char>(map_from[i])] = static_cast<UInt8>(map_to[i]);
        }

        // Handle any remaining characters in map_from by assigning a default value
        for (size_t i = min_size; i < map_from.size(); ++i)
        {
            if (!isASCII(map_from[i]))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument must be ASCII strings");
            map[static_cast<unsigned char>(map_from[i])] = ascii_upper_bound + 1;
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        Map map;
        fillMapWithValues(map, map_from, map_to);

        res_data.resize(data.size());
        res_offsets.resize(input_rows_count);

        UInt8 * dst = res_data.data();

        UInt64 data_size = 0;
        for (UInt64 i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * src = data.data() + offsets[i - 1];
            const UInt8 * src_end = data.data() + offsets[i] - 1;

            while (src < src_end)
            {
                if (*src <= ascii_upper_bound && map[*src] != ascii_upper_bound + 1)
                {
                    *dst = map[*src];
                    ++dst;
                    ++data_size;
                }
                else if (*src > ascii_upper_bound)
                {
                    *dst = *src;
                    ++dst;
                    ++data_size;
                }

                ++src;
            }

            /// Technically '\0' can be mapped into other character,
            ///  so we need to process '\0' delimiter separately
            *dst = 0;
            ++dst;
            ++data_size;
            res_offsets[i] = data_size;
        }

        res_data.resize(data_size);
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t /*n*/,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data)
    {
        if (map_from.size() != map_to.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second and third arguments must be the same length");

        std::array<UInt8, 128> map;
        fillMapWithValues(map, map_from, map_to);

        res_data.resize(data.size());

        const UInt8 * src = data.data();
        const UInt8 * src_end = data.data() + data.size();
        UInt8 * dst = res_data.data();

        while (src < src_end)
        {
            if (*src <= ascii_upper_bound)
                *dst = map[*src];
            else
                *dst = *src;

            ++src;
            ++dst;
        }
    }

private:
    static constexpr auto ascii_upper_bound = '\x7f';
};

struct TranslateUTF8Impl
{
    using MapASCII = std::array<UInt32, 128>;
    using MapUTF8 = HashMap<UInt32, UInt32, HashCRC32<UInt32>>;

    static void fillMapWithValues(
        MapASCII & map_ascii,
        MapUTF8 & map,
        const std::string & map_from,
        const std::string & map_to)
    {
        iota(map_ascii.data(), map_ascii.size(), UInt32(0));

        const UInt8 * map_from_ptr = reinterpret_cast<const UInt8 *>(map_from.data());
        const UInt8 * map_from_end = map_from_ptr + map_from.size();
        const UInt8 * map_to_ptr = reinterpret_cast<const UInt8 *>(map_to.data());
        const UInt8 * map_to_end = map_to_ptr + map_to.size();

        while (map_from_ptr < map_from_end)
        {
            size_t len_from = UTF8::seqLength(*map_from_ptr);

            std::optional<UInt32> res_from;
            std::optional<UInt32> res_to;

            if (map_from_ptr + len_from <= map_from_end)
                res_from = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(map_from_ptr), len_from);

            if (!res_from)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument must be a valid UTF-8 string");

            if (map_to_ptr < map_to_end)
            {
                size_t len_to = UTF8::seqLength(*map_to_ptr);

                if (map_to_ptr + len_to <= map_to_end)
                    res_to = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(map_to_ptr), len_to);

                if (!res_to)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Third argument must be a valid UTF-8 string");

                if (*map_from_ptr <= ascii_upper_bound)
                    map_ascii[*map_from_ptr] = *res_to;
                else
                    map[*res_from] = *res_to;

                map_to_ptr += len_to;
            }
            else
            {
                if (*map_from_ptr <= ascii_upper_bound)
                    map_ascii[*map_from_ptr] = max_uint32;
                else
                    map[*res_from] = max_uint32;
            }

            map_from_ptr += len_from;
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        MapASCII map_ascii;
        MapUTF8 map;
        fillMapWithValues(map_ascii, map, map_from, map_to);

        res_data.resize(data.size());
        res_offsets.resize(input_rows_count);

        UInt8 * dst = res_data.data();
        UInt64 data_size = 0;

        for (UInt64 i = 0; i < input_rows_count; ++i)
        {
            const UInt8 * src = data.data() + offsets[i - 1];
            const UInt8 * src_end = data.data() + offsets[i] - 1;

            while (src < src_end)
            {
                /// Maximum length of UTF-8 sequence is 4 bytes + 1 zero byte
                if (data_size + 5 > res_data.size())
                {
                    res_data.resize(data_size * 2 + 5);
                    dst = res_data.data() + data_size;
                }

                if (*src <= ascii_upper_bound)
                {
                    if (map_ascii[*src] == max_uint32)
                    {
                        src += 1;
                        continue;
                    }

                    size_t dst_len = UTF8::convertCodePointToUTF8(map_ascii[*src], reinterpret_cast<char *>(dst), 4);
                    assert(0 < dst_len && dst_len <= 4);

                    src += 1;
                    dst += dst_len;
                    data_size += dst_len;
                    continue;
                }

                size_t src_len = UTF8::seqLength(*src);
                assert(0 < src_len && src_len <= 4);

                if (src + src_len <= src_end)
                {
                    auto src_code_point = UTF8::convertUTF8ToCodePoint(reinterpret_cast<const char *>(src), src_len);

                    if (src_code_point)
                    {
                        auto * it = map.find(*src_code_point);
                        if (it != map.end())
                        {
                            src += src_len;
                            if (it->getMapped() == max_uint32)
                                continue;

                            size_t dst_len = UTF8::convertCodePointToUTF8(it->getMapped(), reinterpret_cast<char *>(dst), 4);
                            assert(0 < dst_len && dst_len <= 4);

                            dst += dst_len;
                            data_size += dst_len;
                            continue;
                        }
                    }
                }
                else
                {
                    src_len = src_end - src;
                }

                memcpy(dst, src, src_len);
                dst += src_len;
                src += src_len;
                data_size += src_len;
            }

            /// Technically '\0' can be mapped into other character,
            ///  so we need to process '\0' delimiter separately
            *dst = 0;
            ++dst;

            ++data_size;
            res_offsets[i] = data_size;
        }

        res_data.resize(data_size);
    }

    [[noreturn]] static void vectorFixed(
        const ColumnString::Chars & /*data*/,
        size_t /*n*/,
        const std::string & /*map_from*/,
        const std::string & /*map_to*/,
        ColumnString::Chars & /*res_data*/)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function translateUTF8 does not support FixedString argument");
    }

private:
    static constexpr auto ascii_upper_bound = '\x7f';
    static constexpr auto max_uint32 = 0xffffffff;
};


template <typename Impl, typename Name>
class FunctionTranslate : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTranslate>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}",
                arguments[0]->getName(), getName());

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}",
                arguments[1]->getName(), getName());

        if (!isStringOrFixedString(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}",
                arguments[2]->getName(), getName());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column_src = arguments[0].column;
        const ColumnPtr column_map_from = arguments[1].column;
        const ColumnPtr column_map_to = arguments[2].column;

        if (!isColumnConst(*column_map_from) || !isColumnConst(*column_map_to))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "2nd and 3rd arguments of function {} must be constants", getName());

        const IColumn * c1 = arguments[1].column.get();
        const IColumn * c2 = arguments[2].column.get();
        const ColumnConst * c1_const = typeid_cast<const ColumnConst *>(c1);
        const ColumnConst * c2_const = typeid_cast<const ColumnConst *>(c2);
        String map_from = c1_const->getValue<String>();
        String map_to = c2_const->getValue<String>();

        size_t map_from_size;
        size_t map_to_size;
        if constexpr (std::is_same_v<Impl, TranslateUTF8Impl>)
        {
            map_from_size = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(map_from.data()), map_from.size());
            map_to_size = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(map_to.data()), map_to.size());
        }
        else
        {
            map_from_size = map_from.size();
            map_to_size = map_to.size();
        }
        if (map_from_size < map_to_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must not be shorter than the third argument. Size of the second argument: {}, size of the third argument: {}", getName(), map_from.size(), map_to.size());

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), map_from, map_to, col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
        if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), map_from, map_to, col_res->getChars());
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};


namespace
{

struct NameTranslate
{
    static constexpr auto name = "translate";
};

struct NameTranslateUTF8
{
    static constexpr auto name = "translateUTF8";
};

using FunctionTranslateASCII = FunctionTranslate<TranslateImpl, NameTranslate>;
using FunctionTranslateUTF8 = FunctionTranslate<TranslateUTF8Impl, NameTranslateUTF8>;

}

REGISTER_FUNCTION(Translate)
{
    factory.registerFunction<FunctionTranslateASCII>();
    factory.registerFunction<FunctionTranslateUTF8>();
}

}
