#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/HashTable/HashMap.h>

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
        if (map_from.size() != map_to.size())
            throw Exception("Second and trird arguments must be the same length", ErrorCodes::BAD_ARGUMENTS);

        std::iota(map.begin(), map.end(), 0);

        for (size_t i = 0; i < map_from.size(); ++i)
        {
            if (!isASCII(map_from[i]) || !isASCII(map_to[i]))
                throw Exception("Second and trird arguments must be ASCII strings", ErrorCodes::BAD_ARGUMENTS);

            map[map_from[i]] = map_to[i];
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        Map map;
        fillMapWithValues(map, map_from, map_to);

        res_data.resize(data.size());
        res_offsets.assign(offsets);

        UInt8 * dst = res_data.data();

        for (UInt64 i = 0; i < offsets.size(); ++i)
        {
            const UInt8 * src = data.data() + offsets[i - 1];
            const UInt8 * src_end = data.data() + offsets[i] - 1;

            while (src < src_end)
            {
                if (*src <= ascii_upper_bound)
                    *dst = map[*src];
                else
                    *dst = *src;

                ++src;
                ++dst;
            }

            /// Technically '\0' can be mapped into other character,
            ///  so we need to process '\0' delimiter separately
            *dst++ = 0;
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & data,
        size_t /*n*/,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data)
    {
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
        auto map_from_size = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(map_from.data()), map_from.size());
        auto map_to_size = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(map_to.data()), map_to.size());

        if (map_from_size != map_to_size)
            throw Exception("Second and trird arguments must be the same length", ErrorCodes::BAD_ARGUMENTS);

        std::iota(map_ascii.begin(), map_ascii.end(), 0);

        const UInt8 * map_from_ptr = reinterpret_cast<const UInt8 *>(map_from.data());
        const UInt8 * map_from_end = map_from_ptr + map_from.size();
        const UInt8 * map_to_ptr = reinterpret_cast<const UInt8 *>(map_to.data());
        const UInt8 * map_to_end = map_to_ptr + map_to.size();

        while (map_from_ptr < map_from_end && map_to_ptr < map_to_end)
        {
            size_t len_from = UTF8::seqLength(*map_from_ptr);
            size_t len_to = UTF8::seqLength(*map_to_ptr);

            std::optional<UInt32> res_from, res_to;

            if (map_from_ptr + len_from <= map_from_end)
                res_from = UTF8::convertUTF8ToCodePoint(map_from_ptr, len_from);

            if (map_to_ptr + len_to <= map_to_end)
                res_to = UTF8::convertUTF8ToCodePoint(map_to_ptr, len_to);

            if (!res_from)
                throw Exception("Second argument must be a valid UTF-8 string", ErrorCodes::BAD_ARGUMENTS);

            if (!res_to)
                throw Exception("Third argument must be a valid UTF-8 string", ErrorCodes::BAD_ARGUMENTS);

            if (*map_from_ptr <= ascii_upper_bound)
                map_ascii[*map_from_ptr] = *res_to;
            else
                map[*res_from] = *res_to;

            map_from_ptr += len_from;
            map_to_ptr += len_to;
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & map_from,
        const std::string & map_to,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        MapASCII map_ascii;
        MapUTF8 map;
        fillMapWithValues(map_ascii, map, map_from, map_to);

        res_data.resize(data.size());
        res_offsets.resize(offsets.size());

        UInt8 * dst = res_data.data();
        UInt64 data_size = 0;

        for (UInt64 i = 0; i < offsets.size(); ++i)
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
                    size_t dst_len = UTF8::convertCodePointToUTF8(map_ascii[*src], dst, 4);
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
                    auto src_code_point = UTF8::convertUTF8ToCodePoint(src, src_len);

                    if (src_code_point)
                    {
                        auto * it = map.find(*src_code_point);
                        if (it != map.end())
                        {
                            size_t dst_len = UTF8::convertCodePointToUTF8(it->getMapped(), dst, 4);
                            assert(0 < dst_len && dst_len <= 4);

                            src += src_len;
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
            *dst++ = 0;

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
        throw Exception("Function translateUTF8 does not support FixedString argument", ErrorCodes::BAD_ARGUMENTS);
    }

private:
    static constexpr auto ascii_upper_bound = '\x7f';
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
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[2]))
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column_src = arguments[0].column;
        const ColumnPtr column_map_from = arguments[1].column;
        const ColumnPtr column_map_to = arguments[2].column;

        if (!isColumnConst(*column_map_from) || !isColumnConst(*column_map_to))
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.", ErrorCodes::ILLEGAL_COLUMN);

        const IColumn * c1 = arguments[1].column.get();
        const IColumn * c2 = arguments[2].column.get();
        const ColumnConst * c1_const = typeid_cast<const ColumnConst *>(c1);
        const ColumnConst * c2_const = typeid_cast<const ColumnConst *>(c2);
        String map_from = c1_const->getValue<String>();
        String map_to = c2_const->getValue<String>();

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), map_from, map_to, col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), map_from, map_to, col_res->getChars());
            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
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

void registerFunctionTranslate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTranslateASCII>();
    factory.registerFunction<FunctionTranslateUTF8>();
}

}
