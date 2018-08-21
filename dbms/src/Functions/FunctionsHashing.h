#pragma once

#include <openssl/md5.h>
#include <openssl/sha.h>
#include <city.h>
#include <farmhash.h>
#include <metrohash.h>
#include <murmurhash2.h>
#include <murmurhash3.h>

#include <Poco/ByteOrder.h>

#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/HashTable/Hash.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <ext/range.h>
#include <ext/bit_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Hashing functions.
  *
  * halfMD5: String -> UInt64
  *
  * A faster cryptographic hash function:
  * sipHash64: String -> UInt64
  *
  * Fast non-cryptographic hash function for strings:
  * cityHash64: String -> UInt64
  *
  * A non-cryptographic hash from a tuple of values of any types (uses cityHash64 for strings and intHash64 for numbers):
  * cityHash64: any* -> UInt64
  *
  * Fast non-cryptographic hash function from any integer:
  * intHash32: number -> UInt32
  * intHash64: number -> UInt64
  *
  */

struct HalfMD5Impl
{
    using ReturnType = UInt64;

    static UInt64 apply(const char * begin, size_t size)
    {
        union
        {
            unsigned char char_data[16];
            uint64_t uint64_data;
        } buf;

        MD5_CTX ctx;
        MD5_Init(&ctx);
        MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        MD5_Final(buf.char_data, &ctx);

        return Poco::ByteOrder::flipBytes(buf.uint64_data);        /// Compatibility with existing code.
    }
};

struct MD5Impl
{
    static constexpr auto name = "MD5";
    enum { length = 16 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        MD5_CTX ctx;
        MD5_Init(&ctx);
        MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        MD5_Final(out_char_data, &ctx);
    }
};

struct SHA1Impl
{
    static constexpr auto name = "SHA1";
    enum { length = 20 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA_CTX ctx;
        SHA1_Init(&ctx);
        SHA1_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA1_Final(out_char_data, &ctx);
    }
};

struct SHA224Impl
{
    static constexpr auto name = "SHA224";
    enum { length = 28 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA256_CTX ctx;
        SHA224_Init(&ctx);
        SHA224_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA224_Final(out_char_data, &ctx);
    }
};

struct SHA256Impl
{
    static constexpr auto name = "SHA256";
    enum { length = 32 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA256_Final(out_char_data, &ctx);
    }
};

struct SipHash64Impl
{
    using ReturnType = UInt64;

    static UInt64 apply(const char * begin, size_t size)
    {
        return sipHash64(begin, size);
    }
};


struct SipHash128Impl
{
    static constexpr auto name = "sipHash128";
    enum { length = 16 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        sipHash128(begin, size, reinterpret_cast<char*>(out_char_data));
    }
};

struct IntHash32Impl
{
    using ReturnType = UInt32;

    static UInt32 apply(UInt64 x)
    {
        /// seed is taken from /dev/urandom. It allows you to avoid undesirable dependencies with hashes in different data structures.
        return intHash32<0x75D9543DE018BF45ULL>(x);
    }
};

struct IntHash64Impl
{
    using ReturnType = UInt64;

    static UInt64 apply(UInt64 x)
    {
        return intHash64(x ^ 0x4CF2D2BAAE6DA887ULL);
    }
};


template <typename Impl>
class FunctionStringHashFixedString : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringHashFixedString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(Impl::length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);

            const typename ColumnString::Chars_t & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            auto & chars_to = col_to->getChars();
            const auto size = offsets.size();
            chars_to.resize(size * Impl::length);

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                Impl::apply(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1,
                    &chars_to[i * Impl::length]);

                current_offset = offsets[i];
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename Impl, typename Name>
class FunctionIntHash : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionIntHash>(); }

private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType>
    void executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (auto col_from = checkAndGetColumn<ColumnVector<FromType>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const typename ColumnVector<FromType>::Container & vec_from = col_from->getData();
            typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

            size_t size = vec_from.size();
            vec_to.resize(size);
            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Impl::apply(vec_from[i]);

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isValueRepresentedByNumber())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

        if      (checkDataType<DataTypeUInt8>(from_type)) executeType<UInt8>(block, arguments, result);
        else if (checkDataType<DataTypeUInt16>(from_type)) executeType<UInt16>(block, arguments, result);
        else if (checkDataType<DataTypeUInt32>(from_type)) executeType<UInt32>(block, arguments, result);
        else if (checkDataType<DataTypeUInt64>(from_type)) executeType<UInt64>(block, arguments, result);
        else if (checkDataType<DataTypeInt8>(from_type)) executeType<Int8>(block, arguments, result);
        else if (checkDataType<DataTypeInt16>(from_type)) executeType<Int16>(block, arguments, result);
        else if (checkDataType<DataTypeInt32>(from_type)) executeType<Int32>(block, arguments, result);
        else if (checkDataType<DataTypeInt64>(from_type)) executeType<Int64>(block, arguments, result);
        else if (checkDataType<DataTypeDate>(from_type)) executeType<UInt16>(block, arguments, result);
        else if (checkDataType<DataTypeDateTime>(from_type)) executeType<UInt32>(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


/** We use hash functions called CityHash, FarmHash, MetroHash.
  * In this regard, this template is named with the words `NeighborhoodHash`.
  */
template <typename Impl>
class FunctionNeighbourhoodHash64 : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNeighbourhoodHash64>(); }

private:
    template <typename FromType, bool first>
    void executeIntType(const IColumn * column, ColumnUInt64::Container & vec_to)
    {
        if (const ColumnVector<FromType> * col_from = checkAndGetColumn<ColumnVector<FromType>>(column))
        {
            const typename ColumnVector<FromType>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                UInt64 h = IntHash64Impl::apply(ext::bit_cast<UInt64>(vec_from[i]));
                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));
            }
        }
        else if (auto col_from = checkAndGetColumnConst<ColumnVector<FromType>>(column))
        {
            const UInt64 hash = IntHash64Impl::apply(ext::bit_cast<UInt64>(col_from->template getValue<FromType>()));
            size_t size = vec_to.size();
            if (first)
            {
                vec_to.assign(size, hash);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], hash));
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeString(const IColumn * column, ColumnUInt64::Container & vec_to)
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
        {
            const typename ColumnString::Chars_t & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                const UInt64 h = Impl::Hash64(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1);

                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));

                current_offset = offsets[i];
            }
        }
        else if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(column))
        {
            const typename ColumnString::Chars_t & data = col_from->getChars();
            size_t n = col_from->getN();
            size_t size = data.size() / n;

            for (size_t i = 0; i < size; ++i)
            {
                const UInt64 h = Impl::Hash64(reinterpret_cast<const char *>(&data[i * n]), n);
                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));
            }
        }
        else if (const ColumnConst * col_from = checkAndGetColumnConstStringOrFixedString(column))
        {
            String value = col_from->getValue<String>().data();
            const UInt64 hash = Impl::Hash64(value.data(), value.size());
            const size_t size = vec_to.size();

            if (first)
            {
                vec_to.assign(size, hash);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], hash));
                }
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeArray(const IDataType * type, const IColumn * column, ColumnUInt64::Container & vec_to)
    {
        const IDataType * nested_type = typeid_cast<const DataTypeArray *>(type)->getNestedType().get();

        if (const ColumnArray * col_from = checkAndGetColumn<ColumnArray>(column))
        {
            const IColumn * nested_column = &col_from->getData();
            const ColumnArray::Offsets & offsets = col_from->getOffsets();
            const size_t nested_size = nested_column->size();

            ColumnUInt64::Container vec_temp(nested_size);
            executeAny<true>(nested_type, nested_column, vec_temp);

            const size_t size = offsets.size();

            ColumnArray::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ColumnArray::Offset next_offset = offsets[i];

                UInt64 h = IntHash64Impl::apply(next_offset - current_offset);
                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));

                for (size_t j = current_offset; j < next_offset; ++j)
                    vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], vec_temp[j]));

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConst * col_from = checkAndGetColumnConst<ColumnArray>(column))
        {
            /// NOTE: here, of course, you can do without the materialization of the column.
            ColumnPtr full_column = col_from->convertToFullColumn();
            executeArray<first>(type, &*full_column, vec_to);
        }
        else
            throw Exception("Illegal column " + column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeAny(const IDataType * from_type, const IColumn * icolumn, ColumnUInt64::Container & vec_to)
    {
        if      (checkDataType<DataTypeUInt8>(from_type)) executeIntType<UInt8, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt16>(from_type)) executeIntType<UInt16, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt32>(from_type)) executeIntType<UInt32, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt64>(from_type)) executeIntType<UInt64, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt8>(from_type)) executeIntType<Int8, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt16>(from_type)) executeIntType<Int16, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt32>(from_type)) executeIntType<Int32, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt64>(from_type)) executeIntType<Int64, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeEnum8>(from_type)) executeIntType<Int8, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeEnum16>(from_type)) executeIntType<Int16, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeDate>(from_type)) executeIntType<UInt16, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeDateTime>(from_type)) executeIntType<UInt32, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeFloat32>(from_type)) executeIntType<Float32, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeFloat64>(from_type)) executeIntType<Float64, first>(icolumn, vec_to);
        else if (checkDataType<DataTypeString>(from_type)) executeString<first>(icolumn, vec_to);
        else if (checkDataType<DataTypeFixedString>(from_type)) executeString<first>(icolumn, vec_to);
        else if (checkDataType<DataTypeArray>(from_type)) executeArray<first>(from_type, icolumn, vec_to);
        else
            throw Exception("Unexpected type " + from_type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void executeForArgument(const IDataType * type, const IColumn * column, ColumnUInt64::Container & vec_to, bool & is_first)
    {
        /// Flattening of tuples.
        if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(column))
        {
            const Columns & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                executeForArgument(tuple_types[i].get(), tuple_columns[i].get(), vec_to, is_first);
        }
        else if (const ColumnTuple * tuple = checkAndGetColumnConstData<ColumnTuple>(column))
        {
            const Columns & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
            {
                auto tmp = ColumnConst::create(tuple_columns[i], column->size());
                executeForArgument(tuple_types[i].get(), tmp.get(), vec_to, is_first);
            }
        }
        else
        {
            if (is_first)
                executeAny<true>(type, column, vec_to);
            else
                executeAny<false>(type, column, vec_to);
        }

        is_first = false;
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t rows = input_rows_count;
        auto col_to = ColumnUInt64::create(rows);

        ColumnUInt64::Container & vec_to = col_to->getData();

        if (arguments.empty())
        {
            /// Constant random number from /dev/urandom is used as a hash value of empty list of arguments.
            vec_to.assign(rows, static_cast<UInt64>(0xe28dbde7fe22e41c));
        }

        /// The function supports arbitary number of arguments of arbitary types.

        bool is_first_argument = true;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const ColumnWithTypeAndName & col = block.getByPosition(arguments[i]);
            executeForArgument(col.type.get(), col.column.get(), vec_to, is_first_argument);
        }

        block.getByPosition(result).column = std::move(col_to);
    }
};


template <typename Impl, typename Name>
class FunctionStringHash : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringHash>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments */) const override
    { return std::make_shared<DataTypeNumber<ToType>>(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);
        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        const ColumnWithTypeAndName & col = block.getByPosition(arguments[0]);
        const IDataType * from_type = col.type.get();
        const IColumn * icolumn = col.column.get();

        if      (checkDataType<DataTypeUInt8>(from_type)) executeIntType<UInt8>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt16>(from_type)) executeIntType<UInt16>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt32>(from_type)) executeIntType<UInt32>(icolumn, vec_to);
        else if (checkDataType<DataTypeUInt64>(from_type)) executeIntType<UInt64>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt8>(from_type)) executeIntType<Int8>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt16>(from_type)) executeIntType<Int16>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt32>(from_type)) executeIntType<Int32>(icolumn, vec_to);
        else if (checkDataType<DataTypeInt64>(from_type)) executeIntType<Int64>(icolumn, vec_to);
        else if (checkDataType<DataTypeEnum8>(from_type)) executeIntType<Int8>(icolumn, vec_to);
        else if (checkDataType<DataTypeEnum16>(from_type)) executeIntType<Int16>(icolumn, vec_to);
        else if (checkDataType<DataTypeDate>(from_type)) executeIntType<UInt16>(icolumn, vec_to);
        else if (checkDataType<DataTypeDateTime>(from_type)) executeIntType<UInt32>(icolumn, vec_to);
        else if (checkDataType<DataTypeFloat32>(from_type)) executeIntType<Float32>(icolumn, vec_to);
        else if (checkDataType<DataTypeFloat64>(from_type)) executeIntType<Float64>(icolumn, vec_to);
        else if (checkDataType<DataTypeString>(from_type)) executeString(icolumn, vec_to);
        else if (checkDataType<DataTypeFixedString>(from_type)) executeString(icolumn, vec_to);
        else
            throw Exception("Unexpected type " + from_type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(col_to);
    }
private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType>
    void executeIntType(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to)
    {
        if (const ColumnVector<FromType> * col_from = checkAndGetColumn<ColumnVector<FromType>>(column))
        {
            const typename ColumnVector<FromType>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                vec_to[i] = Impl::apply(reinterpret_cast<const char *>(&vec_from[i]), sizeof(FromType));
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    void executeString(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to)
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
        {
            const typename ColumnString::Chars_t & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                vec_to[i] = Impl::apply(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnFixedString * col_from = checkAndGetColumn<ColumnFixedString>(column))
        {
            const typename ColumnString::Chars_t & data = col_from->getChars();
            size_t n = col_from->getN();
            size_t size = data.size() / n;
            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Impl::apply(reinterpret_cast<const char *>(&data[i * n]), n);
        }
        else
            throw Exception("Illegal column " + column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** Why we need MurmurHash2?
  * MurmurHash2 is an outdated hash function, superseded by MurmurHash3 and subsequently by CityHash, xxHash, HighwayHash.
  * Usually there is no reason to use MurmurHash.
  * It is needed for the cases when you already have MurmurHash in some applications and you want to reproduce it
  * in ClickHouse as is. For example, it is needed to reproduce the behaviour
  * for NGINX a/b testing module: https://nginx.ru/en/docs/http/ngx_http_split_clients_module.html
  */
struct MurmurHash2Impl32
{
    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        return MurmurHash2(data, size, 0);
    }
};

struct MurmurHash2Impl64
{
    using ReturnType = UInt64;

    static UInt64 apply(const char * data, const size_t size)
    {
        return MurmurHash64A(data, size, 0);
    }
};

struct MurmurHash3Impl32
{
    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        union
        {
            UInt32 h;
            char bytes[sizeof(h)];
        };
        MurmurHash3_x86_32(data, size, 0, bytes);
        return h;
    }
};

struct MurmurHash3Impl64
{
    using ReturnType = UInt64;

    static UInt64 apply(const char * data, const size_t size)
    {
        union
        {
            UInt64 h[2];
            char bytes[16];
        };
        MurmurHash3_x64_128(data, size, 0, bytes);
        return h[0] ^ h[1];
    }
};

struct MurmurHash3Impl128
{
    static constexpr auto name = "murmurHash3_128";
    enum { length = 16 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        MurmurHash3_x64_128(begin, size, 0, out_char_data);
    }
};


struct URLHashImpl
{
    static UInt64 apply(const char * data, const size_t size)
    {
        /// do not take last slash, '?' or '#' character into account
        if (size > 0 && (data[size - 1] == '/' || data[size - 1] == '?' || data[size - 1] == '#'))
            return CityHash_v1_0_2::CityHash64(data, size - 1);

        return CityHash_v1_0_2::CityHash64(data, size);
    }
};


struct URLHierarchyHashImpl
{
    static size_t findLevelLength(const UInt64 level, const char * begin, const char * end)
    {
        auto pos = begin;

        /// Let's parse everything that goes before the path

        /// Suppose that the protocol has already been changed to lowercase.
        while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
            ++pos;

        /** We will calculate the hierarchy only for URLs in which there is a protocol, and after it there are two slashes.
        *    (http, file - fit, mailto, magnet - do not fit), and after two slashes there is still something
        *    For the rest, simply return the full URL as the only element of the hierarchy.
        */
        if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
        {
            pos = end;
            return 0 == level ? pos - begin : 0;
        }

        /// The domain for simplicity is everything that after the protocol and the two slashes, until the next slash or before `?` or `#`
        while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;

        if (pos != end)
            ++pos;

        if (0 == level)
            return pos - begin;

        UInt64 current_level = 0;

        while (current_level != level && pos < end)
        {
            /// We go to the next `/` or `?` or `#`, skipping all at the beginning.
            while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;
            if (pos == end)
                break;
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            if (pos != end)
                ++pos;

            ++current_level;
        }

        return current_level == level ? pos - begin : 0;
    }

    static UInt64 apply(const UInt64 level, const char * data, const size_t size)
    {
        return URLHashImpl::apply(data, findLevelLength(level, data, data + size));
    }
};


class FunctionURLHash : public IFunction
{
public:
    static constexpr auto name = "URLHash";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionURLHash>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto arg_count = arguments.size();
        if (arg_count != 1 && arg_count != 2)
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed " +
                toString(arg_count) + ", should be 1 or 2.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto first_arg = arguments.front().get();
        if (!checkDataType<DataTypeString>(first_arg))
            throw Exception{"Illegal type " + first_arg->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (arg_count == 2)
        {
            const auto second_arg = arguments.back().get();
            if (!second_arg->isInteger())
                throw Exception{"Illegal type " + second_arg->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto arg_count = arguments.size();

        if (arg_count == 1)
            executeSingleArg(block, arguments, result);
        else if (arg_count == 2)
            executeTwoArgs(block, arguments, result);
        else
            throw Exception{"got into IFunction::execute with unexpected number of arguments", ErrorCodes::LOGICAL_ERROR};
    }

private:
    void executeSingleArg(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const auto col_untyped = block.getByPosition(arguments.front()).column.get();

        if (const auto col_from = checkAndGetColumn<ColumnString>(col_untyped))
        {
            const auto size = col_from->size();
            auto col_to = ColumnUInt64::create(size);

            const auto & chars = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            auto & out = col_to->getData();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                out[i] = URLHashImpl::apply(
                    reinterpret_cast<const char *>(&chars[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName() +
                " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    void executeTwoArgs(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        const auto level_col = block.getByPosition(arguments.back()).column.get();
        if (!level_col->isColumnConst())
            throw Exception{"Second argument of function " + getName() + " must be an integral constant", ErrorCodes::ILLEGAL_COLUMN};

        const auto level = level_col->get64(0);

        const auto col_untyped = block.getByPosition(arguments.front()).column.get();
        if (const auto col_from = checkAndGetColumn<ColumnString>(col_untyped))
        {
            const auto size = col_from->size();
            auto col_to = ColumnUInt64::create(size);

            const auto & chars = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            auto & out = col_to->getData();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                out[i] = URLHierarchyHashImpl::apply(
                    level,
                    reinterpret_cast<const char *>(&chars[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName() +
                " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }
};


struct NameHalfMD5   { static constexpr auto name = "halfMD5"; };
struct NameSipHash64 { static constexpr auto name = "sipHash64"; };
struct NameIntHash32 { static constexpr auto name = "intHash32"; };
struct NameIntHash64 { static constexpr auto name = "intHash64"; };
struct NameMurmurHash2_32 { static constexpr auto name = "murmurHash2_32"; };
struct NameMurmurHash2_64 { static constexpr auto name = "murmurHash2_64"; };
struct NameMurmurHash3_32 { static constexpr auto name = "murmurHash3_32"; };
struct NameMurmurHash3_64 { static constexpr auto name = "murmurHash3_64"; };
struct NameMurmurHash3_128 { static constexpr auto name = "murmurHash3_128"; };


struct ImplCityHash64
{
    static constexpr auto name = "cityHash64";
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto Hash128to64(const uint128_t & x) { return CityHash_v1_0_2::Hash128to64(x); }
    static auto Hash64(const char * s, const size_t len) { return CityHash_v1_0_2::CityHash64(s, len); }
};

// see farmhash.h for definition of NAMESPACE_FOR_HASH_FUNCTIONS
struct ImplFarmHash64
{
    static constexpr auto name = "farmHash64";
    using uint128_t = NAMESPACE_FOR_HASH_FUNCTIONS::uint128_t;

    static auto Hash128to64(const uint128_t & x) { return NAMESPACE_FOR_HASH_FUNCTIONS::Hash128to64(x); }
    static auto Hash64(const char * s, const size_t len) { return NAMESPACE_FOR_HASH_FUNCTIONS::Hash64(s, len); }
};

struct ImplMetroHash64
{
    static constexpr auto name = "metroHash64";
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto Hash128to64(const uint128_t & x) { return CityHash_v1_0_2::Hash128to64(x); }
    static auto Hash64(const char * s, const size_t len)
    {
        union
        {
            UInt64 u64;
            UInt8 u8[sizeof(u64)];
        };

        metrohash64_1(reinterpret_cast<const UInt8 *>(s), len, 0, u8);

        return u64;
    }
};

using FunctionHalfMD5 = FunctionStringHash<HalfMD5Impl, NameHalfMD5>;
using FunctionSipHash64 = FunctionStringHash<SipHash64Impl, NameSipHash64>;
using FunctionIntHash32 = FunctionIntHash<IntHash32Impl, NameIntHash32>;
using FunctionIntHash64 = FunctionIntHash<IntHash64Impl, NameIntHash64>;
using FunctionMD5 = FunctionStringHashFixedString<MD5Impl>;
using FunctionSHA1 = FunctionStringHashFixedString<SHA1Impl>;
using FunctionSHA224 = FunctionStringHashFixedString<SHA224Impl>;
using FunctionSHA256 = FunctionStringHashFixedString<SHA256Impl>;
using FunctionSipHash128 = FunctionStringHashFixedString<SipHash128Impl>;
using FunctionCityHash64 = FunctionNeighbourhoodHash64<ImplCityHash64>;
using FunctionFarmHash64 = FunctionNeighbourhoodHash64<ImplFarmHash64>;
using FunctionMetroHash64 = FunctionNeighbourhoodHash64<ImplMetroHash64>;
using FunctionMurmurHash2_32 = FunctionStringHash<MurmurHash2Impl32, NameMurmurHash2_32>;
using FunctionMurmurHash2_64 = FunctionStringHash<MurmurHash2Impl64, NameMurmurHash2_64>;
using FunctionMurmurHash3_32 = FunctionStringHash<MurmurHash3Impl32, NameMurmurHash3_32>;
using FunctionMurmurHash3_64 = FunctionStringHash<MurmurHash3Impl64, NameMurmurHash3_64>;
using FunctionMurmurHash3_128 = FunctionStringHashFixedString<MurmurHash3Impl128>;
}
