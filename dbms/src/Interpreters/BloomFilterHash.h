#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/bit_cast.h>
#include <Common/HashTable/Hash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

struct BloomFilterHash
{
    static constexpr UInt64 bf_hash_seed[15] = {
        13635471485423070496ULL, 10336109063487487899ULL, 17779957404565211594ULL, 8988612159822229247ULL, 4954614162757618085ULL,
        12980113590177089081ULL, 9263883436177860930ULL, 3656772712723269762ULL, 10362091744962961274ULL, 7582936617938287249ULL,
        15033938188484401405ULL, 18286745649494826751ULL, 6852245486148412312ULL, 8886056245089344681ULL, 10151472371158292780ULL
    };

    static ColumnPtr hashWithField(const IDataType * data_type, const Field & field)
    {
        WhichDataType which(data_type);

        if (which.isUInt())
            return ColumnConst::create(ColumnUInt64::create(1, intHash64(field.safeGet<UInt64>())), 1);
        else if (which.isInt())
            return ColumnConst::create(ColumnUInt64::create(1, intHash64(ext::bit_cast<UInt64>(field.safeGet<Int64>()))), 1);
        else if (which.isString() || which.isFixedString())
        {
            const auto & value = field.safeGet<String>();
            return ColumnConst::create(ColumnUInt64::create(1, CityHash_v1_0_2::CityHash64(value.data(), value.size())), 1);
        }
        else
            throw Exception("Unexpected type " + data_type->getName() + " of bloom filter index.", ErrorCodes::LOGICAL_ERROR);
    }

    static ColumnPtr hashWithColumn(const IDataType * data_type, const IColumn * column, size_t pos, size_t limit)
    {
        auto index_column = ColumnUInt64::create(limit);
        ColumnUInt64::Container & index_column_vec = index_column->getData();
        getAnyTypeHash<true>(data_type, column, index_column_vec, pos);
        return index_column;
    }

    template <bool is_first>
    static void getAnyTypeHash(const IDataType *data_type, const IColumn *column, ColumnUInt64::Container &vec, size_t pos)
    {
        WhichDataType which(data_type);

        if      (which.isUInt8()) getNumberTypeHash<UInt8, is_first>(column, vec, pos);
        else if (which.isUInt16()) getNumberTypeHash<UInt16, is_first>(column, vec, pos);
        else if (which.isUInt32()) getNumberTypeHash<UInt32, is_first>(column, vec, pos);
        else if (which.isUInt64()) getNumberTypeHash<UInt64, is_first>(column, vec, pos);
        else if (which.isInt8()) getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isInt16()) getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isInt32()) getNumberTypeHash<Int32, is_first>(column, vec, pos);
        else if (which.isInt64()) getNumberTypeHash<Int64, is_first>(column, vec, pos);
        else if (which.isEnum8()) getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isEnum16()) getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isDate()) getNumberTypeHash<UInt16, is_first>(column, vec, pos);
        else if (which.isDateTime()) getNumberTypeHash<UInt32, is_first>(column, vec, pos);
        else if (which.isFloat32()) getNumberTypeHash<Float32, is_first>(column, vec, pos);
        else if (which.isFloat64()) getNumberTypeHash<Float64, is_first>(column, vec, pos);
        else if (which.isString()) getStringTypeHash<is_first>(column, vec, pos);
        else if (which.isFixedString()) getStringTypeHash<is_first>(column, vec, pos);
        else throw Exception("Unexpected type " + data_type->getName() + " of bloom filter index.", ErrorCodes::LOGICAL_ERROR);
    }

    template <typename Type, bool is_first>
    static void getNumberTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        const auto * index_column = typeid_cast<const ColumnVector<Type> *>(column);

        if (unlikely(!index_column))
            throw Exception("Illegal column type was passed to the bloom filter index.", ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<Type>::Container & vec_from = index_column->getData();

        for (size_t index = 0, size = vec.size(); index < size; ++index)
        {
            UInt64 hash = intHash64(ext::bit_cast<UInt64>(vec_from[index + pos]));

            if constexpr (is_first)
                vec[index] = hash;
            else
                vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash));
        }
    }

    template <bool is_first>
    static void getStringTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        if (const auto * index_column = typeid_cast<const ColumnString *>(column))
        {
            const ColumnString::Chars & data = index_column->getChars();
            const ColumnString::Offsets & offsets = index_column->getOffsets();

            ColumnString::Offset current_offset = pos;
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 city_hash = CityHash_v1_0_2::CityHash64(
                    reinterpret_cast<const char *>(&data[current_offset]), offsets[index + pos] - current_offset - 1);

                if constexpr (is_first)
                    vec[index] = city_hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], city_hash));

                current_offset = offsets[index + pos];
            }
        }
        else if (const auto * fixed_string_index_column = typeid_cast<const ColumnFixedString *>(column))
        {
            size_t fixed_len = fixed_string_index_column->getN();
            const auto & data = fixed_string_index_column->getChars();

            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 city_hash = CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(&data[(index + pos) * fixed_len]), fixed_len);

                if constexpr (is_first)
                    vec[index] = city_hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], city_hash));
            }
        }
        else
            throw Exception("Illegal column type was passed to the bloom filter index.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
