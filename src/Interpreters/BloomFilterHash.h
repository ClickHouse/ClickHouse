#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/bit_cast.h>
#include <Common/HashTable/Hash.h>
#include <Interpreters/BloomFilter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

struct BloomFilterHash
{
    static constexpr UInt64 bf_hash_seed[15] = {
        13635471485423070496ULL, 10336109063487487899ULL, 17779957404565211594ULL, 8988612159822229247ULL, 4954614162757618085ULL,
        12980113590177089081ULL, 9263883436177860930ULL, 3656772712723269762ULL, 10362091744962961274ULL, 7582936617938287249ULL,
        15033938188484401405ULL, 18286745649494826751ULL, 6852245486148412312ULL, 8886056245089344681ULL, 10151472371158292780ULL
    };

    template <typename FieldGetType, typename FieldType>
    static UInt64 getNumberTypeHash(const Field & field)
    {
        /// For negative, we should convert the type to make sure the symbol is in right place
        return field.isNull() ? intHash64(0) : intHash64(ext::bit_cast<UInt64>(FieldType(field.safeGet<FieldGetType>())));
    }

    static UInt64 getStringTypeHash(const Field & field)
    {
        if (!field.isNull())
        {
            const auto & value = field.safeGet<String>();
            return CityHash_v1_0_2::CityHash64(value.data(), value.size());
        }

        return CityHash_v1_0_2::CityHash64("", 0);
    }

    static UInt64 getFixedStringTypeHash(const Field & field, const IDataType * type)
    {
        if (!field.isNull())
        {
            const auto & value = field.safeGet<String>();
            return CityHash_v1_0_2::CityHash64(value.data(), value.size());
        }

        const auto * fixed_string_type = typeid_cast<const DataTypeFixedString *>(type);
        const std::vector<char> value(fixed_string_type->getN(), 0);
        return CityHash_v1_0_2::CityHash64(value.data(), value.size());
    }

    static ColumnPtr hashWithField(const IDataType * data_type, const Field & field)
    {
        const auto & build_hash_column = [&](const UInt64 & hash) -> ColumnPtr
        {
            return ColumnConst::create(ColumnUInt64::create(1, hash), 1);
        };


        WhichDataType which(data_type);

        if (which.isUInt8()) return build_hash_column(getNumberTypeHash<UInt64, UInt8>(field));
        else if (which.isUInt16()) return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        else if (which.isUInt32()) return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        else if (which.isUInt64()) return build_hash_column(getNumberTypeHash<UInt64, UInt64>(field));
        else if (which.isInt8()) return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        else if (which.isInt16()) return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        else if (which.isInt32()) return build_hash_column(getNumberTypeHash<Int64, Int32>(field));
        else if (which.isInt64()) return build_hash_column(getNumberTypeHash<Int64, Int64>(field));
        else if (which.isEnum8()) return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        else if (which.isEnum16()) return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        else if (which.isDate()) return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        else if (which.isDateTime()) return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        else if (which.isFloat32()) return build_hash_column(getNumberTypeHash<Float64, Float64>(field));
        else if (which.isFloat64()) return build_hash_column(getNumberTypeHash<Float64, Float64>(field));
        else if (which.isString()) return build_hash_column(getStringTypeHash(field));
        else if (which.isFixedString()) return build_hash_column(getFixedStringTypeHash(field, data_type));
        else throw Exception("Unexpected type " + data_type->getName() + " of bloom filter index.", ErrorCodes::BAD_ARGUMENTS);
    }

    static ColumnPtr hashWithColumn(const DataTypePtr & data_type, const ColumnPtr & column, size_t pos, size_t limit)
    {
        WhichDataType which(data_type);
        if (which.isArray())
        {
            const auto * array_col = typeid_cast<const ColumnArray *>(column.get());

            if (checkAndGetColumn<ColumnNullable>(array_col->getData()))
                throw Exception("Unexpected type " + data_type->getName() + " of bloom filter index.", ErrorCodes::BAD_ARGUMENTS);

            const auto & offsets = array_col->getOffsets();
            limit = offsets[pos + limit - 1] - offsets[pos - 1];    /// PaddedPODArray allows access on index -1.
            pos = offsets[pos - 1];

            if (limit == 0)
            {
                auto index_column = ColumnUInt64::create(1);
                ColumnUInt64::Container & index_column_vec = index_column->getData();
                index_column_vec[0] = 0;
                return index_column;
            }
        }

        const ColumnPtr actual_col = BloomFilter::getPrimitiveColumn(column);
        const DataTypePtr actual_type = BloomFilter::getPrimitiveType(data_type);

        auto index_column = ColumnUInt64::create(limit);
        ColumnUInt64::Container & index_column_vec = index_column->getData();
        getAnyTypeHash<true>(actual_type.get(), actual_col.get(), index_column_vec, pos);
        return index_column;
    }

    template <bool is_first>
    static void getAnyTypeHash(const IDataType * data_type, const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        WhichDataType which(data_type);

        if (which.isUInt8()) getNumberTypeHash<UInt8, is_first>(column, vec, pos);
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
        else throw Exception("Unexpected type " + data_type->getName() + " of bloom filter index.", ErrorCodes::BAD_ARGUMENTS);
    }

    template <typename Type, bool is_first>
    static void getNumberTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        const auto * index_column = typeid_cast<const ColumnVector<Type> *>(column);

        if (unlikely(!index_column))
            throw Exception("Illegal column type was passed to the bloom filter index.", ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<Type>::Container & vec_from = index_column->getData();

        /// Because we're missing the precision of float in the Field.h
        /// to be consistent, we need to convert Float32 to Float64 processing, also see: BloomFilterHash::hashWithField
        if constexpr (std::is_same_v<ColumnVector<Type>, ColumnFloat32>)
        {
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 hash = intHash64(ext::bit_cast<UInt64>(Float64(vec_from[index + pos])));

                if constexpr (is_first)
                    vec[index] = hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash));
            }
        }
        else
        {
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 hash = intHash64(ext::bit_cast<UInt64>(vec_from[index + pos]));

                if constexpr (is_first)
                    vec[index] = hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash));
            }
        }
    }

    template <bool is_first>
    static void getStringTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        if (const auto * index_column = typeid_cast<const ColumnString *>(column))
        {
            const ColumnString::Chars & data = index_column->getChars();
            const ColumnString::Offsets & offsets = index_column->getOffsets();

            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                ColumnString::Offset current_offset = offsets[index + pos - 1];
                size_t length = offsets[index + pos] - current_offset - 1 /* terminating zero */;
                UInt64 city_hash = CityHash_v1_0_2::CityHash64(
                    reinterpret_cast<const char *>(&data[current_offset]), length);

                if constexpr (is_first)
                    vec[index] = city_hash;
                else
                    vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], city_hash));
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

    static std::pair<size_t, size_t> calculationBestPractices(double max_conflict_probability)
    {
        static const size_t MAX_BITS_PER_ROW = 20;
        static const size_t MAX_HASH_FUNCTION_COUNT = 15;

        /// For the smallest index per level in probability_lookup_table
        static const size_t min_probability_index_each_bits[] = {0, 0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14};

        static const long double probability_lookup_table[MAX_BITS_PER_ROW + 1][MAX_HASH_FUNCTION_COUNT] =
            {
                {1.0},  /// dummy, 0 bits per row
                {1.0, 1.0},
                {1.0, 0.393,  0.400},
                {1.0, 0.283,  0.237,   0.253},
                {1.0, 0.221,  0.155,   0.147,   0.160},
                {1.0, 0.181,  0.109,   0.092,   0.092,   0.101}, // 5
                {1.0, 0.154,  0.0804,  0.0609,  0.0561,  0.0578,   0.0638},
                {1.0, 0.133,  0.0618,  0.0423,  0.0359,  0.0347,   0.0364},
                {1.0, 0.118,  0.0489,  0.0306,  0.024,   0.0217,   0.0216,   0.0229},
                {1.0, 0.105,  0.0397,  0.0228,  0.0166,  0.0141,   0.0133,   0.0135,   0.0145},
                {1.0, 0.0952, 0.0329,  0.0174,  0.0118,  0.00943,  0.00844,  0.00819,  0.00846}, // 10
                {1.0, 0.0869, 0.0276,  0.0136,  0.00864, 0.0065,   0.00552,  0.00513,  0.00509},
                {1.0, 0.08,   0.0236,  0.0108,  0.00646, 0.00459,  0.00371,  0.00329,  0.00314},
                {1.0, 0.074,  0.0203,  0.00875, 0.00492, 0.00332,  0.00255,  0.00217,  0.00199,  0.00194},
                {1.0, 0.0689, 0.0177,  0.00718, 0.00381, 0.00244,  0.00179,  0.00146,  0.00129,  0.00121,  0.0012},
                {1.0, 0.0645, 0.0156,  0.00596, 0.003,   0.00183,  0.00128,  0.001,    0.000852, 0.000775, 0.000744}, // 15
                {1.0, 0.0606, 0.0138,  0.005,   0.00239, 0.00139,  0.000935, 0.000702, 0.000574, 0.000505, 0.00047,  0.000459},
                {1.0, 0.0571, 0.0123,  0.00423, 0.00193, 0.00107,  0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
                {1.0, 0.054,  0.0111,  0.00362, 0.00158, 0.000839, 0.000519, 0.00036,  0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
                {1.0, 0.0513, 0.00998, 0.00312, 0.0013,  0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
                {1.0, 0.0488, 0.00906, 0.0027,  0.00108, 0.00053,  0.000303, 0.000196, 0.00014,  0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
            };

        for (size_t bits_per_row = 1; bits_per_row < MAX_BITS_PER_ROW; ++bits_per_row)
        {
            if (probability_lookup_table[bits_per_row][min_probability_index_each_bits[bits_per_row]] <= max_conflict_probability)
            {
                size_t max_size_of_hash_functions = min_probability_index_each_bits[bits_per_row];
                for (size_t size_of_hash_functions = max_size_of_hash_functions; size_of_hash_functions > 0; --size_of_hash_functions)
                    if (probability_lookup_table[bits_per_row][size_of_hash_functions] > max_conflict_probability)
                        return std::pair<size_t, size_t>(bits_per_row, size_of_hash_functions + 1);
            }
        }

        return std::pair<size_t, size_t>(MAX_BITS_PER_ROW - 1, min_probability_index_each_bits[MAX_BITS_PER_ROW - 1]);
    }
};

}
