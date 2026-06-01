#pragma once

#include <Common/HashTable/Hash.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
        return field.isNull() ? intHash64(0) : DefaultHash64<FieldType>(FieldType(field.safeGet<FieldGetType>()));
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

        if (which.isUInt8())
            return build_hash_column(getNumberTypeHash<UInt64, UInt8>(field));
        if (which.isUInt16())
            return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        if (which.isUInt32())
            return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        if (which.isUInt64())
            return build_hash_column(getNumberTypeHash<UInt64, UInt64>(field));
        if (which.isUInt128())
            return build_hash_column(getNumberTypeHash<UInt128, UInt256>(field));
        if (which.isUInt256())
            return build_hash_column(getNumberTypeHash<UInt256, UInt256>(field));
        if (which.isInt8())
            return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        if (which.isInt16())
            return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        if (which.isInt32())
            return build_hash_column(getNumberTypeHash<Int64, Int32>(field));
        if (which.isInt64())
            return build_hash_column(getNumberTypeHash<Int64, Int64>(field));
        if (which.isInt128())
            return build_hash_column(getNumberTypeHash<Int128, Int128>(field));
        if (which.isInt256())
            return build_hash_column(getNumberTypeHash<Int256, Int256>(field));
        if (which.isEnum8())
            return build_hash_column(getNumberTypeHash<Int64, Int8>(field));
        if (which.isEnum16())
            return build_hash_column(getNumberTypeHash<Int64, Int16>(field));
        if (which.isDate())
            return build_hash_column(getNumberTypeHash<UInt64, UInt16>(field));
        if (which.isDate32())
            return build_hash_column(getNumberTypeHash<UInt64, Int32>(field));
        if (which.isDateTime())
            return build_hash_column(getNumberTypeHash<UInt64, UInt32>(field));
        if (which.isDateTime64())
            return build_hash_column(getNumberTypeHash<DateTime64, DateTime64>(field));
        if (which.isFloat32())
            return build_hash_column(getNumberTypeHash<Float64, Float64>(field));
        if (which.isFloat64())
            return build_hash_column(getNumberTypeHash<Float64, Float64>(field));
        if (which.isUUID())
            return build_hash_column(getNumberTypeHash<UUID, UUID>(field));
        if (which.isIPv4())
            return build_hash_column(getNumberTypeHash<IPv4, IPv4>(field));
        if (which.isIPv6())
            return build_hash_column(getNumberTypeHash<IPv6, IPv6>(field));
        if (which.isString())
            return build_hash_column(getStringTypeHash(field));
        if (which.isFixedString())
            return build_hash_column(getFixedStringTypeHash(field, data_type));

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());
    }

    static ColumnPtr hashWithColumn(const DataTypePtr & data_type, const ColumnPtr & column, size_t pos, size_t limit)
    {
        WhichDataType which(data_type);
        if (which.isArray())
        {
            const auto * array_col = typeid_cast<const ColumnArray *>(column.get());

            if (checkAndGetColumn<ColumnNullable>(&array_col->getData()))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());

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
        else if (which.isUInt128()) getNumberTypeHash<UInt128, is_first>(column, vec, pos);
        else if (which.isUInt256()) getNumberTypeHash<UInt256, is_first>(column, vec, pos);
        else if (which.isInt8()) getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isInt16()) getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isInt32()) getNumberTypeHash<Int32, is_first>(column, vec, pos);
        else if (which.isInt64()) getNumberTypeHash<Int64, is_first>(column, vec, pos);
        else if (which.isInt128()) getNumberTypeHash<Int128, is_first>(column, vec, pos);
        else if (which.isInt256()) getNumberTypeHash<Int256, is_first>(column, vec, pos);
        else if (which.isEnum8()) getNumberTypeHash<Int8, is_first>(column, vec, pos);
        else if (which.isEnum16()) getNumberTypeHash<Int16, is_first>(column, vec, pos);
        else if (which.isDate()) getNumberTypeHash<UInt16, is_first>(column, vec, pos);
        else if (which.isDate32()) getNumberTypeHash<Int32, is_first>(column, vec, pos);
        else if (which.isDateTime()) getNumberTypeHash<UInt32, is_first>(column, vec, pos);
        else if (which.isDateTime64()) getDecimalTypeHash<DateTime64, is_first>(column, vec, pos);
        else if (which.isFloat32()) getNumberTypeHash<Float32, is_first>(column, vec, pos);
        else if (which.isFloat64()) getNumberTypeHash<Float64, is_first>(column, vec, pos);
        else if (which.isUUID()) getNumberTypeHash<UUID, is_first>(column, vec, pos);
        else if (which.isIPv4()) getNumberTypeHash<IPv4, is_first>(column, vec, pos);
        else if (which.isIPv6()) getNumberTypeHash<IPv6, is_first>(column, vec, pos);
        else if (which.isString()) getStringTypeHash<is_first>(column, vec, pos);
        else if (which.isFixedString()) getStringTypeHash<is_first>(column, vec, pos);
        else throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());
    }

    template <typename Type, bool is_first>
    static void getDecimalTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        const auto * index_column = typeid_cast<const ColumnDecimal<Type> *>(column);

        if (unlikely(!index_column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type was passed to the bloom filter index.");

        const typename ColumnDecimal<Type>::Container & vec_from = index_column->getData();

        for (size_t index = 0, size = vec.size(); index < size; ++index)
        {
            UInt64 hash_value = DefaultHash64<Type>(Type(vec_from[index + pos]));

            if constexpr (is_first)
                vec[index] = hash_value;
            else
                vec[index] = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(vec[index], hash_value));
        }
    }

    template <typename Type, bool is_first>
    static void getNumberTypeHash(const IColumn * column, ColumnUInt64::Container & vec, size_t pos)
    {
        const auto * index_column = typeid_cast<const ColumnVector<Type> *>(column);

        if (unlikely(!index_column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} was passed to the bloom filter index", column->getName());

        const typename ColumnVector<Type>::Container & vec_from = index_column->getData();

        /// Because we're missing the precision of float in the Field.h
        /// to be consistent, we need to convert Float32 to Float64 processing, also see: BloomFilterHash::hashWithField
        if constexpr (std::is_same_v<ColumnVector<Type>, ColumnFloat32>)
        {
            for (size_t index = 0, size = vec.size(); index < size; ++index)
            {
                UInt64 hash = DefaultHash64<Float64>(Float64(vec_from[index + pos]));

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
                UInt64 hash = DefaultHash64<Type>(vec_from[index + pos]);

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
                size_t length = offsets[index + pos] - current_offset;
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
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column type was passed to the bloom filter index.");
    }

    static std::pair<size_t, size_t> calculationBestPractices(double false_positive_rate)
    {
        static const size_t MAX_BITS_PER_ROW = 20;
        static const size_t MAX_HASH_FUNCTION_COUNT = 15;

        static const size_t MIN_BITS_PER_ROW = 2;
        static const size_t MIN_HASH_FUNCTION_COUNT = 2;

        /// Return the smaller possible parameters for false positive rates higher or equal than 0.283
        /// Otherwise, for those rates the loop won't find any possible values in the lookup table
        /// returning bits_per_row = 19 & size_of_hash_functions = 13. Which are the most restrictive values
        /// to be used with the smallest false positive rates.
        if (false_positive_rate >= 0.283)
            return std::pair<size_t, size_t>(MIN_BITS_PER_ROW, MIN_HASH_FUNCTION_COUNT);

        /// For the smallest index per level in probability_lookup_table
        static const size_t min_probability_index_each_bits[] = {0, 0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14};

        static const long double probability_lookup_table[MAX_BITS_PER_ROW + 1][MAX_HASH_FUNCTION_COUNT] =
            {
                {1.0L},  /// dummy, 0 bits per row
                {1.0L, 1.0L},
                {1.0L, 0.393L,  0.400L},
                {1.0L, 0.283L,  0.237L,   0.253L},
                {1.0L, 0.221L,  0.155L,   0.147L,   0.160L},
                {1.0L, 0.181L,  0.109L,   0.092L,   0.092L,   0.101L}, // 5
                {1.0L, 0.154L,  0.0804L,  0.0609L,  0.0561L,  0.0578L,   0.0638L},
                {1.0L, 0.133L,  0.0618L,  0.0423L,  0.0359L,  0.0347L,   0.0364L},
                {1.0L, 0.118L,  0.0489L,  0.0306L,  0.024L,   0.0217L,   0.0216L,   0.0229L},
                {1.0L, 0.105L,  0.0397L,  0.0228L,  0.0166L,  0.0141L,   0.0133L,   0.0135L,   0.0145L},
                {1.0L, 0.0952L, 0.0329L,  0.0174L,  0.0118L,  0.00943L,  0.00844L,  0.00819L,  0.00846L}, // 10
                {1.0L, 0.0869L, 0.0276L,  0.0136L,  0.00864L, 0.0065L,   0.00552L,  0.00513L,  0.00509L},
                {1.0L, 0.08L,   0.0236L,  0.0108L,  0.00646L, 0.00459L,  0.00371L,  0.00329L,  0.00314L},
                {1.0L, 0.074L,  0.0203L,  0.00875L, 0.00492L, 0.00332L,  0.00255L,  0.00217L,  0.00199L,  0.00194L},
                {1.0L, 0.0689L, 0.0177L,  0.00718L, 0.00381L, 0.00244L,  0.00179L,  0.00146L,  0.00129L,  0.00121L,  0.0012L},
                {1.0L, 0.0645L, 0.0156L,  0.00596L, 0.003L,   0.00183L,  0.00128L,  0.001L,    0.000852L, 0.000775L, 0.000744L}, // 15
                {1.0L, 0.0606L, 0.0138L,  0.005L,   0.00239L, 0.00139L,  0.000935L, 0.000702L, 0.000574L, 0.000505L, 0.00047L,  0.000459L},
                {1.0L, 0.0571L, 0.0123L,  0.00423L, 0.00193L, 0.00107L,  0.000692L, 0.000499L, 0.000394L, 0.000335L, 0.000302L, 0.000287L, 0.000284L},
                {1.0L, 0.054L,  0.0111L,  0.00362L, 0.00158L, 0.000839L, 0.000519L, 0.00036L,  0.000275L, 0.000226L, 0.000198L, 0.000183L, 0.000176L},
                {1.0L, 0.0513L, 0.00998L, 0.00312L, 0.0013L,  0.000663L, 0.000394L, 0.000264L, 0.000194L, 0.000155L, 0.000132L, 0.000118L, 0.000111L, 0.000109L},
                {1.0L, 0.0488L, 0.00906L, 0.0027L,  0.00108L, 0.00053L,  0.000303L, 0.000196L, 0.00014L,  0.000108L, 8.89e-05L, 7.77e-05L, 7.12e-05L, 6.79e-05L, 6.71e-05L} // 20
            };

        for (size_t bits_per_row = 1; bits_per_row < MAX_BITS_PER_ROW; ++bits_per_row)
        {
            if (probability_lookup_table[bits_per_row][min_probability_index_each_bits[bits_per_row]] <= static_cast<long double>(false_positive_rate))
            {
                size_t max_size_of_hash_functions = min_probability_index_each_bits[bits_per_row];
                for (size_t size_of_hash_functions = max_size_of_hash_functions; size_of_hash_functions > 0; --size_of_hash_functions)
                    if (probability_lookup_table[bits_per_row][size_of_hash_functions] > static_cast<long double>(false_positive_rate))
                        return std::pair<size_t, size_t>(bits_per_row, size_of_hash_functions + 1);
            }
        }

        return std::pair<size_t, size_t>(MAX_BITS_PER_ROW - 1, min_probability_index_each_bits[MAX_BITS_PER_ROW - 1]);
    }
};

}
