#include <Interpreters/AggregationMethod.h>

namespace DB
{
template <typename FieldType, typename TData, bool consecutive_keys_optimization, bool nullable>
void AggregationMethodOneNumber<FieldType, TData, consecutive_keys_optimization, nullable>::insertKeyIntoColumns(
    const AggregationMethodOneNumber::Key & key, std::vector<IColumn *> & key_columns, const Sizes & /*key_sizes*/)
{
    ColumnFixedSizeHelper * column;
    if constexpr (nullable)
    {
        ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[0]);
        ColumnUInt8 * null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
        null_map->insertDefault();
        column = static_cast<ColumnFixedSizeHelper *>(&nullable_col.getNestedColumn());
    }
    else
    {
        column = static_cast<ColumnFixedSizeHelper *>(key_columns[0]);
    }
    static_assert(sizeof(FieldType) <= sizeof(Key));
    const auto * key_holder = reinterpret_cast<const char *>(&key);
    if constexpr (sizeof(FieldType) < sizeof(Key) && std::endian::native == std::endian::big)
        column->insertRawData<sizeof(FieldType)>(key_holder + (sizeof(Key) - sizeof(FieldType)));
    else
        column->insertRawData<sizeof(FieldType)>(key_holder);
}

template struct AggregationMethodOneNumber<UInt8, AggregatedDataWithUInt8Key, false>;
template struct AggregationMethodOneNumber<UInt16, AggregatedDataWithUInt16Key, false>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32Key>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64Key>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithUInt32KeyTwoLevel>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyTwoLevel>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithUInt64KeyHash64>;
template struct AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false, true>;
template struct AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false, true>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32Key, true, true>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key, true, true>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt32KeyTwoLevel, true, true>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel, true, true>;
template struct AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>;
template struct AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64Key>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>;
template struct AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64KeyTwoLevel>;
template struct AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel>;

template <typename TData, bool nullable>
void AggregationMethodStringNoCache<TData, nullable>::insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
{
    if constexpr (nullable)
    {
        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*key_columns[0]);
        assert_cast<ColumnString &>(column_nullable.getNestedColumn()).insertData(key.data, key.size);
        column_nullable.getNullMapData().push_back(0);
    }
    else
    {
        assert_cast<ColumnString &>(*key_columns[0]).insertData(key.data, key.size);
    }
}
template struct AggregationMethodStringNoCache<AggregatedDataWithShortStringKey>;
template struct AggregationMethodStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>;
template struct AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKey, true>;
template struct AggregationMethodStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>;

template <typename TData>
void AggregationMethodFixedString<TData>::insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
{
    assert_cast<ColumnFixedString &>(*key_columns[0]).insertData(key.data, key.size);
}
template struct AggregationMethodFixedString<AggregatedDataWithStringKeyHash64>;
template struct AggregationMethodFixedString<AggregatedDataWithNullableStringKey>;
template struct AggregationMethodFixedString<AggregatedDataWithNullableStringKeyTwoLevel>;


template <typename TData, bool nullable>
void AggregationMethodFixedStringNoCache<TData, nullable>::insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
{
    if constexpr (nullable)
        assert_cast<ColumnNullable &>(*key_columns[0]).insertData(key.data, key.size);
    else
        assert_cast<ColumnFixedString &>(*key_columns[0]).insertData(key.data, key.size);
}
template struct AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKey>;
template struct AggregationMethodFixedStringNoCache<AggregatedDataWithShortStringKeyTwoLevel>;
template struct AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKey, true>;
template struct AggregationMethodFixedStringNoCache<AggregatedDataWithNullableShortStringKeyTwoLevel, true>;


template <typename SingleColumnMethod>
void AggregationMethodSingleLowCardinalityColumn<SingleColumnMethod>::insertKeyIntoColumns(
    const Key & key, std::vector<IColumn *> & key_columns_low_cardinality, const Sizes & /*key_sizes*/)
{
    auto * col = assert_cast<ColumnLowCardinality *>(key_columns_low_cardinality[0]);

    if constexpr (std::is_same_v<Key, StringRef>)
        col->insertData(key.data, key.size);
    else
        col->insertData(reinterpret_cast<const char *>(&key), sizeof(key));
}
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt8, AggregatedDataWithNullableUInt8Key, false>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt16, AggregatedDataWithNullableUInt16Key, false>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64Key>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64Key>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKey>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKey>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt32, AggregatedDataWithNullableUInt64KeyTwoLevel>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodOneNumber<UInt64, AggregatedDataWithNullableUInt64KeyTwoLevel>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodString<AggregatedDataWithNullableStringKeyTwoLevel>>;
template struct AggregationMethodSingleLowCardinalityColumn<AggregationMethodFixedString<AggregatedDataWithNullableStringKeyTwoLevel>>;


template <typename TData, bool has_nullable_keys, bool has_low_cardinality, bool consecutive_keys_optimization>
void AggregationMethodKeysFixed<TData, has_nullable_keys, has_low_cardinality,consecutive_keys_optimization>::insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

    static constexpr auto bitmap_size = has_nullable_keys ? std::tuple_size<KeysNullMap<Key>>::value : 0;
    /// In any hash key value, column values to be read start just after the bitmap, if it exists.
    size_t pos = bitmap_size;

    for (size_t i = 0; i < keys_size; ++i)
    {
        IColumn * observed_column;
        ColumnUInt8 * null_map;

        bool column_nullable = false;
        if constexpr (has_nullable_keys)
            column_nullable = isColumnNullable(*key_columns[i]);

        /// If we have a nullable column, get its nested column and its null map.
        if (column_nullable)
        {
            ColumnNullable & nullable_col = assert_cast<ColumnNullable &>(*key_columns[i]);
            observed_column = &nullable_col.getNestedColumn();
            null_map = assert_cast<ColumnUInt8 *>(&nullable_col.getNullMapColumn());
        }
        else
        {
            observed_column = key_columns[i];
            null_map = nullptr;
        }

        bool is_null = false;
        if (column_nullable)
        {
            /// The current column is nullable. Check if the value of the
            /// corresponding key is nullable. Update the null map accordingly.
            size_t bucket = i / 8;
            size_t offset = i % 8;
            UInt8 val = (reinterpret_cast<const UInt8 *>(&key)[bucket] >> offset) & 1;
            null_map->insertValue(val);
            is_null = val == 1;
        }

        if (has_nullable_keys && is_null)
            observed_column->insertDefault();
        else
        {
            size_t size = key_sizes[i];
            observed_column->insertData(reinterpret_cast<const char *>(&key) + pos, size);
            pos += size;
        }
    }
}
template struct AggregationMethodKeysFixed<AggregatedDataWithUInt16Key>;
template struct AggregationMethodKeysFixed<AggregatedDataWithUInt32Key>;
template struct AggregationMethodKeysFixed<AggregatedDataWithUInt64Key>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256>;
template struct AggregationMethodKeysFixed<AggregatedDataWithUInt32KeyTwoLevel>;
template struct AggregationMethodKeysFixed<AggregatedDataWithUInt64KeyTwoLevel>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128Hash64>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256Hash64>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128, false, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256, false, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys128TwoLevel, false, true>;
template struct AggregationMethodKeysFixed<AggregatedDataWithKeys256TwoLevel, false, true>;


template <typename TData, bool nullable, bool prealloc>
void AggregationMethodSerialized<TData, nullable, prealloc>::insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
{
    const auto * pos = key.data;
    for (auto & column : key_columns)
        pos = column->deserializeAndInsertFromArena(pos);
}
template struct AggregationMethodSerialized<AggregatedDataWithStringKey>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyHash64>;
// AggregationMethodNullableSerialized
template struct AggregationMethodSerialized<AggregatedDataWithStringKey, true, false>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel, true, false>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyHash64, true, false>;
// AggregationMethodPreallocSerialized
template struct AggregationMethodSerialized<AggregatedDataWithStringKey, false, true>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel, false, true>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyHash64, false, true>;
// AggregationMethodNullablePreallocSerialized
template struct AggregationMethodSerialized<AggregatedDataWithStringKey, true, true>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyTwoLevel, true, true>;
template struct AggregationMethodSerialized<AggregatedDataWithStringKeyHash64, true, true>;

}
