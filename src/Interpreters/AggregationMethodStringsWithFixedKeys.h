#pragma once
#include <Interpreters/AggregationMethod.h>

namespace DB
{

using AggregateDataPtr = char *;

template <typename TData, typename TFixed, bool one_number, bool one_string>
struct AggregationMethodStringsWithFixedKeys
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodStringsWithFixedKeys() = default;

    explicit AggregationMethodStringsWithFixedKeys(size_t size_hint) : data(size_hint) {}

    template <typename Other>
    explicit AggregationMethodStringsWithFixedKeys(const Other & other) : data(other.data) {}

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodStringsWithFixedKeys<typename Data::value_type, Mapped, TFixed, one_number, one_string>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        return State::shuffleKeyColumns(key_columns, key_sizes);
    }

    using NumberData = HashMap<TFixed, AggregateDataPtr>;
    using NumberMethod = std::conditional_t<one_number, AggregationMethodOneNumber<TFixed, NumberData>, AggregationMethodKeysFixed<NumberData>>;

    using StringData = std::conditional_t<one_string, StringHashMap<AggregateDataPtr>, HashMapWithSavedHash<StringRef, AggregateDataPtr>>;
    using StringMethod = std::conditional_t<one_string, AggregationMethodStringNoCache<StringData>, AggregationMethodSerialized<StringData>>;

    static void insertKeyIntoColumns(const StringRefWithFixedKey<TFixed> & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        Sizes number_sizes;
        std::vector<IColumn *> number_columns;
        std::vector<IColumn *> string_columns;

        for (size_t i = 0; i < key_columns.size(); ++i)
        {
            if (key_sizes[i] == 0)
            {
                string_columns.push_back(key_columns[i]);
            }
            else
            {
                number_columns.push_back(key_columns[i]);
                number_sizes.push_back(key_sizes[i]);
            }
        }

        NumberMethod::insertKeyIntoColumns(key.fixed, number_columns, number_sizes);
        StringMethod::insertKeyIntoColumns(key.ref, string_columns, {});
    }
};

}
