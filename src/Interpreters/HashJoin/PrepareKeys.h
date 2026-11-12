#pragma once

#include <Interpreters/HashJoin/LookupMethod.h>

/// The prepare step of the hash join probe: builds the keys of a batch of rows before a key search that reads ready keys. Only
/// the key types that build their keys (`BuildsKeys`) have one.
namespace DB::JoinProbe
{

/// See `PrepareKeysFn`.
template <HashJoin::Type TYPE, typename Maps, bool fast_path, bool row_list>
NO_INLINE void prepareKeys(const FindKeysArgs & args)
{
    using KeyGetter = typename KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type;
    using Key = KeyOfGetter<KeyGetter>;
    constexpr bool is_asof_join = std::is_same_v<Maps, HashJoin::MapsAsof>;
    static_assert(BuildsKeys<KeyGetter>);
    static_assert(std::is_trivially_copyable_v<Key> && alignof(Key) <= alignof(UInt64));

    chassert(row_list == (args.rows.rows != nullptr));
    /// The kept getter of the key columns, so that every batch reuses its buffers and the keys that a `HashMethodKeysFixed`
    /// getter packs for the whole block.
    KeyGetter & key_getter = keptKeyGetter<KeyGetter, is_asof_join>(args);
    const size_t size = args.rows.size;
    const size_t first [[maybe_unused]] = args.rows.first;
    const UInt32 * const list [[maybe_unused]] = args.rows.rows;
    const auto row_of = [&](size_t i) __attribute__((always_inline)) -> size_t
    {
        if constexpr (row_list)
            return list[i];
        else
            return first + i;
    };

    if constexpr (detail::PacksKeysIntoBlob<KeyGetter> && !row_list)
    {
        /// The getter was built for the columns of the whole block, so the keys that it packed then are indexed by the block row.
        if (!key_getter.prepared_keys.empty())
        {
            args.keys.ready_keys = key_getter.prepared_keys.data() + first;
            return;
        }
    }

    auto & buffer = args.keys.ready_keys_buffer;
    if (const size_t words = (size * sizeof(Key) + sizeof(UInt64) - 1) / sizeof(UInt64); buffer.size() < words)
        buffer.resize(words);
    Key * const keys = reinterpret_cast<Key *>(buffer.data());
    args.keys.ready_keys = keys;
    Arena pool;
    if constexpr (detail::PacksKeysIntoBlob<KeyGetter>)
    {
        for (size_t i = 0; i < size; ++i)
        {
            auto key_holder = key_getter.getKeyHolder(row_of(i), pool);
            keys[i] = keyHolderGetKey(key_holder);
        }
    }
    else
    {
        /// A prepared key getter builds no key for a row that its skip bytes mark, so this step always gives it skip bytes.
        const UInt8 * skip = nullptr;
        IColumn::Filter skip_buffer;
        if constexpr (!fast_path)
        {
            if (size)
                skip = args.keys.buildRowSkipData(skip_buffer, row_of(0), row_of(size - 1) + 1 - row_of(0));
        }
        skip = key_getter.skipOrAllSelected(skip);

        if constexpr (row_list)
        {
            prepareListedRows(key_getter, args.rows, skip);
            for (size_t i = 0; i < size; ++i)
            {
                const size_t row = list[i];
                if (!skip[row])
                    keys[i] = key_getter.getKeyHolder(row, pool);
            }
        }
        else
            key_getter.buildKeys(first, first + size, keys, skip);
    }
}

template <typename Maps>
PrepareKeysFn selectPrepareKeys(HashJoin::Type key_type, bool row_list, bool fast_path)
{
    /// Only a join with several disjuncts, which never has an ASOF map, visits a row list.
    constexpr bool asof = std::is_same_v<Maps, HashJoin::MapsAsof>;

    const auto of_type = [&]<HashJoin::Type TYPE>() -> PrepareKeysFn
    {
        if constexpr (!BuildsKeys<typename KeyGetterForType<TYPE, const JoinMapOf<TYPE, Maps>>::Type>)
            return nullptr;
        else
        {
            if (!row_list)
                return fast_path ? &prepareKeys<TYPE, Maps, true, false> : &prepareKeys<TYPE, Maps, false, false>;
            if constexpr (!asof)
                return fast_path ? &prepareKeys<TYPE, Maps, true, true> : &prepareKeys<TYPE, Maps, false, true>;
            UNREACHABLE();
        }
    };

    switch (key_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return of_type.template operator()<HashJoin::Type::TYPE>();
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

/// Instantiated in `PrepareKeysMaps*.cpp`, one file per map.
extern template PrepareKeysFn selectPrepareKeys<HashJoin::MapsOne>(HashJoin::Type, bool, bool);
extern template PrepareKeysFn selectPrepareKeys<HashJoin::MapsAll>(HashJoin::Type, bool, bool);
extern template PrepareKeysFn selectPrepareKeys<HashJoin::MapsAsof>(HashJoin::Type, bool, bool);
extern template PrepareKeysFn selectPrepareKeys<HashJoin::MapsSet>(HashJoin::Type, bool, bool);

}
