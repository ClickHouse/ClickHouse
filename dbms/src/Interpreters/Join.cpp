#include <array>
#include <common/constexpr_helpers.h>
#include <common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/Join.h>
#include <Interpreters/NullableUtils.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Core/ColumnNumbers.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SET_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_COLUMN;
}

static NameSet requiredRightKeys(const Names & key_names, const NamesAndTypesList & columns_added_by_join)
{
    NameSet required;

    NameSet right_keys;
    for (const auto & name : key_names)
        right_keys.insert(name);

    for (const auto & column : columns_added_by_join)
    {
        if (right_keys.count(column.name))
            required.insert(column.name);
    }

    return required;
}


Join::Join(const Names & key_names_right_, bool use_nulls_, const SizeLimits & limits,
    ASTTableJoin::Kind kind_, ASTTableJoin::Strictness strictness_, bool any_take_last_row_)
    : kind(kind_), strictness(strictness_),
    key_names_right(key_names_right_),
    use_nulls(use_nulls_),
    any_take_last_row(any_take_last_row_),
    log(&Logger::get("Join")),
    limits(limits)
{
}


Join::Type Join::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

    if (keys_size == 0)
        return Type::CROSS;

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (key_columns[0]->isColumnConst() && typeid_cast<const ColumnString *>(&static_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}


template <typename Maps>
static void initImpl(Maps & maps, Join::Type type)
{
    switch (type)
    {
        case Join::Type::EMPTY:            break;
        case Join::Type::CROSS:            break;

    #define M(TYPE) \
        case Join::Type::TYPE: maps.TYPE = std::make_unique<typename decltype(maps.TYPE)::element_type>(); break;
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M
    }
}

template <typename Maps>
static size_t getTotalRowCountImpl(const Maps & maps, Join::Type type)
{
    switch (type)
    {
        case Join::Type::EMPTY:            return 0;
        case Join::Type::CROSS:            return 0;

    #define M(NAME) \
        case Join::Type::NAME: return maps.NAME ? maps.NAME->size() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M
    }

    __builtin_unreachable();
}

template <typename Maps>
static size_t getTotalByteCountImpl(const Maps & maps, Join::Type type)
{
    switch (type)
    {
        case Join::Type::EMPTY:            return 0;
        case Join::Type::CROSS:            return 0;

    #define M(NAME) \
        case Join::Type::NAME: return maps.NAME ? maps.NAME->getBufferSizeInBytes() : 0;
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M
    }

    __builtin_unreachable();
}


template <Join::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<Join::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false>;
};

template <Join::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};


/// Do I need to use the hash table maps_*_full, in which we remember whether the row was joined.
static bool getFullness(ASTTableJoin::Kind kind)
{
    return kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full;
}


void Join::init(Type type_)
{
    type = type_;

    if (kind == ASTTableJoin::Kind::Cross)
        return;
    dispatch(MapInitTag());
    dispatch([&](auto, auto, auto & map) { initImpl(map, type); });
}

size_t Join::getTotalRowCount() const
{
    size_t res = 0;

    if (type == Type::CROSS)
    {
        for (const auto & block : blocks)
            res += block.rows();
    }
    else
    {
        dispatch([&](auto, auto, auto & map) { res += getTotalRowCountImpl(map, type); });
    }

    return res;
}

size_t Join::getTotalByteCount() const
{
    size_t res = 0;

    if (type == Type::CROSS)
    {
        for (const auto & block : blocks)
            res += block.bytes();
    }
    else
    {
        dispatch([&](auto, auto, auto & map) { res += getTotalByteCountImpl(map, type); });
        res += pool.size();
    }

    return res;
}


static void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}


void Join::setSampleBlock(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (!empty())
        return;

    size_t keys_size = key_names_right.size();
    ColumnRawPtrs key_columns(keys_size);
    Columns materialized_columns;

    for (size_t i = 0; i < keys_size; ++i)
    {
        auto & column = block.getByName(key_names_right[i]).column;
        key_columns[i] = column.get();
        auto column_no_lc = recursiveRemoveLowCardinality(column);
        if (column.get() != column_no_lc.get())
        {
            materialized_columns.emplace_back(std::move(column_no_lc));
            key_columns[i] = materialized_columns[i].get();
        }

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i]->isColumnNullable())
            key_columns[i] = &static_cast<const ColumnNullable &>(*key_columns[i]).getNestedColumn();
    }

    /// Choose data structure to use for JOIN.
    init(chooseMethod(key_columns, key_sizes));

    sample_block_with_columns_to_add = materializeBlock(block);

    /// Move from `sample_block_with_columns_to_add` key columns to `sample_block_with_keys`, keeping the order.
    size_t pos = 0;
    while (pos < sample_block_with_columns_to_add.columns())
    {
        const auto & name = sample_block_with_columns_to_add.getByPosition(pos).name;
        if (key_names_right.end() != std::find(key_names_right.begin(), key_names_right.end(), name))
        {
            auto & col = sample_block_with_columns_to_add.getByPosition(pos);
            col.column = recursiveRemoveLowCardinality(col.column);
            col.type = recursiveRemoveLowCardinality(col.type);
            sample_block_with_keys.insert(col);
            sample_block_with_columns_to_add.erase(pos);
        }
        else
            ++pos;
    }

    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        auto & column = sample_block_with_columns_to_add.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (use_nulls && (kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Full))
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(sample_block_with_columns_to_add.getByPosition(i));
}


namespace
{
    /// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
    template <ASTTableJoin::Strictness STRICTNESS, typename Map, typename KeyGetter>
    struct Inserter
    {
        static void insert(Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool);
    };

    template <typename Map, typename KeyGetter>
    struct Inserter<ASTTableJoin::Strictness::Any, Map, KeyGetter>
    {
        static ALWAYS_INLINE void insert(Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || emplace_result.getMapped().overwrite)
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }
    };

    template <typename Map, typename KeyGetter>
    struct Inserter<ASTTableJoin::Strictness::All, Map, KeyGetter>
    {
        static ALWAYS_INLINE void insert(Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
            else
            {
                /** The first element of the list is stored in the value of the hash table, the rest in the pool.
                 * We will insert each time the element into the second place.
                 * That is, the former second element, if it was, will be the third, and so on.
                 */
                auto elem = pool.alloc<typename Map::mapped_type>();
                auto & mapped = emplace_result.getMapped();

                elem->next = mapped.next;
                mapped.next = elem;
                elem->block = stored_block;
                elem->row_num = i;
            }
        }
    };


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    void NO_INLINE insertFromBlockImplTypeCase(
        Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        KeyGetter key_getter(key_columns, key_sizes, nullptr);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            Inserter<STRICTNESS, Map, KeyGetter>::insert(map, key_getter, stored_block, i, pool);
        }
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    void insertFromBlockImplType(
        Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(map, rows, key_columns, key_sizes, stored_block, null_map, pool);
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void insertFromBlockImpl(
        Join::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        switch (type)
        {
            case Join::Type::EMPTY:            break;
            case Join::Type::CROSS:            break;    /// Do nothing. We have already saved block, and it is enough.

        #define M(TYPE) \
            case Join::Type::TYPE: \
                insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
    }
}


bool Join::insertFromBlock(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (empty())
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t keys_size = key_names_right.size();
    ColumnRawPtrs key_columns(keys_size);

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    Columns materialized_columns;
    materialized_columns.reserve(keys_size);

    /// Memoize key columns to work.
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(recursiveRemoveLowCardinality(block.getByName(key_names_right[i]).column->convertToFullColumnIfConst()));
        key_columns[i] = materialized_columns.back().get();
    }

    /// We will insert to the map only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    size_t rows = block.rows();

    blocks.push_back(block);
    Block * stored_block = &blocks.back();

    if (getFullness(kind))
    {
        /** Move the key columns to the beginning of the block.
          * This is where NonJoinedBlockInputStream will expect.
          */
        size_t key_num = 0;
        for (const auto & name : key_names_right)
        {
            size_t pos = stored_block->getPositionByName(name);
            ColumnWithTypeAndName col = stored_block->safeGetByPosition(pos);
            stored_block->erase(pos);
            stored_block->insert(key_num, std::move(col));
            ++key_num;
        }
    }
    else
    {
        NameSet erased; /// HOTFIX: there could be duplicates in JOIN ON section

        /// Remove the key columns from stored_block, as they are not needed.
        for (const auto & name : key_names_right)
        {
            if (!erased.count(name))
                stored_block->erase(stored_block->getPositionByName(name));
            erased.insert(name);
        }
    }

    size_t size = stored_block->columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < size; ++i)
        stored_block->safeGetByPosition(i).column = stored_block->safeGetByPosition(i).column->convertToFullColumnIfConst();

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (use_nulls && (kind == ASTTableJoin::Kind::Left || kind == ASTTableJoin::Kind::Full))
    {
        for (size_t i = getFullness(kind) ? keys_size : 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    if (kind != ASTTableJoin::Kind::Cross)
    {
        dispatch([&](auto, auto strictness_, auto & map)
        {
            insertFromBlockImpl<strictness_>(type, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        });
    }

    return limits.check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


namespace
{
    template <bool fill_left, ASTTableJoin::Strictness STRICTNESS, typename Map>
    struct Adder;

    template <typename Map>
    struct Adder<true, ASTTableJoin::Strictness::Any, Map>
    {
        static void addFound(const typename Map::mapped_type & mapped, size_t num_columns_to_add, MutableColumns & added_columns,
            size_t i, IColumn::Filter & filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/,
            const std::vector<size_t> & right_indexes)
        {
            filter[i] = 1;

            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(*mapped.block->getByPosition(right_indexes[j]).column, mapped.row_num);
        }

        static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns,
            size_t i, IColumn::Filter & filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
        {
            filter[i] = 0;

            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertDefault();
        }
    };

    template <typename Map>
    struct Adder<false, ASTTableJoin::Strictness::Any, Map>
    {
        static void addFound(const typename Map::mapped_type & mapped, size_t num_columns_to_add, MutableColumns & added_columns,
            size_t i, IColumn::Filter & filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/,
            const std::vector<size_t> & right_indexes)
        {
            filter[i] = 1;

            for (size_t j = 0; j < num_columns_to_add; ++j)
                added_columns[j]->insertFrom(*mapped.block->getByPosition(right_indexes[j]).column, mapped.row_num);
        }

        static void addNotFound(size_t /*num_columns_to_add*/, MutableColumns & /*added_columns*/,
            size_t i, IColumn::Filter & filter, IColumn::Offset & /*current_offset*/, IColumn::Offsets * /*offsets*/)
        {
            filter[i] = 0;
        }
    };

    template <bool fill_left, typename Map>
    struct Adder<fill_left, ASTTableJoin::Strictness::All, Map>
    {
        static void addFound(const typename Map::mapped_type & mapped, size_t num_columns_to_add, MutableColumns & added_columns,
            size_t i, IColumn::Filter & filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets,
            const std::vector<size_t> & right_indexes)
        {
            filter[i] = 1;

            size_t rows_joined = 0;
            for (auto current = &static_cast<const typename Map::mapped_type::Base_t &>(mapped); current != nullptr; current = current->next)
            {
                for (size_t j = 0; j < num_columns_to_add; ++j)
                    added_columns[j]->insertFrom(*current->block->getByPosition(right_indexes[j]).column.get(), current->row_num);

                ++rows_joined;
            }

            current_offset += rows_joined;
            (*offsets)[i] = current_offset;
        }

        static void addNotFound(size_t num_columns_to_add, MutableColumns & added_columns,
            size_t i, IColumn::Filter & filter, IColumn::Offset & current_offset, IColumn::Offsets * offsets)
        {
            filter[i] = 0;

            if (!fill_left)
            {
                (*offsets)[i] = current_offset;
            }
            else
            {
                ++current_offset;
                (*offsets)[i] = current_offset;

                for (size_t j = 0; j < num_columns_to_add; ++j)
                    added_columns[j]->insertDefault();
            }
        }
    };

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    void NO_INLINE joinBlockImplTypeCase(
        const Map & map, size_t rows, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
        MutableColumns & added_columns, ConstNullMapPtr null_map, IColumn::Filter & filter,
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate,
        const std::vector<size_t> & right_indexes)
    {
        IColumn::Offset current_offset = 0;
        size_t num_columns_to_add = right_indexes.size();

        Arena pool;
        KeyGetter key_getter(key_columns, key_sizes, nullptr);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
            {
                Adder<Join::KindTrait<KIND>::fill_left, STRICTNESS, Map>::addNotFound(
                    num_columns_to_add, added_columns, i, filter, current_offset, offsets_to_replicate.get());
            }
            else
            {
                auto find_result = key_getter.findKey(map, i, pool);

                if (find_result.isFound())
                {
                    auto & mapped = find_result.getMapped();
                    mapped.setUsed();
                    Adder<Join::KindTrait<KIND>::fill_left, STRICTNESS, Map>::addFound(
                        mapped, num_columns_to_add, added_columns, i, filter, current_offset, offsets_to_replicate.get(), right_indexes);
                }
                else
                    Adder<Join::KindTrait<KIND>::fill_left, STRICTNESS, Map>::addNotFound(
                        num_columns_to_add, added_columns, i, filter, current_offset, offsets_to_replicate.get());
            }
        }
    }

    using BlockFilterData = std::pair<
        std::unique_ptr<IColumn::Filter>,
        std::unique_ptr<IColumn::Offsets>>;

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    BlockFilterData joinBlockImplType(
        const Map & map, size_t rows, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
        MutableColumns & added_columns, ConstNullMapPtr null_map, const std::vector<size_t> & right_indexes)
    {
        std::unique_ptr<IColumn::Filter> filter = std::make_unique<IColumn::Filter>(rows);
        std::unique_ptr<IColumn::Offsets> offsets_to_replicate;

        if (STRICTNESS == ASTTableJoin::Strictness::All)
            offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

        if (null_map)
            joinBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, true>(
                map, rows, key_columns, key_sizes, added_columns, null_map, *filter,
                offsets_to_replicate, right_indexes);
        else
            joinBlockImplTypeCase<KIND, STRICTNESS, KeyGetter, Map, false>(
                map, rows, key_columns, key_sizes, added_columns, null_map, *filter,
                offsets_to_replicate, right_indexes);

        return {std::move(filter), std::move(offsets_to_replicate)};
    }
}


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(
    Block & block,
    const Names & key_names_left,
    const NamesAndTypesList & columns_added_by_join,
    const Block & block_with_columns_to_add,
    const Maps & maps_) const
{
    size_t keys_size = key_names_left.size();
    ColumnRawPtrs key_columns(keys_size);

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    Columns materialized_columns;
    materialized_columns.reserve(keys_size);

    /// Memoize key columns to work with.
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(recursiveRemoveLowCardinality(block.getByName(key_names_left[i]).column->convertToFullColumnIfConst()));
        key_columns[i] = materialized_columns.back().get();
    }

    /// Keys with NULL value in any column won't join to anything.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (getFullness(kind))
    {
        for (size_t i = 0; i < existing_columns; ++i)
        {
            block.getByPosition(i).column = block.getByPosition(i).column->convertToFullColumnIfConst();

            /// If use_nulls, convert left columns (except keys) to Nullable.
            if (use_nulls)
            {
                if (std::end(key_names_left) == std::find(key_names_left.begin(), key_names_left.end(), block.getByPosition(i).name))
                    convertColumnToNullable(block.getByPosition(i));
            }
        }
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      */
    size_t num_columns_to_skip = 0;
    if (getFullness(kind))
        num_columns_to_skip = keys_size;

    /// Add new columns to the block.
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();
    MutableColumns added_columns;
    added_columns.reserve(num_columns_to_add);

    std::vector<std::pair<decltype(ColumnWithTypeAndName::type), decltype(ColumnWithTypeAndName::name)>> added_type_name;
    added_type_name.reserve(num_columns_to_add);

    std::vector<size_t> right_indexes;
    right_indexes.reserve(num_columns_to_add);

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.safeGetByPosition(i);

        /// Don't insert column if it's in left block or not explicitly required.
        if (!block.has(src_column.name) && block_with_columns_to_add.has(src_column.name))
        {
            added_columns.push_back(src_column.column->cloneEmpty());
            added_columns.back()->reserve(src_column.column->size());
            added_type_name.emplace_back(src_column.type, src_column.name);
            right_indexes.push_back(num_columns_to_skip + i);
        }
    }

    std::unique_ptr<IColumn::Filter> filter;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;

    switch (type)
    {
    #define M(TYPE) \
        case Join::Type::TYPE: \
            std::tie(filter, offsets_to_replicate) = \
                joinBlockImplType<KIND, STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, const std::remove_reference_t<decltype(*maps_.TYPE)>>::Type>(\
                *maps_.TYPE, block.rows(), key_columns, key_sizes, added_columns, null_map, right_indexes); \
            break;
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }

    const auto added_columns_size = added_columns.size();
    for (size_t i = 0; i < added_columns_size; ++i)
        block.insert(ColumnWithTypeAndName(std::move(added_columns[i]), added_type_name[i].first, added_type_name[i].second));

    if (!filter)
        throw Exception("No data to filter columns", ErrorCodes::LOGICAL_ERROR);

    NameSet needed_key_names_right = requiredRightKeys(key_names_right, columns_added_by_join);

    if (strictness == ASTTableJoin::Strictness::Any)
    {
        if (kind == ASTTableJoin::Kind::Inner || kind == ASTTableJoin::Kind::Right)
        {
            /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
            for (size_t i = 0; i < existing_columns; ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(*filter, -1);

            /// Add join key columns from right block if they has different name.
            for (size_t i = 0; i < key_names_right.size(); ++i)
            {
                auto & right_name = key_names_right[i];
                auto & left_name = key_names_left[i];

                if (needed_key_names_right.count(right_name) && !block.has(right_name))
                {
                    const auto & col = block.getByName(left_name);
                    block.insert({col.column, col.type, right_name});
                }
            }
        }
        else
        {
            /// Add join key columns from right block if they has different name.
            for (size_t i = 0; i < key_names_right.size(); ++i)
            {
                auto & right_name = key_names_right[i];
                auto & left_name = key_names_left[i];

                if (needed_key_names_right.count(right_name) && !block.has(right_name))
                {
                    const auto & col = block.getByName(left_name);
                    auto & column = col.column;
                    MutableColumnPtr mut_column = column->cloneEmpty();

                    for (size_t col_no = 0; col_no < filter->size(); ++col_no)
                    {
                        if ((*filter)[col_no])
                            mut_column->insertFrom(*column, col_no);
                        else
                            mut_column->insertDefault();
                    }

                    block.insert({std::move(mut_column), col.type, right_name});
                }
            }
        }
    }
    else
    {
        if (!offsets_to_replicate)
            throw Exception("No data to filter columns", ErrorCodes::LOGICAL_ERROR);

        /// Add join key columns from right block if they has different name.
        for (size_t i = 0; i < key_names_right.size(); ++i)
        {
            auto & right_name = key_names_right[i];
            auto & left_name = key_names_left[i];

            if (needed_key_names_right.count(right_name) && !block.has(right_name))
            {
                const auto & col = block.getByName(left_name);
                auto & column = col.column;
                MutableColumnPtr mut_column = column->cloneEmpty();

                size_t last_offset = 0;
                for (size_t col_no = 0; col_no < column->size(); ++col_no)
                {
                    if (size_t to_insert = (*offsets_to_replicate)[col_no] - last_offset)
                    {
                        if (!(*filter)[col_no])
                            mut_column->insertDefault();
                        else
                            for (size_t dup = 0; dup < to_insert; ++dup)
                                mut_column->insertFrom(*column, col_no);
                    }

                    last_offset = (*offsets_to_replicate)[col_no];
                }

                block.insert({std::move(mut_column), col.type, right_name});
            }
        }

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);
    }
}


void Join::joinBlockImplCross(Block & block) const
{
    /// Add new columns to the block.
    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    size_t rows_left = block.rows();

    ColumnRawPtrs src_left_columns(num_existing_columns);
    MutableColumns dst_columns(num_existing_columns + num_columns_to_add);

    for (size_t i = 0; i < num_existing_columns; ++i)
    {
        src_left_columns[i] = block.getByPosition(i).column.get();
        dst_columns[i] = src_left_columns[i]->cloneEmpty();
    }

    for (size_t i = 0; i < num_columns_to_add; ++i)
    {
        const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.getByPosition(i);
        dst_columns[num_existing_columns + i] = src_column.column->cloneEmpty();
        block.insert(src_column);
    }

    /// NOTE It would be better to use `reserve`, as well as `replicate` methods to duplicate the values of the left block.

    for (size_t i = 0; i < rows_left; ++i)
    {
        for (const Block & block_right : blocks)
        {
            size_t rows_right = block_right.rows();

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                for (size_t j = 0; j < rows_right; ++j)
                    dst_columns[col_num]->insertFrom(*src_left_columns[col_num], i);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn * column_right = block_right.getByPosition(col_num).column.get();

                for (size_t j = 0; j < rows_right; ++j)
                    dst_columns[num_existing_columns + col_num]->insertFrom(*column_right, j);
            }
        }
    }

    block = block.cloneWithColumns(std::move(dst_columns));
}


void Join::checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right) const
{
    size_t keys_size = key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        /// Compare up to Nullability.

        DataTypePtr left_type = removeNullable(recursiveRemoveLowCardinality(block_left.getByName(key_names_left[i]).type));
        DataTypePtr right_type = removeNullable(recursiveRemoveLowCardinality(block_right.getByName(key_names_right[i]).type));

        if (!left_type->equals(*right_type))
            throw Exception("Type mismatch of columns to JOIN by: "
                + key_names_left[i] + " " + left_type->getName() + " at left, "
                + key_names_right[i] + " " + right_type->getName() + " at right",
                ErrorCodes::TYPE_MISMATCH);
    }
}


static void checkTypeOfKey(const Block & block_left, const Block & block_right)
{
    auto & [c1, left_type_origin, left_name] = block_left.safeGetByPosition(0);
    auto & [c2, right_type_origin, right_name] = block_right.safeGetByPosition(0);
    auto left_type = removeNullable(left_type_origin);
    auto right_type = removeNullable(right_type_origin);

    if (!left_type->equals(*right_type))
        throw Exception("Type mismatch of columns to joinGet by: "
            + left_name + " " + left_type->getName() + " at left, "
            + right_name + " " + right_type->getName() + " at right",
            ErrorCodes::TYPE_MISMATCH);
}


DataTypePtr Join::joinGetReturnType(const String & column_name) const
{
    std::shared_lock lock(rwlock);

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception("StorageJoin doesn't contain column " + column_name, ErrorCodes::LOGICAL_ERROR);
    return sample_block_with_columns_to_add.getByName(column_name).type;
}


template <typename Maps>
void Join::joinGetImpl(Block & block, const String & column_name, const Maps & maps_) const
{
    joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(
        block, {block.getByPosition(0).name}, {}, {sample_block_with_columns_to_add.getByName(column_name)}, maps_);
}


// TODO: support composite key
// TODO: return multiple columns as named tuple
// TODO: return array of values when strictness == ASTTableJoin::Strictness::All
void Join::joinGet(Block & block, const String & column_name) const
{
    std::shared_lock lock(rwlock);

    if (key_names_right.size() != 1)
        throw Exception("joinGet only supports StorageJoin containing exactly one key", ErrorCodes::LOGICAL_ERROR);

    checkTypeOfKey(block, sample_block_with_keys);

    if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::Any)
    {
        if (any_take_last_row)
            joinGetImpl(block, column_name, std::get<MapsAnyOverwrite>(maps));
        else
            joinGetImpl(block, column_name, std::get<MapsAny>(maps));
    }
    else
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::LOGICAL_ERROR);
}


void Join::joinBlock(Block & block, const Names & key_names_left, const NamesAndTypesList & columns_added_by_join) const
{
//    std::cerr << "joinBlock: " << block.dumpStructure() << "\n";

    std::shared_lock lock(rwlock);

    checkTypesOfKeys(block, key_names_left, sample_block_with_keys);

    if (dispatch([&](auto kind_, auto strictness_, auto & map)
        {
            joinBlockImpl<kind_, strictness_>(block, key_names_left, columns_added_by_join, sample_block_with_columns_to_add, map);
        }))
    {
        /// Joined
    }
    else if (kind == ASTTableJoin::Kind::Cross)
        joinBlockImplCross(block);
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}


void Join::joinTotals(Block & block) const
{
    Block totals_without_keys = totals;

    if (totals_without_keys)
    {
        for (const auto & name : key_names_right)
            totals_without_keys.erase(totals_without_keys.getPositionByName(name));

        for (size_t i = 0; i < totals_without_keys.columns(); ++i)
            block.insert(totals_without_keys.safeGetByPosition(i));
    }
    else
    {
        /// We will join empty `totals` - from one row with the default values.

        for (size_t i = 0; i < sample_block_with_columns_to_add.columns(); ++i)
        {
            const auto & col = sample_block_with_columns_to_add.getByPosition(i);
            block.insert({
                col.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst(),
                col.type,
                col.name});
        }
    }
}


template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Any, Mapped>
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_left, MutableColumns & columns_right)
    {
        for (size_t j = 0; j < columns_left.size(); ++j)
            columns_left[j]->insertDefault();

        for (size_t j = 0; j < columns_right.size(); ++j)
            columns_right[j]->insertFrom(*mapped.block->getByPosition(j).column.get(), mapped.row_num);

        ++rows_added;
    }
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_left, MutableColumns & columns_right)
    {
        for (auto current = &static_cast<const typename Mapped::Base_t &>(mapped); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < columns_left.size(); ++j)
                columns_left[j]->insertDefault();

            for (size_t j = 0; j < columns_right.size(); ++j)
                columns_right[j]->insertFrom(*current->block->getByPosition(j).column.get(), current->row_num);

            ++rows_added;
        }
    }
};


/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : public IBlockInputStream
{
public:
    NonJoinedBlockInputStream(const Join & parent_, const Block & left_sample_block, const Names & key_names_left,
                              const NamesAndTypesList & columns_added_by_join, UInt64 max_block_size_)
        : parent(parent_), max_block_size(max_block_size_)
    {
        /** left_sample_block contains keys and "left" columns.
          * result_sample_block - keys, "left" columns, and "right" columns.
          */

        std::unordered_map<String, String> key_renames;
        makeResultSampleBlock(left_sample_block, key_names_left, columns_added_by_join, key_renames);

        const Block & right_sample_block = parent.sample_block_with_columns_to_add;

        size_t num_keys = key_names_left.size();
        size_t num_columns_left = left_sample_block.columns() - num_keys;
        size_t num_columns_right = right_sample_block.columns();

        column_indices_left.reserve(num_columns_left);
        column_indices_keys_and_right.reserve(num_keys + num_columns_right);

        std::vector<bool> is_left_key(left_sample_block.columns(), false);

        for (const std::string & key : key_names_left)
        {
            size_t key_pos = left_sample_block.getPositionByName(key);
            is_left_key[key_pos] = true;
            /// Here we establish the mapping between key columns of the left- and right-side tables.
            /// key_pos index is inserted in the position corresponding to key column in parent.blocks
            /// (saved blocks of the right-side table) and points to the same key column
            /// in the left_sample_block and thus in the result_sample_block.
            column_indices_keys_and_right.push_back(key_pos);

            auto it = key_renames.find(key);
            if (it != key_renames.end())
                key_renames_indices[key_pos] = result_sample_block.getPositionByName(it->second);
        }

        size_t num_src_columns = left_sample_block.columns() + right_sample_block.columns();

        for (size_t i = 0; i < result_sample_block.columns(); ++i)
        {
            if (i < left_sample_block.columns())
            {
                if (!is_left_key[i])
                {
                    column_indices_left.emplace_back(i);

                    /// If use_nulls, convert left columns to Nullable.
                    if (parent.use_nulls)
                        convertColumnToNullable(result_sample_block.getByPosition(i));
                }
            }
            else if (i < num_src_columns)
                column_indices_keys_and_right.emplace_back(i);
        }
    }

    String getName() const override { return "NonJoined"; }

    Block getHeader() const override { return result_sample_block; }


protected:
    Block readImpl() override
    {
        if (parent.blocks.empty())
            return Block();

        Block block;
        if (parent.dispatch([&](auto, auto strictness, auto & map) { block = createBlock<strictness>(map); }))
            ;
        else
            throw Exception("Logical error: unknown JOIN strictness (must be ANY or ALL)", ErrorCodes::LOGICAL_ERROR);
        return block;
    }

private:
    const Join & parent;
    UInt64 max_block_size;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that come from the left-side table (except key columns).
    ColumnNumbers column_indices_left;
    /// Indices of key columns in result_sample_block or columns that come from the right-side table.
    /// Order is significant: it is the same as the order of columns in the blocks of the right-side table that are saved in parent.blocks.
    ColumnNumbers column_indices_keys_and_right;
    std::unordered_map<size_t, size_t> key_renames_indices;

    std::unique_ptr<void, std::function<void(void *)>> position;    /// type erasure


    void makeResultSampleBlock(const Block & left_sample_block, const Names & key_names_left,
                               const NamesAndTypesList & columns_added_by_join, std::unordered_map<String, String> & key_renames)
    {
        const Block & right_sample_block = parent.sample_block_with_columns_to_add;

        result_sample_block = materializeBlock(left_sample_block);

        /// Add columns from the right-side table to the block.
        for (size_t i = 0; i < right_sample_block.columns(); ++i)
        {
            const ColumnWithTypeAndName & src_column = right_sample_block.getByPosition(i);
            result_sample_block.insert(src_column.cloneEmpty());
        }

        const auto & key_names_right = parent.key_names_right;
        NameSet needed_key_names_right = requiredRightKeys(key_names_right, columns_added_by_join);

        /// Add join key columns from right block if they has different name.
        for (size_t i = 0; i < key_names_right.size(); ++i)
        {
            auto & right_name = key_names_right[i];
            auto & left_name = key_names_left[i];

            if (needed_key_names_right.count(right_name) && !result_sample_block.has(right_name))
            {
                const auto & col = result_sample_block.getByName(left_name);
                result_sample_block.insert({col.column, col.type, right_name});

                key_renames[left_name] = right_name;
            }
        }
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Block createBlock(const Maps & maps)
    {
        MutableColumns columns_left = columnsForIndex(result_sample_block, column_indices_left);
        MutableColumns columns_keys_and_right = columnsForIndex(result_sample_block, column_indices_keys_and_right);

        size_t rows_added = 0;

        switch (parent.type)
        {
        #define M(TYPE) \
            case Join::Type::TYPE: \
                rows_added = fillColumns<STRICTNESS>(*maps.TYPE, columns_left, columns_keys_and_right); \
                break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }

        if (!rows_added)
            return {};

        Block res = result_sample_block.cloneEmpty();

        for (size_t i = 0; i < columns_left.size(); ++i)
            res.getByPosition(column_indices_left[i]).column = std::move(columns_left[i]);

        if (key_renames_indices.empty())
        {
            for (size_t i = 0; i < columns_keys_and_right.size(); ++i)
                res.getByPosition(column_indices_keys_and_right[i]).column = std::move(columns_keys_and_right[i]);
        }
        else
        {
            for (size_t i = 0; i < columns_keys_and_right.size(); ++i)
            {
                size_t key_idx = column_indices_keys_and_right[i];

                auto it = key_renames_indices.find(key_idx);
                if (it != key_renames_indices.end())
                {
                    auto & key_column = res.getByPosition(key_idx).column;
                    if (key_column->empty())
                        key_column = key_column->cloneResized(columns_keys_and_right[i]->size());
                    res.getByPosition(it->second).column = std::move(columns_keys_and_right[i]);
                }
                else
                    res.getByPosition(key_idx).column = std::move(columns_keys_and_right[i]);
            }
        }

        return res;
    }

    static MutableColumns columnsForIndex(const Block & block, const ColumnNumbers & indices)
    {
        size_t num_columns = indices.size();

        MutableColumns columns;
        columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; ++i)
        {
            const auto & src_col = block.safeGetByPosition(indices[i]);
            columns[i] = src_col.type->createColumn();
        }

        return columns;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_left, MutableColumns & columns_keys_and_right)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())), //-V572
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if (it->second.getUsed())
                continue;

            AdderNonJoined<STRICTNESS, typename Map::mapped_type>::add(it->second, rows_added, columns_left, columns_keys_and_right);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }
};


BlockInputStreamPtr Join::createStreamWithNonJoinedRows(const Block & left_sample_block, const Names & key_names_left,
                                                        const NamesAndTypesList & columns_added_by_join, UInt64 max_block_size) const
{
    return std::make_shared<NonJoinedBlockInputStream>(*this, left_sample_block, key_names_left, columns_added_by_join, max_block_size);
}


}
