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


static std::unordered_map<String, DataTypePtr> requiredRightKeys(const Names & key_names, const NamesAndTypesList & columns_added_by_join)
{
    NameSet right_keys;
    for (const auto & name : key_names)
        right_keys.insert(name);

    std::unordered_map<String, DataTypePtr> required;
    for (const auto & column : columns_added_by_join)
        if (right_keys.count(column.name))
            required.insert({column.name, column.type});

    return required;
}

static void convertColumnToNullable(ColumnWithTypeAndName & column)
{
    if (column.type->isNullable())
        return;

    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}

/// Converts column to nullable if needed. No backward convertion.
static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
        convertColumnToNullable(column);
    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        convertColumnToNullable(column);
        if (negative_null_map.size())
        {
            MutableColumnPtr mutable_column = (*std::move(column.column)).mutate();
            static_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    return std::move(column);
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

static const IColumn * extractAsofColumn(const ColumnRawPtrs & key_columns)
{
    return key_columns.back();
}

template<typename KeyGetter, ASTTableJoin::Strictness STRICTNESS>
static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
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


void Join::init(Type type_)
{
    type = type_;

    if (kind == ASTTableJoin::Kind::Cross)
        return;
    dispatch(MapInitTag());
    dispatch([&](auto, auto, auto & map) { map.create(type); });
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
        dispatch([&](auto, auto, auto & map) { res += map.getTotalRowCount(type); });
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
        dispatch([&](auto, auto, auto & map) { res += map.getTotalByteCountImpl(type); });
        res += pool.size();
    }

    return res;
}

void Join::setSampleBlock(const Block & block)
{
    std::unique_lock lock(rwlock);
    LOG_DEBUG(log, "setSampleBlock: " << block.dumpStructure());

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
            key_columns[i] = materialized_columns.back().get();
        }

        /// We will join only keys, where all components are not NULL.
        if (key_columns[i]->isColumnNullable())
            key_columns[i] = &static_cast<const ColumnNullable &>(*key_columns[i]).getNestedColumn();
    }

    if (strictness == ASTTableJoin::Strictness::Asof)
    {
        if (kind != ASTTableJoin::Kind::Left and kind != ASTTableJoin::Kind::Inner)
            throw Exception("ASOF only supports LEFT and INNER as base joins", ErrorCodes::NOT_IMPLEMENTED);

        const IColumn * asof_column = key_columns.back();
        size_t asof_size;

        asof_type = AsofRowRefs::getTypeSize(asof_column, asof_size);
        if (!asof_type)
        {
            std::string msg = "ASOF join not supported for type";
            msg += asof_column->getFamilyName();
            throw Exception(msg, ErrorCodes::BAD_TYPE_OF_FIELD);
        }

        key_columns.pop_back();

        if (key_columns.empty())
            throw Exception("ASOF join cannot be done without a joining column", ErrorCodes::LOGICAL_ERROR);

        /// this is going to set up the appropriate hash table for the direct lookup part of the join
        /// However, this does not depend on the size of the asof join key (as that goes into the BST)
        /// Therefore, add it back in such that it can be extracted appropriately from the full stored
        /// key_columns and key_sizes
        init(chooseMethod(key_columns, key_sizes));
        key_sizes.push_back(asof_size);
    }
    else
    {
        /// Choose data structure to use for JOIN.
        init(chooseMethod(key_columns, key_sizes));
    }


    sample_block_with_columns_to_add = materializeBlock(block);

    blocklist_sample = Block(block.getColumnsWithTypeAndName());
    prepareBlockListStructure(blocklist_sample);

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
    if (use_nulls && isLeftOrFull(kind))
        for (size_t i = 0; i < num_columns_to_add; ++i)
            convertColumnToNullable(sample_block_with_columns_to_add.getByPosition(i));
}

namespace
{
    /// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
    template <ASTTableJoin::Strictness STRICTNESS, typename Map, typename KeyGetter>
    struct Inserter
    {
        static void insert(const Join &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool);
    };

    template <typename Map, typename KeyGetter>
    struct Inserter<ASTTableJoin::Strictness::Any, Map, KeyGetter>
    {
        static ALWAYS_INLINE void insert(const Join & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }
    };

    template <typename Map, typename KeyGetter>
    struct Inserter<ASTTableJoin::Strictness::All, Map, KeyGetter>
    {
        static ALWAYS_INLINE void insert(const Join &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
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

    template <typename Map, typename KeyGetter>
    struct Inserter<ASTTableJoin::Strictness::Asof, Map, KeyGetter>
    {
        static ALWAYS_INLINE void insert(Join & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool,
                                         const IColumn * asof_column)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);
            typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) typename Map::mapped_type(join.getAsofType());
            time_series_map->insert(join.getAsofType(), asof_column, stored_block, i);
        }
    };


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    void NO_INLINE insertFromBlockImplTypeCase(
        Join & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
            asof_column = extractAsofColumn(key_columns);

        auto key_getter = createKeyGetter<KeyGetter, STRICTNESS>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
                Inserter<STRICTNESS, Map, KeyGetter>::insert(join, map, key_getter, stored_block, i, pool, asof_column);
            else
                Inserter<STRICTNESS, Map, KeyGetter>::insert(join, map, key_getter, stored_block, i, pool);
        }
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    void insertFromBlockImplType(
        Join & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void insertFromBlockImpl(
        Join & join, Join::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        switch (type)
        {
            case Join::Type::EMPTY:            break;
            case Join::Type::CROSS:            break;    /// Do nothing. We have already saved block, and it is enough.

        #define M(TYPE) \
            case Join::Type::TYPE: \
                insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
    }
}

void Join::prepareBlockListStructure(Block & stored_block)
{
    if (isRightOrFull(kind))
    {
        /** Move the key columns to the beginning of the block.
          * This is where NonJoinedBlockInputStream will expect.
          */
        size_t key_num = 0;
        for (const auto & name : key_names_right)
        {
            size_t pos = stored_block.getPositionByName(name);
            ColumnWithTypeAndName col = stored_block.safeGetByPosition(pos);
            stored_block.erase(pos);
            stored_block.insert(key_num, std::move(col));
            ++key_num;
        }
    }
    else
    {
        NameSet erased; /// HOTFIX: there could be duplicates in JOIN ON section

        /// Remove the key columns from stored_block, as they are not needed.
        /// However, do not erase the ASOF column if this is an asof join
        for (const auto & name : key_names_right)
        {
            if (strictness == ASTTableJoin::Strictness::Asof && name == key_names_right.back())
                break; // this is the last column so break is OK

            if (!erased.count(name))
                stored_block.erase(stored_block.getPositionByName(name));
            erased.insert(name);
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

    prepareBlockListStructure(*stored_block);

    size_t size = stored_block->columns();

    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    for (size_t i = 0; i < size; ++i)
        stored_block->safeGetByPosition(i).column = stored_block->safeGetByPosition(i).column->convertToFullColumnIfConst();

    /// In case of LEFT and FULL joins, if use_nulls, convert joined columns to Nullable.
    if (use_nulls && isLeftOrFull(kind))
    {
        for (size_t i = isFull(kind) ? keys_size : 0; i < size; ++i)
        {
            convertColumnToNullable(stored_block->getByPosition(i));
        }
    }

    if (kind != ASTTableJoin::Kind::Cross)
    {
        dispatch([&](auto, auto strictness_, auto & map)
        {
            insertFromBlockImpl<strictness_>(*this, type, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        });
    }

    return limits.check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


namespace
{

class AddedColumns
{
public:
    using TypeAndNames = std::vector<std::pair<decltype(ColumnWithTypeAndName::type), decltype(ColumnWithTypeAndName::name)>>;

    AddedColumns(const Block & sample_block_with_columns_to_add,
                 const Block & block_with_columns_to_add,
                 const Block & block,
                 const Block & blocklist_sample,
                 const ColumnsWithTypeAndName & extras)
    {
        size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (size_t i = 0; i < num_columns_to_add; ++i)
        {
            const ColumnWithTypeAndName & src_column = sample_block_with_columns_to_add.safeGetByPosition(i);

            /// Don't insert column if it's in left block or not explicitly required.
            if (!block.has(src_column.name) && block_with_columns_to_add.has(src_column.name))
                addColumn(src_column);
        }

        for (auto & extra : extras)
            addColumn(extra);

        for (auto & tn : type_name)
            right_indexes.push_back(blocklist_sample.getPositionByName(tn.second));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].first, type_name[i].second);
    }

    void appendFromBlock(const Block & block, size_t row_num)
    {
        for (size_t j = 0; j < right_indexes.size(); ++j)
            columns[j]->insertFrom(*block.getByPosition(right_indexes[j]).column, row_num);
    }


    void appendDefaultRow()
    {
        for (size_t j = 0; j < right_indexes.size(); ++j)
            columns[j]->insertDefault();
    }

private:
    TypeAndNames type_name;
    MutableColumns columns;
    std::vector<size_t> right_indexes;

    void addColumn(const ColumnWithTypeAndName & src_column)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name);
    }
};

template <ASTTableJoin::Strictness STRICTNESS, typename Map>
void addFoundRow(const typename Map::mapped_type & mapped, AddedColumns & added, IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any)
    {
        added.appendFromBlock(*mapped.block, mapped.row_num);
    }

    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
    {
        for (auto current = &static_cast<const typename Map::mapped_type::Base &>(mapped); current != nullptr; current = current->next)
        {
            added.appendFromBlock(*current->block, current->row_num);
            ++current_offset;
        }
    }
};

template <bool _add_missing>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (_add_missing)
    {
        added.appendDefaultRow();
        ++current_offset;
    }
}


/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <bool _add_missing, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool _has_null_map>
std::unique_ptr<IColumn::Offsets> NO_INLINE joinRightIndexedColumns(
    const Join & join, const Map & map, size_t rows, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    AddedColumns & added_columns, ConstNullMapPtr null_map, IColumn::Filter & filter)
{
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
        offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    Arena pool;

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
        asof_column = extractAsofColumn(key_columns);
    auto key_getter = createKeyGetter<KeyGetter, STRICTNESS>(key_columns, key_sizes);


    IColumn::Offset current_offset = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        if (_has_null_map && (*null_map)[i])
        {
            addNotFoundRow<_add_missing>(added_columns, current_offset);
        }
        else
        {
            auto find_result = key_getter.findKey(map, i, pool);

            if (find_result.isFound())
            {
                auto & mapped = find_result.getMapped();

                if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
                {
                    if (const RowRef * found = mapped.findAsof(join.getAsofType(), asof_column, i))
                    {
                        filter[i] = 1;
                        mapped.setUsed();
                        added_columns.appendFromBlock(*found->block, found->row_num);
                    }
                    else
                        addNotFoundRow<_add_missing>(added_columns, current_offset);
                }
                else
                {
                    filter[i] = 1;
                    mapped.setUsed();
                    addFoundRow<STRICTNESS, Map>(mapped, added_columns, current_offset);
                }
            }
            else
                addNotFoundRow<_add_missing>(added_columns, current_offset);
        }

        if constexpr (STRICTNESS == ASTTableJoin::Strictness::All)
            (*offsets_to_replicate)[i] = current_offset;
    }

    return offsets_to_replicate;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinRightColumns(
    const Join & join, const Map & map, size_t rows, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    AddedColumns & added_columns, ConstNullMapPtr null_map, std::unique_ptr<IColumn::Offsets> & offsets_to_replicate)
{
    constexpr bool left_or_full = static_in_v<KIND, ASTTableJoin::Kind::Left, ASTTableJoin::Kind::Full>;

    IColumn::Filter filter(rows, 0);

    if (null_map)
        offsets_to_replicate = joinRightIndexedColumns<left_or_full, STRICTNESS, KeyGetter, Map, true>(
            join, map, rows, key_columns, key_sizes, added_columns, null_map, filter);
    else
        offsets_to_replicate = joinRightIndexedColumns<left_or_full, STRICTNESS, KeyGetter, Map, false>(
            join, map, rows, key_columns, key_sizes, added_columns, null_map, filter);

    return filter;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinRightColumns(
    Join::Type type, const Join & join,
    const Maps & maps_, size_t rows, const ColumnRawPtrs & key_columns, const Sizes & key_sizes,
    AddedColumns & added_columns, ConstNullMapPtr null_map,
    std::unique_ptr<IColumn::Offsets> & offsets_to_replicate)
{
    switch (type)
    {
    #define M(TYPE) \
        case Join::Type::TYPE: \
            return joinRightColumns<KIND, STRICTNESS, typename KeyGetterForType<Join::Type::TYPE, const std::remove_reference_t<decltype(*maps_.TYPE)>>::Type>(\
                join, *maps_.TYPE, rows, key_columns, key_sizes, added_columns, null_map, offsets_to_replicate);
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
    }
}

} /// nameless


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
    constexpr bool right_or_full = static_in_v<KIND, ASTTableJoin::Kind::Right, ASTTableJoin::Kind::Full>;
    if constexpr (right_or_full)
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
      * For ASOF, the last column is used as the ASOF column
      */
    ColumnsWithTypeAndName extras;
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
        extras.push_back(sample_block_with_keys.getByName(key_names_right.back()));
    AddedColumns added(sample_block_with_columns_to_add, block_with_columns_to_add, block, blocklist_sample, extras);

    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(
        type, *this, maps_, block.rows(), key_columns, key_sizes, added, null_map, offsets_to_replicate);

    for (size_t i = 0; i < added.size(); ++i)
        block.insert(added.moveColumn(i));

    /// Filter & insert missing rows
    auto right_keys = requiredRightKeys(key_names_right, columns_added_by_join);

    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Any || STRICTNESS == ASTTableJoin::Strictness::Asof)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = static_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        constexpr bool inner_or_right = static_in_v<KIND, ASTTableJoin::Kind::Inner, ASTTableJoin::Kind::Right>;
        if constexpr (inner_or_right)
        {
            /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
            for (size_t i = 0; i < existing_columns; ++i)
                block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(filter, -1);

            /// Add join key columns from right block if they has different name.
            for (size_t i = 0; i < key_names_right.size(); ++i)
            {
                auto & right_name = key_names_right[i];
                auto & left_name = key_names_left[i];

                auto it = right_keys.find(right_name);
                if (it != right_keys.end() && !block.has(right_name))
                {
                    const auto & col = block.getByName(left_name);
                    bool is_nullable = it->second->isNullable();
                    block.insert(correctNullability({col.column, col.type, right_name}, is_nullable));
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

                auto it = right_keys.find(right_name);
                if (it != right_keys.end() && !block.has(right_name))
                {
                    const auto & col = block.getByName(left_name);
                    ColumnPtr column = col.column->convertToFullColumnIfConst();
                    MutableColumnPtr mut_column = column->cloneEmpty();

                    for (size_t row = 0; row < filter.size(); ++row)
                    {
                        if (filter[row])
                            mut_column->insertFrom(*column, row);
                        else
                            mut_column->insertDefault();
                    }

                    bool is_nullable = use_nulls || it->second->isNullable();
                    block.insert(correctNullability({std::move(mut_column), col.type, right_name}, is_nullable, null_map_filter));
                }
            }
        }
    }
    else
    {
        constexpr bool left_or_full = static_in_v<KIND, ASTTableJoin::Kind::Left, ASTTableJoin::Kind::Full>;
        if (!offsets_to_replicate)
            throw Exception("No data to filter columns", ErrorCodes::LOGICAL_ERROR);

        /// Add join key columns from right block if they has different name.
        for (size_t i = 0; i < key_names_right.size(); ++i)
        {
            auto & right_name = key_names_right[i];
            auto & left_name = key_names_left[i];

            auto it = right_keys.find(right_name);
            if (it != right_keys.end() && !block.has(right_name))
            {
                const auto & col = block.getByName(left_name);
                ColumnPtr column = col.column->convertToFullColumnIfConst();
                MutableColumnPtr mut_column = column->cloneEmpty();

                size_t last_offset = 0;
                for (size_t row = 0; row < column->size(); ++row)
                {
                    if (size_t to_insert = (*offsets_to_replicate)[row] - last_offset)
                    {
                        if (!row_filter[row])
                            mut_column->insertDefault();
                        else
                            for (size_t dup = 0; dup < to_insert; ++dup)
                                mut_column->insertFrom(*column, row);
                    }

                    last_offset = (*offsets_to_replicate)[row];
                }

                /// TODO: null_map_filter
                bool is_nullable = (use_nulls && left_or_full) || it->second->isNullable();
                block.insert(correctNullability({std::move(mut_column), col.type, right_name}, is_nullable));
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
        joinGetImpl(block, column_name, std::get<MapsAny>(maps));
    }
    else
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::LOGICAL_ERROR);
}


void Join::joinBlock(Block & block, const Names & key_names_left, const NamesAndTypesList & columns_added_by_join) const
{
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
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        for (size_t j = 0; j < columns_right.size(); ++j)
            columns_right[j]->insertFrom(*mapped.block->getByPosition(j).column.get(), mapped.row_num);

        ++rows_added;
    }
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        for (auto current = &static_cast<const typename Mapped::Base &>(mapped); current != nullptr; current = current->next)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
                columns_right[j]->insertFrom(*current->block->getByPosition(j).column.get(), current->row_num);

            ++rows_added;
        }
    }
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Asof, Mapped>
{
    static void add(const Mapped & /*mapped*/, size_t & /*rows_added*/, MutableColumns & /*columns_right*/)
    {
        // If we have a leftover match in the right hand side, not required to join because we are only support asof left/inner
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

        std::vector<bool> is_left_key(left_sample_block.columns(), false);
        std::vector<size_t> key_positions_left;
        key_positions_left.reserve(key_names_left.size());

        for (const std::string & key : key_names_left)
        {
            size_t key_pos = left_sample_block.getPositionByName(key);
            key_positions_left.push_back(key_pos);
            is_left_key[key_pos] = true;
        }

        const Block & right_sample_block = parent.sample_block_with_columns_to_add;

        std::unordered_map<size_t, size_t> left_to_right_key_map;
        makeResultSampleBlock(left_sample_block, right_sample_block, columns_added_by_join,
                              key_positions_left, is_left_key, left_to_right_key_map);

        auto nullability_changes = getNullabilityChanges(parent.sample_block_with_keys, result_sample_block,
                                                         key_positions_left, left_to_right_key_map);

        column_indices_left.reserve(left_sample_block.columns() - key_names_left.size());
        column_indices_keys_and_right.reserve(key_names_left.size() + right_sample_block.columns());
        key_nullability_changes.reserve(key_positions_left.size());

        /// Use right key columns if present. @note left & right key columns could have different nullability.
        for (size_t key_pos : key_positions_left)
        {
            /// Here we establish the mapping between key columns of the left- and right-side tables.
            /// key_pos index is inserted in the position corresponding to key column in parent.blocks
            /// (saved blocks of the right-side table) and points to the same key column
            /// in the left_sample_block and thus in the result_sample_block.

            auto it = left_to_right_key_map.find(key_pos);
            if (it != left_to_right_key_map.end())
            {
                column_indices_left.push_back(key_pos);
                key_pos = it->second;
            }

            column_indices_keys_and_right.push_back(key_pos);
            key_nullability_changes.push_back(nullability_changes.count(key_pos));
        }

        for (size_t i = 0; i < left_sample_block.columns(); ++i)
            if (!is_left_key[i])
                column_indices_left.emplace_back(i);

        size_t num_additional_keys = left_to_right_key_map.size();
        for (size_t i = left_sample_block.columns(); i < result_sample_block.columns() - num_additional_keys; ++i)
            column_indices_keys_and_right.emplace_back(i);
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
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);
        return block;
    }

private:
    const Join & parent;
    UInt64 max_block_size;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that come from the left-side table (except shared right+left key columns).
    ColumnNumbers column_indices_left;
    /// Indices of key columns in result_sample_block or columns that come from the right-side table.
    /// Order is significant: it is the same as the order of columns in the blocks of the right-side table that are saved in parent.blocks.
    ColumnNumbers column_indices_keys_and_right;
    /// Which key columns need change nullability (right is nullable and left is not or vice versa)
    std::vector<bool> key_nullability_changes;

    std::unique_ptr<void, std::function<void(void *)>> position;    /// type erasure


    void makeResultSampleBlock(const Block & left_sample_block, const Block & right_sample_block,
                               const NamesAndTypesList & columns_added_by_join,
                               const std::vector<size_t> & key_positions_left, const std::vector<bool> & is_left_key,
                               std::unordered_map<size_t, size_t> & left_to_right_key_map)
    {
        result_sample_block = materializeBlock(left_sample_block);

        /// Convert left columns to Nullable if allowed
        if (parent.use_nulls)
        {
            for (size_t i = 0; i < result_sample_block.columns(); ++i)
                if (!is_left_key[i])
                    convertColumnToNullable(result_sample_block.getByPosition(i));
        }

        /// Add columns from the right-side table to the block.
        for (size_t i = 0; i < right_sample_block.columns(); ++i)
        {
            const ColumnWithTypeAndName & src_column = right_sample_block.getByPosition(i);
            if (!result_sample_block.has(src_column.name))
                result_sample_block.insert(src_column.cloneEmpty());
        }

        const auto & key_names_right = parent.key_names_right;
        auto right_keys = requiredRightKeys(key_names_right, columns_added_by_join);

        /// Add join key columns from right block if they has different name.
        for (size_t i = 0; i < key_names_right.size(); ++i)
        {
            auto & right_name = key_names_right[i];
            size_t left_key_pos = key_positions_left[i];

            auto it = right_keys.find(right_name);
            if (it != right_keys.end() && !result_sample_block.has(right_name))
            {
                const auto & col = result_sample_block.getByPosition(left_key_pos);
                bool is_nullable = (parent.use_nulls && isFull(parent.kind)) || it->second->isNullable();
                result_sample_block.insert(correctNullability({col.column, col.type, right_name}, is_nullable));

                size_t right_key_pos = result_sample_block.getPositionByName(right_name);
                left_to_right_key_map[left_key_pos] = right_key_pos;
            }
        }
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    Block createBlock(const Maps & maps)
    {
        MutableColumns columns_left = columnsForIndex(result_sample_block, column_indices_left);
        MutableColumns columns_keys_and_right = columnsForIndex(result_sample_block, column_indices_keys_and_right);

        /// Temporary change destination key columns' nullability according to mapped block
        changeNullability(columns_keys_and_right, key_nullability_changes);

        size_t rows_added = 0;

        switch (parent.type)
        {
        #define M(TYPE) \
            case Join::Type::TYPE: \
                rows_added = fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right); \
                break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M

            default:
                throw Exception("Unknown JOIN keys variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
        }

        if (!rows_added)
            return {};

        /// Revert columns nullability
        changeNullability(columns_keys_and_right, key_nullability_changes);

        Block res = result_sample_block.cloneEmpty();

        /// @note it's possible to make ColumnConst here and materialize it later
        for (size_t i = 0; i < columns_left.size(); ++i)
            res.getByPosition(column_indices_left[i]).column = columns_left[i]->cloneResized(rows_added);

        for (size_t i = 0; i < columns_keys_and_right.size(); ++i)
            res.getByPosition(column_indices_keys_and_right[i]).column = std::move(columns_keys_and_right[i]);

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
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
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
            if (it->getSecond().getUsed())
                continue;

            AdderNonJoined<STRICTNESS, typename Map::mapped_type>::add(it->getSecond(), rows_added, columns_keys_and_right);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    static std::unordered_set<size_t> getNullabilityChanges(const Block & sample_block_with_keys, const Block & out_block,
                                                            const std::vector<size_t> & key_positions,
                                                            const std::unordered_map<size_t, size_t> & left_to_right_key_map)
    {
        std::unordered_set<size_t> nullability_changes;

        for (size_t i = 0; i < key_positions.size(); ++i)
        {
            size_t key_pos = key_positions[i];

            auto it = left_to_right_key_map.find(key_pos);
            if (it != left_to_right_key_map.end())
                key_pos = it->second;

            const auto & dst = out_block.getByPosition(key_pos).column;
            const auto & src = sample_block_with_keys.getByPosition(i).column;
            if (dst->isColumnNullable() != src->isColumnNullable())
                nullability_changes.insert(key_pos);
        }

        return nullability_changes;
    }

    static void changeNullability(MutableColumns & columns, const std::vector<bool> & changes_bitmap)
    {
        /// @note changes_bitmap.size() <= columns.size()
        for (size_t i = 0; i < changes_bitmap.size(); ++i)
        {
            if (changes_bitmap[i])
            {
                ColumnPtr column = std::move(columns[i]);
                if (column->isColumnNullable())
                    column = static_cast<const ColumnNullable &>(*column).getNestedColumnPtr();
                else
                    column = makeNullable(column);

                columns[i] = (*std::move(column)).mutate();
            }
        }
    }
};


BlockInputStreamPtr Join::createStreamWithNonJoinedRows(const Block & left_sample_block, const Names & key_names_left,
                                                        const NamesAndTypesList & columns_added_by_join, UInt64 max_block_size) const
{
    return std::make_shared<NonJoinedBlockInputStream>(*this, left_sample_block, key_names_left, columns_added_by_join, max_block_size);
}


}
