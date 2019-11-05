#include <any>

#include <common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/Join.h>
#include <Interpreters/join_common.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Core/ColumnNumbers.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_COLUMN;
}


static ColumnPtr filterWithBlanks(ColumnPtr src_column, const IColumn::Filter & filter, bool inverse_filter = false)
{
    ColumnPtr column = src_column->convertToFullColumnIfConst();
    MutableColumnPtr mut_column = column->cloneEmpty();
    mut_column->reserve(column->size());

    if (inverse_filter)
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertDefault();
            else
                mut_column->insertFrom(*column, row);
        }
    }
    else
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertFrom(*column, row);
            else
                mut_column->insertDefault();
        }
    }

    return mut_column;
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column))
                column.column = filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }

    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
        if (column.type->isNullable() && negative_null_map.size())
        {
            MutableColumnPtr mutable_column = (*std::move(column.column)).mutate();
            assert_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    else
        JoinCommon::removeColumnNullability(column);

    return std::move(column);
}

static void changeNullability(MutableColumnPtr & mutable_column)
{
    ColumnPtr column = std::move(mutable_column);
    if (auto * nullable = checkAndGetColumn<ColumnNullable>(*column))
        column = nullable->getNestedColumnPtr();
    else
        column = makeNullable(column);

    mutable_column = (*std::move(column)).mutate();
}


Join::Join(std::shared_ptr<AnalyzedJoin> table_join_, const Block & right_sample_block, bool any_take_last_row_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , key_names_right(table_join->keyNamesRight())
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , log(&Logger::get("Join"))
{
    setSampleBlock(right_sample_block);
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
            || (isColumnConst(*key_columns[0]) && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
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
    joinDispatchInit(kind, strictness, maps);
    joinDispatch(kind, strictness, maps, [&](auto, auto, auto & map) { map.create(type); });
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
        joinDispatch(kind, strictness, maps, [&](auto, auto, auto & map) { res += map.getTotalRowCount(type); });
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
        joinDispatch(kind, strictness, maps, [&](auto, auto, auto & map) { res += map.getTotalByteCountImpl(type); });
        res += pool.size();
    }

    return res;
}

void Join::setSampleBlock(const Block & block)
{
    /// You have to restore this lock if you call the fuction outside of ctor.
    //std::unique_lock lock(rwlock);

    LOG_DEBUG(log, "setSampleBlock: " << block.dumpStructure());

    if (!empty())
        return;

    ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(key_names_right, block, right_table_keys, sample_block_with_columns_to_add);

    initRightBlockStructure();
    initRequiredRightKeys();

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(sample_block_with_columns_to_add);

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
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.getMapped().insert({stored_block, i}, pool);
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

void Join::initRequiredRightKeys()
{
    const Names & left_keys = table_join->keyNamesLeft();
    const Names & right_keys = table_join->keyNamesRight();
    NameSet required_keys(table_join->requiredRightKeys().begin(), table_join->requiredRightKeys().end());

    for (size_t i = 0; i < right_keys.size(); ++i)
    {
        const String & right_key_name = right_keys[i];

        if (required_keys.count(right_key_name) && !required_right_keys.has(right_key_name))
        {
            const auto & right_key = right_table_keys.getByName(right_key_name);
            required_right_keys.insert(right_key);
            required_right_keys_sources.push_back(left_keys[i]);
        }
    }
}

void Join::initRightBlockStructure()
{
    if (isRightOrFull(kind))
    {
        /// Save keys for NonJoinedBlockInputStream
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == ASTTableJoin::Strictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
        saved_block_sample.insert(column);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(saved_block_sample, (isFull(kind) ? right_table_keys.columns() : 0));
}

Block * Join::storeRightBlock(const Block & source_block)
{
    /// Rare case, when joined columns are constant. To avoid code bloat, simply materialize them.
    Block block = materializeBlock(source_block);

    Block structured_block;
    for (auto & sample_column : saved_block_sample.getColumnsWithTypeAndName())
    {
        auto & column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);
        structured_block.insert(column);
    }

    blocks.push_back(structured_block);
    return &blocks.back();
}

bool Join::addJoinedBlock(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (empty())
        throw Exception("Logical error: Join was not initialized", ErrorCodes::LOGICAL_ERROR);

    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = JoinCommon::temporaryMaterializeColumns(block, key_names_right, materialized_columns);

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    size_t rows = block.rows();
    if (rows)
        has_no_rows_in_maps = false;

    Block * stored_block = storeRightBlock(block);

    if (kind != ASTTableJoin::Kind::Cross)
    {
        joinDispatch(kind, strictness, maps, [&](auto, auto strictness_, auto & map)
        {
            insertFromBlockImpl<strictness_>(*this, type, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        });
    }

    /// If RIGHT or FULL save blocks with nulls for NonJoinedBlockInputStream
    if (isRightOrFull(kind) && null_map)
    {
        UInt8 has_null = 0;
        for (size_t i = 0; !has_null && i < null_map->size(); ++i)
            has_null |= (*null_map)[i];

        if (has_null)
            blocks_nullmaps.emplace_back(stored_block, null_map_holder);
    }

    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
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
                 const Block & saved_block_sample,
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
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.second));
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
        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            added.appendFromBlock(*it->block, it->row_num);
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
                    if (const RowRef * found = mapped.findAsof(join.getAsofType(), join.getAsofInequality(), asof_column, i))
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
            throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(type)), ErrorCodes::UNSUPPORTED_JOIN_KEYS);
    }
}

} /// nameless


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void Join::joinBlockImpl(
    Block & block,
    const Names & key_names_left,
    const Block & block_with_columns_to_add,
    const Maps & maps_) const
{
    /// Rare case, when keys are constant. To avoid code bloat, simply materialize them.
    Columns materialized_columns;
    ColumnRawPtrs key_columns = JoinCommon::temporaryMaterializeColumns(block, key_names_left, materialized_columns);

    /// Keys with NULL value in any column won't join to anything.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    constexpr bool right_or_full = static_in_v<KIND, ASTTableJoin::Kind::Right, ASTTableJoin::Kind::Full>;
    if constexpr (right_or_full)
    {
        materializeBlockInplace(block);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    ColumnsWithTypeAndName extras;
    if constexpr (STRICTNESS == ASTTableJoin::Strictness::Asof)
        extras.push_back(right_table_keys.getByName(key_names_right.back()));
    AddedColumns added(sample_block_with_columns_to_add, block_with_columns_to_add, block, saved_block_sample, extras);

    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(
        type, *this, maps_, block.rows(), key_columns, key_sizes, added, null_map, offsets_to_replicate);

    for (size_t i = 0; i < added.size(); ++i)
        block.insert(added.moveColumn(i));

    /// Filter & insert missing rows
    constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
    constexpr bool inner_or_right = static_in_v<KIND, ASTTableJoin::Kind::Inner, ASTTableJoin::Kind::Right>;

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (!is_all_join && inner_or_right)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            const auto & left_name = required_right_keys_sources[i];

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();
            block.insert(correctNullability({col.column, col.type, right_key.name}, is_nullable));
        }
    }
    else
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            const auto & left_name = required_right_keys_sources[i];

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();

            ColumnPtr thin_column = filterWithBlanks(col.column, filter);
            block.insert(correctNullability({thin_column, col.type, right_key.name}, is_nullable, null_map_filter));

            if constexpr (is_all_join)
                right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
        }
    }

    if constexpr (is_all_join)
    {
        if (!offsets_to_replicate)
            throw Exception("No data to filter columns", ErrorCodes::LOGICAL_ERROR);

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
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
        block, {block.getByPosition(0).name}, {sample_block_with_columns_to_add.getByName(column_name)}, maps_);
}


// TODO: support composite key
// TODO: return multiple columns as named tuple
// TODO: return array of values when strictness == ASTTableJoin::Strictness::All
void Join::joinGet(Block & block, const String & column_name) const
{
    std::shared_lock lock(rwlock);

    if (key_names_right.size() != 1)
        throw Exception("joinGet only supports StorageJoin containing exactly one key", ErrorCodes::LOGICAL_ERROR);

    checkTypeOfKey(block, right_table_keys);

    if (kind == ASTTableJoin::Kind::Left && strictness == ASTTableJoin::Strictness::Any)
    {
        joinGetImpl(block, column_name, std::get<MapsAny>(maps));
    }
    else
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::LOGICAL_ERROR);
}


void Join::joinBlock(Block & block)
{
    std::shared_lock lock(rwlock);

    const Names & key_names_left = table_join->keyNamesLeft();
    JoinCommon::checkTypesOfKeys(block, key_names_left, right_table_keys, key_names_right);

    if (joinDispatch(kind, strictness, maps, [&](auto kind_, auto strictness_, auto & map)
        {
            joinBlockImpl<kind_, strictness_>(block, key_names_left, sample_block_with_columns_to_add, map);
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
    JoinCommon::joinTotals(totals, sample_block_with_columns_to_add, key_names_right, block);
}


template <ASTTableJoin::Strictness STRICTNESS, typename Mapped>
struct AdderNonJoined;

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::Any, Mapped>
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        for (size_t j = 0; j < columns_right.size(); ++j)
        {
            const auto & mapped_column = mapped.block->getByPosition(j).column;
            columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
        }

        ++rows_added;
    }
};

template <typename Mapped>
struct AdderNonJoined<ASTTableJoin::Strictness::All, Mapped>
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = it->block->getByPosition(j).column;
                columns_right[j]->insertFrom(*mapped_column, it->row_num);
            }

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
    NonJoinedBlockInputStream(const Join & parent_, const Block & left_sample_block, UInt64 max_block_size_)
        : parent(parent_)
        , max_block_size(max_block_size_)
    {
        bool remap_keys = parent.table_join->hasUsing();
        std::unordered_map<size_t, size_t> left_to_right_key_remap;

        for (size_t i = 0; i < parent.table_join->keyNamesLeft().size(); ++i)
        {
            const String & left_key_name = parent.table_join->keyNamesLeft()[i];
            const String & right_key_name = parent.table_join->keyNamesRight()[i];

            size_t left_key_pos = left_sample_block.getPositionByName(left_key_name);
            size_t right_key_pos = parent.saved_block_sample.getPositionByName(right_key_name);

            if (remap_keys && !parent.required_right_keys.has(right_key_name))
                left_to_right_key_remap[left_key_pos] = right_key_pos;
        }

        makeResultSampleBlock(left_sample_block);

        for (size_t left_pos = 0; left_pos < left_sample_block.columns(); ++left_pos)
        {
            /// We need right 'x' for 'RIGHT JOIN ... USING(x)'.
            if (left_to_right_key_remap.count(left_pos))
            {
                size_t right_key_pos = left_to_right_key_remap[left_pos];
                setRightIndex(right_key_pos, left_pos);
            }
            else
                column_indices_left.emplace_back(left_pos);
        }

        for (size_t right_pos = 0; right_pos < parent.saved_block_sample.columns(); ++right_pos)
        {
            const String & name = parent.saved_block_sample.getByPosition(right_pos).name;
            if (!result_sample_block.has(name))
                continue;

            size_t result_position = result_sample_block.getPositionByName(name);

            /// Don't remap left keys twice. We need only qualified right keys here
            if (result_position < left_sample_block.columns())
                continue;

            setRightIndex(right_pos, result_position);
        }

        if (column_indices_left.size() + column_indices_right.size() + same_result_keys.size() != result_sample_block.columns())
            throw Exception("Error in columns mapping in RIGHT|FULL JOIN. Left: " + toString(column_indices_left.size()) +
                            ", right: " + toString(column_indices_right.size()) +
                            ", same: " + toString(same_result_keys.size()) +
                            ", result: " + toString(result_sample_block.columns()),
                            ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "NonJoined"; }

    Block getHeader() const override { return result_sample_block; }


protected:
    Block readImpl() override
    {
        if (parent.blocks.empty())
            return Block();
        return createBlock();
    }

private:
    const Join & parent;
    UInt64 max_block_size;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that come from the left-side table: left_pos == result_pos
    std::vector<size_t> column_indices_left;
    /// Indices of columns that come from the right-side table: right_pos -> result_pos
    std::unordered_map<size_t, size_t> column_indices_right;
    ///
    std::unordered_map<size_t, size_t> same_result_keys;
    /// Which right columns (saved in parent) need nullability change before placing them in result block
    std::vector<size_t> right_nullability_changes;

    std::any position;
    std::optional<Join::BlockNullmapList::const_iterator> nulls_position;


    /// "left" columns, "right" not key columns, some "right keys"
    void makeResultSampleBlock(const Block & left_sample_block)
    {
        result_sample_block = materializeBlock(left_sample_block);
        if (parent.nullable_left_side)
            JoinCommon::convertColumnsToNullable(result_sample_block);

        for (const ColumnWithTypeAndName & column : parent.sample_block_with_columns_to_add)
        {
            bool is_nullable = parent.nullable_right_side || column.column->isNullable();
            result_sample_block.insert(correctNullability({column.column, column.type, column.name}, is_nullable));
        }

        for (const ColumnWithTypeAndName & right_key : parent.required_right_keys)
        {
            bool is_nullable = parent.nullable_right_side || right_key.column->isNullable();
            result_sample_block.insert(correctNullability({right_key.column, right_key.type, right_key.name}, is_nullable));
        }
    }

    void setRightIndex(size_t right_pos, size_t result_position)
    {
        if (!column_indices_right.count(right_pos))
        {
            column_indices_right[right_pos] = result_position;

            if (hasNullabilityChange(right_pos, result_position))
                right_nullability_changes.push_back(right_pos);
        }
        else
            same_result_keys[result_position] = column_indices_right[right_pos];
    }

    bool hasNullabilityChange(size_t right_pos, size_t result_pos) const
    {
        const auto & src = parent.saved_block_sample.getByPosition(right_pos).column;
        const auto & dst = result_sample_block.getByPosition(result_pos).column;
        return src->isNullable() != dst->isNullable();
    }

    Block createBlock()
    {
        MutableColumns columns_right = parent.saved_block_sample.cloneEmptyColumns();

        size_t rows_added = 0;

        auto fill_callback = [&](auto, auto strictness, auto & map)
        {
            rows_added = fillColumnsFromMap<strictness>(map, columns_right);
        };

        if (!joinDispatch(parent.kind, parent.strictness, parent.maps, fill_callback))
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);

        fillNullsFromBlocks(columns_right, rows_added);

        if (!rows_added)
            return {};

        for (size_t pos : right_nullability_changes)
            changeNullability(columns_right[pos]);

        Block res = result_sample_block.cloneEmpty();

        /// @note it's possible to make ColumnConst here and materialize it later
        for (size_t pos : column_indices_left)
            res.getByPosition(pos).column = res.getByPosition(pos).column->cloneResized(rows_added);

        for (auto & pr : column_indices_right)
        {
            auto & right_column = columns_right[pr.first];
            auto & result_column = res.getByPosition(pr.second).column;
#ifndef NDEBUG
            if (result_column->getName() != right_column->getName())
                throw Exception("Wrong columns assign in RIGHT|FULL JOIN: " + result_column->getName() +
                                " " + right_column->getName(), ErrorCodes::LOGICAL_ERROR);
#endif
            result_column = std::move(right_column);
        }

        for (auto & pr : same_result_keys)
        {
            auto & src_column = res.getByPosition(pr.second).column;
            auto & dst_column = res.getByPosition(pr.first).column;

            if (src_column->isNullable() && !dst_column->isNullable())
            {
                auto * nullable = checkAndGetColumn<ColumnNullable>(*src_column);
                dst_column = nullable->getNestedColumnPtr();
            }
            else if (!src_column->isNullable() && dst_column->isNullable())
                dst_column = makeNullable(src_column);
            else
                dst_column = src_column;
        }

        return res;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parent.type)
        {
        #define M(TYPE) \
            case Join::Type::TYPE: \
                return fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
            default:
                throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(parent.type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        __builtin_unreachable();
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        using Mapped = typename Map::mapped_type;
        using Iterator = typename Map::const_iterator;

        size_t rows_added = 0;

        if (!position.has_value())
            position = std::make_any<Iterator>(map.begin());

        Iterator & it = std::any_cast<Iterator &>(position);
        auto end = map.end();

        for (; it != end; ++it)
        {
            const Mapped & mapped = it->getSecond();
            if (mapped.getUsed())
                continue;

            AdderNonJoined<STRICTNESS, Mapped>::add(mapped, rows_added, columns_keys_and_right);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parent.blocks_nullmaps.begin();

        auto end = parent.blocks_nullmaps.end();

        for (auto & it = *nulls_position; it != end && rows_added < max_block_size; ++it)
        {
            const Block * block = it->first;
            const NullMap & nullmap = assert_cast<const ColumnUInt8 &>(*it->second).getData();

            for (size_t row = 0; row < nullmap.size(); ++row)
            {
                if (nullmap[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*block->getByPosition(col).column, row);
                    ++rows_added;
                }
            }
        }
    }
};


BlockInputStreamPtr Join::createStreamWithNonJoinedRows(const Block & left_sample_block, UInt64 max_block_size) const
{
    if (isRightOrFull(table_join->kind()))
        return std::make_shared<NonJoinedBlockInputStream>(*this, left_sample_block, max_block_size);
    return {};
}

}
