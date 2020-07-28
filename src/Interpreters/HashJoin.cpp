#include <any>
#include <limits>

#include <common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/HashJoin.h>
#include <Interpreters/join_common.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/DictionaryReader.h>

#include <Storages/StorageDictionary.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Core/ColumnNumbers.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
};

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
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column))
                column.column = filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }

    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column, true);
        if (column.type->isNullable() && !negative_null_map.empty())
        {
            MutableColumnPtr mutable_column = IColumn::mutate(std::move(column.column));
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
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*column))
        column = nullable->getNestedColumnPtr();
    else
        column = makeNullable(column);

    mutable_column = IColumn::mutate(std::move(column));
}

static ColumnPtr emptyNotNullableClone(const ColumnPtr & column)
{
    if (column->isNullable())
        return checkAndGetColumn<ColumnNullable>(*column)->getNestedColumnPtr()->cloneEmpty();
    return column->cloneEmpty();
}

static ColumnPtr changeLowCardinality(const ColumnPtr & column, const ColumnPtr & dst_sample)
{
    if (dst_sample->lowCardinality())
    {
        MutableColumnPtr lc = dst_sample->cloneEmpty();
        typeid_cast<ColumnLowCardinality &>(*lc).insertRangeFromFullColumn(*column, 0, column->size());
        return lc;
    }

    return column->convertToFullColumnIfLowCardinality();
}

/// Change both column nullability and low cardinality
static void changeColumnRepresentation(const ColumnPtr & src_column, ColumnPtr & dst_column)
{
    bool nullable_src = src_column->isNullable();
    bool nullable_dst = dst_column->isNullable();

    ColumnPtr dst_not_null = emptyNotNullableClone(dst_column);
    bool lowcard_src = emptyNotNullableClone(src_column)->lowCardinality();
    bool lowcard_dst = dst_not_null->lowCardinality();
    bool change_lowcard = (!lowcard_src && lowcard_dst) || (lowcard_src && !lowcard_dst);

    if (nullable_src && !nullable_dst)
    {
        const auto * nullable = checkAndGetColumn<ColumnNullable>(*src_column);
        if (change_lowcard)
            dst_column = changeLowCardinality(nullable->getNestedColumnPtr(), dst_column);
        else
            dst_column = nullable->getNestedColumnPtr();
    }
    else if (!nullable_src && nullable_dst)
    {
        if (change_lowcard)
            dst_column = makeNullable(changeLowCardinality(src_column, dst_not_null));
        else
            dst_column = makeNullable(src_column);
    }
    else /// same nullability
    {
        if (change_lowcard)
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*src_column))
            {
                dst_column = makeNullable(changeLowCardinality(nullable->getNestedColumnPtr(), dst_not_null));
                assert_cast<ColumnNullable &>(*dst_column->assumeMutable()).applyNullMap(nullable->getNullMapColumn());
            }
            else
                dst_column = changeLowCardinality(src_column, dst_not_null);
        }
        else
            dst_column = src_column;
    }
}


HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block, bool any_take_last_row_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , key_names_right(table_join->keyNamesRight())
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , log(&Poco::Logger::get("HashJoin"))
{
    setSampleBlock(right_sample_block);
}


HashJoin::Type HashJoin::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
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

template<typename KeyGetter, bool is_asof_join>
static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
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

class KeyGetterForDict
{
public:
    using Mapped = JoinStuff::MappedOne;
    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped>;

    KeyGetterForDict(const ColumnRawPtrs & key_columns_, const Sizes &, void *)
        : key_columns(key_columns_)
    {}

    FindResult findKey(const TableJoin & table_join, size_t row, const Arena &)
    {
        const DictionaryReader & reader = *table_join.dictionary_reader;
        if (!read_result)
        {
            reader.readKeys(*key_columns[0], read_result, found, positions);
            result.block = &read_result;

            if (table_join.forceNullableRight())
                for (auto & column : read_result)
                    if (table_join.rightBecomeNullable(column.type))
                        JoinCommon::convertColumnToNullable(column);
        }

        result.row_num = positions[row];
        return FindResult(&result, found[row]);
    }

private:
    const ColumnRawPtrs & key_columns;
    Block read_result;
    Mapped result;
    ColumnVector<UInt8>::Container found;
    std::vector<size_t> positions;
};

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};


void HashJoin::init(Type type_)
{
    data->type = type_;

    if (kind == ASTTableJoin::Kind::Cross)
        return;
    joinDispatchInit(kind, strictness, data->maps);
    joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { map.create(data->type); });
}

size_t HashJoin::getTotalRowCount() const
{
    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & block : data->blocks)
            res += block.rows();
    }
    else if (data->type != Type::DICT)
    {
        joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { res += map.getTotalRowCount(data->type); });
    }

    return res;
}

size_t HashJoin::getTotalByteCount() const
{
    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & block : data->blocks)
            res += block.bytes();
    }
    else if (data->type != Type::DICT)
    {
        joinDispatch(kind, strictness, data->maps, [&](auto, auto, auto & map) { res += map.getTotalByteCountImpl(data->type); });
        res += data->pool.size();
    }

    return res;
}

void HashJoin::setSampleBlock(const Block & block)
{
    /// You have to restore this lock if you call the function outside of ctor.
    //std::unique_lock lock(rwlock);

    LOG_DEBUG(log, "setSampleBlock: {}", block.dumpStructure());

    if (!empty())
        return;

    JoinCommon::splitAdditionalColumns(block, key_names_right, right_table_keys, sample_block_with_columns_to_add);

    initRequiredRightKeys();

    JoinCommon::removeLowCardinalityInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);

    ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(sample_block_with_columns_to_add);

    if (table_join->dictionary_reader)
    {
        data->type = Type::DICT;
        std::get<MapsOne>(data->maps).create(Type::DICT);
        chooseMethod(key_columns, key_sizes); /// init key_sizes
    }
    else if (strictness == ASTTableJoin::Strictness::Asof)
    {
        if (kind != ASTTableJoin::Kind::Left and kind != ASTTableJoin::Kind::Inner)
            throw Exception("ASOF only supports LEFT and INNER as base joins", ErrorCodes::NOT_IMPLEMENTED);

        const IColumn * asof_column = key_columns.back();
        size_t asof_size;

        asof_type = AsofRowRefs::getTypeSize(asof_column, asof_size);
        if (!asof_type)
        {
            std::string msg = "ASOF join not supported for type: ";
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
    template <typename Map, typename KeyGetter>
    struct Inserter
    {
        static ALWAYS_INLINE void insertOne(const HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i,
                                            Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }

        static ALWAYS_INLINE void insertAll(const HashJoin &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
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

        static ALWAYS_INLINE void insertAsof(HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool,
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
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, JoinStuff::MappedOne> ||
                                    std::is_same_v<typename Map::mapped_type, JoinStuff::MappedOneFlagged>;
        constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = extractAsofColumn(key_columns);

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, asof_column);
            else if constexpr (mapped_one)
                Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    void insertFromBlockImplType(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        if (null_map)
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        else
            insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    void insertFromBlockImpl(
        HashJoin & join, HashJoin::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        switch (type)
        {
            case HashJoin::Type::EMPTY: break;
            case HashJoin::Type::CROSS: break; /// Do nothing. We have already saved block, and it is enough.
            case HashJoin::Type::DICT:  break; /// Noone should call it with Type::DICT.

        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
    }
}

void HashJoin::initRequiredRightKeys()
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

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->forceHashJoin() || isRightOrFull(kind);
    if (save_key_columns)
    {
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

Block HashJoin::structureRightBlock(const Block & block) const
{
    Block structured_block;
    for (const auto & sample_column : savedBlockSample().getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);
        structured_block.insert(column);
    }

    return structured_block;
}

bool HashJoin::addJoinedBlock(const Block & source_block, bool check_limits)
{
    if (empty())
        throw Exception("Logical error: HashJoin was not initialized", ErrorCodes::LOGICAL_ERROR);
    if (overDictionary())
        throw Exception("Logical error: insert into hash-map in HashJoin over dictionary", ErrorCodes::LOGICAL_ERROR);

    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception("Too many rows in right table block for HashJoin: " + toString(source_block.rows()), ErrorCodes::NOT_IMPLEMENTED);

    /// There's no optimization for right side const columns. Remove constness if any.
    Block block = materializeBlock(source_block);
    size_t rows = block.rows();

    ColumnRawPtrs key_columns = JoinCommon::materializeColumnsInplace(block, key_names_right);

    /// We will insert to the map only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    /// If RIGHT or FULL save blocks with nulls for NonJoinedBlockInputStream
    UInt8 save_nullmap = 0;
    if (isRightOrFull(kind) && null_map)
    {
        for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
            save_nullmap |= (*null_map)[i];
    }

    Block structured_block = structureRightBlock(block);
    size_t total_rows = 0;
    size_t total_bytes = 0;

    {
        std::unique_lock lock(data->rwlock);

        data->blocks.emplace_back(std::move(structured_block));
        Block * stored_block = &data->blocks.back();

        if (rows)
            data->empty = false;

        if (kind != ASTTableJoin::Kind::Cross)
        {
            joinDispatch(kind, strictness, data->maps, [&](auto, auto strictness_, auto & map)
            {
                insertFromBlockImpl<strictness_>(*this, data->type, map, rows, key_columns, key_sizes, stored_block, null_map, data->pool);
            });
        }

        if (save_nullmap)
            data->blocks_nullmaps.emplace_back(stored_block, null_map_holder);

        if (!check_limits)
            return true;

        /// TODO: Do not calculate them every time
        total_rows = getTotalRowCount();
        total_bytes = getTotalByteCount();
    }

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
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
                 const ColumnsWithTypeAndName & extras,
                 const HashJoin & join_,
                 const ColumnRawPtrs & key_columns_,
                 const Sizes & key_sizes_)
        : join(join_)
        , key_columns(key_columns_)
        , key_sizes(key_sizes_)
        , rows_to_add(block.rows())
        , need_filter(false)
    {
        size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Don't insert column if it's in left block
            if (!block.has(src_column.name))
                addColumn(src_column);
        }

        for (const auto & extra : extras)
            addColumn(extra);

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.second));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].first, type_name[i].second);
    }

    template <bool has_defaults>
    void appendFromBlock(const Block & block, size_t row_num)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

        for (size_t j = 0; j < right_indexes.size(); ++j)
            columns[j]->insertFrom(*block.getByPosition(right_indexes[j]).column, row_num);
    }

    void appendDefaultRow()
    {
        ++lazy_defaults_count;
    }

    void applyLazyDefaults()
    {
        if (lazy_defaults_count)
        {
            for (size_t j = 0; j < right_indexes.size(); ++j)
                columns[j]->insertManyDefaults(lazy_defaults_count);
            lazy_defaults_count = 0;
        }
    }

    const HashJoin & join;
    const ColumnRawPtrs & key_columns;
    const Sizes & key_sizes;
    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter;

private:
    TypeAndNames type_name;
    MutableColumns columns;
    std::vector<size_t> right_indexes;
    size_t lazy_defaults_count = 0;

    void addColumn(const ColumnWithTypeAndName & src_column)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name);
    }
};

template <typename Map, bool add_missing>
void addFoundRowAll(const typename Map::mapped_type & mapped, AddedColumns & added, IColumn::Offset & current_offset)
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    for (auto it = mapped.begin(); it.ok(); ++it)
    {
        added.appendFromBlock<false>(*it->block, it->row_num);
        ++current_offset;
    }
};

template <bool add_missing, bool need_offset>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]])
{
    if constexpr (need_filter)
        filter[pos] = 1;
}


/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map>
NO_INLINE IColumn::Filter joinRightColumns(const Map & map, AddedColumns & added_columns, const ConstNullMapPtr & null_map [[maybe_unused]])
{
    constexpr bool is_any_join = STRICTNESS == ASTTableJoin::Strictness::Any;
    constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
    constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
    constexpr bool is_semi_join = STRICTNESS == ASTTableJoin::Strictness::Semi;
    constexpr bool is_anti_join = STRICTNESS == ASTTableJoin::Strictness::Anti;
    constexpr bool left = KIND == ASTTableJoin::Kind::Left;
    constexpr bool right = KIND == ASTTableJoin::Kind::Right;
    constexpr bool full = KIND == ASTTableJoin::Kind::Full;

    constexpr bool add_missing = (left || full) && !is_semi_join;
    constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    Arena pool;

    if constexpr (need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_asof_join)
        asof_column = extractAsofColumn(added_columns.key_columns);

    auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(added_columns.key_columns, added_columns.key_sizes);

    IColumn::Offset current_offset = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                addNotFoundRow<add_missing, need_replication>(added_columns, current_offset);

                if constexpr (need_replication)
                    (*added_columns.offsets_to_replicate)[i] = current_offset;
                continue;
            }
        }

        auto find_result = key_getter.findKey(map, i, pool);

        if (find_result.isFound())
        {
            auto & mapped = find_result.getMapped();

            if constexpr (is_asof_join)
            {
                const HashJoin & join = added_columns.join;
                if (const RowRef * found = mapped.findAsof(join.getAsofType(), join.getAsofInequality(), asof_column, i))
                {
                    setUsed<need_filter>(filter, i);
                    mapped.setUsed();
                    added_columns.appendFromBlock<add_missing>(*found->block, found->row_num);
                }
                else
                    addNotFoundRow<add_missing, need_replication>(added_columns, current_offset);
            }
            else if constexpr (is_all_join)
            {
                setUsed<need_filter>(filter, i);
                mapped.setUsed();
                addFoundRowAll<Map, add_missing>(mapped, added_columns, current_offset);
            }
            else if constexpr ((is_any_join || is_semi_join) && right)
            {
                /// Use first appeared left key + it needs left columns replication
                if (mapped.setUsedOnce())
                {
                    setUsed<need_filter>(filter, i);
                    addFoundRowAll<Map, add_missing>(mapped, added_columns, current_offset);
                }
            }
            else if constexpr (is_any_join && KIND == ASTTableJoin::Kind::Inner)
            {
                /// Use first appeared left key only
                if (mapped.setUsedOnce())
                {
                    setUsed<need_filter>(filter, i);
                    added_columns.appendFromBlock<add_missing>(*mapped.block, mapped.row_num);
                }
            }
            else if constexpr (is_any_join && full)
            {
                /// TODO
            }
            else if constexpr (is_anti_join)
            {
                if constexpr (right)
                    mapped.setUsed();
            }
            else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
            {
                setUsed<need_filter>(filter, i);
                mapped.setUsed();
                added_columns.appendFromBlock<add_missing>(*mapped.block, mapped.row_num);
            }
        }
        else
        {
            if constexpr (is_anti_join && left)
                setUsed<need_filter>(filter, i);
            addNotFoundRow<add_missing, need_replication>(added_columns, current_offset);
        }

        if constexpr (need_replication)
            (*added_columns.offsets_to_replicate)[i] = current_offset;
    }

    added_columns.applyLazyDefaults();
    return filter;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinRightColumnsSwitchNullability(const Map & map, AddedColumns & added_columns, const ConstNullMapPtr & null_map)
{
    if (added_columns.need_filter)
    {
        if (null_map)
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, true>(map, added_columns, null_map);
        else
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, false>(map, added_columns, nullptr);
    }
    else
    {
        if (null_map)
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, false, true>(map, added_columns, null_map);
        else
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, false, false>(map, added_columns, nullptr);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinRightColumns(const Maps & maps_, AddedColumns & added_columns, HashJoin::Type type, const ConstNullMapPtr & null_map)
{
    switch (type)
    {
    #define M(TYPE) \
        case HashJoin::Type::TYPE: \
            return joinRightColumnsSwitchNullability<KIND, STRICTNESS,\
                typename KeyGetterForType<HashJoin::Type::TYPE, const std::remove_reference_t<decltype(*maps_.TYPE)>>::Type>(\
                *maps_.TYPE, added_columns, null_map);
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(type)), ErrorCodes::UNSUPPORTED_JOIN_KEYS);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
IColumn::Filter dictionaryJoinRightColumns(const TableJoin & table_join, AddedColumns & added_columns, const ConstNullMapPtr & null_map)
{
    if constexpr (KIND == ASTTableJoin::Kind::Left &&
        (STRICTNESS == ASTTableJoin::Strictness::Any ||
        STRICTNESS == ASTTableJoin::Strictness::Semi ||
        STRICTNESS == ASTTableJoin::Strictness::Anti))
    {
        return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetterForDict>(table_join, added_columns, null_map);
    }

    throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
}

} /// nameless


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImpl(
    Block & block,
    const Names & key_names_left,
    const Block & block_with_columns_to_add,
    const Maps & maps_) const
{
    constexpr bool is_any_join = STRICTNESS == ASTTableJoin::Strictness::Any;
    constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
    constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
    constexpr bool is_semi_join = STRICTNESS == ASTTableJoin::Strictness::Semi;
    constexpr bool is_anti_join = STRICTNESS == ASTTableJoin::Strictness::Anti;

    constexpr bool left = KIND == ASTTableJoin::Kind::Left;
    constexpr bool right = KIND == ASTTableJoin::Kind::Right;
    constexpr bool inner = KIND == ASTTableJoin::Kind::Inner;
    constexpr bool full = KIND == ASTTableJoin::Kind::Full;

    constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);
    constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));

    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    Columns materialized_keys = JoinCommon::materializeColumns(block, key_names_left);
    ColumnRawPtrs key_columns = JoinCommon::getRawPointers(materialized_keys);

    /// Keys with NULL value in any column won't join to anything.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if constexpr (right || full)
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
    if constexpr (is_asof_join)
        extras.push_back(right_table_keys.getByName(key_names_right.back()));

    AddedColumns added_columns(sample_block_with_columns_to_add, block_with_columns_to_add, block, savedBlockSample(),
                               extras, *this, key_columns, key_sizes);
    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = need_filter || has_required_right_keys;

    IColumn::Filter row_filter = overDictionary() ?
        dictionaryJoinRightColumns<KIND, STRICTNESS>(*table_join, added_columns, null_map) :
        switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, data->type, null_map);

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (need_filter)
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
    else if (has_required_right_keys)
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

            if constexpr (need_replication)
                right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
        }
    }

    if constexpr (need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
    }
}

void HashJoin::joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const
{
    size_t max_joined_block_rows = table_join->maxJoinedBlockRows();
    size_t start_left_row = 0;
    size_t start_right_block = 0;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        not_processed.reset();
    }

    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (const ColumnWithTypeAndName & left_column : block)
        {
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : sample_block_with_columns_to_add)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        for (auto & dst : dst_columns)
            dst->reserve(max_joined_block_rows);
    }

    size_t rows_left = block.rows();
    size_t rows_added = 0;

    for (size_t left_row = start_left_row; left_row < rows_left; ++left_row)
    {
        size_t block_number = 0;
        for (const Block & block_right : data->blocks)
        {
            ++block_number;
            if (block_number < start_right_block)
                continue;

            size_t rows_right = block_right.rows();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *block_right.getByPosition(col_num).column;
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1});
            not_processed->block.swap(block);
            break;
        }
    }

    for (const ColumnWithTypeAndName & src_column : sample_block_with_columns_to_add)
        block.insert(src_column);

    block = block.cloneWithColumns(std::move(dst_columns));
}

static void checkTypeOfKey(const Block & block_left, const Block & block_right)
{
    const auto & [c1, left_type_origin, left_name] = block_left.safeGetByPosition(0);
    const auto & [c2, right_type_origin, right_name] = block_right.safeGetByPosition(0);
    auto left_type = removeNullable(left_type_origin);
    auto right_type = removeNullable(right_type_origin);

    if (!left_type->equals(*right_type))
        throw Exception("Type mismatch of columns to joinGet by: "
            + left_name + " " + left_type->getName() + " at left, "
            + right_name + " " + right_type->getName() + " at right",
            ErrorCodes::TYPE_MISMATCH);
}


DataTypePtr HashJoin::joinGetReturnType(const String & column_name, bool or_null) const
{
    std::shared_lock lock(data->rwlock);

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception("StorageJoin doesn't contain column " + column_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null)
        elem.type = makeNullable(elem.type);
    return elem.type;
}


template <typename Maps>
void HashJoin::joinGetImpl(Block & block, const Block & block_with_columns_to_add, const Maps & maps_) const
{
    joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::RightAny>(
        block, {block.getByPosition(0).name}, block_with_columns_to_add, maps_);
}


// TODO: support composite key
// TODO: return multiple columns as named tuple
// TODO: return array of values when strictness == ASTTableJoin::Strictness::All
void HashJoin::joinGet(Block & block, const String & column_name, bool or_null) const
{
    std::shared_lock lock(data->rwlock);

    if (key_names_right.size() != 1)
        throw Exception("joinGet only supports StorageJoin containing exactly one key", ErrorCodes::UNSUPPORTED_JOIN_KEYS);

    checkTypeOfKey(block, right_table_keys);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null)
        elem.type = makeNullable(elem.type);
    elem.column = elem.type->createColumn();

    if ((strictness == ASTTableJoin::Strictness::Any || strictness == ASTTableJoin::Strictness::RightAny) &&
        kind == ASTTableJoin::Kind::Left)
    {
        joinGetImpl(block, {elem}, std::get<MapsOne>(data->maps));
    }
    else
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);
}


void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    std::shared_lock lock(data->rwlock);

    const Names & key_names_left = table_join->keyNamesLeft();
    JoinCommon::checkTypesOfKeys(block, key_names_left, right_table_keys, key_names_right);

    if (overDictionary())
    {
        using Kind = ASTTableJoin::Kind;
        using Strictness = ASTTableJoin::Strictness;

        auto & map = std::get<MapsOne>(data->maps);
        if (kind == Kind::Left)
        {
            switch (strictness)
            {
                case Strictness::Any:
                case Strictness::All:
                    joinBlockImpl<Kind::Left, Strictness::Any>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                case Strictness::Semi:
                    joinBlockImpl<Kind::Left, Strictness::Semi>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                case Strictness::Anti:
                    joinBlockImpl<Kind::Left, Strictness::Anti>(block, key_names_left, sample_block_with_columns_to_add, map);
                    break;
                default:
                    throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
            }
        }
        else if (kind == Kind::Inner && strictness == Strictness::All)
            joinBlockImpl<Kind::Left, Strictness::Semi>(block, key_names_left, sample_block_with_columns_to_add, map);
        else
            throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
    }
    else if (joinDispatch(kind, strictness, data->maps, [&](auto kind_, auto strictness_, auto & map)
        {
            joinBlockImpl<kind_, strictness_>(block, key_names_left, sample_block_with_columns_to_add, map);
        }))
    {
        /// Joined
    }
    else if (kind == ASTTableJoin::Kind::Cross)
        joinBlockImplCross(block, not_processed);
    else
        throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
}


void HashJoin::joinTotals(Block & block) const
{
    JoinCommon::joinTotals(totals, sample_block_with_columns_to_add, key_names_right, block);
}


template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, JoinStuff::MappedAsof>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, JoinStuff::MappedOne> || std::is_same_v<Mapped, JoinStuff::MappedOneFlagged>;

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = mapped.block->getByPosition(j).column;
                columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
            }

            ++rows_added;
        }
        else
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
    }
};


/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : public IBlockInputStream
{
public:
    NonJoinedBlockInputStream(const HashJoin & parent_, const Block & result_sample_block_, UInt64 max_block_size_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , result_sample_block(materializeBlock(result_sample_block_))
    {
        bool remap_keys = parent.table_join->hasUsing();
        std::unordered_map<size_t, size_t> left_to_right_key_remap;

        for (size_t i = 0; i < parent.table_join->keyNamesLeft().size(); ++i)
        {
            const String & left_key_name = parent.table_join->keyNamesLeft()[i];
            const String & right_key_name = parent.table_join->keyNamesRight()[i];

            size_t left_key_pos = result_sample_block.getPositionByName(left_key_name);
            size_t right_key_pos = parent.savedBlockSample().getPositionByName(right_key_name);

            if (remap_keys && !parent.required_right_keys.has(right_key_name))
                left_to_right_key_remap[left_key_pos] = right_key_pos;
        }

        /// result_sample_block: left_sample_block + left expressions, right not key columns, required right keys
        size_t left_columns_count = result_sample_block.columns() -
            parent.sample_block_with_columns_to_add.columns() - parent.required_right_keys.columns();

        for (size_t left_pos = 0; left_pos < left_columns_count; ++left_pos)
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

        const auto & saved_block_sample = parent.savedBlockSample();
        for (size_t right_pos = 0; right_pos < saved_block_sample.columns(); ++right_pos)
        {
            const String & name = saved_block_sample.getByPosition(right_pos).name;
            if (!result_sample_block.has(name))
                continue;

            size_t result_position = result_sample_block.getPositionByName(name);

            /// Don't remap left keys twice. We need only qualified right keys here
            if (result_position < left_columns_count)
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
        if (parent.data->blocks.empty())
            return Block();
        return createBlock();
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;

    Block result_sample_block;
    /// Indices of columns in result_sample_block that should be generated
    std::vector<size_t> column_indices_left;
    /// Indices of columns that come from the right-side table: right_pos -> result_pos
    std::unordered_map<size_t, size_t> column_indices_right;
    ///
    std::unordered_map<size_t, size_t> same_result_keys;
    /// Which right columns (saved in parent) need nullability change before placing them in result block
    std::vector<size_t> right_nullability_adds;
    std::vector<size_t> right_nullability_removes;
    /// Which right columns (saved in parent) need LowCardinality change before placing them in result block
    std::vector<std::pair<size_t, ColumnPtr>> right_lowcard_changes;

    std::any position;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;

    void setRightIndex(size_t right_pos, size_t result_position)
    {
        if (!column_indices_right.count(right_pos))
        {
            column_indices_right[right_pos] = result_position;
            extractColumnChanges(right_pos, result_position);
        }
        else
            same_result_keys[result_position] = column_indices_right[right_pos];
    }

    void extractColumnChanges(size_t right_pos, size_t result_pos)
    {
        const auto & src = parent.savedBlockSample().getByPosition(right_pos).column;
        const auto & dst = result_sample_block.getByPosition(result_pos).column;

        if (!src->isNullable() && dst->isNullable())
            right_nullability_adds.push_back(right_pos);

        if (src->isNullable() && !dst->isNullable())
            right_nullability_removes.push_back(right_pos);

        ColumnPtr src_not_null = emptyNotNullableClone(src);
        ColumnPtr dst_not_null = emptyNotNullableClone(dst);

        if (src_not_null->lowCardinality() != dst_not_null->lowCardinality())
            right_lowcard_changes.push_back({right_pos, dst_not_null});
    }

    Block createBlock()
    {
        MutableColumns columns_right = parent.savedBlockSample().cloneEmptyColumns();

        size_t rows_added = 0;

        auto fill_callback = [&](auto, auto strictness, auto & map)
        {
            rows_added = fillColumnsFromMap<strictness>(map, columns_right);
        };

        if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps, fill_callback))
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);

        fillNullsFromBlocks(columns_right, rows_added);

        if (!rows_added)
            return {};

        for (size_t pos : right_nullability_removes)
            changeNullability(columns_right[pos]);

        for (auto & [pos, dst_sample] : right_lowcard_changes)
            columns_right[pos] = changeLowCardinality(std::move(columns_right[pos]), dst_sample)->assumeMutable();

        for (size_t pos : right_nullability_adds)
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
            changeColumnRepresentation(src_column, dst_column);
        }

        return res;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parent.data->type)
        {
        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
            default:
                throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(parent.data->type)),
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
            const Mapped & mapped = it->getMapped();

            if (mapped.getUsed())
                continue;

            AdderNonJoined<Mapped>::add(mapped, rows_added, columns_keys_and_right);

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
            nulls_position = parent.data->blocks_nullmaps.begin();

        auto end = parent.data->blocks_nullmaps.end();

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


BlockInputStreamPtr HashJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi)
        return {};

    if (isRightOrFull(table_join->kind()))
        return std::make_shared<NonJoinedBlockInputStream>(*this, result_sample_block, max_block_size);
    return {};
}


bool HashJoin::hasStreamWithNonJoinedRows() const
{
     if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi)
        return false;

    return isRightOrFull(table_join->kind());
}

}
