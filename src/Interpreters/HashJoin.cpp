#include <any>
#include <limits>
#include <unordered_map>
#include <vector>

#include <Common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>

#include <Storages/IStorage.h>

#include <Core/ColumnNumbers.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
};

}

namespace JoinStuff
{
    /// for single disjunct
    bool JoinUsedFlags::getUsedSafe(size_t i) const
    {
        return getUsedSafe(nullptr, i);
    }

    /// for multiple disjuncts
    bool JoinUsedFlags::getUsedSafe(const Block * block_ptr, size_t row_idx) const
    {
        if (auto it = flags.find(block_ptr); it != flags.end())
            return it->second[row_idx].load();
        return !need_flags;
    }

    /// for single disjunct
    template <JoinKind KIND, JoinStrictness STRICTNESS>
    void JoinUsedFlags::reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS>::flagged)
        {
            assert(flags[nullptr].size() <= size);
            need_flags = true;
            flags[nullptr] = std::vector<std::atomic_bool>(size);
        }
    }

    /// for multiple disjuncts
    template <JoinKind KIND, JoinStrictness STRICTNESS>
    void JoinUsedFlags::reinit(const Block * block_ptr)
    {
        if constexpr (MapGetter<KIND, STRICTNESS>::flagged)
        {
            assert(flags[block_ptr].size() <= block_ptr->rows());
            need_flags = true;
            flags[block_ptr] = std::vector<std::atomic_bool>(block_ptr->rows());
        }
    }

    template <bool use_flags, bool multiple_disjuncts, typename FindResult>
    void JoinUsedFlags::setUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (multiple_disjuncts)
        {
            auto & mapped = f.getMapped();
            flags[mapped.block][mapped.row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][f.getOffset()].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool multiple_disjuncts>
    void JoinUsedFlags::setUsed(const Block * block, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (multiple_disjuncts)
        {
            flags[block][row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][offset].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool multiple_disjuncts, typename FindResult>
    bool JoinUsedFlags::getUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (multiple_disjuncts)
        {
            auto & mapped = f.getMapped();
            return flags[mapped.block][mapped.row_num].load();
        }
        else
        {
            return flags[nullptr][f.getOffset()].load();
        }
    }

    template <bool use_flags, bool multiple_disjuncts, typename FindResult>
    bool JoinUsedFlags::setUsedOnce(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (multiple_disjuncts)
        {
            auto & mapped = f.getMapped();

            /// fast check to prevent heavy CAS with seq_cst order
            if (flags[mapped.block][mapped.row_num].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[mapped.block][mapped.row_num].compare_exchange_strong(expected, true);
        }
        else
        {
            auto off = f.getOffset();

            /// fast check to prevent heavy CAS with seq_cst order
            if (flags[nullptr][off].load(std::memory_order_relaxed))
                return false;

            bool expected = false;
            return flags[nullptr][off].compare_exchange_strong(expected, true);
        }
    }
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
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }

    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
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

HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , right_sample_block(right_sample_block_)
    , log(&Poco::Logger::get("HashJoin"))
{
    LOG_DEBUG(log, "Right sample block: {}", right_sample_block.dumpStructure());

    if (isCrossOrComma(kind))
    {
        data->type = Type::CROSS;
        sample_block_with_columns_to_add = right_sample_block;
    }
    else if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, sample_block_with_columns_to_add);
        required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        sample_block_with_columns_to_add = right_table_keys = materializeBlock(right_sample_block);
    }

    LOG_TRACE(log, "Columns to add: [{}], required right [{}]",
              sample_block_with_columns_to_add.dumpStructure(), fmt::join(required_right_keys.getNames(), ", "));
    {
        std::vector<String> log_text;
        for (const auto & clause : table_join->getClauses())
            log_text.push_back(clause.formatDebug());
        LOG_TRACE(log, "Joining on: {}", fmt::join(log_text, " | "));
    }

    JoinCommon::convertToFullColumnsInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    size_t disjuncts_num = table_join->getClauses().size();
    data->maps.resize(disjuncts_num);
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

        if (strictness == JoinStrictness::Asof)
        {
            assert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception("Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported", ErrorCodes::NOT_IMPLEMENTED);

            if (key_columns.size() <= 1)
                throw Exception("ASOF join needs at least one equi-join column", ErrorCodes::SYNTAX_ERROR);

            size_t asof_size;
            asof_type = SortedLookupVectorBase::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            data->type = chooseMethod(kind, key_columns, asof_key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto current_join_method = chooseMethod(kind, key_columns, key_sizes.emplace_back());
            if (data->type == Type::EMPTY)
                data->type = current_join_method;
            else if (data->type != current_join_method)
                data->type = Type::hashed;
        }
    }

    for (auto & maps : data->maps)
        dataMapInit(maps);

    LOG_DEBUG(log, "Join type: {}, kind: {}, strictness: {}", data->type, kind, strictness);
}

HashJoin::Type HashJoin::chooseMethod(JoinKind kind, const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

    if (keys_size == 0)
    {
        if (isCrossOrComma(kind))
            return Type::CROSS;
        return Type::EMPTY;
    }

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
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
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

template <typename KeyGetter, bool is_asof_join>
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

template <typename Mapped, bool need_offset = false>
using FindResultImpl = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

/// Dummy key getter, always find nothing, used for JOIN ON NULL
template <typename Mapped>
class KeyGetterEmpty
{
public:
    struct MappedType
    {
        using mapped_type = Mapped;
    };

    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

    KeyGetterEmpty() = default;

    FindResult findKey(MappedType, size_t, const Arena &) { return FindResult(); }
};

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

constexpr bool use_offset = true;

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};

void HashJoin::dataMapInit(MapsVariant & map)
{

    if (kind == JoinKind::Cross)
        return;
    joinDispatchInit(kind, strictness, map);
    joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { map_.create(data->type); });
}

bool HashJoin::empty() const
{
    return data->type == Type::EMPTY;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    return isInnerOrRight(getKind()) && data->empty;
}

size_t HashJoin::getTotalRowCount() const
{
    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & block : data->blocks)
            res += block.rows();
    }
    else
    {
        for (const auto & map : data->maps)
        {
            joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalRowCount(data->type); });
        }
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
    else
    {
        for (const auto & map : data->maps)
        {
            joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
        }
        res += data->pool.size();
    }

    return res;
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
                                             const IColumn & asof_column)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);
            typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

            TypeIndex asof_type = *join.getAsofType();
            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) typename Map::mapped_type(createAsofRowRef(asof_type, join.getAsofInequality()));
            (*time_series_map)->insert(asof_column, stored_block, i);
        }
    };


    template <JoinStrictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    size_t NO_INLINE insertFromBlockImplTypeCase(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRef>;
        constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = key_columns.back();

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            /// Check condition for right table from ON section
            if (join_mask && !(*join_mask)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, *asof_column);
            else if constexpr (mapped_one)
                Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
        return map.getBufferSizeInCells();
    }


    template <JoinStrictness STRICTNESS, typename KeyGetter, typename Map>
    size_t insertFromBlockImplType(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool)
    {
        if (null_map)
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(
                join, map, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool);
        else
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(
                join, map, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool);
    }


    template <JoinStrictness STRICTNESS, typename Maps>
    size_t insertFromBlockImpl(
        HashJoin & join, HashJoin::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool)
    {
        switch (type)
        {
            case HashJoin::Type::EMPTY: return 0;
            case HashJoin::Type::CROSS: return 0; /// Do nothing. We have already saved block, and it is enough.

        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
        __builtin_unreachable();
    }
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    if (isCrossOrComma(kind))
    {
        /// cross join doesn't have keys, just add all columns
        saved_block_sample = sample_block_with_columns_to_add.cloneEmpty();
        return;
    }

    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) || isRightOrFull(kind) || multiple_disjuncts;
    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == JoinStrictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
    {
        if (!saved_block_sample.findByName(column.name))
            saved_block_sample.insert(column);
    }
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
    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception("Too many rows in right table block for HashJoin: " + toString(source_block.rows()), ErrorCodes::NOT_IMPLEMENTED);

    /// There's no optimization for right side const columns. Remove constness if any.
    Block block = materializeBlock(source_block);
    size_t rows = block.rows();

    ColumnRawPtrMap all_key_columns = JoinCommon::materializeColumnsInplaceMap(block, table_join->getAllNames(JoinTableSide::Right));

    Block structured_block = structureRightBlock(block);
    size_t total_rows = 0;
    size_t total_bytes = 0;
    {
        if (storage_join_lock)
            throw DB::Exception("addJoinedBlock called when HashJoin locked to prevent updates",
                                ErrorCodes::LOGICAL_ERROR);

        data->blocks.emplace_back(std::move(structured_block));
        Block * stored_block = &data->blocks.back();

        if (rows)
            data->empty = false;

        bool multiple_disjuncts = !table_join->oneDisjunct();
        const auto & onexprs = table_join->getClauses();
        for (size_t onexpr_idx = 0; onexpr_idx < onexprs.size(); ++onexpr_idx)
        {
            ColumnRawPtrs key_columns;
            for (const auto & name : onexprs[onexpr_idx].key_names_right)
                key_columns.push_back(all_key_columns[name]);

            /// We will insert to the map only keys, where all components are not NULL.
            ConstNullMapPtr null_map{};
            ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

            /// If RIGHT or FULL save blocks with nulls for NotJoinedBlocks
            UInt8 save_nullmap = 0;
            if (isRightOrFull(kind) && null_map)
            {
                /// Save rows with NULL keys
                for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
                    save_nullmap |= (*null_map)[i];
            }

            auto join_mask_col = JoinCommon::getColumnAsMask(block, onexprs[onexpr_idx].condColumnNames().second);
            /// Save blocks that do not hold conditions in ON section
            ColumnUInt8::MutablePtr not_joined_map = nullptr;
            if (!multiple_disjuncts && isRightOrFull(kind) && !join_mask_col.isConstant())
            {
                const auto & join_mask = join_mask_col.getData();
                /// Save rows that do not hold conditions
                not_joined_map = ColumnUInt8::create(block.rows(), 0);
                for (size_t i = 0, sz = join_mask->size(); i < sz; ++i)
                {
                    /// Condition hold, do not save row
                    if ((*join_mask)[i])
                        continue;

                    /// NULL key will be saved anyway because, do not save twice
                    if (save_nullmap && (*null_map)[i])
                        continue;

                    not_joined_map->getData()[i] = 1;
                }
            }

            if (kind != JoinKind::Cross)
            {
                joinDispatch(kind, strictness, data->maps[onexpr_idx], [&](auto kind_, auto strictness_, auto & map)
                {
                    size_t size = insertFromBlockImpl<strictness_>(
                        *this, data->type, map, rows, key_columns, key_sizes[onexpr_idx], stored_block, null_map,
                        /// If mask is false constant, rows are added to hashmap anyway. It's not a happy-flow, so this case is not optimized
                        join_mask_col.getData(),
                        data->pool);

                    if (multiple_disjuncts)
                        used_flags.reinit<kind_, strictness_>(stored_block);
                    else
                        /// Number of buckets + 1 value from zero storage
                        used_flags.reinit<kind_, strictness_>(size + 1);
                });
            }

            if (!multiple_disjuncts && save_nullmap)
                data->blocks_nullmaps.emplace_back(stored_block, null_map_holder);

            if (!multiple_disjuncts && not_joined_map)
                data->blocks_nullmaps.emplace_back(stored_block, std::move(not_joined_map));

            if (!check_limits)
                return true;

            /// TODO: Do not calculate them every time
            total_rows = getTotalRowCount();
            total_bytes = getTotalByteCount();
        }
    }

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


namespace
{

struct JoinOnKeyColumns
{
    Names key_names;

    Columns materialized_keys_holder;
    ColumnRawPtrs key_columns;

    ConstNullMapPtr null_map;
    ColumnPtr null_map_holder;

    /// Only rows where mask == true can be joined
    JoinCommon::JoinMask join_mask_column;

    Sizes key_sizes;


    explicit JoinOnKeyColumns(const Block & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_)
        : key_names(key_names_)
        , materialized_keys_holder(JoinCommon::materializeColumns(block, key_names)) /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
        , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
        , null_map(nullptr)
        , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
        , join_mask_column(JoinCommon::getColumnAsMask(block, cond_column_name))
        , key_sizes(key_sizes_)
    {
    }

    bool isRowFiltered(size_t i) const { return join_mask_column.isRowFiltered(i); }
};

class AddedColumns
{
public:
    struct TypeAndName
    {
        DataTypePtr type;
        String name;
        String qualified_name;

        TypeAndName(DataTypePtr type_, const String & name_, const String & qualified_name_)
            : type(type_), name(name_), qualified_name(qualified_name_)
        {
        }
    };

    AddedColumns(
        const Block & block_with_columns_to_add,
        const Block & block,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        bool is_asof_join,
        bool is_join_get_)
        : join_on_keys(join_on_keys_)
        , rows_to_add(block.rows())
        , sample_block(saved_block_sample)
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!block.has(qualified_name))
                addColumn(src_column, qualified_name);
        }

        if (is_asof_join)
        {
            assert(join_on_keys.size() == 1);
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column, right_asof_column.name);
            left_asof_key = join_on_keys[0].key_columns.back();
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }

    static void assertBlockEqualsStructureUpToLowCard(const Block & lhs_block, const Block & rhs_block)
    {
        if (lhs_block.columns() != rhs_block.columns())
            throw Exception("Different number of columns in blocks", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < lhs_block.columns(); ++i)
        {
            const auto & lhs = lhs_block.getByPosition(i);
            const auto & rhs = rhs_block.getByPosition(i);
            if (lhs.name != rhs.name)
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Block structure mismatch: [{}] != [{}]",
                    lhs_block.dumpStructure(), rhs_block.dumpStructure());

            const auto & ltype = recursiveRemoveLowCardinality(lhs.type);
            const auto & rtype = recursiveRemoveLowCardinality(rhs.type);
            if (!ltype->equals(*rtype))
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Block structure mismatch: [{}] != [{}]",
                    lhs_block.dumpStructure(), rhs_block.dumpStructure());

            const auto & lcol = recursiveRemoveLowCardinality(lhs.column);
            const auto & rcol = recursiveRemoveLowCardinality(rhs.column);
            if (lcol->getDataType() != rcol->getDataType())
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Block structure mismatch: [{}] != [{}]",
                    lhs_block.dumpStructure(), rhs_block.dumpStructure());
        }
    }

    template <bool has_defaults>
    void appendFromBlock(const Block & block, size_t row_num)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

#ifndef NDEBUG
        /// Like assertBlocksHaveEqualStructure but doesn't check low cardinality
        assertBlockEqualsStructureUpToLowCard(sample_block, block);
#else
        UNUSED(assertBlockEqualsStructureUpToLowCard);
#endif

        if (is_join_get)
        {
            /// If it's joinGetOrNull, we need to wrap not-nullable columns in StorageJoin.
            for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            {
                const auto & column_from_block = block.getByPosition(right_indexes[j]);
                if (auto * nullable_col = typeid_cast<ColumnNullable *>(columns[j].get());
                    nullable_col && !column_from_block.column->isNullable())
                    nullable_col->insertFromNotNullable(*column_from_block.column, row_num);
                else if (auto * lowcard_col = typeid_cast<ColumnLowCardinality *>(columns[j].get());
                         lowcard_col && !typeid_cast<const ColumnLowCardinality *>(column_from_block.column.get()))
                    lowcard_col->insertFromFullColumn(*column_from_block.column, row_num);
                else
                    columns[j]->insertFrom(*column_from_block.column, row_num);
            }
        }
        else
        {
            for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            {
                const auto & column_from_block = block.getByPosition(right_indexes[j]);
                if (auto * lowcard_col = typeid_cast<ColumnLowCardinality *>(columns[j].get());
                    lowcard_col && !typeid_cast<const ColumnLowCardinality *>(column_from_block.column.get()))
                    lowcard_col->insertFromFullColumn(*column_from_block.column, row_num);
                else
                    columns[j]->insertFrom(*column_from_block.column, row_num);
            }
        }
    }

    void appendDefaultRow()
    {
        ++lazy_defaults_count;
    }

    void applyLazyDefaults()
    {
        if (lazy_defaults_count)
        {
            for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
                JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
            lazy_defaults_count = 0;
        }
    }

    const IColumn & leftAsofKey() const { return *left_asof_key; }

    std::vector<JoinOnKeyColumns> join_on_keys;

    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;

private:
    std::vector<TypeAndName> type_name;
    MutableColumns columns;
    std::vector<size_t> right_indexes;
    size_t lazy_defaults_count = 0;
    /// for ASOF
    const IColumn * left_asof_key = nullptr;
    Block sample_block;

    bool is_join_get;

    void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
    {

        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name, qualified_name);
    }
};

template <JoinKind KIND, JoinStrictness STRICTNESS>
struct JoinFeatures
{
    static constexpr bool is_any_join = STRICTNESS == JoinStrictness::Any;
    static constexpr bool is_any_or_semi_join = STRICTNESS == JoinStrictness::Any || STRICTNESS == JoinStrictness::RightAny || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Left);
    static constexpr bool is_all_join = STRICTNESS == JoinStrictness::All;
    static constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;
    static constexpr bool is_semi_join = STRICTNESS == JoinStrictness::Semi;
    static constexpr bool is_anti_join = STRICTNESS == JoinStrictness::Anti;

    static constexpr bool left = KIND == JoinKind::Left;
    static constexpr bool right = KIND == JoinKind::Right;
    static constexpr bool inner = KIND == JoinKind::Inner;
    static constexpr bool full = KIND == JoinKind::Full;

    static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);
    static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));
    static constexpr bool add_missing = (left || full) && !is_semi_join;

    static constexpr bool need_flags = MapGetter<KIND, STRICTNESS>::flagged;

};

template <bool multiple_disjuncts>
class KnownRowsHolder;

/// Keep already joined rows to prevent duplication if many disjuncts
///   if for a particular pair of rows condition looks like TRUE or TRUE or TRUE
///   we want to have it once in resultset
template<>
class KnownRowsHolder<true>
{
public:
    using Type = std::pair<const Block *, DB::RowRef::SizeT>;

private:
    static const size_t MAX_LINEAR = 16; // threshold to switch from Array to Set
    using ArrayHolder = std::array<Type, MAX_LINEAR>;
    using SetHolder = std::set<Type>;
    using SetHolderPtr = std::unique_ptr<SetHolder>;

    ArrayHolder array_holder;
    SetHolderPtr set_holder_ptr;

    size_t items;

public:
    KnownRowsHolder()
        : items(0)
    {
    }


    template<class InputIt>
    void add(InputIt from, InputIt to)
    {
        const size_t new_items = std::distance(from, to);
        if (items + new_items <= MAX_LINEAR)
        {
            std::copy(from, to, &array_holder[items]);
        }
        else
        {
            if (items <= MAX_LINEAR)
            {
                set_holder_ptr = std::make_unique<SetHolder>();
                set_holder_ptr->insert(std::cbegin(array_holder), std::cbegin(array_holder) + items);
            }
            set_holder_ptr->insert(from, to);
        }
        items += new_items;
    }

    template<class Needle>
    bool isKnown(const Needle & needle)
    {
        return items <= MAX_LINEAR
            ? std::find(std::cbegin(array_holder), std::cbegin(array_holder) + items, needle) != std::cbegin(array_holder) + items
            : set_holder_ptr->find(needle) != set_holder_ptr->end();
    }
};

template<>
class KnownRowsHolder<false>
{
public:
    template<class InputIt>
    void add(InputIt, InputIt)
    {
    }

    template<class Needle>
    static bool isKnown(const Needle &)
    {
        return false;
    }
};

template <typename Map, bool add_missing, bool multiple_disjuncts>
void addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<multiple_disjuncts> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * used_flags [[maybe_unused]])
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    if constexpr (multiple_disjuncts)
    {
        std::unique_ptr<std::vector<KnownRowsHolder<true>::Type>> new_known_rows_ptr;

        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            if (!known_rows.isKnown(std::make_pair(it->block, it->row_num)))
            {
                added.appendFromBlock<false>(*it->block, it->row_num);
                ++current_offset;
                if (!new_known_rows_ptr)
                {
                    new_known_rows_ptr = std::make_unique<std::vector<KnownRowsHolder<true>::Type>>();
                }
                new_known_rows_ptr->push_back(std::make_pair(it->block, it->row_num));
                if (used_flags)
                {
                    used_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, multiple_disjuncts>(
                        FindResultImpl<const RowRef, false>(*it, true, 0));
                }
            }
        }

        if (new_known_rows_ptr)
        {
            known_rows.add(std::cbegin(*new_known_rows_ptr), std::cend(*new_known_rows_ptr));
        }
    }
    else
    {
        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            added.appendFromBlock<false>(*it->block, it->row_num);
            ++current_offset;
        }
    }
}

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
template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map, bool multiple_disjuncts>
NO_INLINE IColumn::Filter joinRightColumns(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    Arena pool;

    if constexpr (jf.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        bool right_row_found = false;
        bool null_element_found = false;

        KnownRowsHolder<multiple_disjuncts> known_rows;
        for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
            if constexpr (has_null_map)
            {
                if (join_keys.null_map && (*join_keys.null_map)[i])
                {
                    null_element_found = true;
                    continue;
                }
            }

            bool row_acceptable = !join_keys.isRowFiltered(i);
            using FindResult = typename KeyGetter::FindResult;
            auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), i, pool) : FindResult();

            if (find_result.isFound())
            {
                right_row_found = true;
                auto & mapped = find_result.getMapped();
                if constexpr (jf.is_asof_join)
                {
                    const IColumn & left_asof_key = added_columns.leftAsofKey();

                    auto row_ref = mapped->findAsof(left_asof_key, i);
                    if (row_ref.block)
                    {
                        setUsed<need_filter>(filter, i);
                        if constexpr (multiple_disjuncts)
                            used_flags.template setUsed<jf.need_flags, multiple_disjuncts>(row_ref.block, row_ref.row_num, 0);
                        else
                            used_flags.template setUsed<jf.need_flags, multiple_disjuncts>(find_result);

                        added_columns.appendFromBlock<jf.add_missing>(*row_ref.block, row_ref.row_num);
                    }
                    else
                        addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
                }
                else if constexpr (jf.is_all_join)
                {
                    setUsed<need_filter>(filter, i);
                    used_flags.template setUsed<jf.need_flags, multiple_disjuncts>(find_result);
                    auto used_flags_opt = jf.need_flags ? &used_flags : nullptr;
                    addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                }
                else if constexpr ((jf.is_any_join || jf.is_semi_join) && jf.right)
                {
                    /// Use first appeared left key + it needs left columns replication
                    bool used_once = used_flags.template setUsedOnce<jf.need_flags, multiple_disjuncts>(find_result);
                    if (used_once)
                    {
                        auto used_flags_opt = jf.need_flags ? &used_flags : nullptr;
                        setUsed<need_filter>(filter, i);
                        addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                    }
                }
                else if constexpr (jf.is_any_join && KIND == JoinKind::Inner)
                {
                    bool used_once = used_flags.template setUsedOnce<jf.need_flags, multiple_disjuncts>(find_result);

                    /// Use first appeared left key only
                    if (used_once)
                    {
                        setUsed<need_filter>(filter, i);
                        added_columns.appendFromBlock<jf.add_missing>(*mapped.block, mapped.row_num);
                    }

                    break;
                }
                else if constexpr (jf.is_any_join && jf.full)
                {
                    /// TODO
                }
                else if constexpr (jf.is_anti_join)
                {
                    if constexpr (jf.right && jf.need_flags)
                        used_flags.template setUsed<jf.need_flags, multiple_disjuncts>(find_result);
                }
                else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
                {
                    setUsed<need_filter>(filter, i);
                    used_flags.template setUsed<jf.need_flags, multiple_disjuncts>(find_result);
                    added_columns.appendFromBlock<jf.add_missing>(*mapped.block, mapped.row_num);

                    if (jf.is_any_or_semi_join)
                    {
                        break;
                    }
                }
            }
        }

        if constexpr (has_null_map)
        {
            if (!right_row_found && null_element_found)
            {
                addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);

                if constexpr (jf.need_replication)
                {
                   (*added_columns.offsets_to_replicate)[i] = current_offset;
                }
                continue;
            }
        }

        if (!right_row_found)
        {
            if constexpr (jf.is_anti_join && jf.left)
                setUsed<need_filter>(filter, i);
            addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
        }

        if constexpr (jf.need_replication)
        {
           (*added_columns.offsets_to_replicate)[i] = current_offset;
        }
    }

    added_columns.applyLazyDefaults();
    return filter;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map>
IColumn::Filter joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    return mapv.size() > 1
        ? joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags)
        : joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, has_null_map, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags)
{
    bool has_null_map = std::any_of(added_columns.join_on_keys.begin(), added_columns.join_on_keys.end(),
                                    [](const auto & k) { return k.null_map; });
    if (added_columns.need_filter)
    {
        if (has_null_map)
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
        else
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
    }
    else
    {
        if (has_null_map)
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
        else
            return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinRightColumns(
    const std::vector<const Maps *> & mapv,
    AddedColumns & added_columns,
    HashJoin::Type type,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;
    switch (type)
    {
        case HashJoin::Type::EMPTY:
        {
            if constexpr (!is_asof_join)
            {
                using KeyGetter = KeyGetterEmpty<typename Maps::MappedType>;
                std::vector<KeyGetter> key_getter_vector;
                key_getter_vector.emplace_back();

                using MapTypeVal = typename KeyGetter::MappedType;
                std::vector<const MapTypeVal *> a_map_type_vector;
                a_map_type_vector.emplace_back();
                return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetter>(
                        std::move(key_getter_vector), a_map_type_vector, added_columns, used_flags);
            }
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys. Type: {}", type);
        }
    #define M(TYPE) \
        case HashJoin::Type::TYPE: \
            {                                                           \
            using MapTypeVal = const typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
            using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, MapTypeVal>::Type; \
            std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
            std::vector<KeyGetter> key_getter_vector;                     \
            for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d)                   \
            {       \
                const auto & join_on_key = added_columns.join_on_keys[d];     \
                a_map_type_vector[d] = mapv[d]->TYPE.get();              \
                key_getter_vector.push_back(std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes))); \
            }                                                           \
            return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetter>( \
                              std::move(key_getter_vector), a_map_type_vector, added_columns, used_flags); \
    }
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", type);
    }
}

} /// nameless

template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImpl(
    Block & block,
    const Block & block_with_columns_to_add,
    const std::vector<const Maps *> & maps_,
    bool is_join_get) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & onexprs = table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = !is_join_get ? onexprs[i].key_names_left : onexprs[i].key_names_right;
        join_on_keys.emplace_back(block, key_names, onexprs[i].condColumnNames().first, key_sizes[i]);
    }
    size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if constexpr (jf.right || jf.full)
    {
        materializeBlockInplace(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns added_columns(
        block_with_columns_to_add,
        block,
        savedBlockSample(),
        *this,
        std::move(join_on_keys),
        jf.is_asof_join,
        is_join_get);

    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = jf.need_filter || has_required_right_keys;

    IColumn::Filter row_filter = switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, data->type, used_flags);

    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(row_filter, -1);

        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            // renamed ???
            if (!block.findByName(right_key.name))
            {
                const auto & left_name = required_right_keys_sources[i];

                /// asof column is already in block.
                if (jf.is_asof_join && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = JoinCommon::isNullable(right_key.type);
                auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
                ColumnWithTypeAndName right_col(col.column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = correctNullability(std::move(right_col), is_nullable);
                block.insert(std::move(right_col));
            }
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
            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            if (!block.findByName(right_col_name /*right_key.name*/))
            {
                const auto & left_name = required_right_keys_sources[i];

                /// asof column is already in block.
                if (jf.is_asof_join && right_key.name == table_join->getOnlyClause().key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = JoinCommon::isNullable(right_key.type);

                ColumnPtr thin_column = JoinCommon::filterWithBlanks(col.column, filter);

                ColumnWithTypeAndName right_col(thin_column, col.type, right_col_name);
                if (right_col.type->lowCardinality() != right_key.type->lowCardinality())
                    JoinCommon::changeLowCardinalityInplace(right_col);
                right_col = correctNullability(std::move(right_col), is_nullable, null_map_filter);
                block.insert(std::move(right_col));

                if constexpr (jf.need_replication)
                    right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
            }
        }
    }

    if constexpr (jf.need_replication)
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

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            "Number of arguments for function joinGet" + toString(or_null ? "OrNull" : "")
                + " doesn't match: passed, should be equal to " + toString(num_keys),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(
                "Type mismatch in joinGet key " + toString(i) + ": found type " + left_type->getName() + ", while the needed type is "
                    + right_type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception("StorageJoin doesn't contain column " + column_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null && JoinCommon::canBecomeNullable(elem.type))
        elem.type = makeNullable(elem.type);
    return elem.type;
}

/// TODO: return multiple columns as named tuple
/// TODO: return array of values when strictness == JoinStrictness::All
ColumnWithTypeAndName HashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add) const
{
    bool is_valid = (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny)
        && kind == JoinKind::Left;
    if (!is_valid)
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);
    const auto & key_names_right = table_join->getOnlyClause().key_names_right;

    /// Assemble the key block with correct names.
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    static_assert(!MapGetter<JoinKind::Left, JoinStrictness::Any>::flagged,
                  "joinGet are not protected from hash table changes between block processing");

    std::vector<const MapsOne *> maps_vector;
    maps_vector.push_back(&std::get<MapsOne>(data->maps[0]));
    joinBlockImpl<JoinKind::Left, JoinStrictness::Any>(
        keys, block_with_columns_to_add, maps_vector, true);
    return keys.getByPosition(keys.columns() - 1);
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block, onexpr.key_names_left, cond_column_name.first,
            right_sample_block, onexpr.key_names_right, cond_column_name.second);
    }

    if (kind == JoinKind::Cross)
    {
        joinBlockImplCross(block, not_processed);
        return;
    }

    if (kind == JoinKind::Right || kind == JoinKind::Full)
    {
        materializeBlockInplace(block);
    }

    {
        std::vector<const std::decay_t<decltype(data->maps[0])> * > maps_vector;
        for (size_t i = 0; i < table_join->getClauses().size(); ++i)
            maps_vector.push_back(&data->maps[i]);

        if (joinDispatch(kind, strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_)
        {
            joinBlockImpl<kind_, strictness_>(block, sample_block_with_columns_to_add, maps_vector_);
        }))
        {
            /// Joined
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
    }
}

template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

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
/// Based on:
///   - map offsetInternal saved in used_flags for single disjuncts
///   - flags in BlockWithFlags for multiple disjuncts
template<bool multiple_disjuncts>
class NotJoinedHash final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedHash(const HashJoin & parent_, UInt64 max_block_size_)
        : parent(parent_), max_block_size(max_block_size_), current_block_start(0)
    {}

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        if (unlikely(parent.data->type == HashJoin::Type::EMPTY))
        {
            rows_added = fillColumnsFromData(parent.data->blocks, columns_right);
        }
        else
        {
            auto fill_callback = [&](auto, auto strictness, auto & map)
            {
                rows_added = fillColumnsFromMap<strictness>(map, columns_right);
            };

            if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps.front(), fill_callback))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness '{}' (must be on of: ANY, ALL, ASOF)", parent.strictness);
        }

        if constexpr (!multiple_disjuncts)
        {
            fillNullsFromBlocks(columns_right, rows_added);
        }

        return rows_added;
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;

    size_t current_block_start;

    std::any position;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;
    std::optional<BlocksList::const_iterator> used_position;

    size_t fillColumnsFromData(const BlocksList & blocks, MutableColumns & columns_right)
    {
        if (!position.has_value())
            position = std::make_any<BlocksList::const_iterator>(blocks.begin());

        auto & block_it = std::any_cast<BlocksList::const_iterator &>(position);
        auto end = blocks.end();

        size_t rows_added = 0;
        for (; block_it != end; ++block_it)
        {
            size_t rows_from_block = std::min<size_t>(max_block_size - rows_added, block_it->rows() - current_block_start);
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & col = block_it->getByPosition(j).column;
                columns_right[j]->insertRangeFrom(*col, current_block_start, rows_from_block);
            }
            rows_added += rows_from_block;

            if (rows_added >= max_block_size)
            {
                /// How many rows have been read
                current_block_start += rows_from_block;
                if (block_it->rows() <= current_block_start)
                {
                    /// current block was fully read
                    ++block_it;
                    current_block_start = 0;
                }
                break;
            }
            current_block_start = 0;
        }
        return rows_added;
    }

    template <JoinStrictness STRICTNESS, typename Maps>
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
                throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", parent.data->type)   ;
        }

        __builtin_unreachable();
    }

    template <JoinStrictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        size_t rows_added = 0;

        if constexpr (multiple_disjuncts)
        {
            if (!used_position.has_value())
                used_position = parent.data->blocks.begin();

            auto end = parent.data->blocks.end();

            for (auto & it = *used_position; it != end && rows_added < max_block_size; ++it)
            {
                const Block & mapped_block = *it;

                for (size_t row = 0; row < mapped_block.rows(); ++row)
                {
                    if (!parent.isUsed(&mapped_block, row))
                    {
                        for (size_t colnum = 0; colnum < columns_keys_and_right.size(); ++colnum)
                        {
                            columns_keys_and_right[colnum]->insertFrom(*mapped_block.getByPosition(colnum).column, row);
                        }

                        ++rows_added;
                    }
                }
            }
        }
        else
        {
            using Mapped = typename Map::mapped_type;
            using Iterator = typename Map::const_iterator;


            if (!position.has_value())
                position = std::make_any<Iterator>(map.begin());

            Iterator & it = std::any_cast<Iterator &>(position);
            auto end = map.end();

            for (; it != end; ++it)
            {
                const Mapped & mapped = it->getMapped();

                size_t off = map.offsetInternal(it.getPtr());
                if (parent.isUsed(off))
                    continue;
                AdderNonJoined<Mapped>::add(mapped, rows_added, columns_keys_and_right);

                if (rows_added >= max_block_size)
                {
                    ++it;
                    break;
                }
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
            const auto * block = it->first;
            ConstNullMapPtr nullmap = nullptr;
            if (it->second)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->second).getData();

            for (size_t row = 0; row < block->rows(); ++row)
            {
                if (nullmap && (*nullmap)[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*block->getByPosition(col).column, row);
                    ++rows_added;
                }
            }
        }
    }
};

std::shared_ptr<NotJoinedBlocks> HashJoin::getNonJoinedBlocks(const Block & left_sample_block,
                                                              const Block & result_sample_block,
                                                              UInt64 max_block_size) const
{
    if (table_join->strictness() == JoinStrictness::Asof ||
        table_join->strictness() == JoinStrictness::Semi ||
        !isRightOrFull(table_join->kind()))
    {
        return {};
    }
    bool multiple_disjuncts = !table_join->oneDisjunct();

    if (multiple_disjuncts)
    {
        /// ... calculate `left_columns_count` ...
        size_t left_columns_count = left_sample_block.columns();
        auto non_joined = std::make_unique<NotJoinedHash<true>>(*this, max_block_size);
        return std::make_shared<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, table_join->leftToRightKeyRemap());

    }
    else
    {
        size_t left_columns_count = left_sample_block.columns();
        assert(left_columns_count == result_sample_block.columns() - required_right_keys.columns() - sample_block_with_columns_to_add.columns());
        auto non_joined = std::make_unique<NotJoinedHash<false>>(*this, max_block_size);
        return std::make_shared<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, table_join->leftToRightKeyRemap());
    }
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    from_storage_join = true;

    bool multiple_disjuncts = !table_join->oneDisjunct();
    if (multiple_disjuncts)
        throw Exception("StorageJoin with ORs is not supported", ErrorCodes::NOT_IMPLEMENTED);

    for (auto & map : data->maps)
    {
        joinDispatch(kind, strictness, map, [this](auto kind_, auto strictness_, auto & map_)
        {
            used_flags.reinit<kind_, strictness_>(map_.getBufferSizeInCells(data->type) + 1);
        });
    }
}

const ColumnWithTypeAndName & HashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable when right side is nullable
    return savedBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

}
