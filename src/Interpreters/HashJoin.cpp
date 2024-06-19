#include <any>
#include <limits>
#include <unordered_map>
#include <vector>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/CurrentThread.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>


#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/RowRefs.h>

#include <Storages/IStorage.h>

#include <Core/ColumnNumbers.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include "Core/Joins.h"
#include "Interpreters/TemporaryDataOnDisk.h"

#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForJoin;
}

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
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
    std::unique_ptr<TemporaryFileStream::Reader> reader;
};


Int64 getCurrentQueryMemoryUsage()
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            return memory_tracker->get();
    return 0;
}

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
            // For one disjunct clause case, we don't need to reinit each time we call addBlockToJoin.
            // and there is no value inserted in this JoinUsedFlags before addBlockToJoin finish.
            // So we reinit only when the hash table is rehashed to a larger size.
            if (flags.empty() || flags[nullptr].size() < size) [[unlikely]]
            {
                flags[nullptr] = std::vector<std::atomic_bool>(size);
            }
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

    template <bool use_flags, bool flag_per_row, typename FindResult>
    void JoinUsedFlags::setUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            auto & mapped = f.getMapped();
            flags[mapped.block][mapped.row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][f.getOffset()].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool flag_per_row>
    void JoinUsedFlags::setUsed(const Block * block, size_t row_num, size_t offset)
    {
        if constexpr (!use_flags)
            return;

        /// Could be set simultaneously from different threads.
        if constexpr (flag_per_row)
        {
            flags[block][row_num].store(true, std::memory_order_relaxed);
        }
        else
        {
            flags[nullptr][offset].store(true, std::memory_order_relaxed);
        }
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool JoinUsedFlags::getUsed(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
        {
            auto & mapped = f.getMapped();
            return flags[mapped.block][mapped.row_num].load();
        }
        else
        {
            return flags[nullptr][f.getOffset()].load();
        }
    }

    template <bool use_flags, bool flag_per_row, typename FindResult>
    bool JoinUsedFlags::setUsedOnce(const FindResult & f)
    {
        if constexpr (!use_flags)
            return true;

        if constexpr (flag_per_row)
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

static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&*column.column))
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable, const IColumn::Filter & negative_null_map)
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
}

HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_,
                   bool any_take_last_row_, size_t reserve_num_, const String & instance_id_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , reserve_num(reserve_num_)
    , instance_id(instance_id_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , tmp_data(
          table_join_->getTempDataOnDisk()
              ? std::make_unique<TemporaryDataOnDisk>(table_join_->getTempDataOnDisk(), CurrentMetrics::TemporaryFilesForJoin)
              : nullptr)
    , right_sample_block(right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , instance_log_id(!instance_id_.empty() ? "(" + instance_id_ + ") " : "")
    , log(getLogger("HashJoin"))
{
    LOG_TRACE(log, "{}Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
        instance_log_id, TableJoin::formatClauses(table_join->getClauses(), true), data->type, kind, strictness, right_sample_block.dumpStructure());

    validateAdditionalFilterExpression(table_join->getMixedJoinExpression());

    if (isCrossOrComma(kind))
    {
        data->type = Type::CROSS;
        sample_block_with_columns_to_add = right_sample_block;
    }
    else if (table_join->getClauses().empty())
    {
        data->type = Type::EMPTY;
        /// We might need to insert default values into the right columns, materialize them
        sample_block_with_columns_to_add = materializeBlock(right_sample_block);
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

    materializeBlockInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);
    data->sample_block = prepareRightBlock(data->sample_block);

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
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ASOF join needs at least one equi-join column");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1)
    {
        auto is_string_column = [](const IColumn * column_ptr) -> bool
        {
            if (const auto * lc_column_ptr = typeid_cast<const ColumnLowCardinality *>(column_ptr))
                return typeid_cast<const ColumnString *>(lc_column_ptr->getDictionary().getNestedColumn().get());
            return typeid_cast<const ColumnString *>(column_ptr);
        };

        const auto * key_column = key_columns[0];
        if (is_string_column(key_column) ||
            (isColumnConst(*key_column) && is_string_column(assert_cast<const ColumnConst *>(key_column)->getDataColumnPtr().get())))
            return Type::key_string;
    }

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

    if (reserve_num)
    {
        joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { map_.reserve(data->type, reserve_num); });
    }

    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin::dataMapInit called with empty data");
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
    if (!data)
        return 0;

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
    if (!data)
        return 0;

#ifndef NDEBUG
    size_t debug_blocks_allocated_size = 0;
    for (const auto & block : data->blocks)
        debug_blocks_allocated_size += block.allocatedBytes();

    if (data->blocks_allocated_size != debug_blocks_allocated_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "data->blocks_allocated_size != debug_blocks_allocated_size ({} != {})",
                        data->blocks_allocated_size, debug_blocks_allocated_size);

    size_t debug_blocks_nullmaps_allocated_size = 0;
    for (const auto & nullmap : data->blocks_nullmaps)
        debug_blocks_nullmaps_allocated_size += nullmap.second->allocatedBytes();

    if (data->blocks_nullmaps_allocated_size != debug_blocks_nullmaps_allocated_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "data->blocks_nullmaps_allocated_size != debug_blocks_nullmaps_allocated_size ({} != {})",
                        data->blocks_nullmaps_allocated_size, debug_blocks_nullmaps_allocated_size);
#endif

    size_t res = 0;

    res += data->blocks_allocated_size;
    res += data->blocks_nullmaps_allocated_size;
    res += data->pool.allocatedBytes();

    if (data->type != Type::CROSS)
    {
        for (const auto & map : data->maps)
        {
            joinDispatch(kind, strictness, map, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
        }
    }
    return res;
}

namespace
{
    /// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
    template <typename Map, typename KeyGetter>
    struct Inserter
    {
        static ALWAYS_INLINE bool insertOne(const HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i,
                                            Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
            {
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
                return true;
            }
            return false;
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


    template <JoinStrictness STRICTNESS, typename KeyGetter, typename Map>
    size_t NO_INLINE insertFromBlockImplTypeCase(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool, bool & is_inserted)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRef>;
        constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = key_columns.back();

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        /// For ALL and ASOF join always insert values
        is_inserted = !mapped_one || is_asof_join;

        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                /// nulls are not inserted into hash table,
                /// keep them for RIGHT and FULL joins
                is_inserted = true;
                continue;
            }

            /// Check condition for right table from ON section
            if (join_mask && !(*join_mask)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, *asof_column);
            else if constexpr (mapped_one)
                is_inserted |= Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
        return map.getBufferSizeInCells();
    }

    template <JoinStrictness STRICTNESS, typename Maps>
    size_t insertFromBlockImpl(
        HashJoin & join, HashJoin::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, UInt8ColumnDataPtr join_mask, Arena & pool, bool & is_inserted)
    {
        switch (type)
        {
            case HashJoin::Type::EMPTY:
                [[fallthrough]];
            case HashJoin::Type::CROSS:
                /// Do nothing. We will only save block, and it is enough
                is_inserted = true;
                return 0;

        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return insertFromBlockImplTypeCase<STRICTNESS, typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, join_mask, pool, is_inserted); \
                    break;

            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
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
    bool save_key_columns = table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) ||
                            table_join->isEnabledAlgorithm(JoinAlgorithm::GRACE_HASH) ||
                            isRightOrFull(kind) ||
                            multiple_disjuncts ||
                            table_join->getMixedJoinExpression();
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
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }
}

Block HashJoin::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block structured_block;
    for (const auto & sample_column : saved_block_sample_.getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);

        /// There's no optimization for right side const columns. Remove constness if any.
        column.column = recursiveRemoveSparse(column.column->convertToFullColumnIfConst());

        if (column.column->lowCardinality() && !sample_column.column->lowCardinality())
        {
            column.column = column.column->convertToFullColumnIfLowCardinality();
            column.type = removeLowCardinality(column.type);
        }

        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);

        structured_block.insert(std::move(column));
    }

    return structured_block;
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

bool HashJoin::addBlockToJoin(const Block & source_block_, bool check_limits)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block_.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", source_block_.rows());

    /** We do not allocate memory for stored blocks inside HashJoin, only for hash table.
      * In case when we have all the blocks allocated before the first `addBlockToJoin` call, will already be quite high.
      * In that case memory consumed by stored blocks will be underestimated.
      */
    if (!memory_usage_before_adding_blocks)
        memory_usage_before_adding_blocks = getCurrentQueryMemoryUsage();

    Block source_block = source_block_;
    if (strictness == JoinStrictness::Asof)
    {
        chassert(kind == JoinKind::Left || kind == JoinKind::Inner);

        /// Filter out rows with NULLs in ASOF key, nulls are not joined with anything since they are not comparable
        /// We support only INNER/LEFT ASOF join, so rows with NULLs never return from the right joined table.
        /// So filter them out here not to handle in implementation.
        const auto & asof_key_name = table_join->getOnlyClause().key_names_right.back();
        auto & asof_column = source_block.getByName(asof_key_name);

        if (asof_column.type->isNullable())
        {
            /// filter rows with nulls in asof key
            if (const auto * asof_const_column = typeid_cast<const ColumnConst *>(asof_column.column.get()))
            {
                if (asof_const_column->isNullAt(0))
                    return false;
            }
            else
            {
                const auto & asof_column_nullable = assert_cast<const ColumnNullable &>(*asof_column.column).getNullMapData();

                NullMap negative_null_map(asof_column_nullable.size());
                for (size_t i = 0; i < asof_column_nullable.size(); ++i)
                    negative_null_map[i] = !asof_column_nullable[i];

                for (auto & column : source_block)
                    column.column = column.column->filter(negative_null_map, -1);
            }
        }
    }

    size_t rows = source_block.rows();

    const auto & right_key_names = table_join->getAllNames(JoinTableSide::Right);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
    {
        const auto & column = source_block.getByName(column_name).column;
        all_key_columns[column_name] = recursiveRemoveSparse(column->convertToFullColumnIfConst())->convertToFullColumnIfLowCardinality();
    }

    Block block_to_save = prepareRightBlock(source_block);
    if (shrink_blocks)
        block_to_save = block_to_save.shrinkToFit();

    size_t max_bytes_in_join = table_join->sizeLimits().max_bytes;
    size_t max_rows_in_join = table_join->sizeLimits().max_rows;

    if (kind == JoinKind::Cross && tmp_data
        && (tmp_stream || (max_bytes_in_join && getTotalByteCount() + block_to_save.allocatedBytes() >= max_bytes_in_join)
            || (max_rows_in_join && getTotalRowCount() + block_to_save.rows() >= max_rows_in_join)))
    {
        if (tmp_stream == nullptr)
        {
            tmp_stream = &tmp_data->createStream(right_sample_block);
        }
        tmp_stream->write(block_to_save);
        return true;
    }

    size_t total_rows = 0;
    size_t total_bytes = 0;
    {
        if (storage_join_lock)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

        assertBlocksHaveEqualStructure(data->sample_block, block_to_save, "joined block");

        size_t min_bytes_to_compress = table_join->crossJoinMinBytesToCompress();
        size_t min_rows_to_compress = table_join->crossJoinMinRowsToCompress();

        if (kind == JoinKind::Cross
            && ((min_bytes_to_compress && getTotalByteCount() >= min_bytes_to_compress)
                || (min_rows_to_compress && getTotalRowCount() >= min_rows_to_compress)))
        {
            block_to_save = block_to_save.compress();
            have_compressed = true;
        }

        data->blocks_allocated_size += block_to_save.allocatedBytes();
        data->blocks.emplace_back(std::move(block_to_save));
        Block * stored_block = &data->blocks.back();

        if (rows)
            data->empty = false;

        bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
        const auto & onexprs = table_join->getClauses();
        for (size_t onexpr_idx = 0; onexpr_idx < onexprs.size(); ++onexpr_idx)
        {
            ColumnRawPtrs key_columns;
            for (const auto & name : onexprs[onexpr_idx].key_names_right)
                key_columns.push_back(all_key_columns[name].get());

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

            auto join_mask_col = JoinCommon::getColumnAsMask(source_block, onexprs[onexpr_idx].condColumnNames().second);
            /// Save blocks that do not hold conditions in ON section
            ColumnUInt8::MutablePtr not_joined_map = nullptr;
            if (!flag_per_row && isRightOrFull(kind) && join_mask_col.hasData())
            {
                const auto & join_mask = join_mask_col.getData();
                /// Save rows that do not hold conditions
                not_joined_map = ColumnUInt8::create(rows, 0);
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

            bool is_inserted = false;
            if (kind != JoinKind::Cross)
            {
                joinDispatch(kind, strictness, data->maps[onexpr_idx], [&](auto kind_, auto strictness_, auto & map)
                {
                    size_t size = insertFromBlockImpl<strictness_>(
                        *this, data->type, map, rows, key_columns, key_sizes[onexpr_idx], stored_block, null_map,
                        /// If mask is false constant, rows are added to hashmap anyway. It's not a happy-flow, so this case is not optimized
                        join_mask_col.getData(),
                        data->pool, is_inserted);

                    if (flag_per_row)
                        used_flags.reinit<kind_, strictness_>(stored_block);
                    else if (is_inserted)
                        /// Number of buckets + 1 value from zero storage
                        used_flags.reinit<kind_, strictness_>(size + 1);
                });
            }

            if (!flag_per_row && save_nullmap && is_inserted)
            {
                data->blocks_nullmaps_allocated_size += null_map_holder->allocatedBytes();
                data->blocks_nullmaps.emplace_back(stored_block, null_map_holder);
            }

            if (!flag_per_row && not_joined_map && is_inserted)
            {
                data->blocks_nullmaps_allocated_size += not_joined_map->allocatedBytes();
                data->blocks_nullmaps.emplace_back(stored_block, std::move(not_joined_map));
            }

            if (!flag_per_row && !is_inserted)
            {
                LOG_TRACE(log, "Skipping inserting block with {} rows", rows);
                data->blocks_allocated_size -= stored_block->allocatedBytes();
                data->blocks.pop_back();
            }

            if (!check_limits)
                return true;

            /// TODO: Do not calculate them every time
            total_rows = getTotalRowCount();
            total_bytes = getTotalByteCount();
        }
    }

    shrinkStoredBlocksToFit(total_bytes);

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join)
{
    if (shrink_blocks)
        return; /// Already shrunk

    Int64 current_memory_usage = getCurrentQueryMemoryUsage();
    Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before_adding_blocks;
    Int64 max_total_bytes_for_query = memory_usage_before_adding_blocks ? table_join->getMaxMemoryUsage() : 0;

    auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    /** If accounted data size is more than half of `max_bytes_in_join`
      * or query memory consumption growth from the beginning of adding blocks (estimation of memory consumed by join using memory tracker)
      * is bigger than half of all memory available for query,
      * then shrink stored blocks to fit.
      */
    shrink_blocks = (max_total_bytes_in_join && total_bytes_in_join > max_total_bytes_in_join / 2) ||
                    (max_total_bytes_for_query && query_memory_usage_delta > max_total_bytes_for_query / 2);
    if (!shrink_blocks)
        return;

    LOG_DEBUG(log, "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join), max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta), max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    for (auto & stored_block : data->blocks)
    {
        size_t old_size = stored_block.allocatedBytes();
        stored_block = stored_block.shrinkToFit();
        size_t new_size = stored_block.allocatedBytes();

        if (old_size >= new_size)
        {
            if (data->blocks_allocated_size < old_size - new_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: "
                    "blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    data->blocks_allocated_size, old_size, new_size);

            data->blocks_allocated_size -= old_size - new_size;
        }
        else
            /// Sometimes after clone resized block can be bigger than original
            data->blocks_allocated_size += new_size - old_size;
    }

    auto new_total_bytes_in_join = getTotalByteCount();

    Int64 new_current_memory_usage = getCurrentQueryMemoryUsage();

    LOG_DEBUG(log, "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join), ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join), ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
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

template <bool lazy>
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

    struct LazyOutput
    {
        PaddedPODArray<UInt64> blocks;
        PaddedPODArray<UInt32> row_nums;
    };

    AddedColumns(
        const Block & left_block_,
        const Block & block_with_columns_to_add,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        ExpressionActionsPtr additional_filter_expression_,
        bool is_asof_join,
        bool is_join_get_)
        : left_block(left_block_)
        , join_on_keys(join_on_keys_)
        , additional_filter_expression(additional_filter_expression_)
        , rows_to_add(left_block.rows())
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        if constexpr (lazy)
        {
            has_columns_to_add = num_columns_to_add > 0;
            lazy_output.blocks.reserve(rows_to_add);
            lazy_output.row_nums.reserve(rows_to_add);
        }

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!left_block.has(qualified_name))
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

        nullable_column_ptrs.resize(right_indexes.size(), nullptr);
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
            const auto & saved_column = saved_block_sample.getByPosition(right_indexes[j]).column;
            if (columns[j]->isNullable() && !saved_column->isNullable())
                nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
        }
    }

    size_t size() const { return columns.size(); }

    void buildOutput();

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }

    void appendFromBlock(const Block & block, size_t row_num, bool has_default);

    void appendDefaultRow();

    void applyLazyDefaults();

    const IColumn & leftAsofKey() const { return *left_asof_key; }

    Block left_block;
    std::vector<JoinOnKeyColumns> join_on_keys;
    ExpressionActionsPtr additional_filter_expression;

    size_t max_joined_block_rows = 0;
    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;
    IColumn::Filter filter;

    void reserve(bool need_replicate)
    {
        if (!max_joined_block_rows)
            return;

        /// Do not allow big allocations when user set max_joined_block_rows to huge value
        size_t reserve_size = std::min<size_t>(max_joined_block_rows, DEFAULT_BLOCK_SIZE * 2);

        if (need_replicate)
            /// Reserve 10% more space for columns, because some rows can be repeated
            reserve_size = static_cast<size_t>(1.1 * reserve_size);

        for (auto & column : columns)
            column->reserve(reserve_size);
    }

private:

    void checkBlock(const Block & block)
    {
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            const auto * column_from_block = block.getByPosition(right_indexes[j]).column.get();
            const auto * dest_column = columns[j].get();
            if (auto * nullable_col = nullable_column_ptrs[j])
            {
                if (!is_join_get)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Columns {} and {} can have different nullability only in joinGetOrNull",
                                    dest_column->getName(), column_from_block->getName());
                dest_column = nullable_col->getNestedColumnPtr().get();
            }
            /** Using dest_column->structureEquals(*column_from_block) will not work for low cardinality columns,
              * because dictionaries can be different, while calling insertFrom on them is safe, for example:
              * ColumnLowCardinality(size = 0, UInt8(size = 0), ColumnUnique(size = 1, String(size = 1)))
              * and
              * ColumnLowCardinality(size = 0, UInt16(size = 0), ColumnUnique(size = 1, String(size = 1)))
              */
            if (typeid(*dest_column) != typeid(*column_from_block))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} have different types {} and {}",
                                dest_column->getName(), column_from_block->getName(),
                                demangle(typeid(*dest_column).name()), demangle(typeid(*column_from_block).name()));
        }
    }

    MutableColumns columns;
    bool is_join_get;
    std::vector<size_t> right_indexes;
    std::vector<TypeAndName> type_name;
    std::vector<ColumnNullable *> nullable_column_ptrs;
    size_t lazy_defaults_count = 0;

    /// for lazy
    // The default row is represented by an empty RowRef, so that fixed-size blocks can be generated sequentially,
    // default_count cannot represent the position of the row
    LazyOutput lazy_output;
    bool has_columns_to_add;

    /// for ASOF
    const IColumn * left_asof_key = nullptr;


    void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name, qualified_name);
    }
};
template<> void AddedColumns<false>::buildOutput()
{
}

template<>
void AddedColumns<true>::buildOutput()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto& col = columns[i];
        size_t default_count = 0;
        auto apply_default = [&]()
        {
            if (default_count > 0)
            {
                JoinCommon::addDefaultValues(*col, type_name[i].type, default_count);
                default_count = 0;
            }
        };

        for (size_t j = 0; j < lazy_output.blocks.size(); ++j)
        {
            if (!lazy_output.blocks[j])
            {
                default_count++;
                continue;
            }
            apply_default();
            const auto & column_from_block = reinterpret_cast<const Block *>(lazy_output.blocks[j])->getByPosition(right_indexes[i]);
            /// If it's joinGetOrNull, we need to wrap not-nullable columns in StorageJoin.
            if (is_join_get)
            {
                if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get());
                    nullable_col && !column_from_block.column->isNullable())
                {
                    nullable_col->insertFromNotNullable(*column_from_block.column, lazy_output.row_nums[j]);
                    continue;
                }
            }
            col->insertFrom(*column_from_block.column, lazy_output.row_nums[j]);
        }
        apply_default();
    }
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
        lazy_defaults_count = 0;
    }
}

template<>
void AddedColumns<true>::applyLazyDefaults()
{
}

template <>
void AddedColumns<false>::appendFromBlock(const Block & block, size_t row_num,const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

#ifndef NDEBUG
    checkBlock(block);
#endif
    if (is_join_get)
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = block.getByPosition(right_indexes[j]);
            if (auto * nullable_col = nullable_column_ptrs[j])
                nullable_col->insertFromNotNullable(*column_from_block.column, row_num);
            else
                columns[j]->insertFrom(*column_from_block.column, row_num);
        }
    }
    else
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = block.getByPosition(right_indexes[j]);
            columns[j]->insertFrom(*column_from_block.column, row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const Block & block, size_t row_num, bool)
{
#ifndef NDEBUG
    checkBlock(block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.blocks.emplace_back(reinterpret_cast<UInt64>(&block));
        lazy_output.row_nums.emplace_back(static_cast<uint32_t>(row_num));
    }
}
template<>
void AddedColumns<false>::appendDefaultRow()
{
    ++lazy_defaults_count;
}

template<>
void AddedColumns<true>::appendDefaultRow()
{
    if (has_columns_to_add)
    {
        lazy_output.blocks.emplace_back(0);
        lazy_output.row_nums.emplace_back(0);
    }
}

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

template <bool flag_per_row>
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

template <typename Map, bool add_missing, bool flag_per_row, typename AddedColumns>
void addFoundRowAll(
    const typename Map::mapped_type & mapped,
    AddedColumns & added,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows [[maybe_unused]],
    JoinStuff::JoinUsedFlags * used_flags [[maybe_unused]])
{
    if constexpr (add_missing)
        added.applyLazyDefaults();

    if constexpr (flag_per_row)
    {
        std::unique_ptr<std::vector<KnownRowsHolder<true>::Type>> new_known_rows_ptr;

        for (auto it = mapped.begin(); it.ok(); ++it)
        {
            if (!known_rows.isKnown(std::make_pair(it->block, it->row_num)))
            {
                added.appendFromBlock(*it->block, it->row_num, false);
                ++current_offset;
                if (!new_known_rows_ptr)
                {
                    new_known_rows_ptr = std::make_unique<std::vector<KnownRowsHolder<true>::Type>>();
                }
                new_known_rows_ptr->push_back(std::make_pair(it->block, it->row_num));
                if (used_flags)
                {
                    used_flags->JoinStuff::JoinUsedFlags::setUsedOnce<true, flag_per_row>(
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
            added.appendFromBlock(*it->block, it->row_num, false);
            ++current_offset;
        }
    }
}

template <bool add_missing, bool need_offset, typename AddedColumns>
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

template<typename AddedColumns>
ColumnPtr buildAdditionalFilter(
    size_t left_start_row,
    const std::vector<RowRef> & selected_rows,
    const std::vector<size_t> & row_replicate_offset,
    AddedColumns & added_columns)
{
    ColumnPtr result_column;
    do
    {
        if (selected_rows.empty())
        {
            result_column = ColumnUInt8::create();
            break;
        }
        const Block & sample_right_block = *selected_rows.begin()->block;
        if (!sample_right_block || !added_columns.additional_filter_expression)
        {
            auto filter = ColumnUInt8::create();
            filter->insertMany(1, selected_rows.size());
            result_column = std::move(filter);
            break;
        }

        auto required_cols = added_columns.additional_filter_expression->getRequiredColumnsWithTypes();
        if (required_cols.empty())
        {
            Block block;
            added_columns.additional_filter_expression->execute(block);
            result_column = block.getByPosition(0).column->cloneResized(selected_rows.size());
            break;
        }
        NameSet required_column_names;
        for (auto & col : required_cols)
            required_column_names.insert(col.name);

        Block executed_block;
        size_t right_col_pos = 0;
        for (const auto & col : sample_right_block.getColumnsWithTypeAndName())
        {
            if (required_column_names.contains(col.name))
            {
                auto new_col = col.column->cloneEmpty();
                for (const auto & selected_row : selected_rows)
                {
                    const auto & src_col = selected_row.block->getByPosition(right_col_pos);
                    new_col->insertFrom(*src_col.column, selected_row.row_num);
                }
                executed_block.insert({std::move(new_col), col.type, col.name});
            }
            right_col_pos += 1;
        }
        if (!executed_block)
        {
            result_column = ColumnUInt8::create();
            break;
        }

        for (const auto & col_name : required_column_names)
        {
            const auto * src_col = added_columns.left_block.findByName(col_name);
            if (!src_col)
                continue;
            auto new_col = src_col->column->cloneEmpty();
            size_t prev_left_offset = 0;
            for (size_t i = 1; i < row_replicate_offset.size(); ++i)
            {
                const size_t & left_offset = row_replicate_offset[i];
                size_t rows = left_offset - prev_left_offset;
                if (rows)
                    new_col->insertManyFrom(*src_col->column, left_start_row + i - 1, rows);
                prev_left_offset = left_offset;
            }
            executed_block.insert({std::move(new_col), src_col->type, col_name});
        }
        if (!executed_block)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "required columns: [{}], but not found any in left/right table. right table: {}, left table: {}",
                required_cols.toString(),
                sample_right_block.dumpNames(),
                added_columns.left_block.dumpNames());
        }

        for (const auto & col : executed_block.getColumnsWithTypeAndName())
            if (!col.column || !col.type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal nullptr column in input block: {}", executed_block.dumpStructure());

        added_columns.additional_filter_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column->convertToFullColumnIfConst();
        executed_block.clear();
    } while (false);

    result_column = result_column->convertToFullIfNeeded();
    if (result_column->isNullable())
    {
        /// Convert Nullable(UInt8) to UInt8 ensuring that nulls are zeros
        /// Trying to avoid copying data, since we are the only owner of the column.
        ColumnPtr mask_column = assert_cast<const ColumnNullable &>(*result_column).getNullMapColumnPtr();

        MutableColumnPtr mutable_column;
        {
            ColumnPtr nested_column = assert_cast<const ColumnNullable &>(*result_column).getNestedColumnPtr();
            result_column.reset();
            mutable_column = IColumn::mutate(std::move(nested_column));
        }

        auto & column_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const auto & mask_column_data = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
        for (size_t i = 0; i < column_data.size(); ++i)
        {
            if (mask_column_data[i])
                column_data[i] = 0;
        }
        return mutable_column;
    }
    return result_column;
}

/// Adapter class to pass into addFoundRowAll
/// In joinRightColumnsWithAdditionalFilter we don't want to add rows directly into AddedColumns,
/// because they need to be filtered by additional_filter_expression.
class PreSelectedRows : public std::vector<RowRef>
{
public:
    void appendFromBlock(const Block & block, size_t row_num, bool /* has_default */) { this->emplace_back(&block, row_num); }
};

/// First to collect all matched rows refs by join keys, then filter out rows which are not true in additional filter expression.
template <
    typename KeyGetter,
    typename Map,
    bool need_replication,
    typename AddedColumns>
NO_INLINE size_t joinRightColumnsWithAddtitionalFilter(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
    bool need_filter [[maybe_unused]],
    bool need_flags [[maybe_unused]],
    bool add_missing [[maybe_unused]],
    bool flag_per_row [[maybe_unused]])
{
    size_t left_block_rows = added_columns.rows_to_add;
    if (need_filter)
        added_columns.filter = IColumn::Filter(left_block_rows, 0);

    std::unique_ptr<Arena> pool;

    if constexpr (need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(left_block_rows);

    std::vector<size_t> row_replicate_offset;
    row_replicate_offset.reserve(left_block_rows);

    using FindResult = typename KeyGetter::FindResult;
    size_t max_joined_block_rows = added_columns.max_joined_block_rows;
    size_t left_row_iter = 0;
    PreSelectedRows selected_rows;
    selected_rows.reserve(left_block_rows);
    std::vector<FindResult> find_results;
    find_results.reserve(left_block_rows);
    bool exceeded_max_block_rows = false;
    IColumn::Offset total_added_rows = 0;
    IColumn::Offset current_added_rows = 0;

    auto collect_keys_matched_rows_refs = [&]()
    {
        pool = std::make_unique<Arena>();
        find_results.clear();
        row_replicate_offset.clear();
        row_replicate_offset.push_back(0);
        current_added_rows = 0;
        selected_rows.clear();
        for (; left_row_iter < left_block_rows; ++left_row_iter)
        {
            if constexpr (need_replication)
            {
                if (unlikely(total_added_rows + current_added_rows >= max_joined_block_rows))
                {
                    break;
                }
            }
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_know_rows;
            for (size_t join_clause_idx = 0; join_clause_idx < added_columns.join_on_keys.size(); ++join_clause_idx)
            {
                const auto & join_keys = added_columns.join_on_keys[join_clause_idx];
                if (join_keys.null_map && (*join_keys.null_map)[left_row_iter])
                    continue;

                bool row_acceptable = !join_keys.isRowFiltered(left_row_iter);
                auto find_result = row_acceptable
                    ? key_getter_vector[join_clause_idx].findKey(*(mapv[join_clause_idx]), left_row_iter, *pool)
                    : FindResult();

                if (find_result.isFound())
                {
                    auto & mapped = find_result.getMapped();
                    find_results.push_back(find_result);
                    if (flag_per_row)
                        addFoundRowAll<Map, false, true>(mapped, selected_rows, current_added_rows, all_flag_known_rows, nullptr);
                    else
                        addFoundRowAll<Map, false, false>(mapped, selected_rows, current_added_rows, single_flag_know_rows, nullptr);
                }
            }
            row_replicate_offset.push_back(current_added_rows);
        }
    };

    auto copy_final_matched_rows = [&](size_t left_start_row, ColumnPtr filter_col)
    {
        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        size_t prev_replicated_row = 0;
        auto selected_right_row_it = selected_rows.begin();
        size_t find_result_index = 0;
        for (size_t i = 1, n = row_replicate_offset.size(); i < n; ++i)
        {
            bool any_matched = false;
            /// For all right join, flag_per_row is true, we need mark used flags for each row.
            if (flag_per_row)
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        any_matched = true;
                        added_columns.appendFromBlock(*selected_right_row_it->block, selected_right_row_it->row_num, add_missing);
                        total_added_rows += 1;
                        if (need_flags)
                            used_flags.template setUsed<true, true>(selected_right_row_it->block, selected_right_row_it->row_num, 0);
                    }
                    ++selected_right_row_it;
                }
            }
            else
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        any_matched = true;
                        added_columns.appendFromBlock(*selected_right_row_it->block, selected_right_row_it->row_num, add_missing);
                        total_added_rows += 1;
                    }
                    ++selected_right_row_it;
                }
            }
            if (!any_matched)
            {
                if (add_missing)
                    addNotFoundRow<true, need_replication>(added_columns, total_added_rows);
                else
                    addNotFoundRow<false, need_replication>(added_columns, total_added_rows);
            }
            else
            {
                if (!flag_per_row && need_flags)
                    used_flags.template setUsed<true, false>(find_results[find_result_index]);
                if (need_filter)
                    setUsed<true>(added_columns.filter, left_start_row + i - 1);
                if (add_missing)
                    added_columns.applyLazyDefaults();
            }
            find_result_index += (prev_replicated_row != row_replicate_offset[i]);

            if constexpr (need_replication)
            {
                (*added_columns.offsets_to_replicate)[left_start_row + i - 1] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }
    };

    while (left_row_iter < left_block_rows && !exceeded_max_block_rows)
    {
        auto left_start_row = left_row_iter;
        collect_keys_matched_rows_refs();
        if (selected_rows.size() != current_added_rows || row_replicate_offset.size() != left_row_iter - left_start_row + 1)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}, left_row_iter: {}, "
                "left_start_row: {}",
                selected_rows.size(),
                current_added_rows,
                row_replicate_offset.size(),
                left_row_iter,
                left_start_row);
        }
        auto filter_col = buildAdditionalFilter(left_start_row, selected_rows, row_replicate_offset, added_columns);
        copy_final_matched_rows(left_start_row, filter_col);

        if constexpr (need_replication)
        {
            // Add a check for current_added_rows to avoid run the filter expression on too small size batch.
            if (total_added_rows >= max_joined_block_rows || current_added_rows < 1024)
            {
                exceeded_max_block_rows = true;
            }
        }
    }

    if constexpr (need_replication)
    {
        added_columns.offsets_to_replicate->resize_assume_reserved(left_row_iter);
        added_columns.filter.resize_assume_reserved(left_row_iter);
    }
    added_columns.applyLazyDefaults();
    return left_row_iter;
}

/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool flag_per_row, typename AddedColumns>
NO_INLINE size_t joinRightColumns(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS> join_features;

    size_t rows = added_columns.rows_to_add;
    if constexpr (need_filter)
        added_columns.filter = IColumn::Filter(rows, 0);

    Arena pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;
    size_t max_joined_block_rows = added_columns.max_joined_block_rows;
    size_t i = 0;
    for (; i < rows; ++i)
    {
        if constexpr (join_features.need_replication)
        {
            if (unlikely(current_offset >= max_joined_block_rows))
            {
                added_columns.offsets_to_replicate->resize_assume_reserved(i);
                added_columns.filter.resize_assume_reserved(i);
                break;
            }
        }

        bool right_row_found = false;

        KnownRowsHolder<flag_per_row> known_rows;
        for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
            if (join_keys.null_map && (*join_keys.null_map)[i])
                continue;

            bool row_acceptable = !join_keys.isRowFiltered(i);
            using FindResult = typename KeyGetter::FindResult;
            auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), i, pool) : FindResult();

            if (find_result.isFound())
            {
                right_row_found = true;
                auto & mapped = find_result.getMapped();
                if constexpr (join_features.is_asof_join)
                {
                    const IColumn & left_asof_key = added_columns.leftAsofKey();

                    auto row_ref = mapped->findAsof(left_asof_key, i);
                    if (row_ref.block)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        if constexpr (flag_per_row)
                            used_flags.template setUsed<join_features.need_flags, flag_per_row>(row_ref.block, row_ref.row_num, 0);
                        else
                            used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);

                        added_columns.appendFromBlock(*row_ref.block, row_ref.row_num, join_features.add_missing);
                    }
                    else
                        addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
                }
                else if constexpr (join_features.is_all_join)
                {
                    setUsed<need_filter>(added_columns.filter, i);
                    used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                    auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
                    addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                }
                else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
                {
                    /// Use first appeared left key + it needs left columns replication
                    bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);
                    if (used_once)
                    {
                        auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
                        setUsed<need_filter>(added_columns.filter, i);
                        addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                    }
                }
                else if constexpr (join_features.is_any_join && KIND == JoinKind::Inner)
                {
                    bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);

                    /// Use first appeared left key only
                    if (used_once)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        added_columns.appendFromBlock(*mapped.block, mapped.row_num, join_features.add_missing);
                    }

                    break;
                }
                else if constexpr (join_features.is_any_join && join_features.full)
                {
                    /// TODO
                }
                else if constexpr (join_features.is_anti_join)
                {
                    if constexpr (join_features.right && join_features.need_flags)
                        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                }
                else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
                {
                    setUsed<need_filter>(added_columns.filter, i);
                    used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                    added_columns.appendFromBlock(*mapped.block, mapped.row_num, join_features.add_missing);

                    if (join_features.is_any_or_semi_join)
                    {
                        break;
                    }
                }
            }
        }

        if (!right_row_found)
        {
            if constexpr (join_features.is_anti_join && join_features.left)
                setUsed<need_filter>(added_columns.filter, i);
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
        }

        if constexpr (join_features.need_replication)
        {
           (*added_columns.offsets_to_replicate)[i] = current_offset;
        }
    }

    added_columns.applyLazyDefaults();
    return i;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
size_t joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS> join_features;
    if constexpr (join_features.is_all_join)
    {
        if (added_columns.additional_filter_expression)
        {
            bool mark_per_row_used = join_features.right || join_features.full || mapv.size() > 1;
            return joinRightColumnsWithAddtitionalFilter<KeyGetter, Map, join_features.need_replication>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector),
                mapv,
                added_columns,
                used_flags,
                need_filter,
                join_features.need_flags,
                join_features.add_missing,
                mark_per_row_used);
        }
    }

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");

    return mapv.size() > 1
        ? joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags)
        : joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, need_filter, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename KeyGetter, typename Map, typename AddedColumns>
size_t joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags)
{
    if (added_columns.need_filter)
    {
        return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, true>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
    }
    else
    {
        return joinRightColumnsSwitchMultipleDisjuncts<KIND, STRICTNESS, KeyGetter, Map, false>(std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps, typename AddedColumns>
size_t switchJoinRightColumns(
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

/** Since we do not store right key columns,
  * this function is used to copy left key columns to right key columns.
  * If the user requests some right columns, we just copy left key columns to right, since they are equal.
  * Example: SELECT t1.key, t2.key FROM t1 FULL JOIN t2 ON t1.key = t2.key;
  * In that case for matched rows in t2.key we will use values from t1.key.
  * However, in some cases we might need to adjust the type of column, e.g. t1.key :: LowCardinality(String) and t2.key :: String
  * Also, the nullability of the column might be different.
  * Returns the right column after with necessary adjustments.
  */
ColumnWithTypeAndName copyLeftKeyColumnToRight(
    const DataTypePtr & right_key_type, const String & renamed_right_column, const ColumnWithTypeAndName & left_column, const IColumn::Filter * null_map_filter = nullptr)
{
    ColumnWithTypeAndName right_column = left_column;
    right_column.name = renamed_right_column;

    if (null_map_filter)
        right_column.column = JoinCommon::filterWithBlanks(right_column.column, *null_map_filter);

    bool should_be_nullable = isNullableOrLowCardinalityNullable(right_key_type);
    if (null_map_filter)
        correctNullabilityInplace(right_column, should_be_nullable, *null_map_filter);
    else
        correctNullabilityInplace(right_column, should_be_nullable);

    if (!right_column.type->equals(*right_key_type))
    {
        right_column.column = castColumnAccurate(right_column, right_key_type);
        right_column.type = right_key_type;
    }

    right_column.column = right_column.column->convertToFullColumnIfConst();
    return right_column;
}

/// Cut first num_rows rows from block in place and returns block with remaining rows
Block sliceBlock(Block & block, size_t num_rows)
{
    size_t total_rows = block.rows();
    if (num_rows >= total_rows)
        return {};
    size_t remaining_rows = total_rows - num_rows;
    Block remaining_block = block.cloneEmpty();
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & col = block.getByPosition(i);
        remaining_block.getByPosition(i).column = col.column->cut(num_rows, remaining_rows);
        col.column = col.column->cut(0, num_rows);
    }
    return remaining_block;
}

} /// nameless

template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
Block HashJoin::joinBlockImpl(
    Block & block,
    const Block & block_with_columns_to_add,
    const std::vector<const Maps *> & maps_,
    bool is_join_get) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> join_features;

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
    if constexpr (join_features.right || join_features.full)
    {
        materializeBlockInplace(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns<!join_features.is_any_join> added_columns(
        block,
        block_with_columns_to_add,
        savedBlockSample(),
        *this,
        std::move(join_on_keys),
        table_join->getMixedJoinExpression(),
        join_features.is_asof_join,
        is_join_get);

    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = max_joined_block_rows;
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_features.need_replication);

    size_t num_joined = switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, data->type, used_flags);
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();
    Block remaining_block = sliceBlock(block, num_joined);

    added_columns.buildOutput();
    for (size_t i = 0; i < added_columns.size(); ++i)
        block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (join_features.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(added_columns.filter, -1);

        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            /// asof column is already in block.
            if (join_features.is_asof_join && right_key.name == table_join->getOnlyClause().key_names_right.back())
                continue;

            const auto & left_column = block.getByName(required_right_keys_sources[i]);
            const auto & right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column);
            block.insert(std::move(right_col));
        }
    }
    else if (has_required_right_keys)
    {
        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            auto right_col_name = getTableJoin().renamedRightColumnName(right_key.name);
            /// asof column is already in block.
            if (join_features.is_asof_join && right_key.name == table_join->getOnlyClause().key_names_right.back())
                continue;

            const auto & left_column = block.getByName(required_right_keys_sources[i]);
            auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column, &added_columns.filter);
            block.insert(std::move(right_col));

            if constexpr (join_features.need_replication)
                right_keys_to_replicate.push_back(block.getPositionByName(right_col_name));
        }
    }

    if constexpr (join_features.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns.offsets_to_replicate;

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
        {
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);
        }

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
        {
            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
        }
    }

    return remaining_block;
}

void HashJoin::joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const
{
    size_t start_left_row = 0;
    size_t start_right_block = 0;
    std::unique_ptr<TemporaryFileStream::Reader> reader = nullptr;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        reader = std::move(continuation.reader);
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

        auto process_right_block = [&](const Block & block_right)
        {
            size_t rows_right = block_right.rows();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *block_right.getByPosition(col_num).column;
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        };

        for (const Block & block_right : data->blocks)
        {
            ++block_number;
            if (block_number < start_right_block)
                continue;

            /// The following statement cannot be substituted with `process_right_block(!have_compressed ? block_right : block_right.decompress())`
            /// because it will lead to copying of `block_right` even if its branch is taken (because common type of `block_right` and `block_right.decompress()` is `Block`).
            if (!have_compressed)
                process_right_block(block_right);
            else
                process_right_block(block_right.decompress());

            if (rows_added > max_joined_block_rows)
            {
                break;
            }
        }

        if (tmp_stream && rows_added <= max_joined_block_rows)
        {
            if (reader == nullptr)
            {
                tmp_stream->finishWritingAsyncSafe();
                reader = tmp_stream->getReadStream();
            }
            while (auto block_right = reader->read())
            {
                ++block_number;
                process_right_block(block_right);
                if (rows_added > max_joined_block_rows)
                {
                    break;
                }
            }

            /// It means, that reader->read() returned {}
            if (rows_added <= max_joined_block_rows)
            {
                reader.reset();
            }
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1, std::move(reader)});
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
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function joinGet{} doesn't match: passed, should be equal to {}",
            toString(or_null ? "OrNull" : ""), toString(num_keys));

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch in joinGet key {}: "
                "found type {}, while the needed type is {}", i, left_type->getName(), right_type->getName());
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "StorageJoin doesn't contain column {}", column_name);

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
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "joinGet only supports StorageJoin of type Left Any");
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
        keys, block_with_columns_to_add, maps_vector, /* is_join_get = */ true);
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
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

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
            Block remaining_block = joinBlockImpl<kind_, strictness_>(block, sample_block_with_columns_to_add, maps_vector_);
            if (remaining_block.rows())
                not_processed = std::make_shared<ExtraBlock>(ExtraBlock{std::move(remaining_block)});
            else
                not_processed.reset();
        }))
        {
            /// Joined
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
    }
}

HashJoin::~HashJoin()
{
    if (!data)
    {
        LOG_TEST(log, "{}Join data has been already released", instance_log_id);
        return;
    }
    LOG_TEST(
        log,
        "{}Join data is being destroyed, {} bytes and {} rows in hash table",
        instance_log_id,
        getTotalByteCount(),
        getTotalRowCount());
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
class NotJoinedHash final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedHash(const HashJoin & parent_, UInt64 max_block_size_, bool flag_per_row_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , flag_per_row(flag_per_row_)
        , current_block_start(0)
    {
        if (parent.data == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");
    }

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
            auto fill_callback = [&](auto, auto, auto & map)
            {
                rows_added = fillColumnsFromMap(map, columns_right);
            };

            if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps.front(), fill_callback))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness '{}' (must be on of: ANY, ALL, ASOF)", parent.strictness);
        }

        if (!flag_per_row)
        {
            fillNullsFromBlocks(columns_right, rows_added);
        }

        return rows_added;
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;
    bool flag_per_row;

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

    template <typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parent.data->type)
        {
        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return fillColumns(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
            default:
                throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", parent.data->type);
        }
    }

    template <typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        size_t rows_added = 0;

        if (flag_per_row)
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

                size_t offset = map.offsetInternal(it.getPtr());
                if (parent.isUsed(offset))
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

IBlocksStreamPtr HashJoin::getNonJoinedBlocks(const Block & left_sample_block,
                                                              const Block & result_sample_block,
                                                              UInt64 max_block_size) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};
    size_t left_columns_count = left_sample_block.columns();

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (!flag_per_row)
    {
        /// With multiple disjuncts, all keys are in sample_block_with_columns_to_add, so invariant is not held
        size_t expected_columns_count = left_columns_count + required_right_keys.columns() + sample_block_with_columns_to_add.columns();
        if (expected_columns_count != result_sample_block.columns())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of columns in result sample block: {} instead of {} ({} + {} + {})",
                            result_sample_block.columns(), expected_columns_count,
                            left_columns_count, required_right_keys.columns(), sample_block_with_columns_to_add.columns());
        }
    }

    auto non_joined = std::make_unique<NotJoinedHash>(*this, max_block_size, flag_per_row);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    from_storage_join = true;

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (flag_per_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin with ORs is not supported");

    for (auto & map : data->maps)
    {
        joinDispatch(kind, strictness, map, [this](auto kind_, auto strictness_, auto & map_)
        {
            used_flags.reinit<kind_, strictness_>(map_.getBufferSizeInCells(data->type) + 1);
        });
    }
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure)
{
    LOG_TRACE(log, "{}Join data is being released, {} bytes and {} rows in hash table", instance_log_id, getTotalByteCount(), getTotalRowCount());

    BlocksList right_blocks = std::move(data->blocks);
    if (!restructure)
    {
        data.reset();
        return right_blocks;
    }

    data->maps.clear();
    data->blocks_nullmaps.clear();

    BlocksList restored_blocks;

    /// names to positions optimization
    std::vector<size_t> positions;
    std::vector<bool> is_nullable;
    if (!right_blocks.empty())
    {
        positions.reserve(right_sample_block.columns());
        const Block & tmp_block = *right_blocks.begin();
        for (const auto & sample_column : right_sample_block)
        {
            positions.emplace_back(tmp_block.getPositionByName(sample_column.name));
            is_nullable.emplace_back(isNullableOrLowCardinalityNullable(sample_column.type));
        }
    }

    for (Block & saved_block : right_blocks)
    {
        Block restored_block;
        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto & column = saved_block.getByPosition(positions[i]);
            correctNullabilityInplace(column, is_nullable[i]);
            restored_block.insert(column);
        }
        restored_blocks.emplace_back(std::move(restored_block));
    }

    data.reset();
    return restored_blocks;
}

const ColumnWithTypeAndName & HashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable when right side is nullable
    return savedBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::validateAdditionalFilterExpression(ExpressionActionsPtr additional_filter_expression)
{
    if (!additional_filter_expression)
        return;

    Block expression_sample_block = additional_filter_expression->getSampleBlock();

    if (expression_sample_block.columns() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected single column, got '{}'",
            expression_sample_block.dumpStructure());
    }

    auto type = removeNullable(expression_sample_block.getByPosition(0).type);
    if (!type->equals(*std::make_shared<DataTypeUInt8>()))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got '{}'. expression:\n{}",
            expression_sample_block.getByPosition(0).type->getName(),
            additional_filter_expression->dumpActions());
    }

    bool is_supported = (strictness == JoinStrictness::All) && (isInnerOrLeft(kind) || isRightOrFull(kind));
    if (!is_supported)
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "Non equi condition '{}' from JOIN ON section is supported only for ALL INNER/LEFT/FULL/RIGHT JOINs",
            expression_sample_block.getByPosition(0).name);
    }
}

bool HashJoin::needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const
{
    if (!table_join_->oneDisjunct())
        return true;
    /// If it'a a all right join with inequal conditions, we need to mark each row
    if (table_join_->getMixedJoinExpression() && isRightOrFull(table_join_->kind()))
        return true;
    return false;
}

}
