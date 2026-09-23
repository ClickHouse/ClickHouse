#pragma once

#include <algorithm>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

#include <Interpreters/IJoin.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/RowRefs.h>

#include <Core/Block_fwd.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/HashJoin/HashJoinTypes.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Common/Arena.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>

namespace DB
{

class TableJoin;
class ExpressionActions;
class JoinSource;

namespace JoinStuff
{
/// Flags needed to implement RIGHT and FULL JOINs.
class JoinUsedFlags;
}

/** Data structure for implementation of hash JOIN.
  * It is a hash table: keys -> rows of joined ("right") table.
  *
  * JOIN-s could be of these types:
  * - ALL × LEFT/INNER/RIGHT/FULL
  * - ANY × LEFT/INNER/RIGHT
  * - SEMI/ANTI x LEFT/RIGHT
  * - ASOF x LEFT/INNER
  *
  * ALL means usual JOIN, when rows are multiplied by number of matching rows from the "right" table.
  * ANY uses one line per unique key from right table. For LEFT JOIN it would be any row (with needed joined key) from the right table,
  * for RIGHT JOIN it would be any row from the left table and for INNER one it would be any row from right and any row from left.
  * SEMI JOIN filter left table by keys that are present in right table for LEFT JOIN, and filter right table by keys from left table
  * for RIGHT JOIN. In other words SEMI JOIN returns only rows which joining keys present in another table.
  * ANTI JOIN is the same as SEMI JOIN but returns rows with joining keys that are NOT present in another table.
  * SEMI/ANTI JOINs allow to get values from both tables. For filter table it gets any row with joining same key. For ANTI JOIN it returns
  * defaults other table columns.
  * ASOF JOIN is not-equi join. For one key column it finds nearest value to join according to join inequality.
  * It's expected that ANY|SEMI LEFT JOIN is more efficient that ALL one.
  *
  * If INNER is specified - leave only rows that have matching rows from "right" table.
  * If LEFT is specified - in case when there is no matching row in "right" table, fill it with default values instead.
  * If RIGHT is specified - first process as INNER, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  * If FULL is specified - first process as LEFT, but track what rows from the right table was joined,
  *  and at the end, add rows from right table that was not joined and substitute default values for columns of left table.
  *
  * Thus, LEFT and RIGHT JOINs are not symmetric in terms of implementation.
  *
  * All JOINs are done by equality condition on keys (equijoin).
  * Non-equality and other conditions are not supported.
  *
  * Implementation:
  *
  * 1. Build hash table in memory from "right" table.
  * This hash table is in form of keys -> row in case of ANY or keys -> [rows...] in case of ALL.
  *
  * 2. Process "left" table and join corresponding rows from "right" table by lookups in the map.
  *
  * In case of ANY LEFT JOIN - form new columns with found values or default values.
  * This is the most simple. Number of rows in left table does not change.
  *
  * In case of ANY INNER JOIN - form new columns with found values,
  *  and also build a filter - in what rows nothing was found.
  * Then filter columns of "left" table.
  *
  * In case of ALL ... JOIN - form new columns with all found rows,
  *  and also fill 'offsets' array, describing how many times we need to replicate values of "left" table.
  * Then replicate columns of "left" table.
  *
  * How Nullable keys are processed:
  *
  * NULLs never join to anything, even to each other.
  * During building of map, we just skip keys with NULL value of any component.
  * During joining, we simply treat rows with any NULLs in key as non joined.
  *
  * Default values for outer joins (LEFT, RIGHT, FULL):
  *
  * Behaviour is controlled by 'join_use_nulls' settings.
  * If it is false, we substitute (global) default value for the data type, for non-joined rows
  *  (zero, empty string, etc. and NULL for Nullable data types).
  * If it is true, we always generate Nullable column and substitute NULLs for non-joined rows,
  *  as in standard SQL.
  */
class HashJoin : public HashJoinTypes
{
public:
    HashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_sample_block,
        /// `PartitionedHashJoin` passes false: its `HashJoinTable` has no key-only counterpart.
        bool allow_set_maps_ = true);

    ~HashJoin();

    const TableJoin & getTableJoin() const { return *table_join; }

    void checkTypesOfKeys(const Block & block) const;

    /// Check joinGet arguments and infer the return type.
    DataTypePtr joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const;

    void onBuildPhaseFinish();

    /// Number of unique keys in all built JOIN maps.
    size_t getTotalRowCount() const;

    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }
    const std::optional<TypeIndex> & getAsofType() const { return asof_type; }
    ASOFJoinInequality getAsofInequality() const { return asof_inequality; }

    const ColumnWithTypeAndName & rightAsofKeyColumn() const;

    /// For INNER/LEFT ALL JOINs, if the right side has no duplicates inside the join key columns,
    /// we can switch from ALL to RightAny strictness for better performance. Only ever goes from
    /// true to false, so a relaxed store needs no further ordering.
    std::atomic<bool> all_values_unique = true;
    bool all_join_was_promoted_to_right_any = false;

    void reuseJoinedData(const HashJoin & join);

    RightTableDataPtr getJoinedData() const { return data; }

    using HashJoinTypes::prepareRightBlock;
    Block prepareRightBlock(const Block & block) const;

    const Block & savedBlockSample() const { return data->sample_block; }

    bool isUsed(size_t off) const;
    bool isUsed(UInt32 block_no, size_t row_idx) const;

    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join);

    void materializeColumnsFromLeftBlock(Block & block) const;
    Block materializeColumnsFromRightBlock(Block block) const;

    /// Packs a prepared right block (`prepareRightBlock`) into its stored form. When the row store is
    /// initialized, the columns its layout admits go into a `RowDataStore` and the rest stay columnar.
    /// Otherwise every column stays columnar. A caller that already built this block's row store passes it in.
    StoredBlock createStoredBlock(
        const Block & block_to_save, ScatteredBlock::Selector selector, RowDataStorePtr row_store = nullptr) const;

    const std::vector<Sizes> & getKeySizes() const { return key_sizes; }

    bool enableSoftwarePrefetch() const { return enable_prefetch; }

    void setEnableLazyColumnsIndexing(bool value) { enable_lazy_columns_indexing = value; }

private:
    friend class JoinSource;
    /// Uses a `HashJoin` as its schema delegate and row-store owner while building and probing its
    /// own partitioned maps.
    friend class PartitionedHashJoin;
    friend class HashJoinClause;

    std::shared_ptr<TableJoin> table_join;
    JoinKind kind;
    JoinStrictness strictness;

    std::optional<TypeIndex> asof_type;
    const ASOFJoinInequality asof_inequality;

    /// Right table data. StorageJoin shares it between many Join objects.
    /// Flags that indicate that particular row already used in join.
    /// Flag is stored for every record in hash map.
    /// Number of this flags equals to hashtable buffer size (plus one for zero value).
    /// Changes in hash table broke correspondence,
    /// so we must guarantee constantness of hash table during HashJoin lifetime
    mutable std::shared_ptr<JoinStuff::JoinUsedFlags> used_flags;

    RightTableDataPtr data;

    std::vector<Sizes> key_sizes;

    /// Block with columns from the right-side table.
    Block right_sample_block;
    /// Block with columns from the right-side table except key columns.
    Block sample_block_with_columns_to_add;
    /// Block with key columns in the same order they appear in the right-side table (duplicates appear once).
    Block right_table_keys;
    /// Block with key columns right-side table keys that are needed in result (would be attached after joined columns).
    Block required_right_keys;
    /// Left table column names that are sources for required_right_keys columns
    std::vector<String> required_right_keys_sources;

    std::vector<std::pair<size_t, size_t>> additional_filter_required_rhs_pos;

    /// Maximum number of rows in result block. If it is 0, then no limits.
    size_t max_joined_block_rows = 0;
    size_t max_joined_block_bytes = 0;
    bool joined_block_split_single_row = false;
    bool enable_lazy_columns_replication = false;
    bool enable_lazy_columns_indexing = false;
    bool enable_prefetch = true;

    /// Whether the maps store keys alone, see `JoinMapsKind::Set`. Decided once, before they are created.
    bool use_set_maps = false;
    /// False when the owner cannot consume key-only maps, whatever `canUseSetMaps` would otherwise say.
    const bool allow_set_maps = true;

    LoggerPtr log;

    /// Unchecked as in without `doDebugAsserts`. That walk cannot run while `PartitionedHashJoin`'s threads append.
    size_t getTotalByteCountUnchecked() const;

    void recomputeMapsBytes();

    void dataMapInit(MapsVariant & map);

    void initRightBlockStructure(Block & saved_block_sample);

    bool preferUseMapsAll() const;

    bool canUseSetMaps() const;

    /// The maps flavour this join runs on. All the dispatch entry points take it.
    JoinMapsKind getMapsKind() const;

    bool isUsedByAnotherAlgorithm() const;
    bool canRemoveColumnsFromLeftBlock() const;

    void validateAdditionalFilterExpression(std::shared_ptr<ExpressionActions> additional_filter_expression);
    bool needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const;

    bool isRowStoreSupported() const;

    /// Layout is from the sample block, before any fill thread.
    void initRowStore(const Block & block);

    void reinitUsedFlags();

    bool recordsRowRefsForStats() const;

    void doDebugAsserts() const;
};
}
