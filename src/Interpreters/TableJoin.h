#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsEnums.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <QueryPipeline/SizeLimits.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/IKeyValueEntity.h>

#include <Common/Exception.h>
#include <Parsers/IAST_fwd.h>

#include <cstddef>
#include <unordered_map>

#include <utility>
#include <memory>
#include <base/types.h>
#include <Common/logger_useful.h>

namespace DB
{

class Context;
class ASTSelectQuery;
struct DatabaseAndTableWithAlias;
class Block;
class DictionaryJoinAdapter;
class StorageJoin;
class StorageDictionary;
class IKeyValueEntity;

struct ColumnWithTypeAndName;
using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;

struct Settings;

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class TableJoin
{
public:
    using NameToTypeMap = std::unordered_map<String, DataTypePtr>;

    /// Corresponds to one disjunct
    struct JoinOnClause
    {
        Names key_names_left;
        Names key_names_right; /// Duplicating right key names are qualified

        ASTPtr on_filter_condition_left;
        ASTPtr on_filter_condition_right;

        JoinOnClause() = default;

        std::pair<String, String> condColumnNames() const
        {
            std::pair<String, String> res;
            if (on_filter_condition_left)
                res.first = on_filter_condition_left->getColumnName();
            if (on_filter_condition_right)
                res.second = on_filter_condition_right->getColumnName();
            return res;
        }

        size_t keysCount() const
        {
            assert(key_names_left.size() == key_names_right.size());
            return key_names_right.size();
        }

        String formatDebug(bool short_format = false) const
        {
            const auto & [left_cond, right_cond] = condColumnNames();

            if (short_format)
            {
                return fmt::format("({}) = ({}){}{}", fmt::join(key_names_left, ", "), fmt::join(key_names_right, ", "),
                                   !left_cond.empty() ? " AND " + left_cond : "", !right_cond.empty() ? " AND " + right_cond : "");
            }

            return fmt::format(
                "Left keys: [{}] Right keys [{}] Condition columns: '{}', '{}'",
                 fmt::join(key_names_left, ", "), fmt::join(key_names_right, ", "), left_cond, right_cond);
        }
    };

    using Clauses = std::vector<JoinOnClause>;

    static std::string formatClauses(const Clauses & clauses, bool short_format = false)
    {
        std::vector<std::string> res;
        for (const auto & clause : clauses)
            res.push_back("[" + clause.formatDebug(short_format) + "]");
        return fmt::format("{}", fmt::join(res, "; "));
    }

private:
    /** Query of the form `SELECT expr(x) AS k FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k`
      * The join is made by column k.
      * During the JOIN,
      *  - in the "right" table, it will be available by alias `k`, since `Project` action for the subquery was executed.
      *  - in the "left" table, it will be accessible by the name `expr(x)`, since `Project` action has not been executed yet.
      * You must remember both of these options.
      *
      * Query of the form `SELECT ... from t1 ANY LEFT JOIN (SELECT ... from t2) ON expr(t1 columns) = expr(t2 columns)`
      *     to the subquery will be added expression `expr(t2 columns)`.
      * It's possible to use name `expr(t2 columns)`.
      */

    friend class TreeRewriter;

    SizeLimits size_limits;
    const size_t default_max_bytes = 0;
    const bool join_use_nulls = false;
    const size_t max_joined_block_rows = 0;
    MultiEnum<JoinAlgorithm> join_algorithm = MultiEnum<JoinAlgorithm>(JoinAlgorithm::AUTO);
    const size_t partial_merge_join_rows_in_right_blocks = 0;
    const size_t partial_merge_join_left_table_buffer_bytes = 0;
    const size_t max_files_to_merge = 0;
    const String temporary_files_codec = "LZ4";

    /// the limit has no technical reasons, it supposed to improve safety
    const size_t MAX_DISJUNCTS = 16; /// NOLINT

    ASTs key_asts_left;
    ASTs key_asts_right;

    Clauses clauses;

    ASTTableJoin table_join;

    ASOFJoinInequality asof_inequality = ASOFJoinInequality::GreaterOrEquals;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    NamesAndTypesList columns_from_joined_table;
    /// Columns will be added to block by JOIN.
    /// It's a subset of columns_from_joined_table
    /// Note: without corrected Nullability or type, see correctedColumnsAddedByJoin
    NamesAndTypesList columns_added_by_join;

    /// Target type to convert key columns before join
    NameToTypeMap left_type_map;
    NameToTypeMap right_type_map;

    /// Name -> original name. Names are the same as in columns_from_joined_table list.
    std::unordered_map<String, String> original_names;
    /// Original name -> name. Only renamed columns.
    std::unordered_map<String, String> renames;

    VolumePtr tmp_volume;

    std::shared_ptr<StorageJoin> right_storage_join;

    std::shared_ptr<const IKeyValueEntity> right_kv_storage;

    std::string right_storage_name;

    Names requiredJoinedNames() const;

    /// Create converting actions and change key column names if required
    ActionsDAGPtr applyKeyConvertToTable(
        const ColumnsWithTypeAndName & cols_src, const NameToTypeMap & type_mapping,
        NameToNameMap & key_column_rename,
        bool make_nullable) const;

    void addKey(const String & left_name, const String & right_name, const ASTPtr & left_ast, const ASTPtr & right_ast = nullptr);

    void assertHasOneOnExpr() const;

    /// Calculates common supertypes for corresponding join key columns.
    template <typename LeftNamesAndTypes, typename RightNamesAndTypes>
    void inferJoinKeyCommonType(const LeftNamesAndTypes & left, const RightNamesAndTypes & right, bool allow_right, bool strict);

    NamesAndTypesList correctedColumnsAddedByJoin() const;

public:
    TableJoin() = default;

    TableJoin(const Settings & settings, VolumePtr tmp_volume_);

    /// for StorageJoin
    TableJoin(SizeLimits limits, bool use_nulls, JoinKind kind, JoinStrictness strictness,
              const Names & key_names_right)
        : size_limits(limits)
        , default_max_bytes(0)
        , join_use_nulls(use_nulls)
        , join_algorithm(JoinAlgorithm::HASH)
    {
        clauses.emplace_back().key_names_right = key_names_right;
        table_join.kind = kind;
        table_join.strictness = strictness;
    }

    JoinKind kind() const { return table_join.kind; }
    JoinStrictness strictness() const { return table_join.strictness; }
    bool sameStrictnessAndKind(JoinStrictness, JoinKind) const;
    const SizeLimits & sizeLimits() const { return size_limits; }
    VolumePtr getTemporaryVolume() { return tmp_volume; }

    bool isEnabledAlgorithm(JoinAlgorithm val) const
    {
        /// When join_algorithm = 'default' (not specified by user) we use hash or direct algorithm.
        /// It's behaviour that was initially supported by clickhouse.
        bool is_enbaled_by_default = val == JoinAlgorithm::DEFAULT
                                  || val == JoinAlgorithm::HASH
                                  || val == JoinAlgorithm::DIRECT;
        if (join_algorithm.isSet(JoinAlgorithm::DEFAULT) && is_enbaled_by_default)
            return true;
        return join_algorithm.isSet(val);
    }

    bool allowParallelHashJoin() const;

    bool joinUseNulls() const { return join_use_nulls; }
    bool forceNullableRight() const { return join_use_nulls && isLeftOrFull(table_join.kind); }
    bool forceNullableLeft() const { return join_use_nulls && isRightOrFull(table_join.kind); }
    size_t defaultMaxBytes() const { return default_max_bytes; }
    size_t maxJoinedBlockRows() const { return max_joined_block_rows; }
    size_t maxRowsInRightBlock() const { return partial_merge_join_rows_in_right_blocks; }
    size_t maxBytesInLeftBuffer() const { return partial_merge_join_left_table_buffer_bytes; }
    size_t maxFilesToMerge() const { return max_files_to_merge; }
    const String & temporaryFilesCodec() const { return temporary_files_codec; }
    bool needStreamWithNonJoinedRows() const;

    bool oneDisjunct() const;

    JoinOnClause & getOnlyClause() { assertHasOneOnExpr(); return clauses[0]; }
    const JoinOnClause & getOnlyClause() const { assertHasOneOnExpr(); return clauses[0]; }

    std::vector<JoinOnClause> & getClauses() { return clauses; }
    const std::vector<JoinOnClause> & getClauses() const { return clauses; }

    Names getAllNames(JoinTableSide side) const;

    void resetCollected();
    void addUsingKey(const ASTPtr & ast);

    void addDisjunct();

    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    /* Conditions for left/right table from JOIN ON section.
     *
     * Conditions for left and right tables stored separately and united with 'and' function into one column.
     * For example for query:
     * SELECT ... JOIN ... ON t1.id == t2.id AND expr11(t1) AND expr21(t2) AND expr12(t1) AND expr22(t2)
     *
     * We will build two new ASTs: `expr11(t1) AND expr12(t1)`, `expr21(t2) AND expr22(t2)`
     * Such columns will be added and calculated for left and right tables respectively.
     * Only rows where conditions are met (where new columns have non-zero value) will be joined.
     *
     * NOTE: non-equi condition containing columns from different tables (like `... ON t1.id = t2.id AND t1.val > t2.val)
     *     doesn't supported yet, it can be added later.
     */
    void addJoinCondition(const ASTPtr & ast, bool is_left);

    bool hasUsing() const { return table_join.using_expression_list != nullptr; }
    bool hasOn() const { return table_join.on_expression != nullptr; }

    String getOriginalName(const String & column_name) const;
    NamesWithAliases getNamesWithAliases(const NameSet & required_columns) const;
    NamesWithAliases getRequiredColumns(const Block & sample, const Names & action_required_columns) const;

    void deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix);
    size_t rightKeyInclusion(const String & name) const;
    NameSet requiredRightKeys() const;

    bool leftBecomeNullable(const DataTypePtr & column_type) const;
    bool rightBecomeNullable(const DataTypePtr & column_type) const;
    void addJoinedColumn(const NameAndTypePair & joined_column);

    template <typename TColumns>
    void addJoinedColumnsAndCorrectTypesImpl(TColumns & left_columns, bool correct_nullability);

    void addJoinedColumnsAndCorrectTypes(NamesAndTypesList & left_columns, bool correct_nullability);
    void addJoinedColumnsAndCorrectTypes(ColumnsWithTypeAndName & left_columns, bool correct_nullability);

    /// Calculate converting actions, rename key columns in required
    /// For `USING` join we will convert key columns inplace and affect into types in the result table
    /// For `JOIN ON` we will create new columns with converted keys to join by.
    std::pair<ActionsDAGPtr, ActionsDAGPtr>
    createConvertingActions(
        const ColumnsWithTypeAndName & left_sample_columns,
        const ColumnsWithTypeAndName & right_sample_columns);

    void setAsofInequality(ASOFJoinInequality inequality) { asof_inequality = inequality; }
    ASOFJoinInequality getAsofInequality() { return asof_inequality; }

    ASTPtr leftKeysList() const;
    ASTPtr rightKeysList() const; /// For ON syntax only

    const NamesAndTypesList & columnsFromJoinedTable() const { return columns_from_joined_table; }

    Names columnsAddedByJoin() const
    {
        Names res;
        for (const auto & col : columns_added_by_join)
            res.push_back(col.name);
        return res;
    }

    /// StorageJoin overrides key names (cause of different names qualification)
    void setRightKeys(const Names & keys) { getOnlyClause().key_names_right = keys; }

    Block getRequiredRightKeys(const Block & right_table_keys, std::vector<String> & keys_sources) const;

    String renamedRightColumnName(const String & name) const;

    void resetKeys();
    void resetToCross();

    std::unordered_map<String, String> leftToRightKeyRemap() const;

    /// Remember storage name in case of joining with dictionary or another special storage
    void setRightStorageName(const std::string & storage_name);
    const std::string & getRightStorageName() const;

    void setStorageJoin(std::shared_ptr<const IKeyValueEntity> storage);
    void setStorageJoin(std::shared_ptr<StorageJoin> storage);

    std::shared_ptr<StorageJoin> getStorageJoin() { return right_storage_join; }

    bool isSpecialStorage() const { return !right_storage_name.empty() || right_storage_join || right_kv_storage; }

    std::shared_ptr<const IKeyValueEntity> getStorageKeyValue() { return right_kv_storage; }
};

}
