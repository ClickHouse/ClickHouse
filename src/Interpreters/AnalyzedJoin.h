#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/SettingsCollection.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/asof.h>
#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/SizeLimits.h>

#include <utility>
#include <memory>

namespace DB
{

class Context;
class ASTSelectQuery;
struct DatabaseAndTableWithAlias;
class Block;

struct Settings;

class Volume;
using VolumePtr = std::shared_ptr<Volume>;

class AnalyzedJoin
{
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

    friend class SyntaxAnalyzer;

    const SizeLimits size_limits;
    const size_t default_max_bytes;
    const bool join_use_nulls;
    const size_t max_joined_block_rows = 0;
    JoinAlgorithm join_algorithm;
    const bool partial_merge_join_optimizations = false;
    const size_t partial_merge_join_rows_in_right_blocks = 0;

    Names key_names_left;
    Names key_names_right; /// Duplicating names are qualified.
    ASTs key_asts_left;
    ASTs key_asts_right;
    ASTTableJoin table_join;
    ASOF::Inequality asof_inequality = ASOF::Inequality::GreaterOrEquals;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    NamesAndTypesList columns_from_joined_table;
    /// Columns will be added to block by JOIN. It's a subset of columns_from_joined_table with corrected Nullability
    NamesAndTypesList columns_added_by_join;

    /// Name -> original name. Names are the same as in columns_from_joined_table list.
    std::unordered_map<String, String> original_names;
    /// Original name -> name. Only ranamed columns.
    std::unordered_map<String, String> renames;

    VolumePtr tmp_volume;

public:
    AnalyzedJoin(const Settings &, VolumePtr tmp_volume);

    /// for StorageJoin
    AnalyzedJoin(SizeLimits limits, bool use_nulls, ASTTableJoin::Kind kind, ASTTableJoin::Strictness strictness,
                 const Names & key_names_right_)
        : size_limits(limits)
        , default_max_bytes(0)
        , join_use_nulls(use_nulls)
        , join_algorithm(JoinAlgorithm::HASH)
        , key_names_right(key_names_right_)
    {
        table_join.kind = kind;
        table_join.strictness = strictness;
    }

    ASTTableJoin::Kind kind() const { return table_join.kind; }
    ASTTableJoin::Strictness strictness() const { return table_join.strictness; }
    bool sameStrictnessAndKind(ASTTableJoin::Strictness, ASTTableJoin::Kind) const;
    const SizeLimits & sizeLimits() const { return size_limits; }
    VolumePtr getTemporaryVolume() { return tmp_volume; }
    bool allowMergeJoin() const;
    bool preferMergeJoin() const { return join_algorithm == JoinAlgorithm::PREFER_PARTIAL_MERGE; }
    bool forceMergeJoin() const { return join_algorithm == JoinAlgorithm::PARTIAL_MERGE; }
    bool forceHashJoin() const { return join_algorithm == JoinAlgorithm::HASH; }

    bool forceNullableRight() const { return join_use_nulls && isLeftOrFull(table_join.kind); }
    bool forceNullableLeft() const { return join_use_nulls && isRightOrFull(table_join.kind); }
    size_t defaultMaxBytes() const { return default_max_bytes; }
    size_t maxJoinedBlockRows() const { return max_joined_block_rows; }
    size_t maxRowsInRightBlock() const { return partial_merge_join_rows_in_right_blocks; }
    bool enablePartialMergeJoinOptimizations() const { return partial_merge_join_optimizations; }

    void addUsingKey(const ASTPtr & ast);
    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    bool hasUsing() const { return table_join.using_expression_list != nullptr; }
    bool hasOn() const { return table_join.on_expression != nullptr; }

    NameSet getQualifiedColumnsSet() const;
    NamesWithAliases getNamesWithAliases(const NameSet & required_columns) const;
    NamesWithAliases getRequiredColumns(const Block & sample, const Names & action_required_columns) const;

    void deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix);
    size_t rightKeyInclusion(const String & name) const;
    NameSet requiredRightKeys() const;

    void addJoinedColumn(const NameAndTypePair & joined_column);
    void addJoinedColumnsAndCorrectNullability(Block & sample_block) const;

    void setAsofInequality(ASOF::Inequality inequality) { asof_inequality = inequality; }
    ASOF::Inequality getAsofInequality() { return asof_inequality; }

    ASTPtr leftKeysList() const;
    ASTPtr rightKeysList() const; /// For ON syntax only

    Names requiredJoinedNames() const;
    const Names & keyNamesLeft() const { return key_names_left; }
    const Names & keyNamesRight() const { return key_names_right; }
    const NamesAndTypesList & columnsFromJoinedTable() const { return columns_from_joined_table; }
    const NamesAndTypesList & columnsAddedByJoin() const { return columns_added_by_join; }

    /// StorageJoin overrides key names (cause of different names qualification)
    void setRightKeys(const Names & keys) { key_names_right = keys; }

    static bool sameJoin(const AnalyzedJoin * x, const AnalyzedJoin * y);
};

}
