#pragma once

#include <Core/Types.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Common/PODArray_fwd.h>

#include <memory>
#include <vector>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

using IColumnPermutation = PaddedPODArray<size_t>;

/// Description of projections for Storage
struct ProjectionDescription
{
    enum class Type : uint8_t
    {
        Normal,
        Aggregate,
    };

    static constexpr const char * MINMAX_COUNT_PROJECTION_NAME = "_minmax_count_projection";

    /// Definition AST of projection
    ASTPtr definition_ast;

    /// Subquery AST for projection calculation
    ASTPtr query_ast;

    /// Projection name
    String name;

    /// Projection type (normal, aggregate, etc.)
    Type type = Type::Normal;

    /// Columns which are required for query_ast.
    Names required_columns;

    Names getRequiredColumns() const { return required_columns; }

    /// Sample block with projection columns. (NOTE: columns in block are empty, but not nullptr)
    Block sample_block;

    Block sample_block_for_keys;

    StorageMetadataPtr metadata;

    size_t key_size = 0;

    /// If a primary key expression is used in the minmax_count projection, store the name of max expression.
    String primary_key_max_column_name;

    /// Stores partition value indices of partition value row. It's needed because identical
    /// partition columns will appear only once in projection block, but every column will have a
    /// value in the partition value row. This vector holds the biggest value index of give
    /// partition columns.
    std::vector<size_t> partition_value_indices;

    bool with_parent_part_offset = false;

    /// Parse projection from definition AST
    static ProjectionDescription
    getProjectionFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr query_context);

    static ProjectionDescription getMinMaxCountProjection(
        const ColumnsDescription & columns,
        ASTPtr partition_columns,
        const Names & minmax_columns,
        const ASTs & primary_key_asts,
        ContextPtr query_context);

    ProjectionDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionally share AST variables and modify them.
    ProjectionDescription(const ProjectionDescription & other) = delete;
    ProjectionDescription(ProjectionDescription && other) = default;
    ProjectionDescription & operator=(const ProjectionDescription & other) = delete;
    ProjectionDescription & operator=(ProjectionDescription && other) = default;

    ProjectionDescription clone() const;

    bool operator==(const ProjectionDescription & other) const;
    bool operator!=(const ProjectionDescription & other) const { return !(*this == other); }

    /// Recalculate projection with new columns because projection expression may change
    /// if something change in columns.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr query_context);

    bool isPrimaryKeyColumnPossiblyWrappedInFunctions(const ASTPtr & node) const;

    /**
     * @brief Calculates the projection result for a given input block.
     *
     * @param block The input block used to evaluate the projection.
     * @param context The query context. A copy will be made internally with adjusted settings.
     * @param perm_ptr Optional pointer to a permutation vector. If provided, it is used to map
     *        the output rows back to their original order in the parent block. This is necessary
     *        when generating the `_part_offset` column, which acts as `_parent_part_offset` in
     *        the projection index and reflects the position of each row in the parent part.
     *
     * @return The resulting block after executing the projection query.
     */
    Block calculate(const Block & block, ContextPtr context, const IColumnPermutation * perm_ptr = nullptr) const;

    String getDirectoryName() const { return name + ".proj"; }
};

using ProjectionDescriptionRawPtr = const ProjectionDescription *;

/// All projections in storage
struct ProjectionsDescription : public IHints<>
{
    ProjectionsDescription() = default;
    ProjectionsDescription(ProjectionsDescription && other) = default;
    ProjectionsDescription & operator=(ProjectionsDescription && other) = default;

    ProjectionsDescription clone() const;

    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static ProjectionsDescription parse(const String & str, const ColumnsDescription & columns, ContextPtr query_context);

    /// Return common expression for all stored projections
    ExpressionActionsPtr getSingleExpressionForProjections(const ColumnsDescription & columns, ContextPtr query_context) const;

    bool operator==(const ProjectionsDescription & other) const { return projections == other.projections; }
    bool operator!=(const ProjectionsDescription & other) const { return !(*this == other); }

    auto begin() const { return projections.begin(); }
    auto end() const { return projections.end(); }

    size_t size() const { return projections.size(); }
    bool empty() const { return projections.empty(); }

    bool has(const String & projection_name) const;
    const ProjectionDescription & get(const String & projection_name) const;

    void
    add(ProjectionDescription && projection, const String & after_projection = String(), bool first = false, bool if_not_exists = false);
    void remove(const String & projection_name, bool if_exists);

    std::vector<String> getAllRegisteredNames() const override;

private:
    /// Keep the sequence of columns and allow to lookup by name.
    using Container = std::list<ProjectionDescription>;
    using Map = std::unordered_map<std::string, Container::iterator>;

    Container projections;
    Map map;
};

}
