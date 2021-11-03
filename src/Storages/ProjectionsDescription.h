#pragma once

#include <Core/Types.h>

#include <memory>
#include <vector>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/AggregateDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>

#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

namespace DB
{
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Description of projections for Storage
struct ProjectionDescription
{
    enum class Type
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

    bool is_minmax_count_projection = false;

    /// If a primary key expression is used in the minmax_count projection, store the name of max expression.
    String primary_key_max_column_name;

    /// Parse projection from definition AST
    static ProjectionDescription
    getProjectionFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr query_context);

    static ProjectionDescription getMinMaxCountProjection(
        const ColumnsDescription & columns, const Names & minmax_columns, const ASTs & primary_key_asts, ContextPtr query_context);

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

    Block calculate(const Block & block, ContextPtr context) const;

    String getDirectoryName() const { return name + ".proj"; }
};

using ProjectionDescriptionRawPtr = const ProjectionDescription *;

/// All projections in storage
struct ProjectionsDescription
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

private:
    /// Keep the sequence of columns and allow to lookup by name.
    using Container = std::list<ProjectionDescription>;
    using Map = std::unordered_map<std::string, Container::iterator>;

    Container projections;
    Map map;
};

}
