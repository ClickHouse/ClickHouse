#pragma once

#include <base/types.h>

#include <memory>
#include <vector>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Common/NamePrompter.h>

namespace DB
{

/// Description of non-primary index for Storage
struct IndexDescription
{
    /// Definition AST of index
    ASTPtr definition_ast;

    /// List of expressions for index calculation
    ASTPtr expression_list_ast;

    /// Index name
    String name;

    /// Index type (minmax, set, bloom filter, etc.)
    String type;

    /// Prepared expressions for index calculations
    ExpressionActionsPtr expression;

    /// Index arguments, for example probability for bloom filter
    FieldVector arguments;

    /// Names of index columns (not to be confused with required columns)
    Names column_names;

    /// Data types of index columns
    DataTypes data_types;

    /// Sample block with index columns. (NOTE: columns in block are empty, but not nullptr)
    Block sample_block;

    /// Index granularity, make sense for skip indices
    size_t granularity;

    /// Parse index from definition AST
    static IndexDescription getIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context);

    IndexDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    IndexDescription(const IndexDescription & other);
    IndexDescription & operator=(const IndexDescription & other);

    /// Recalculate index with new columns because index expression may change
    /// if something change in columns.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context);
};

/// All secondary indices in storage
struct IndicesDescription : public std::vector<IndexDescription>, IHints<1, IndicesDescription>
{
    /// Index with name exists
    bool has(const String & name) const;
    /// Convert description to string
    String toString() const;
    /// Parse description from string
    static IndicesDescription parse(const String & str, const ColumnsDescription & columns, ContextPtr context);

    /// Return common expression for all stored indices
    ExpressionActionsPtr getSingleExpressionForIndices(const ColumnsDescription & columns, ContextPtr context) const;

public:
    Names getAllRegisteredNames() const override;
};

}
