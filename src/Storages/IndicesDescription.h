#pragma once

#include <base/types.h>

#include <vector>
#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Common/NamePrompter.h>

constexpr auto IMPLICITLY_ADDED_MINMAX_INDEX_PREFIX = "auto_minmax_index_";
constexpr auto TEXT_INDEX_NAME = "text";

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

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

    /// Index arguments, for example probability for bloom filter.
    /// Stored as ASTExpressionList with children being ASTLiteral or ASTIdentifier nodes.
    ASTPtr arguments;

    /// Names of index columns (not to be confused with required columns)
    Names column_names;

    /// Data types of index columns
    DataTypes data_types;

    /// Sample block with index columns. (NOTE: columns in block are empty, but not nullptr)
    Block sample_block;

    /// Index granularity, make sense for skip indices
    size_t granularity{};

    /// Condition of a partial index (`WHERE` clause): only rows satisfying it are indexed. Null if absent.
    ASTPtr where_ast;
    ExpressionActionsPtr where_expression;
    String where_column_name;
    /// The indexed expressions, analysed like the condition. The writer runs them only on the rows the condition selects.
    ExpressionActionsPtr selected_rows_expression;
    /// `queryConditionCacheSettingsSalt` of the settings the condition's functions were built with: they decide
    /// what the condition evaluated to when the index was written, and the name and types of a function do not.
    UInt64 where_settings_salt = 0;
    /// Identity of the condition: its AST and the settings above. Parts of a partial index are written with a
    /// marker that holds it, so granules built for another condition are never read as this one's.
    UInt128 where_identity = 0;
    /// The time zone the condition was analysed in. Constants of the condition are folded during the analysis, and
    /// a function such as `toDateTime` folds differently under another zone, so the condition can be matched
    /// only by a query that is analysed in the same zone.
    String where_timezone;

    /// True if index is created implicitly using settings:
    /// add_minmax_index_for_numeric_columns, add_minmax_index_for_string_columns, or add_minmax_index_for_temporal_columns
    bool is_implicitly_created{};

    /// This is here for compatibility reasons. Prior to 26.1 index filenames weren't escaped, which could lead to issues with some
    /// characters in index names producing broken parts. We have this flag to maintain compatibility and be able to load older indices
    /// (if using the `escape_index_filenames`).
    bool escape_filenames{};

    /// Parse index from definition AST
    static IndexDescription getIndexFromAST(
        const ASTPtr & definition_ast,
        const ColumnsDescription & columns,
        bool is_implicitly_created,
        bool escape_filenames,
        ContextPtr context);

    IndexDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    IndexDescription(const IndexDescription & other);
    IndexDescription & operator=(const IndexDescription & other);

    /// Recalculate index with new columns because index expression may change
    /// if something change in columns.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context);

    bool isImplicitlyCreated() const { return is_implicitly_created; }

    void initExpressionInfo(ASTPtr index_expression, const ColumnsDescription & columns, ContextPtr context);

    bool isSimpleSingleColumnIndex() const;

    /// Columns needed to calculate the index, including the ones of its `WHERE` condition.
    Names getRequiredColumns() const;
};

/// All secondary indices in storage
struct IndicesDescription : public std::vector<IndexDescription>, IHints<>
{
    /// Index with name exists
    bool has(const String & name) const;
    /// Get index by name; throws if not found
    const IndexDescription & getByName(const String & name) const;
    /// Index with type exists
    bool hasType(const String & type) const;

    /// Convert description to string. Includes only explicitly created indices
    String explicitToString() const;

    /// Convert description to string. Includes all indices.
    /// It should only used for compatibility or debugging. Otherwise prefer explicitToString()
    String allToString() const;

    /// Parse description from string
    static IndicesDescription
    parse(const String & str, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context);

    /// Return common expression for all stored indices
    ExpressionActionsPtr getSingleExpressionForIndices(const ColumnsDescription & columns, ContextPtr context) const;

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override;
};

/// Extract Field value from an index argument AST node.
/// ASTLiteral yields its value; ASTIdentifier yields its name as a String.
Field getFieldFromIndexArgumentAST(const ASTPtr & ast);

/// Convert all children of an index arguments AST (ASTExpressionList) to a FieldVector.
FieldVector getFieldsFromIndexArgumentsAST(const ASTPtr & arguments);

ASTPtr createImplicitMinMaxIndexAST(const String & column_name);
IndexDescription createImplicitMinMaxIndexDescription(
    const String & column_name, const ColumnsDescription & columns, bool escape_index_filenames, ContextPtr context);
}
