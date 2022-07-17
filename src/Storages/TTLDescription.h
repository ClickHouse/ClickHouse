#pragma once
#include <Parsers/IAST_fwd.h>
#include <Storages/DataDestinationType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/AggregateDescription.h>
#include <Storages/TTLMode.h>
#include "Core/NamesAndTypes.h"
#include "DataTypes/Serializations/ISerialization.h"

namespace DB
{

/// Assignment expression in TTL with GROUP BY
struct TTLAggregateDescription
{
    /// Name of column in assignment
    /// x = sum(y)
    /// ^
    String column_name;

    /// Name of column on the right hand of the assignment
    /// x = sum(y)
    ///    ^~~~~~^
    String expression_result_column_name;

    /// Expressions to calculate the value of assignment expression
    ExpressionActionsPtr expression;

    TTLAggregateDescription() = default;
    TTLAggregateDescription(const TTLAggregateDescription & other);
    TTLAggregateDescription & operator=(const TTLAggregateDescription & other);
};

using TTLAggregateDescriptions = std::vector<TTLAggregateDescription>;

/// Common struct for TTL record in storage
struct TTLDescription
{
    TTLMode mode;

    /// Expression part of TTL AST:
    /// TTL d + INTERVAL 1 DAY
    ///    ^~~~~~~~~~~~~~~~~~~^
    ASTPtr expression_ast;

    /// Expression actions evaluated from AST
    ExpressionActionsPtr expression;

    /// Result column of this TTL expression
    String result_column;

    /// WHERE part in TTL expression
    /// TTL ... WHERE x % 10 == 0 and y > 5
    ///              ^~~~~~~~~~~~~~~~~~~~~~^
    ExpressionActionsPtr where_expression;

    /// Name of result column from WHERE expression
    String where_result_column;

    /// Names of key columns in GROUP BY expression
    /// TTL ... GROUP BY toDate(d), x SET ...
    ///                  ^~~~~~~~~~~~^
    Names group_by_keys;

    /// SET parts of TTL expression
    TTLAggregateDescriptions set_parts;

    /// Aggregate descriptions for GROUP BY in TTL
    AggregateDescriptions aggregate_descriptions;

    /// Destination type, only valid for table TTLs.
    /// For example DISK or VOLUME
    DataDestinationType destination_type;

    /// Name of destination disk or volume
    String destination_name;

    /// If true, do nothing if DISK or VOLUME doesn't exist .
    /// Only valid for table MOVE TTLs.
    bool if_exists = false;

    /// Codec name which will be used to recompress data
    ASTPtr recompression_codec;

    /// Parse TTL structure from definition. Able to parse both column and table
    /// TTLs.
    static TTLDescription getTTLFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key);

    TTLDescription() = default;
    TTLDescription(const TTLDescription & other);
    TTLDescription & operator=(const TTLDescription & other);
};

/// Mapping from column name to column TTL
using TTLColumnsDescription = std::unordered_map<String, TTLDescription>;
using TTLDescriptions = std::vector<TTLDescription>;

/// Common TTL for all table. Specified after defining the table columns.
struct TTLTableDescription
{
    /// Definition. Include all parts of TTL:
    /// TTL d + INTERVAL 1 day TO VOLUME 'disk1'
    /// ^~~~~~~~~~~~~~~definition~~~~~~~~~~~~~~~^
    ASTPtr definition_ast;

    /// Unconditional main removing rows TTL. Can be only one for table.
    TTLDescription rows_ttl;

    /// Conditional removing rows TTLs.
    TTLDescriptions rows_where_ttl;

    /// Moving data TTL (to other disks or volumes)
    TTLDescriptions move_ttl;

    TTLDescriptions recompression_ttl;

    TTLDescriptions group_by_ttl;

    TTLTableDescription() = default;
    TTLTableDescription(const TTLTableDescription & other);
    TTLTableDescription & operator=(const TTLTableDescription & other);

    static TTLTableDescription getTTLForTableFromAST(
        const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key);

    /// Parse description from string
    static TTLTableDescription parse(const String & str, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key);
};

struct LightweightDeleteDescription
{
    NameAndTypePair filter_column;
};

}
