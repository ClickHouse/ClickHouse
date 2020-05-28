#pragma once
#include <Parsers/IAST_fwd.h>
#include <Storages/DataDestinationType.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/AggregateDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TTLMode.h>

namespace DB
{

/// Common struct for TTL record in storage
struct TTLDescription
{
    TTLMode mode;

    /// Expression part of TTL AST:
    /// TTL d + INTERVAL 1 DAY
    ///    ^~~~~expression~~~~^
    ASTPtr expression_ast;

    /// Expresion actions evaluated from AST
    ExpressionActionsPtr expression;

    /// Result column of this TTL expression
    String result_column;

    ExpressionActionsPtr where_expression;

    String where_result_column;

    Names group_by_keys;

    std::vector<std::tuple<String, String, ExpressionActionsPtr>> group_by_aggregations;
    AggregateDescriptions aggregate_descriptions;

    /// Destination type, only valid for table TTLs.
    /// For example DISK or VOLUME
    DataDestinationType destination_type;

    /// Name of destination disk or volume
    String destination_name;

    /// Parse TTL structure from definition. Able to parse both column and table
    /// TTLs.
    static TTLDescription getTTLFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context, const StorageMetadataKeyField & primary_key);
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

    /// Rows removing TTL
    TTLDescription rows_ttl;

    /// Moving data TTL (to other disks or volumes)
    TTLDescriptions move_ttl;
};

}
