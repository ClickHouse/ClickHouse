#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/DataDestinationType.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/AggregateDescription.h>
#include <Storages/TTLMode.h>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

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

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

struct ExpressionAndSets
{
    ExpressionActionsPtr expression;
    PreparedSetsPtr sets;
};

/// Common struct for TTL record in storage
struct TTLDescription
{
    TTLMode mode{};

    /// Expression part of TTL AST:
    /// TTL d + INTERVAL 1 DAY
    ///    ^~~~~~~~~~~~~~~~~~~^
    ASTPtr expression_ast;
    NamesAndTypesList expression_columns;

    /// Expression actions evaluated from AST
    ExpressionAndSets buildExpression(const ContextPtr & context) const;

    /// Result column of this TTL expression
    String result_column;

    /// WHERE part in TTL expression
    /// TTL ... WHERE x % 10 == 0 and y > 5
    ///              ^~~~~~~~~~~~~~~~~~~~~~^
    ASTPtr where_expression_ast;
    NamesAndTypesList where_expression_columns;
    ExpressionAndSets buildWhereExpression(const ContextPtr & context) const;

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
    DataDestinationType destination_type{};

    /// Name of destination disk or volume
    String destination_name;

    /// If true, do nothing if DISK or VOLUME doesn't exist .
    /// Only valid for table MOVE TTLs.
    bool if_exists = false;

    /// Codec name which will be used to recompress data
    ASTPtr recompression_codec;

    /// Parse TTL structure from definition. Able to parse both column and table TTLs.
    static TTLDescription getTTLFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key, bool is_attach);

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
        const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key, bool is_attach);

    /// Parse description from string
    static TTLTableDescription parse(const String & str, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key, bool is_attach);
};

/// Used by the fast `MODIFY TTL` optimization to decide whether a rows-TTL change can be applied by
/// merely shifting each part's stored TTL timestamps instead of rewriting the data. That is correct
/// only when the change adds the same constant number of seconds to every row's expiry time, i.e.
/// when `new_ttl(row) - old_ttl(row)` does not depend on the row.
///
/// This proves that condition structurally: both expressions must be the same single date/time column
/// shifted by constant fixed-length intervals (`col`, `col + INTERVAL N DAY`, `col - toIntervalHour(N)`,
/// ...), so each expression is exactly `column + constant seconds` and the delta is the difference of
/// the two constants. Day/week intervals are additionally accepted only when the relevant time zone
/// (the column's one for `DateTime`/`DateTime64`, the server's one for `Date`/`Date32`) has a fixed
/// offset from UTC, because `addDays` preserves the local wall-clock time across DST transitions and
/// is not a constant number of seconds otherwise.
///
/// Returns the constant delta in seconds, or `std::nullopt` when the delta is not provably constant
/// (several referenced columns, calendar month/year intervals, non-literal intervals, arbitrary
/// functions, DST-sensitive day/week intervals, or an unsupported result type). A `std::nullopt` result
/// means the fast path must not be used and the caller must fall back to a regular `MATERIALIZE TTL`
/// rewrite. The function never throws for an unoptimizable input; it returns `std::nullopt` instead.
std::optional<time_t> tryComputeConstantTTLDelta(const TTLDescription & old_ttl, const TTLDescription & new_ttl);

/// Overload where the old rows-TTL is given as its serialized expression string (as stored in a part's
/// TTL info fingerprint). It is parsed and built against `columns`/`primary_key`; any parse/build failure
/// yields `std::nullopt` (fall back). Used by the fast `MODIFY TTL` path to verify, per part, that the
/// part's stored TTL timestamps really shift to the new TTL by a single constant.
std::optional<time_t> tryComputeConstantTTLDelta(
    const String & old_ttl_expression, const TTLDescription & new_ttl,
    const ColumnsDescription & columns, const KeyDescription & primary_key, const ContextPtr & context);

/// The name of the time zone whose semantics a part's stored rows-TTL timestamps depend on. It is the
/// column's time zone when the TTL reads a single `DateTime`/`DateTime64` column (`addDays` preserves the
/// local wall-clock time, so the shift it produces is a property of that zone) and the server time zone
/// otherwise (a `Date`/`Date32` TTL result is turned into a timestamp with `DateLUT::serverTimezoneInstance`).
///
/// The fast `MODIFY TTL` path records this next to the part's TTL expression fingerprint and requires it to
/// still match, because neither the expression text nor the part's column type pins the zone down:
/// `DataTypeDateTime::equals` ignores the time zone, `DataTypeDateTime64::equals` compares only the scale,
/// and the server time zone is not part of the table metadata at all. Without it, a part written under one
/// zone could be shifted by a delta proven under another one.
String getRowsTTLTimeZoneFingerprint(const TTLDescription & rows_ttl);

}
