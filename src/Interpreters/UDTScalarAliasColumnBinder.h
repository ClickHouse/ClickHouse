#pragma once

#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>


namespace DB
{

class ASTCreateQuery;
class ASTColumnDeclaration;

namespace UDT
{

class IAuthorityAdapter;
class QueryResourceLedger;

struct UDTTypeExpressionResolutionStatistics
{
    UInt64 catalog_root_loads = 0;
    UInt64 catalog_name_lookups = 0;
};

/// Query/admission-local resolver for multiple explicit type expressions in
/// one database authority. Each synchronous resolution pins its exact root
/// only for the duration of that call; subsequent calls must observe the same
/// generation identity or fail closed. Name probes, USAGE checks, and exact
/// canonical template applications are deduplicated across calls without
/// retaining a root lease. Returned bound trees retain only independent
/// immutable definition handles.
class UDTTypeExpressionResolutionScope final
{
public:
    UDTTypeExpressionResolutionScope(String database_name, ContextPtr context, const IAuthorityAdapter & authority);
    UDTTypeExpressionResolutionScope(
        String database_name,
        ContextPtr context,
        const IAuthorityAdapter & authority,
        std::shared_ptr<QueryResourceLedger> query_resource_ledger);
    ~UDTTypeExpressionResolutionScope();

    UDTTypeExpressionResolutionScope(const UDTTypeExpressionResolutionScope &) = delete;
    UDTTypeExpressionResolutionScope & operator=(const UDTTypeExpressionResolutionScope &) = delete;
    UDTTypeExpressionResolutionScope(UDTTypeExpressionResolutionScope &&) noexcept;
    UDTTypeExpressionResolutionScope & operator=(UDTTypeExpressionResolutionScope &&) noexcept;

    [[nodiscard]] BoundDeclaredTypeResult resolve(const ASTPtr & declared_type);
    const UDTTypeExpressionResolutionStatistics & getStatistics() const noexcept;
    /// Complete query-effective minimum last admitted from the exact pinned
    /// authority generation. Empty until the first successful pin.
    std::optional<EffectiveResourceLimits> getEffectiveQueryResourceLimits() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

class ScalarAliasColumnBinderError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidInput,
        UnsupportedColumnShape,
        CrossDatabaseReference,
        UnknownDefinition,
        ParameterizedDefinition,
        AuthorityMismatch,
        QueryChanged,
        NormalizedSchemaMismatch,
        InvalidState,
    };

    ScalarAliasColumnBinderError(Code code_, std::string_view message);

    const Code code;
};

/// A strong, pre-normalization preparation for qualified UDT applications in
/// supported physical-column type trees. Every application belongs to the
/// owning Atomic database and carries canonical type/value arguments. No query
/// AST is changed until every lookup, stable-UUID-deduplicated USAGE check,
/// TypeResolver call, and physical replacement AST construction has succeeded.
class PreparedScalarAliasColumns final
{
public:
    PreparedScalarAliasColumns(const PreparedScalarAliasColumns &) = delete;
    PreparedScalarAliasColumns & operator=(const PreparedScalarAliasColumns &) = delete;
    PreparedScalarAliasColumns(PreparedScalarAliasColumns &&) noexcept;
    PreparedScalarAliasColumns & operator=(PreparedScalarAliasColumns &&) noexcept;
    ~PreparedScalarAliasColumns();

    /// Exact physical column list produced before ordinary schema
    /// normalization. `finish` accepts either this list or its exact ordinary
    /// flatten_nested expansion and rejects every other drift.
    const NamesAndTypesList & getExpectedPhysicalColumns() const noexcept;

    /// Replaces each reference-bearing declared type with its complete
    /// canonical physical type AST. All replacements were allocated by the
    /// CREATE or ALTER preparation entry point.
    void applyPhysicalTypeASTs();

    /// Closes the two-phase binder after ordinary schema normalization and
    /// composes the permanent Table sidecar input. This object is one-shot.
    [[nodiscard]] PreparedTableColumnTypeBindings
    finish(const SchemaObjectID & table, UInt64 object_schema_revision, const NamesAndTypesList & normalized_physical_columns) &&;

    /// Closes one aggregate ALTER preparation as column-local sidecar
    /// fragments aligned with its declarations. Resolution and USAGE
    /// authorization still happen once for the complete ALTER.
    [[nodiscard]] std::vector<std::optional<PersistedTypeReferences>>
    finishIndividualColumns(const SchemaObjectID & table, UInt64 object_schema_revision) &&;

private:
    struct Impl;

    explicit PreparedScalarAliasColumns(std::unique_ptr<Impl> impl_);

    friend std::optional<PreparedScalarAliasColumns>
    prepareScalarAliasColumns(ASTCreateQuery &, std::string_view, const ContextPtr &, const IAuthorityAdapter &);

    friend std::optional<PreparedScalarAliasColumns>
    prepareScalarAliasAlterColumn(ASTColumnDeclaration &, std::string_view, const ContextPtr &, const IAuthorityAdapter &);

    friend std::optional<PreparedScalarAliasColumns> prepareScalarAliasAlterColumns(
        std::span<ASTColumnDeclaration * const>, std::string_view, const ContextPtr &, const IAuthorityAdapter &);

    static PreparedScalarAliasColumns prepareActivated(
        ASTCreateQuery * create_source,
        std::span<ASTColumnDeclaration * const> declarations,
        std::string_view database_name,
        const ContextPtr & context,
        const IAuthorityAdapter & authority);

    std::unique_ptr<Impl> impl;
};

/// Fast routing predicate for CREATE's explicit column-type ASTs. A false
/// result guarantees the binder entry point returns without touching Context
/// access state or the authority adapter.
[[nodiscard]] bool hasReferencesInCreateTableColumns(const ASTCreateQuery & create);

/// Allocation-light routing predicate used before ON CLUSTER and replicated
/// database dispatch. It only walks the declared type AST.
[[nodiscard]] bool hasReferencesInAlterColumn(const ASTColumnDeclaration & declaration);

/// Resolves one explicit type expression through the same pinned authority,
/// canonical argument validation, batched USAGE boundary, and TypeResolver
/// path used by durable table columns. The input is not modified. Every UDT
/// occurrence must belong to `database_name`; callers choose the authority
/// before entering this function and retain the returned logical tree only
/// for the lifetime of their analysis/admission scope.
[[nodiscard]] BoundDeclaredTypeResult resolveUDTTypeExpression(
    const ASTPtr & declared_type,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    UDTTypeExpressionResolutionStatistics * statistics = nullptr);

/// Returns nullopt for the ordinary physical-only CREATE path without checking
/// any UDT-only precondition. Once activated, this performs one snapshot name-
/// lookup session, one batched stable-UUID-deduplicated USAGE check, then
/// resolves every explicit physical column and prebuilds all complete physical
/// replacement ASTs.
[[nodiscard]] std::optional<PreparedScalarAliasColumns> prepareScalarAliasColumns(
    ASTCreateQuery & create, std::string_view database_name, const ContextPtr & context, const IAuthorityAdapter & authority);

/// Returns nullopt for an ADD/MODIFY declaration whose type is physical-only.
/// Once activated, this uses exactly the same validation, resolution, USAGE
/// boundary, and two-phase physical-AST replacement as the CREATE binder.
[[nodiscard]] std::optional<PreparedScalarAliasColumns> prepareScalarAliasAlterColumn(
    ASTColumnDeclaration & declaration, std::string_view database_name, const ContextPtr & context, const IAuthorityAdapter & authority);

/// Aggregate ALTER entry point. All reference-bearing ADD/MODIFY declarations
/// share one authority snapshot and one stable-UUID-deduplicated USAGE batch.
/// Returned column fragments remain aligned with `declarations`.
[[nodiscard]] std::optional<PreparedScalarAliasColumns> prepareScalarAliasAlterColumns(
    std::span<ASTColumnDeclaration * const> declarations,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority);

}
}
