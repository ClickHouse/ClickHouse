#pragma once

#include <Interpreters/UDT/StoredObjectTypeSupport.h>

#include <Core/Types.h>

#include <span>
#include <string_view>

namespace DB
{
class ASTFunction;
class IAST;
}

namespace DB::UDT
{

enum class StoredObjectTypeStringSlotMode : UInt8
{
    ExactASTLayout,
    ContextDependentLayout,
    AdapterOwnedLayout,
};

struct TableFunctionTypeStringSlotContract
{
    std::string_view function_name;
    StoredObjectOccurrenceSite occurrence_site{};
    StoredObjectTypeStringSlotMode mode{};
    bool case_insensitive = false;
};

enum class StoredExpressionTypeStringArgumentPosition : UInt8
{
    First,
    Second,
    Last,
};

struct StoredExpressionTypeStringSlotContract
{
    std::string_view function_name;
    StoredObjectOccurrenceSite occurrence_site{};
    StoredExpressionTypeStringArgumentPosition argument_position{};
    bool case_insensitive = false;
};

struct StorageEngineTypeStringSlotContract
{
    std::string_view engine_name;
    StoredObjectTypeStringSlotMode mode{};
};

struct StoredSettingTypeStringSlotContract
{
    std::string_view setting_name;
    StoredObjectOccurrenceSite occurrence_site{};
};

enum class StoredObjectTypeStringSlotStatus : UInt8
{
    Unregistered,
    NoExplicitSchemaString,
    ExactExpression,
    ContextRequired,
    UnclassifiedLayout,
};

/// Table-function ASTs do not encode whether an ASTFunction argument is a
/// scalar expression or another table function. Only these owner grammars may
/// promote one exact argument into a nested table-function context.
enum class StoredObjectNestedTableFunctionSlotStatus : UInt8
{
    UnregisteredOwner,
    NoNestedTableFunction,
    ExactTableFunction,
    UnclassifiedLayout,
};

struct StoredObjectNestedTableFunctionSlotClassification
{
    StoredObjectNestedTableFunctionSlotStatus status = StoredObjectNestedTableFunctionSlotStatus::UnregisteredOwner;
    const ASTFunction * function = nullptr;
    UInt64 argument_ordinal = 0;
};

/// Borrowed, allocation-free result. `expression` is set only when the exact
/// AST expression that may supply a schema string is known. ContextRequired
/// means that constant evaluation, named-collection lookup, or adapter parsing
/// is still necessary before the bytes may be treated as type grammar.
struct StoredObjectTypeStringSlotClassification
{
    StoredObjectTypeStringSlotStatus status = StoredObjectTypeStringSlotStatus::Unregistered;
    StoredObjectOccurrenceSite occurrence_site = StoredObjectOccurrenceSite::Unclassified;
    const IAST * expression = nullptr;
    UInt64 argument_ordinal = 0;

    [[nodiscard]] bool hasExactExpression() const noexcept
    {
        return expression
            && (status == StoredObjectTypeStringSlotStatus::ExactExpression || status == StoredObjectTypeStringSlotStatus::ContextRequired);
    }
};

enum class StoredObjectTableFunctionTypeStringTreeStatus : UInt8
{
    Complete,
    UnclassifiedLayout,
};

/// Closed, allocation-free classification of one table-function root and the
/// only nested-owner chain that its runtime implementation may instantiate.
/// `schema_owner` is the terminal function whose inventoried schema slot is
/// returned in `schema_slot`; registered nested owners do not themselves own a
/// schema-string slot. A malformed/cyclic/over-depth chain is never partially
/// classified.
struct StoredObjectTableFunctionTypeStringTreeClassification
{
    StoredObjectTableFunctionTypeStringTreeStatus status = StoredObjectTableFunctionTypeStringTreeStatus::Complete;
    const ASTFunction * schema_owner = nullptr;
    StoredObjectTypeStringSlotClassification schema_slot;
    UInt64 nested_depth = 0;
};

/// Closed inventory of table functions whose argument grammar contains, or may
/// obtain from an adapter, a column-schema string. Unknown functions are never
/// searched heuristically for qualified-looking literals.
[[nodiscard]] std::span<const TableFunctionTypeStringSlotContract> getTableFunctionTypeStringSlotContracts() noexcept;

/// Sub-inventory for table functions whose owned string is format schema
/// grammar rather than a generic table-function schema.
[[nodiscard]] std::span<const TableFunctionTypeStringSlotContract> getFormatSchemaStringSlotContracts() noexcept;

/// Classifies only the registered function's owned schema slot. The caller may
/// lexically screen a literal ExactExpression immediately; every other exact or
/// adapter-owned ContextRequired result must be resolved by a Context-bearing
/// owner before persistence admission.
[[nodiscard]] StoredObjectTypeStringSlotClassification classifyTableFunctionTypeStringSlot(const ASTFunction & function) noexcept;

/// Closed nested-owner grammar. `loop(f(...))` owns argument zero and
/// `viewIfPermitted(query, f(...))` owns argument one. No other ASTFunction
/// argument is treated as a table function by stored-object admission.
[[nodiscard]] StoredObjectNestedTableFunctionSlotClassification
classifyStoredObjectNestedTableFunctionSlot(const ASTFunction & owner) noexcept;

[[nodiscard]] StoredObjectTableFunctionTypeStringTreeClassification
classifyStoredObjectTableFunctionTypeStringTree(const ASTFunction & root) noexcept;

/// Explicit inventory of scalar functions whose constant argument is parsed as
/// ClickHouse type or column-schema grammar. The classifier identifies only
/// the registered owned slot; it never searches unrelated string arguments.
[[nodiscard]] std::span<const StoredExpressionTypeStringSlotContract> getStoredExpressionTypeStringSlotContracts() noexcept;
[[nodiscard]] StoredObjectTypeStringSlotClassification classifyStoredExpressionTypeStringSlot(const ASTFunction & function) noexcept;

/// Query-local settings can carry type grammar outside the AST child graph.
/// Only settings whose String value is passed to a ClickHouse type/column
/// parser belong here; schema identifiers, paths and format-schema files do
/// not. Session-level values require a later Context-bearing admission check.
[[nodiscard]] std::span<const StoredSettingTypeStringSlotContract> getStoredSettingTypeStringSlotContracts() noexcept;
[[nodiscard]] const StoredSettingTypeStringSlotContract * tryGetStoredSettingTypeStringSlotContract(std::string_view setting_name) noexcept;

/// No registered storage engine has an argument parsed with a context-bearing
/// ClickHouse type/column grammar. The explicit empty inventory
/// is intentional: path, URL, remote table, format-schema-file and data strings
/// must not be screened as types. An engine must provide a closed contract
/// and owner classifier before its logical-string admission can be enabled.
[[nodiscard]] std::span<const StorageEngineTypeStringSlotContract> getStorageEngineTypeStringSlotContracts() noexcept;
[[nodiscard]] StoredObjectTypeStringSlotClassification classifyStorageEngineTypeStringSlot(const ASTFunction & engine) noexcept;

}
