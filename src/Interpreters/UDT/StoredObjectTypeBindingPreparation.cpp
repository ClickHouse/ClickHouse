#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>

#include <Interpreters/UDT/StoredObjectTableFunctionSources.h>
#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>
#include <Interpreters/UDTScalarAliasColumnBinder.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>
#include <DataTypes/dataTypeToAST.h>

#include <Core/UUID.h>

#include <Databases/DatabaseAtomic.h>

#include <Interpreters/DatabaseCatalog.h>

#include <Parsers/ASTCastTarget.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <IO/WriteBufferFromString.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = StoredObjectTypeBindingPreparationError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

bool containsStructuredUDTReference(const ASTPtr & root)
{
    if (!root)
        return false;
    std::vector<const IAST *> pending{root.get()};
    while (!pending.empty())
    {
        const auto * node = pending.back();
        pending.pop_back();
        if (node->as<ASTUDTReference>())
            return true;
        for (const auto & child : node->children)
            if (child)
                pending.push_back(child.get());
    }
    return false;
}

BoundDeclaredTypeResult
resolveDeclaration(const ASTPtr & declared_type, UDTTypeExpressionResolutionScope & resolver, bool & contains_logical_reference)
{
    contains_logical_reference = containsStructuredUDTReference(declared_type);
    if (contains_logical_reference)
        return resolver.resolve(declared_type);
    return BoundDeclaredTypeResult::physicalOnly(DataTypeFactory::instance().get(declared_type));
}

[[noreturn]] void rethrowViewBindingError(const ViewOutputTypeBindingError & error)
{
    using Code = ViewOutputTypeBindingError::Code;
    switch (error.code)
    {
        case Code::CrossDatabaseReference: fail(Error::Code::CrossDatabaseReference, error.what());
        case Code::LimitExceeded: fail(Error::Code::LimitExceeded, error.what());
        case Code::InvalidObject:
        case Code::InvalidOutput: fail(Error::Code::InvalidDeclaration, error.what());
        case Code::InvalidConfiguration:
        case Code::ConflictingDescriptor:
        case Code::SidecarMismatch:
        case Code::PhysicalSchemaMismatch:
        case Code::PathMismatch: fail(Error::Code::MissingLogicalBinding, error.what());
    }
    fail(Error::Code::MissingLogicalBinding, error.what());
}

[[noreturn]] void rethrowDictionaryBindingError(const DictionaryAttributeTypeBindingError & error)
{
    using Code = DictionaryAttributeTypeBindingError::Code;
    switch (error.code)
    {
        case Code::CrossDatabaseReference: fail(Error::Code::CrossDatabaseReference, error.what());
        case Code::LimitExceeded: fail(Error::Code::LimitExceeded, error.what());
        case Code::InvalidObject:
        case Code::InvalidAttribute: fail(Error::Code::InvalidDeclaration, error.what());
        case Code::InvalidConfiguration:
        case Code::ConflictingDescriptor:
        case Code::SidecarMismatch:
        case Code::PhysicalSchemaMismatch:
        case Code::PathMismatch: fail(Error::Code::MissingLogicalBinding, error.what());
    }
    fail(Error::Code::MissingLogicalBinding, error.what());
}

}

StoredObjectTypeBindingPreparationError::StoredObjectTypeBindingPreparationError(Code code_, std::string_view message)
    : std::runtime_error(String("Stored-object UDT binding preparation failed: ") + String(message))
    , code(code_)
{
}

namespace
{

constexpr size_t maximum_auxiliary_ast_nodes = 1U << 20;
constexpr size_t maximum_auxiliary_ast_depth = 256;
constexpr size_t maximum_auxiliary_string_bytes = 16U << 20;
constexpr size_t maximum_auxiliary_parser_backtracks = 1'000'000;

struct AuxiliaryCastTargetReplacement
{
    ASTExpressionList * arguments = nullptr;
    const ASTCastTarget * original_target = nullptr;
    ASTPtr physical_target;
};

struct AuxiliaryStringReplacement
{
    ASTLiteral * literal = nullptr;
    String original_value;
    String physical_value;
};

struct AuxiliarySettingReplacement
{
    ASTSetQuery * settings = nullptr;
    size_t change_ordinal = 0;
    String setting_name;
    String original_value;
    String physical_value;
};

struct PreparedViewAuxiliaryEndpoints
{
    std::vector<ViewAuxiliaryTypeBindingInput> endpoints;
    std::vector<AuxiliaryCastTargetReplacement> cast_replacements;
    std::vector<AuxiliaryStringReplacement> string_replacements;
    std::vector<AuxiliarySettingReplacement> setting_replacements;
};

void validateAuxiliaryASTLimits(const ASTPtr & root, std::string_view description)
{
    if (!root)
        fail(Error::Code::InvalidDeclaration, "an auxiliary type expression has an empty AST");

    size_t visited_nodes = 0;
    std::vector<std::pair<const IAST *, size_t>> pending{{root.get(), 0}};
    while (!pending.empty())
    {
        const auto [node, depth] = pending.back();
        pending.pop_back();
        if (!node)
            fail(Error::Code::InvalidDeclaration, "an auxiliary type expression contains a null AST node");
        if (depth > maximum_auxiliary_ast_depth || visited_nodes == maximum_auxiliary_ast_nodes)
            fail(Error::Code::LimitExceeded, description);
        ++visited_nodes;
        for (const auto & child : node->children)
            pending.emplace_back(child.get(), depth + 1);
    }
}

ASTPtr parseAuxiliaryTypeString(std::string_view value)
{
    if (value.empty() || value.size() > maximum_auxiliary_string_bytes)
        fail(Error::Code::LimitExceeded, "a stored CAST type string exceeds its byte limit");
    ASTPtr parsed;
    try
    {
        ParserDataTypeWithQualifiedReferences parser;
        parsed = parseQuery(
            parser,
            value.data(),
            value.data() + value.size(),
            "stored CAST type string",
            maximum_auxiliary_string_bytes,
            maximum_auxiliary_ast_depth,
            maximum_auxiliary_parser_backtracks);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidDeclaration, "a stored CAST type string is not one complete type expression");
    }
    validateAuxiliaryASTLimits(parsed, "a stored CAST type string exceeds its auxiliary AST limit");
    return parsed;
}

ASTPtr parseAuxiliarySchemaString(std::string_view value)
{
    if (value.empty() || value.size() > maximum_auxiliary_string_bytes)
        fail(Error::Code::LimitExceeded, "a stored schema string exceeds its byte limit");
    ASTPtr parsed;
    try
    {
        ParserColumnDeclarationList parser(true, true);
        parsed = parseQuery(
            parser,
            value.data(),
            value.data() + value.size(),
            "stored table-function schema string",
            maximum_auxiliary_string_bytes,
            maximum_auxiliary_ast_depth,
            maximum_auxiliary_parser_backtracks);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & exception)
    {
        if (isUDTResourceOrControlExceptionCode(exception.code()))
            throw;
        fail(Error::Code::InvalidDeclaration, "a stored schema string is not one complete column declaration list");
    }
    validateAuxiliaryASTLimits(parsed, "a stored schema string exceeds its auxiliary AST limit");
    return parsed;
}

String formatAuxiliarySchemaString(const ASTPtr & schema)
{
    WriteBufferFromOwnString out;
    schema->format(out, IAST::FormatSettings(/*one_line=*/true));
    return out.str();
}

void validateAuxiliarySchemaColumnDeclaration(const ASTColumnDeclaration & declaration)
{
    const auto type = declaration.getType();
    const auto & declaration_ast = static_cast<const IAST &>(declaration);
    if (declaration.name.empty() || !type || declaration_ast.children.size() != 1 || declaration_ast.children.front().get() != type.get())
    {
        fail(Error::Code::InvalidDeclaration, "a stored schema string contains a malformed column declaration");
    }

    /// The approved schema-string occurrence is exactly `name type`.  The
    /// parser deliberately accepts the complete CREATE-column grammar, but
    /// defaults and the remaining modifiers are independent persisted
    /// expression/type contexts.  Treating only getType() as authoritative
    /// would let a logical CAST or another qualified type string survive in
    /// an unbound modifier while a sibling type creates a valid sidecar.
    if (declaration.default_specifier != ColumnDefaultSpecifier::Empty || declaration.ephemeral_default || declaration.primary_key_specifier
        || declaration.null_modifier.has_value() || declaration.getDefaultExpression() || declaration.getComment() || declaration.getCodec()
        || declaration.getStatisticsDesc() || declaration.getTTL() || declaration.getCollation() || declaration.getSettings())
    {
        fail(
            Error::Code::InvalidDeclaration,
            "stored schema-string columns support only an explicit name and type; column modifiers are unsupported");
    }
}

std::optional<String> getAuxiliaryTypeAuthorityDatabase(const ASTPtr & type)
{
    std::optional<String> database_name;
    std::vector<const IAST *> pending{type.get()};
    while (!pending.empty())
    {
        const auto * node = pending.back();
        pending.pop_back();
        if (const auto * reference = node ? node->as<ASTUDTReference>() : nullptr)
        {
            if (reference->database_name.empty())
                fail(Error::Code::InvalidDeclaration, "a schema-string UDT reference has no owning database");
            if (database_name && *database_name != reference->database_name)
            {
                fail(
                    Error::Code::CrossDatabaseReference,
                    "one schema-string column type cannot span multiple user-defined type authorities");
            }
            database_name = reference->database_name;
        }
        if (node)
            for (const auto & child : node->children)
                if (child)
                    pending.push_back(child.get());
    }
    return database_name;
}

class ViewAuxiliaryEndpointWalker final
{
public:
    enum class Mode : UInt8
    {
        All,
        SchemaStringsOnly,
        StoredExpressionsOnly,
    };

    explicit ViewAuxiliaryEndpointWalker(UDTTypeExpressionResolutionScope * resolver_, Mode mode_ = Mode::All)
        : resolver(resolver_)
        , mode(mode_)
    {
    }

    explicit ViewAuxiliaryEndpointWalker(std::span<const ViewAuxiliaryTypePresentation> presentations_)
        : render_presentations(true)
    {
        for (const auto & presentation : presentations_)
        {
            const auto key = std::pair{presentation.site, presentation.object_ordinal};
            const String expected_key
                = (presentation.site == PersistedTypeOccurrenceSite::StoredExpression ? "stored-expression:" : "schema-string:")
                + std::to_string(presentation.object_ordinal);
            if ((presentation.site != PersistedTypeOccurrenceSite::StoredExpression
                 && presentation.site != PersistedTypeOccurrenceSite::SchemaString)
                || presentation.runtime_owner_key != expected_key || !presentation.physical_type || !presentation.declared_type
                || !presentations.emplace(key, std::addressof(presentation)).second)
                fail(Error::Code::InvalidDeclaration, "a View auxiliary introspection presentation is invalid or duplicated");
        }
    }

    void walk(const IAST * root)
    {
        validateTableFunctionOwnerGrammar(root, 0);
        validation_visited.clear();
        visit(root, 0);
    }

    PreparedViewAuxiliaryEndpoints releasePrepared()
    {
        if (!resolver)
            fail(Error::Code::InvalidState, "a physical auxiliary walker has no prepared logical endpoints");
        return std::move(prepared);
    }

    std::vector<ViewAuxiliaryPhysicalTypeBindingInput> releasePhysical()
    {
        if (resolver)
            fail(Error::Code::InvalidState, "a logical auxiliary walker cannot release physical replay endpoints");
        return std::move(physical_endpoints);
    }

    PreparedViewAuxiliaryEndpoints releasePresentations()
    {
        if (!render_presentations || consumed_presentations.size() != presentations.size())
            fail(Error::Code::QueryChanged, "the fetched View CREATE AST omits an exact auxiliary presentation endpoint");
        return std::move(prepared);
    }

private:
    struct AddedEndpoint
    {
        DataTypePtr physical_type;
        bool has_logical_references = false;
        ASTPtr presentation_type;
    };

    AddedEndpoint addEndpoint(PersistedTypeOccurrenceSite site, const ASTPtr & type)
    {
        if (!type)
            fail(Error::Code::InvalidDeclaration, "a stored auxiliary type endpoint is empty");
        UInt64 & next_ordinal
            = site == PersistedTypeOccurrenceSite::StoredExpression ? next_stored_expression_ordinal : next_schema_string_ordinal;
        if (next_ordinal == std::numeric_limits<UInt64>::max())
            fail(Error::Code::LimitExceeded, "stored auxiliary endpoint ordinals are exhausted");
        const UInt64 ordinal = next_ordinal++;
        const String owner_key
            = (site == PersistedTypeOccurrenceSite::StoredExpression ? "stored-expression:" : "schema-string:") + std::to_string(ordinal);

        if (resolver)
        {
            bool has_logical_references = false;
            auto resolved = resolveDeclaration(type, *resolver, has_logical_references);
            auto physical_type = resolved.getPhysicalType();
            prepared.endpoints.push_back({
                .site = site,
                .object_ordinal = ordinal,
                .runtime_owner_key = owner_key,
                .endpoint_type = std::move(resolved),
            });
            return {
                .physical_type = std::move(physical_type),
                .has_logical_references = has_logical_references,
                .presentation_type = {},
            };
        }

        if (containsStructuredUDTReference(type))
            fail(Error::Code::InvalidDeclaration, "physical stored-object metadata still contains a structured logical type endpoint");
        DataTypePtr physical_type;
        try
        {
            physical_type = DataTypeFactory::instance().get(type);
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & exception)
        {
            if (isUDTResourceOrControlExceptionCode(exception.code()))
                throw;
            fail(Error::Code::InvalidDeclaration, "a stored auxiliary endpoint is not a physical ClickHouse type");
        }
        if (render_presentations)
        {
            const auto key = std::pair{site, ordinal};
            const auto found = presentations.find(key);
            if (found == presentations.end() || found->second->runtime_owner_key != owner_key
                || !found->second->physical_type->equals(*physical_type)
                || found->second->physical_type->getName() != physical_type->getName() || !consumed_presentations.emplace(key).second)
                fail(Error::Code::QueryChanged, "the fetched View CREATE AST differs from an auxiliary presentation endpoint");
            return {
                .physical_type = std::move(physical_type),
                .has_logical_references = found->second->has_logical_references,
                .presentation_type = found->second->declared_type->clone(),
            };
        }

        physical_endpoints.push_back({
            .site = site,
            .object_ordinal = ordinal,
            .runtime_owner_key = owner_key,
            .physical_type = physical_type,
        });
        return {
            .physical_type = std::move(physical_type),
            .has_logical_references = false,
            .presentation_type = {},
        };
    }

    const ASTLiteral & requireStringLiteral(const StoredObjectTypeStringSlotClassification & slot) const
    {
        const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
        if (!literal || literal->value.getType() != Field::Types::String)
            fail(Error::Code::InvalidDeclaration, "an approved stored type-string slot is not a literal String");
        return *literal;
    }

    void handleCastType(const ASTFunction & function, const ASTPtr & type, const ASTCastTarget * structured_target)
    {
        const UInt64 ordinal = next_stored_expression_ordinal;
        auto endpoint = addEndpoint(PersistedTypeOccurrenceSite::StoredExpression, type);
        const_cast<ASTFunction &>(function).setUDTStoredExpressionOrdinal(ordinal);
        if ((!resolver && !render_presentations) || !endpoint.has_logical_references)
            return;

        if (structured_target)
        {
            if (!function.arguments || function.arguments->children.size() != 2
                || function.arguments->children[1].get() != structured_target)
                fail(Error::Code::InvalidDeclaration, "a structured CAST target is not owned by its argument list");
            prepared.cast_replacements.push_back({
                .arguments = const_cast<ASTExpressionList *>(function.arguments->as<ASTExpressionList>()),
                .original_target = structured_target,
                .physical_target = make_intrusive<ASTCastTarget>(
                    render_presentations ? std::move(endpoint.presentation_type) : dataTypeToAST(endpoint.physical_type)),
            });
        }
    }

    void handleLegacyCastString(const ASTFunction & function, const StoredObjectTypeStringSlotClassification & slot)
    {
        if (!equalsCaseInsensitive(function.name, "CAST"))
            return;
        const auto & literal = requireStringLiteral(slot);
        const String & original = literal.value.safeGet<String>();
        auto parsed_type = parseAuxiliaryTypeString(original);
        const UInt64 ordinal = next_stored_expression_ordinal;
        auto endpoint = addEndpoint(PersistedTypeOccurrenceSite::StoredExpression, parsed_type);
        const_cast<ASTFunction &>(function).setUDTStoredExpressionOrdinal(ordinal);
        if ((resolver || render_presentations) && endpoint.has_logical_references)
        {
            prepared.string_replacements.push_back({
                .literal = const_cast<ASTLiteral *>(&literal),
                .original_value = original,
                .physical_value
                = render_presentations ? endpoint.presentation_type->formatWithSecretsOneLine() : endpoint.physical_type->getName(),
            });
        }
    }

    void handleSchemaString(const StoredObjectTypeStringSlotClassification & slot)
    {
        if (slot.status == StoredObjectTypeStringSlotStatus::Unregistered
            || slot.status == StoredObjectTypeStringSlotStatus::NoExplicitSchemaString)
            return;
        if (slot.status != StoredObjectTypeStringSlotStatus::ExactExpression)
            fail(Error::Code::InvalidDeclaration, "a stored schema-string slot requires context-owned or unclassified parsing");
        if (slot.occurrence_site != StoredObjectOccurrenceSite::TableFunctionSchemaString
            && slot.occurrence_site != StoredObjectOccurrenceSite::FormatSchemaString)
            fail(Error::Code::InvalidDeclaration, "a stored schema-string slot has an unsupported occurrence owner");

        const auto & literal = requireStringLiteral(slot);
        const String & original = literal.value.safeGet<String>();
        auto schema = parseAuxiliarySchemaString(original);
        auto * columns = schema ? schema->as<ASTExpressionList>() : nullptr;
        if (!columns || columns->children.empty())
            fail(Error::Code::InvalidDeclaration, "a stored schema string has no column declarations");

        bool has_logical_type = false;
        for (auto & child : columns->children)
        {
            auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration)
                fail(Error::Code::InvalidDeclaration, "a stored schema string contains a malformed column declaration");
            validateAuxiliarySchemaColumnDeclaration(*declaration);
            auto endpoint = addEndpoint(PersistedTypeOccurrenceSite::SchemaString, declaration->getType());
            has_logical_type = has_logical_type || endpoint.has_logical_references;
            declaration->setType(
                render_presentations && endpoint.has_logical_references ? std::move(endpoint.presentation_type)
                                                                        : dataTypeToAST(endpoint.physical_type));
        }

        if ((resolver || render_presentations) && has_logical_type)
        {
            prepared.string_replacements.push_back({
                .literal = const_cast<ASTLiteral *>(&literal),
                .original_value = original,
                .physical_value = formatAuxiliarySchemaString(schema),
            });
        }
    }

    void handleStoredSettings(ASTSetQuery & settings)
    {
        if (mode == Mode::StoredExpressionsOnly)
            return;
        for (size_t change_ordinal = 0; change_ordinal < settings.changes.size(); ++change_ordinal)
        {
            auto & change = settings.changes[change_ordinal];
            const auto * contract = tryGetStoredSettingTypeStringSlotContract(change.name);
            if (!contract)
                continue;
            if (contract->occurrence_site != StoredObjectOccurrenceSite::FormatSchemaString)
                fail(Error::Code::InvalidDeclaration, "a stored setting type-string slot has an unsupported occurrence owner");
            if (change.value.getType() != Field::Types::String)
                fail(Error::Code::InvalidDeclaration, "an approved stored setting type-string slot is not a literal String");

            const String & original = change.value.safeGet<String>();
            /// The empty default carries no schema and is a physical-only
            /// endpoint absence, matching FormatSettings' ordinary behavior.
            if (original.empty())
                continue;
            auto schema = parseAuxiliarySchemaString(original);
            auto * columns = schema ? schema->as<ASTExpressionList>() : nullptr;
            if (!columns || columns->children.empty())
                fail(Error::Code::InvalidDeclaration, "a stored setting schema string has no column declarations");

            bool has_logical_type = false;
            for (auto & child : columns->children)
            {
                auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
                if (!declaration)
                    fail(Error::Code::InvalidDeclaration, "a stored setting schema string contains a malformed column declaration");
                validateAuxiliarySchemaColumnDeclaration(*declaration);
                auto endpoint = addEndpoint(PersistedTypeOccurrenceSite::SchemaString, declaration->getType());
                has_logical_type = has_logical_type || endpoint.has_logical_references;
                declaration->setType(
                    render_presentations && endpoint.has_logical_references ? std::move(endpoint.presentation_type)
                                                                            : dataTypeToAST(endpoint.physical_type));
            }

            if ((resolver || render_presentations) && has_logical_type)
            {
                prepared.setting_replacements.push_back({
                    .settings = &settings,
                    .change_ordinal = change_ordinal,
                    .setting_name = change.name,
                    .original_value = original,
                    .physical_value = formatAuxiliarySchemaString(schema),
                });
            }
        }
    }

    void handleFunction(const ASTFunction & function)
    {
        if (const auto * target = function.tryGetStructuredCastTarget())
        {
            if (mode != Mode::SchemaStringsOnly)
                handleCastType(function, target->getType(), target);
            return;
        }

        const auto slot = classifyStoredExpressionTypeStringSlot(function);
        if (slot.status == StoredObjectTypeStringSlotStatus::Unregistered
            || slot.status == StoredObjectTypeStringSlotStatus::NoExplicitSchemaString)
            return;
        if (slot.occurrence_site == StoredObjectOccurrenceSite::FormatSchemaString)
        {
            if (mode != Mode::StoredExpressionsOnly)
                handleSchemaString(slot);
            return;
        }
        if (equalsCaseInsensitive(function.name, "CAST"))
        {
            if (mode == Mode::SchemaStringsOnly)
                return;
            if (slot.status != StoredObjectTypeStringSlotStatus::ExactExpression)
                fail(Error::Code::InvalidDeclaration, "public CAST has no exact owned type-string argument");
            handleLegacyCastString(function, slot);
        }
    }

    void validateTableFunctionOwnerGrammar(const IAST * node, size_t depth)
    {
        if (!node)
            return;
        if (depth > maximum_auxiliary_ast_depth || validation_visited.size() >= maximum_auxiliary_ast_nodes)
            fail(Error::Code::LimitExceeded, "stored table-function owner AST exceeds its traversal limit");
        if (!validation_visited.insert(node).second)
            fail(Error::Code::InvalidDeclaration, "stored table-function owner AST is shared or cyclic");

        if (const auto * table_expression = node->as<ASTTableExpression>(); table_expression && table_expression->table_function)
        {
            const auto direct_count = std::count_if(
                table_expression->children.begin(),
                table_expression->children.end(),
                [&](const ASTPtr & child) { return child.get() == table_expression->table_function.get(); });
            const auto * root_function = table_expression->table_function->as<ASTFunction>();
            if (direct_count != 1 || !root_function)
                fail(Error::Code::InvalidDeclaration, "a stored table function is not owned by one exact table expression");

            const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*root_function);
            if (tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete || !tree.schema_owner
                || (tree.nested_depth != 0
                    && classifyStoredObjectTableFunctionSource(tree.schema_owner->name)
                        == StoredObjectTableFunctionSourceProvenance::Unclassified))
            {
                fail(Error::Code::InvalidDeclaration, "a stored nested table-function owner has unclassified grammar");
            }
            if (tree.schema_slot.status == StoredObjectTypeStringSlotStatus::ContextRequired
                || tree.schema_slot.status == StoredObjectTypeStringSlotStatus::UnclassifiedLayout)
            {
                fail(Error::Code::InvalidDeclaration, "a stored table-function schema depends on mutable or unclassified owner context");
            }
        }

        for (const auto & child : node->children)
            validateTableFunctionOwnerGrammar(child.get(), depth + 1);
    }

    void visit(const IAST * node, size_t depth)
    {
        if (!node)
            return;
        if (depth > maximum_auxiliary_ast_depth || visited.size() >= maximum_auxiliary_ast_nodes)
            fail(Error::Code::LimitExceeded, "stored auxiliary endpoint AST exceeds its traversal limit");
        if (!visited.insert(node).second)
            fail(Error::Code::InvalidDeclaration, "stored auxiliary endpoint AST is shared or cyclic");

        if (const auto * settings = node->as<ASTSetQuery>())
            handleStoredSettings(*const_cast<ASTSetQuery *>(settings));

        if (const auto * table_expression = node->as<ASTTableExpression>(); table_expression && table_expression->table_function)
        {
            const auto direct_count = std::count_if(
                table_expression->children.begin(),
                table_expression->children.end(),
                [&](const ASTPtr & child) { return child.get() == table_expression->table_function.get(); });
            if (direct_count != 1)
                fail(Error::Code::InvalidDeclaration, "a stored table function is not owned by its table expression");
            const auto * root_function = table_expression->table_function->as<ASTFunction>();
            if (!root_function)
                fail(Error::Code::InvalidDeclaration, "a stored table-function expression is not an ASTFunction");
            const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*root_function);
            if (tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete || !tree.schema_owner)
                fail(Error::Code::InvalidDeclaration, "a stored nested table-function owner changed after validation");
            if (mode != Mode::StoredExpressionsOnly)
                handleSchemaString(tree.schema_slot);
        }

        if (const auto * function = node->as<ASTFunction>())
            handleFunction(*function);

        for (const auto & child : node->children)
            visit(child.get(), depth + 1);
    }

    UDTTypeExpressionResolutionScope * resolver = nullptr;
    Mode mode = Mode::All;
    bool render_presentations = false;
    UInt64 next_stored_expression_ordinal = 0;
    UInt64 next_schema_string_ordinal = 0;
    std::unordered_set<const IAST *> visited;
    std::unordered_set<const IAST *> validation_visited;
    std::map<std::pair<PersistedTypeOccurrenceSite, UInt64>, const ViewAuxiliaryTypePresentation *> presentations;
    std::set<std::pair<PersistedTypeOccurrenceSite, UInt64>> consumed_presentations;
    PreparedViewAuxiliaryEndpoints prepared;
    std::vector<ViewAuxiliaryPhysicalTypeBindingInput> physical_endpoints;
};

struct RuntimeStoredExpressionPhysicalization
{
    ASTFunction * function = nullptr;
    ASTExpressionList * arguments = nullptr;
    const IAST * original_target = nullptr;
    UInt64 ordinal = 0;
    ASTPtr structured_physical_type;
    String legacy_physical_type;
    String canonical_physical_type;
};

class RuntimeStoredExpressionPhysicalizationWalker final
{
public:
    void walk(const ASTPtr & root)
    {
        if (!root)
            fail(Error::Code::InvalidObject, "runtime View physicalization requires an exact stored SELECT AST");
        visit(root.get(), 0);
        preparePhysicalTypes();
    }

    void apply()
    {
        for (auto & replacement : replacements)
        {
            if (!replacement.function || !replacement.arguments || !replacement.original_target
                || !replacement.function->hasUDTStoredExpressionOrdinal()
                || replacement.function->getUDTStoredExpressionOrdinal() != replacement.ordinal
                || replacement.function->arguments.get() != replacement.arguments || replacement.arguments->children.size() != 2
                || replacement.arguments->children[1].get() != replacement.original_target)
            {
                fail(Error::Code::QueryChanged, "a runtime View CAST changed after physicalization validation");
            }

            replacement.arguments->children[1] = make_intrusive<ASTLiteral>(std::move(replacement.canonical_physical_type));
            replacement.function->clearUDTStoredExpressionOrdinal();
        }
    }

private:
    void collectTaggedCast(ASTFunction & function)
    {
        if (!function.hasUDTStoredExpressionOrdinal())
            return;

        const UInt64 ordinal = function.getUDTStoredExpressionOrdinal();
        if (ordinal != replacements.size())
            fail(Error::Code::QueryChanged, "runtime View stored-expression CAST ordinals are not contiguous in owner order");

        auto * arguments = function.arguments ? function.arguments->as<ASTExpressionList>() : nullptr;
        if (!arguments || function.parameters || function.getKind() != ASTFunction::Kind::ORDINARY_FUNCTION
            || arguments->children.size() != 2
            || std::count_if(
                   function.children.begin(), function.children.end(), [&](const ASTPtr & child) { return child.get() == arguments; })
                != 1)
        {
            fail(Error::Code::InvalidDeclaration, "a runtime View stored-expression ordinal is not owned by one exact CAST");
        }

        const IAST * original_target = nullptr;
        ASTPtr structured_physical_type;
        String legacy_physical_type;
        if (const auto * structured_target = function.tryGetStructuredCastTarget())
        {
            if (arguments->children[1].get() != structured_target || !structured_target->getType())
                fail(Error::Code::InvalidDeclaration, "a runtime View structured CAST target has invalid ownership");
            original_target = structured_target;
            structured_physical_type = structured_target->getType();
        }
        else
        {
            const auto slot = classifyStoredExpressionTypeStringSlot(function);
            const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
            if (!equalsCaseInsensitive(function.name, "CAST") || slot.status != StoredObjectTypeStringSlotStatus::ExactExpression
                || slot.occurrence_site != StoredObjectOccurrenceSite::UnclassifiedTypeString
                || slot.expression != arguments->children[1].get() || !literal || literal->value.getType() != Field::Types::String)
            {
                fail(Error::Code::InvalidDeclaration, "a runtime View stored-expression ordinal has no exact physical CAST target");
            }
            original_target = literal;
            legacy_physical_type = literal->value.safeGet<String>();
        }

        replacements.push_back({
            .function = &function,
            .arguments = arguments,
            .original_target = original_target,
            .ordinal = ordinal,
            .structured_physical_type = std::move(structured_physical_type),
            .legacy_physical_type = std::move(legacy_physical_type),
            .canonical_physical_type = {},
        });
    }

    void preparePhysicalTypes()
    {
        for (auto & replacement : replacements)
        {
            ASTPtr physical_type_ast = replacement.structured_physical_type;
            if (!physical_type_ast)
                physical_type_ast = parseAuxiliaryTypeString(replacement.legacy_physical_type);
            if (!physical_type_ast || containsStructuredUDTReference(physical_type_ast))
                fail(Error::Code::InvalidDeclaration, "a runtime View annotated CAST target is not fully physical");

            try
            {
                replacement.canonical_physical_type = DataTypeFactory::instance().get(physical_type_ast)->getName();
            }
            catch (const std::bad_alloc &)
            {
                throw;
            }
            catch (const Exception & exception)
            {
                if (isUDTResourceOrControlExceptionCode(exception.code()))
                    throw;
                fail(Error::Code::InvalidDeclaration, "a runtime View annotated CAST target is not a physical ClickHouse type");
            }
        }
    }

    void visit(IAST * node, size_t depth)
    {
        if (!node)
            return;
        if (depth > maximum_auxiliary_ast_depth || visited.size() >= maximum_auxiliary_ast_nodes)
            fail(Error::Code::LimitExceeded, "runtime View SELECT exceeds its physicalization traversal limit");
        if (!visited.insert(node).second)
            fail(Error::Code::InvalidDeclaration, "runtime View SELECT physicalization found a shared or cyclic AST");

        if (auto * function = node->as<ASTFunction>())
            collectTaggedCast(*function);
        for (const auto & child : node->children)
            visit(child.get(), depth + 1);
    }

    std::unordered_set<IAST *> visited;
    std::vector<RuntimeStoredExpressionPhysicalization> replacements;
};

}

void physicalizeInferredTableFunctionSchema(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    const StoredObjectCreatePreparationDecision & decision,
    const ContextPtr & context)
{
    constexpr auto allowed_schema_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    if (!context || decision.route != StoredObjectCreatePreparationRoute::PhysicalizeTableFunctionSchema
        || classification.object_kind != StoredObjectKind::Table || classification.source_mode != StoredObjectSourceMode::AsTableFunction
        || classification.has_explicit_destination_columns
        || classification.source_table_function_provenance != StoredObjectTableFunctionSourceProvenance::PhysicalInference
        || classification.qualified_type_reference_candidate_sites == 0
        || (classification.qualified_type_reference_candidate_sites & ~allowed_schema_sites) != 0
        || classification.structured_udt_occurrence_sites != 0 || classification.has_unclassified_udt_reference
        || !classification.structured_udt_scan_complete || !classification.type_string_scan_complete)
    {
        fail(Error::Code::InvalidDecision, "the CREATE classification does not authorize inferred schema physicalization");
    }

    auto * root_function = create.as_table_function ? create.as_table_function->as<ASTFunction>() : nullptr;
    if (!root_function
        || std::count_if(
               create.children.begin(),
               create.children.end(),
               [&](const ASTPtr & child) { return child.get() == create.as_table_function; })
            != 1)
    {
        fail(Error::Code::InvalidDeclaration, "an inferred table-function schema has invalid AST ownership");
    }

    const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*root_function);
    auto * function = const_cast<ASTFunction *>(tree.schema_owner);
    const auto & slot = tree.schema_slot;
    auto * literal = slot.expression ? const_cast<IAST *>(slot.expression)->as<ASTLiteral>() : nullptr;
    if (tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete || !function
        || function->getKind() != ASTFunction::Kind::ORDINARY_FUNCTION || function->parameters || !function->arguments
        || std::count_if(
               function->children.begin(),
               function->children.end(),
               [&](const ASTPtr & child) { return child.get() == function->arguments; })
            != 1
        || slot.status != StoredObjectTypeStringSlotStatus::ExactExpression
        || (slot.occurrence_site != StoredObjectOccurrenceSite::TableFunctionSchemaString
            && slot.occurrence_site != StoredObjectOccurrenceSite::FormatSchemaString)
        || slot.argument_ordinal >= function->arguments->children.size()
        || function->arguments->children[slot.argument_ordinal].get() != slot.expression || !literal
        || literal->value.getType() != Field::Types::String)
    {
        fail(Error::Code::InvalidDeclaration, "an inferred table-function schema is not one exact literal String slot");
    }

    const String original = literal->value.safeGet<String>();
    auto schema = parseAuxiliarySchemaString(original);
    auto * columns = schema ? schema->as<ASTExpressionList>() : nullptr;
    if (!columns || columns->children.empty())
        fail(Error::Code::InvalidDeclaration, "an inferred table-function schema has no column declarations");

    struct AuthorityResolver
    {
        DatabasePtr database;
        std::unique_ptr<UDTTypeExpressionResolutionScope> resolver;
    };
    auto resource_ledger = std::make_shared<QueryResourceLedger>();
    std::map<String, AuthorityResolver, std::less<>> resolvers;
    bool resolved_logical_type = false;

    for (auto & child : columns->children)
    {
        auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
        if (!declaration)
            fail(Error::Code::InvalidDeclaration, "an inferred table-function schema contains a malformed column declaration");
        validateAuxiliarySchemaColumnDeclaration(*declaration);

        const auto declared_type = declaration->getType();
        DataTypePtr physical_type;
        if (const auto database_name = getAuxiliaryTypeAuthorityDatabase(declared_type))
        {
            auto [resolver_it, inserted] = resolvers.try_emplace(*database_name);
            if (inserted)
            {
                resolver_it->second.database = DatabaseCatalog::instance().getDatabase(*database_name);
                auto atomic_database = std::dynamic_pointer_cast<DatabaseAtomic>(resolver_it->second.database);
                if (!atomic_database)
                    fail(Error::Code::InvalidDeclaration, "a schema-string UDT reference requires an Atomic database authority");
                atomic_database->waitDatabaseStarted();
                if (!atomic_database->hasActiveUDTAuthority())
                    fail(Error::Code::InvalidDeclaration, "a schema-string UDT authority is unavailable");
                resolver_it->second.resolver = std::make_unique<UDTTypeExpressionResolutionScope>(
                    *database_name, context, atomic_database->getUDTAuthorityAdapter(), resource_ledger);
            }
            if (!resolver_it->second.resolver)
                fail(Error::Code::InvalidState, "a schema-string UDT authority has no resolution scope");
            physical_type = resolver_it->second.resolver->resolve(declared_type).getPhysicalType();
            resolved_logical_type = true;
        }
        else
        {
            physical_type = DataTypeFactory::instance().get(declared_type);
        }
        if (!physical_type)
            fail(Error::Code::InvalidState, "an inferred table-function schema produced an empty physical type");
        declaration->setType(dataTypeToAST(physical_type));
    }

    if (!resolved_logical_type)
        fail(Error::Code::InvalidDeclaration, "an inferred table-function schema contains no structured UDT reference");
    const String physical_schema = formatAuxiliarySchemaString(schema);
    if (physical_schema.empty() || containsStructuredUDTReference(schema))
        fail(Error::Code::InvalidState, "an inferred table-function schema was not fully physicalized");
    if (literal->value.getType() != Field::Types::String || literal->value.safeGet<String>() != original
        || function->arguments->children[slot.argument_ordinal].get() != literal)
    {
        fail(Error::Code::QueryChanged, "an inferred table-function schema changed during physicalization");
    }
    literal->value = physical_schema;

    const auto post_classification = classifyStoredObjectCreateQuery(create, /*metadata_load=*/false);
    const auto post_decision = classifyStoredObjectCreatePreparation(create, post_classification, /*udt_feature_enabled=*/true);
    const auto post_tree = classifyStoredObjectTableFunctionTypeStringTree(*root_function);
    if (create.as_table_function != root_function || post_tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete
        || post_tree.schema_owner != function || post_tree.schema_slot.expression != literal
        || !post_classification.structured_udt_scan_complete || !post_classification.type_string_scan_complete
        || post_classification.structured_udt_occurrence_sites != 0 || post_classification.qualified_type_reference_candidate_sites != 0
        || post_classification.unresolved_type_string_occurrence_sites != 0 || post_classification.has_unclassified_udt_reference
        || post_decision.route != StoredObjectCreatePreparationRoute::PhysicalOnly)
    {
        fail(Error::Code::InvalidState, "an inferred table-function schema did not become canonical physical CREATE metadata");
    }
}

namespace
{

using AuxiliaryASTChildPath = std::vector<size_t>;

struct PreparedAuxiliaryReplacementPaths
{
    std::vector<AuxiliaryASTChildPath> strings;
    std::vector<AuxiliaryASTChildPath> settings;
};

PreparedAuxiliaryReplacementPaths captureAuxiliaryReplacementPaths(const IAST & root, const PreparedViewAuxiliaryEndpoints & auxiliary)
{
    std::unordered_set<const IAST *> targets;
    for (const auto & replacement : auxiliary.string_replacements)
    {
        if (!replacement.literal || !targets.emplace(replacement.literal).second)
            fail(Error::Code::InvalidDeclaration, "a selected-output schema literal is empty or duplicated");
    }
    for (const auto & replacement : auxiliary.setting_replacements)
    {
        if (!replacement.settings)
            fail(Error::Code::InvalidDeclaration, "a selected-output schema setting owner is empty");
        targets.emplace(replacement.settings);
    }

    std::unordered_map<const IAST *, AuxiliaryASTChildPath> paths;
    std::unordered_set<const IAST *> visited;
    AuxiliaryASTChildPath path;
    std::function<void(const IAST *, size_t)> visit = [&](const IAST * node, size_t depth)
    {
        if (!node || depth > maximum_auxiliary_ast_depth || visited.size() >= maximum_auxiliary_ast_nodes)
            fail(Error::Code::LimitExceeded, "selected-output schema replacement paths exceed their AST limit");
        if (!visited.emplace(node).second)
            fail(Error::Code::InvalidDeclaration, "selected-output schema replacement paths found a shared or cyclic AST");
        if (targets.contains(node) && !paths.emplace(node, path).second)
            fail(Error::Code::InvalidDeclaration, "a selected-output schema replacement has ambiguous AST ownership");
        for (size_t ordinal = 0; ordinal < node->children.size(); ++ordinal)
        {
            if (!node->children[ordinal])
                fail(Error::Code::InvalidDeclaration, "a selected-output schema replacement path contains a null AST child");
            path.push_back(ordinal);
            visit(node->children[ordinal].get(), depth + 1);
            path.pop_back();
        }
    };
    visit(&root, 0);
    if (paths.size() != targets.size())
        fail(Error::Code::InvalidDeclaration, "a selected-output schema replacement is outside the exact stored SELECT");

    PreparedAuxiliaryReplacementPaths result;
    result.strings.reserve(auxiliary.string_replacements.size());
    for (const auto & replacement : auxiliary.string_replacements)
        result.strings.push_back(paths.at(replacement.literal));
    result.settings.reserve(auxiliary.setting_replacements.size());
    for (const auto & replacement : auxiliary.setting_replacements)
        result.settings.push_back(paths.at(replacement.settings));
    return result;
}

IAST * followAuxiliaryReplacementPath(IAST & root, const AuxiliaryASTChildPath & path)
{
    IAST * node = &root;
    for (const size_t ordinal : path)
    {
        if (ordinal >= node->children.size() || !node->children[ordinal])
            fail(Error::Code::QueryChanged, "a selected-output schema replacement path changed before analysis");
        node = node->children[ordinal].get();
    }
    return node;
}

}

struct PreparedViewSchemaStringBindingHandoff::Impl
{
    StoredObjectKind object_kind = StoredObjectKind::Unclassified;
    StoredObjectSourceMode source_mode = StoredObjectSourceMode::Unclassified;
    UUID target_database_uuid = UUIDHelpers::Nil;
    ASTCreateQuery * create_root = nullptr;
    const IAST * select_root = nullptr;
    PreparedViewAuxiliaryEndpoints auxiliary;
    PreparedAuxiliaryReplacementPaths replacement_paths;
    bool analysis_clone_prepared = false;
    bool consumed = false;
};

PreparedViewSchemaStringBindingHandoff::PreparedViewSchemaStringBindingHandoff(std::unique_ptr<Impl> impl_)
    : impl(std::move(impl_))
{
}

PreparedViewSchemaStringBindingHandoff::PreparedViewSchemaStringBindingHandoff(PreparedViewSchemaStringBindingHandoff &&) noexcept
    = default;
PreparedViewSchemaStringBindingHandoff &
PreparedViewSchemaStringBindingHandoff::operator=(PreparedViewSchemaStringBindingHandoff &&) noexcept = default;
PreparedViewSchemaStringBindingHandoff::~PreparedViewSchemaStringBindingHandoff() = default;

bool PreparedViewSchemaStringBindingHandoff::hasPreparedPhysicalizedAnalysisAST() const noexcept
{
    return impl && impl->analysis_clone_prepared && !impl->consumed;
}

ASTPtr PreparedViewSchemaStringBindingHandoff::clonePhysicalizedSelectForAnalysis()
{
    if (!impl || impl->analysis_clone_prepared || impl->consumed || !impl->create_root || !impl->select_root
        || impl->create_root->select != impl->select_root || !impl->auxiliary.cast_replacements.empty()
        || impl->replacement_paths.strings.size() != impl->auxiliary.string_replacements.size()
        || impl->replacement_paths.settings.size() != impl->auxiliary.setting_replacements.size())
    {
        fail(Error::Code::InvalidState, "a physicalized selected-output analysis clone may be prepared exactly once");
    }

    for (size_t index = 0; index < impl->auxiliary.string_replacements.size(); ++index)
    {
        const auto & replacement = impl->auxiliary.string_replacements[index];
        if (!replacement.literal || replacement.literal->value.getType() != Field::Types::String
            || replacement.literal->value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty()
            || followAuxiliaryReplacementPath(*impl->create_root->select, impl->replacement_paths.strings[index]) != replacement.literal)
        {
            fail(Error::Code::QueryChanged, "a selected-output schema-string endpoint changed after exact UDT resolution");
        }
    }
    for (size_t index = 0; index < impl->auxiliary.setting_replacements.size(); ++index)
    {
        const auto & replacement = impl->auxiliary.setting_replacements[index];
        if (!replacement.settings || replacement.change_ordinal >= replacement.settings->changes.size())
            fail(Error::Code::QueryChanged, "a selected-output schema setting changed after exact UDT resolution");
        const auto & change = replacement.settings->changes[replacement.change_ordinal];
        if (change.name != replacement.setting_name || change.value.getType() != Field::Types::String
            || change.value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty()
            || followAuxiliaryReplacementPath(*impl->create_root->select, impl->replacement_paths.settings[index]) != replacement.settings)
        {
            fail(Error::Code::QueryChanged, "a selected-output schema setting changed after exact UDT resolution");
        }
    }

    auto cloned_create_ast = impl->create_root->clone();
    auto * cloned_create = cloned_create_ast ? cloned_create_ast->as<ASTCreateQuery>() : nullptr;
    if (!cloned_create || !cloned_create->select)
        fail(Error::Code::InvalidState, "the selected-output CREATE clone lost its stored SELECT");
    for (size_t index = 0; index < impl->auxiliary.string_replacements.size(); ++index)
    {
        const auto & replacement = impl->auxiliary.string_replacements[index];
        auto * literal = followAuxiliaryReplacementPath(*cloned_create->select, impl->replacement_paths.strings[index])->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String || literal->value.safeGet<String>() != replacement.original_value)
            fail(Error::Code::QueryChanged, "a cloned selected-output schema literal differs from its retained generation");
        literal->value = replacement.physical_value;
    }
    for (size_t index = 0; index < impl->auxiliary.setting_replacements.size(); ++index)
    {
        const auto & replacement = impl->auxiliary.setting_replacements[index];
        auto * settings
            = followAuxiliaryReplacementPath(*cloned_create->select, impl->replacement_paths.settings[index])->as<ASTSetQuery>();
        if (!settings || replacement.change_ordinal >= settings->changes.size())
            fail(Error::Code::QueryChanged, "a cloned selected-output schema setting lost its retained owner");
        auto & change = settings->changes[replacement.change_ordinal];
        if (change.name != replacement.setting_name || change.value.getType() != Field::Types::String
            || change.value.safeGet<String>() != replacement.original_value)
            fail(Error::Code::QueryChanged, "a cloned selected-output schema setting differs from its retained generation");
        change.value = replacement.physical_value;
    }

    constexpr auto schema_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    const auto post = classifyStoredObjectCreateQuery(*cloned_create, /*metadata_load=*/false);
    if (impl->create_root->select != impl->select_root || !post.structured_udt_scan_complete || !post.type_string_scan_complete
        || post.has_unclassified_udt_reference || (post.qualified_type_reference_candidate_sites & schema_sites) != 0
        || (post.unresolved_type_string_occurrence_sites & schema_sites) != 0)
    {
        fail(Error::Code::InvalidState, "the selected-output analysis clone did not receive complete physical schema metadata");
    }
    impl->analysis_clone_prepared = true;
    return cloned_create->select->ptr();
}

PreparedViewSchemaStringBindingHandoff prepareStoredObjectSelectedOutputSchemaStringBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    StoredObjectKind object_kind,
    UUID target_database_uuid,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    const ViewOutputTypeBindingLimits &)
{
    constexpr auto schema_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    if ((object_kind != StoredObjectKind::View && object_kind != StoredObjectKind::MaterializedView)
        || classification.object_kind != object_kind || classification.has_explicit_destination_columns
        || (classification.source_mode != StoredObjectSourceMode::AsSelect
            && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
        || !classification.structured_udt_scan_complete || !classification.type_string_scan_complete
        || classification.has_unclassified_udt_reference || classification.source_query_has_unclassified_table_function
        || (classification.qualified_type_reference_candidate_sites & schema_sites) == 0 || target_database_uuid == UUIDHelpers::Nil
        || target_database_uuid != authority.getDatabaseUUID() || database_name.empty() || !context || !create.isView() || !create.select)
    {
        fail(Error::Code::InvalidDecision, "the CREATE classification does not authorize pre-analysis schema-string preparation");
    }

    auto result = std::make_unique<PreparedViewSchemaStringBindingHandoff::Impl>();
    result->object_kind = object_kind;
    result->source_mode = classification.source_mode;
    result->target_database_uuid = target_database_uuid;
    result->create_root = &create;
    result->select_root = create.select;

    UDTTypeExpressionResolutionScope resolver(String(database_name), context, authority);
    ViewAuxiliaryEndpointWalker walker(&resolver, ViewAuxiliaryEndpointWalker::Mode::SchemaStringsOnly);
    walker.walk(create.select);
    result->auxiliary = walker.releasePrepared();
    result->replacement_paths = captureAuxiliaryReplacementPaths(*create.select, result->auxiliary);

    bool has_logical_schema_endpoint = false;
    for (const auto & endpoint : result->auxiliary.endpoints)
    {
        if (endpoint.site != PersistedTypeOccurrenceSite::SchemaString)
            fail(Error::Code::InvalidState, "pre-analysis schema preparation collected a stored-expression endpoint");
        const auto & tree = endpoint.endpoint_type.getLogicalTree();
        has_logical_schema_endpoint = has_logical_schema_endpoint || (tree && tree->getOccurrenceCount() != 0);
    }
    if (!has_logical_schema_endpoint || (result->auxiliary.string_replacements.empty() && result->auxiliary.setting_replacements.empty()))
    {
        fail(Error::Code::MissingLogicalBinding, "qualified schema-string evidence produced no exact logical replacement");
    }

    return PreparedViewSchemaStringBindingHandoff(std::move(result));
}

struct PreparedStoredObjectTypeBindingHandoff::Impl
{
    struct ColumnReplacement
    {
        ASTColumnDeclaration * declaration = nullptr;
        ASTPtr original_type;
        ASTPtr physical_type;
    };

    struct DictionaryReplacement
    {
        ASTDictionaryAttributeDeclaration * declaration = nullptr;
        size_t child_ordinal = 0;
        ASTPtr original_type;
        ASTPtr physical_type;
    };

    using Bindings = std::variant<PreparedViewOutputTypeBindings, PreparedDictionaryAttributeTypeBindings>;

    StoredObjectKind object_kind = StoredObjectKind::Unclassified;
    StoredObjectSourceMode source_mode = StoredObjectSourceMode::Unclassified;
    SchemaObjectID object;
    ASTCreateQuery * create_root = nullptr;
    Bindings bindings;
    std::vector<ColumnReplacement> column_replacements;
    std::vector<DictionaryReplacement> dictionary_replacements;
    std::vector<AuxiliaryCastTargetReplacement> auxiliary_cast_replacements;
    std::vector<AuxiliaryStringReplacement> auxiliary_string_replacements;
    std::vector<AuxiliarySettingReplacement> auxiliary_setting_replacements;
    bool uses_selected_output_classification = false;
    bool replacements_applied = false;
    bool bindings_released = false;
};

PreparedStoredObjectTypeBindingHandoff::PreparedStoredObjectTypeBindingHandoff(std::unique_ptr<Impl> impl_)
    : impl(std::move(impl_))
{
}

PreparedStoredObjectTypeBindingHandoff::PreparedStoredObjectTypeBindingHandoff(PreparedStoredObjectTypeBindingHandoff &&) noexcept
    = default;
PreparedStoredObjectTypeBindingHandoff &
PreparedStoredObjectTypeBindingHandoff::operator=(PreparedStoredObjectTypeBindingHandoff &&) noexcept = default;
PreparedStoredObjectTypeBindingHandoff::~PreparedStoredObjectTypeBindingHandoff() = default;

StoredObjectKind PreparedStoredObjectTypeBindingHandoff::getObjectKind() const noexcept
{
    return impl ? impl->object_kind : StoredObjectKind::Unclassified;
}

StoredObjectSourceMode PreparedStoredObjectTypeBindingHandoff::getSourceMode() const noexcept
{
    return impl ? impl->source_mode : StoredObjectSourceMode::Unclassified;
}

const SchemaObjectID & PreparedStoredObjectTypeBindingHandoff::getObject() const noexcept
{
    static const SchemaObjectID empty;
    return impl ? impl->object : empty;
}

bool PreparedStoredObjectTypeBindingHandoff::hasAppliedPhysicalTypeASTs() const noexcept
{
    return impl && impl->replacements_applied;
}

bool PreparedStoredObjectTypeBindingHandoff::usesSelectedOutputClassification() const noexcept
{
    return impl && impl->uses_selected_output_classification;
}

const PreparedViewOutputTypeBindings * PreparedStoredObjectTypeBindingHandoff::tryGetViewBindings() const noexcept
{
    return impl && !impl->bindings_released ? std::get_if<PreparedViewOutputTypeBindings>(&impl->bindings) : nullptr;
}

const PreparedDictionaryAttributeTypeBindings * PreparedStoredObjectTypeBindingHandoff::tryGetDictionaryBindings() const noexcept
{
    return impl && !impl->bindings_released ? std::get_if<PreparedDictionaryAttributeTypeBindings>(&impl->bindings) : nullptr;
}

void PreparedStoredObjectTypeBindingHandoff::applyPhysicalTypeASTs()
{
    if (!impl || impl->replacements_applied || impl->bindings_released)
        fail(Error::Code::InvalidState, "physical type replacements may be applied exactly once before releasing bindings");

    for (const auto & replacement : impl->column_replacements)
    {
        if (!replacement.declaration || replacement.declaration->getType().get() != replacement.original_type.get()
            || !replacement.physical_type)
            fail(Error::Code::QueryChanged, "a View output declaration changed after exact UDT resolution");
    }
    for (const auto & replacement : impl->dictionary_replacements)
    {
        if (!replacement.declaration || replacement.declaration->type.get() != replacement.original_type.get()
            || replacement.child_ordinal >= replacement.declaration->children.size()
            || replacement.declaration->children[replacement.child_ordinal].get() != replacement.original_type.get()
            || !replacement.physical_type)
            fail(Error::Code::QueryChanged, "a Dictionary attribute declaration changed after exact UDT resolution");
    }
    for (const auto & replacement : impl->auxiliary_cast_replacements)
    {
        if (!replacement.arguments || replacement.arguments->children.size() != 2 || !replacement.original_target
            || replacement.arguments->children[1].get() != replacement.original_target || !replacement.physical_target)
            fail(Error::Code::QueryChanged, "a stored structured CAST target changed after exact UDT resolution");
    }
    for (const auto & replacement : impl->auxiliary_string_replacements)
    {
        if (!replacement.literal || replacement.literal->value.getType() != Field::Types::String
            || replacement.literal->value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty())
            fail(Error::Code::QueryChanged, "a stored type-string endpoint changed after exact UDT resolution");
    }
    for (const auto & replacement : impl->auxiliary_setting_replacements)
    {
        if (!replacement.settings || replacement.change_ordinal >= replacement.settings->changes.size())
            fail(Error::Code::QueryChanged, "a stored setting type-string endpoint changed after exact UDT resolution");
        const auto & change = replacement.settings->changes[replacement.change_ordinal];
        if (change.name != replacement.setting_name || change.value.getType() != Field::Types::String
            || change.value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty())
            fail(Error::Code::QueryChanged, "a stored setting type-string endpoint changed after exact UDT resolution");
    }

    for (auto & replacement : impl->column_replacements)
        replacement.declaration->setType(std::move(replacement.physical_type));
    for (auto & replacement : impl->dictionary_replacements)
    {
        replacement.declaration->type = replacement.physical_type;
        replacement.declaration->children[replacement.child_ordinal] = std::move(replacement.physical_type);
    }
    for (auto & replacement : impl->auxiliary_cast_replacements)
        replacement.arguments->children[1] = std::move(replacement.physical_target);
    for (auto & replacement : impl->auxiliary_string_replacements)
        replacement.literal->value = replacement.physical_value;
    for (auto & replacement : impl->auxiliary_setting_replacements)
        replacement.settings->changes[replacement.change_ordinal].value = replacement.physical_value;

    if (impl->create_root)
    {
        const auto post_classification = classifyStoredObjectCreateQuery(*impl->create_root, /*metadata_load=*/false);
        const auto post_decision
            = classifyStoredObjectCreatePreparation(*impl->create_root, post_classification, /*udt_feature_enabled=*/true);
        if (!post_classification.structured_udt_scan_complete || !post_classification.type_string_scan_complete
            || post_classification.structured_udt_occurrence_sites != 0 || post_classification.qualified_type_reference_candidate_sites != 0
            || post_classification.unresolved_type_string_occurrence_sites != 0 || post_classification.has_unclassified_udt_reference
            || post_classification.source_query_has_unclassified_table_function
            || post_decision.route != StoredObjectCreatePreparationRoute::PhysicalOnly)
        {
            fail(Error::Code::InvalidState, "prepared stored-object bindings did not produce canonical physical CREATE metadata");
        }
    }
    impl->replacements_applied = true;
}

void PreparedStoredObjectTypeBindingHandoff::validateNormalizedViewOutputs(const NamesAndTypesList & normalized_outputs) const
{
    const auto * bindings = tryGetViewBindings();
    if (!impl || !impl->replacements_applied || !bindings)
        fail(Error::Code::InvalidState, "normalized View outputs can be checked only for an applied View handoff");
    if (bindings->physical_outputs != normalized_outputs)
        fail(Error::Code::NormalizedSchemaMismatch, "ordinary View normalization changed the exact bound physical output schema");
}

PreparedViewOutputTypeBindings PreparedStoredObjectTypeBindingHandoff::releaseViewBindings() &&
{
    auto * bindings = tryGetViewBindings();
    if (!impl || !impl->replacements_applied || !bindings)
        fail(Error::Code::InvalidState, "View bindings are unavailable or were already released");
    impl->bindings_released = true;
    return std::move(*bindings);
}

PreparedDictionaryAttributeTypeBindings PreparedStoredObjectTypeBindingHandoff::releaseDictionaryBindings() &&
{
    auto * bindings = tryGetDictionaryBindings();
    if (!impl || !impl->replacements_applied || !bindings)
        fail(Error::Code::InvalidState, "Dictionary bindings are unavailable or were already released");
    impl->bindings_released = true;
    return std::move(*bindings);
}

PreparedStoredObjectTypeBindingHandoff prepareStoredObjectExactDeclarationBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    const StoredObjectCreatePreparationDecision & decision,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    const ViewOutputTypeBindingLimits & view_limits,
    const DictionaryAttributeTypeBindingLimits & dictionary_limits)
{
    const bool view_route = decision.route == StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs
        || decision.route == StoredObjectCreatePreparationRoute::PrepareMaterializedViewExplicitOutputs;
    const bool dictionary_route = decision.route == StoredObjectCreatePreparationRoute::PrepareDictionaryAttributes;
    if ((!view_route && !dictionary_route) || decision.isUnsupported() || !decision.has_positive_udt_evidence)
        fail(Error::Code::InvalidDecision, "the interpreter route does not authorize exact declaration preparation");
    if (classification.object_kind
            != (view_route ? decision.route == StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs
                        ? StoredObjectKind::View
                        : StoredObjectKind::MaterializedView
                           : StoredObjectKind::Dictionary)
        || classification.source_mode == StoredObjectSourceMode::Unclassified)
        fail(Error::Code::InvalidDecision, "the structural classification disagrees with the exact preparation route");

    const auto expected_schema_kind = tryGetSchemaObjectKind(classification.object_kind);
    if (!expected_schema_kind || !object.isValid() || object.kind != *expected_schema_kind || object_schema_revision == 0
        || object.database_uuid != authority.getDatabaseUUID() || database_name.empty() || create.uuid != object.object_uuid)
        fail(Error::Code::InvalidObject, "the prepared stored object has an invalid or cross-authority identity");

    auto result = std::make_unique<PreparedStoredObjectTypeBindingHandoff::Impl>();
    result->object_kind = classification.object_kind;
    result->source_mode = classification.source_mode;
    result->object = object;
    result->create_root = &create;
    UDTTypeExpressionResolutionScope resolver(String(database_name), context, authority);

    if (view_route)
    {
        if (!create.columns_list || !create.columns_list->columns || create.columns_list->columns->children.empty())
            fail(Error::Code::InvalidDeclaration, "an exact View preparation has no explicit output declarations");
        std::vector<ViewOutputTypeBindingInput> outputs;
        outputs.reserve(create.columns_list->columns->children.size());
        for (const auto & child : create.columns_list->columns->children)
        {
            auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration || declaration->name.empty() || !declaration->getType())
                fail(Error::Code::InvalidDeclaration, "an exact View output declaration is malformed");
            bool has_logical_references = false;
            auto resolved = resolveDeclaration(declaration->getType(), resolver, has_logical_references);
            if (has_logical_references)
            {
                result->column_replacements.push_back({
                    .declaration = declaration,
                    .original_type = declaration->getType(),
                    .physical_type = dataTypeToAST(resolved.getPhysicalType()),
                });
            }
            outputs.push_back({.output_name = declaration->name, .output_type = std::move(resolved)});
        }

        ViewAuxiliaryEndpointWalker auxiliary_walker(&resolver);
        auxiliary_walker.walk(create.select);
        auto auxiliary = auxiliary_walker.releasePrepared();
        PreparedViewOutputTypeBindings bindings;
        try
        {
            bindings = prepareViewMixedTypeBindings(object, object_schema_revision, outputs, auxiliary.endpoints, view_limits);
        }
        catch (const ViewOutputTypeBindingError & error)
        {
            rethrowViewBindingError(error);
        }
        if (!bindings.persisted_references || !bindings.bound_physical_schema || !bindings.sidecar_expectation
            || bindings.dependency_edges.empty())
            fail(Error::Code::MissingLogicalBinding, "the exact View declarations produced no indivisible logical binding package");
        result->auxiliary_cast_replacements = std::move(auxiliary.cast_replacements);
        result->auxiliary_string_replacements = std::move(auxiliary.string_replacements);
        result->auxiliary_setting_replacements = std::move(auxiliary.setting_replacements);
        result->bindings = std::move(bindings);
    }
    else
    {
        if (!create.dictionary_attributes_list || create.dictionary_attributes_list->children.empty())
            fail(Error::Code::InvalidDeclaration, "an exact Dictionary preparation has no attribute declarations");
        std::vector<DictionaryAttributeTypeBindingInput> attributes;
        attributes.reserve(create.dictionary_attributes_list->children.size());
        for (const auto & child : create.dictionary_attributes_list->children)
        {
            auto * declaration = child ? child->as<ASTDictionaryAttributeDeclaration>() : nullptr;
            if (!declaration || declaration->name.empty() || !declaration->type)
                fail(Error::Code::InvalidDeclaration, "an exact Dictionary attribute declaration is malformed");
            bool has_logical_references = false;
            auto resolved = resolveDeclaration(declaration->type, resolver, has_logical_references);
            if (has_logical_references)
            {
                const auto child_it = std::find_if(
                    declaration->children.begin(),
                    declaration->children.end(),
                    [&](const ASTPtr & candidate) { return candidate.get() == declaration->type.get(); });
                if (child_it == declaration->children.end())
                    fail(Error::Code::InvalidDeclaration, "a Dictionary attribute type is not owned by its declaration AST");
                result->dictionary_replacements.push_back({
                    .declaration = declaration,
                    .child_ordinal = static_cast<size_t>(child_it - declaration->children.begin()),
                    .original_type = declaration->type,
                    .physical_type = dataTypeToAST(resolved.getPhysicalType()),
                });
            }
            attributes.push_back({.attribute_name = declaration->name, .attribute_type = std::move(resolved)});
        }
        PreparedDictionaryAttributeTypeBindings bindings;
        try
        {
            bindings = prepareDictionaryAttributeTypeBindings(object, object_schema_revision, attributes, dictionary_limits);
        }
        catch (const DictionaryAttributeTypeBindingError & error)
        {
            rethrowDictionaryBindingError(error);
        }
        if (!bindings.persisted_references || !bindings.bound_physical_schema || !bindings.sidecar_expectation
            || bindings.dependency_edges.empty())
            fail(Error::Code::MissingLogicalBinding, "the exact Dictionary attributes produced no indivisible logical binding package");
        result->bindings = std::move(bindings);
    }
    return PreparedStoredObjectTypeBindingHandoff(std::move(result));
}

PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    StoredObjectKind object_kind,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    PreparedViewSchemaStringBindingHandoff * prepared_schema_strings,
    const ViewOutputTypeBindingLimits & view_limits)
{
    constexpr auto schema_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    if ((object_kind != StoredObjectKind::View && object_kind != StoredObjectKind::MaterializedView)
        || classification.object_kind != object_kind || classification.has_explicit_destination_columns
        || (classification.source_mode != StoredObjectSourceMode::AsSelect
            && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
        || !classification.structured_udt_scan_complete || !classification.type_string_scan_complete
        || classification.has_unclassified_udt_reference || classification.source_query_has_unclassified_table_function
        || selected_outputs.empty())
        fail(Error::Code::InvalidDecision, "the interpreter route does not authorize selected View output preparation");

    const auto expected_schema_kind = tryGetSchemaObjectKind(object_kind);
    if (!expected_schema_kind || *expected_schema_kind != SchemaObjectKind::View || !object.isValid()
        || object.kind != SchemaObjectKind::View || object_schema_revision == 0 || object.database_uuid != authority.getDatabaseUUID()
        || database_name.empty() || create.uuid != object.object_uuid || !create.isView() || !create.select)
        fail(Error::Code::InvalidObject, "the selected-output View has an invalid or cross-authority identity");

    const bool classification_has_logical_schema_string = (classification.qualified_type_reference_candidate_sites & schema_sites) != 0;
    const bool has_exact_selected_output
        = std::any_of(selected_outputs.begin(), selected_outputs.end(), [](const auto & selected) { return !selected.isPhysicalOnly(); });
    constexpr auto exact_auxiliary_sites = schema_sites | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::ViewStoredCast)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::MaterializedViewStoredCast);
    const bool has_exact_auxiliary
        = ((classification.structured_udt_occurrence_sites | classification.qualified_type_reference_candidate_sites)
           & exact_auxiliary_sites)
        != 0;
    if (classification.source_query_requires_exact_logical_authority && !has_exact_selected_output && !has_exact_auxiliary)
        fail(Error::Code::InvalidDecision, "the query-backed View source has no exact durable provenance proof");
    if (classification_has_logical_schema_string != static_cast<bool>(prepared_schema_strings))
    {
        fail(Error::Code::InvalidDecision, "selected-output schema-string evidence and its pre-analysis binding handoff disagree");
    }

    auto result = std::make_unique<PreparedStoredObjectTypeBindingHandoff::Impl>();
    result->object_kind = object_kind;
    result->source_mode = classification.source_mode;
    result->object = object;
    result->create_root = &create;
    result->uses_selected_output_classification = true;

    UDTTypeExpressionResolutionScope resolver(String(database_name), context, authority);
    PreparedViewAuxiliaryEndpoints auxiliary;
    if (prepared_schema_strings)
    {
        auto & schema = prepared_schema_strings->impl;
        if (!schema || !schema->analysis_clone_prepared || schema->consumed || schema->object_kind != object_kind
            || schema->source_mode != classification.source_mode || schema->target_database_uuid != object.database_uuid
            || schema->create_root != &create || schema->select_root != create.select
            || schema->replacement_paths.strings.size() != schema->auxiliary.string_replacements.size()
            || schema->replacement_paths.settings.size() != schema->auxiliary.setting_replacements.size())
        {
            fail(Error::Code::QueryChanged, "the pre-analysis schema-string handoff does not own this selected-output CREATE");
        }
        for (size_t index = 0; index < schema->auxiliary.string_replacements.size(); ++index)
        {
            const auto & replacement = schema->auxiliary.string_replacements[index];
            if (!replacement.literal || replacement.literal->value.getType() != Field::Types::String
                || replacement.literal->value.safeGet<String>() != replacement.original_value
                || followAuxiliaryReplacementPath(*create.select, schema->replacement_paths.strings[index]) != replacement.literal)
            {
                fail(Error::Code::QueryChanged, "a pre-analyzer schema-string replacement changed before final binding");
            }
        }
        for (size_t index = 0; index < schema->auxiliary.setting_replacements.size(); ++index)
        {
            const auto & replacement = schema->auxiliary.setting_replacements[index];
            if (!replacement.settings || replacement.change_ordinal >= replacement.settings->changes.size())
                fail(Error::Code::QueryChanged, "a pre-analyzer schema setting changed before final binding");
            const auto & change = replacement.settings->changes[replacement.change_ordinal];
            if (change.name != replacement.setting_name || change.value.getType() != Field::Types::String
                || change.value.safeGet<String>() != replacement.original_value
                || followAuxiliaryReplacementPath(*create.select, schema->replacement_paths.settings[index]) != replacement.settings)
            {
                fail(Error::Code::QueryChanged, "a pre-analyzer schema setting changed before final binding");
            }
        }
        /// From this point the attempt is terminal: every retained owner/path
        /// and original byte has passed its final structural check. Poison the
        /// move-only handoff before moving any endpoint so a later allocation,
        /// limit, or admission failure cannot retry a partially moved package.
        schema->consumed = true;
        auxiliary = std::move(schema->auxiliary);
    }

    ViewAuxiliaryEndpointWalker auxiliary_walker(
        &resolver,
        prepared_schema_strings ? ViewAuxiliaryEndpointWalker::Mode::StoredExpressionsOnly : ViewAuxiliaryEndpointWalker::Mode::All);
    auxiliary_walker.walk(create.select);
    auto stored_expressions = auxiliary_walker.releasePrepared();
    auxiliary.endpoints.insert(
        auxiliary.endpoints.end(),
        std::make_move_iterator(stored_expressions.endpoints.begin()),
        std::make_move_iterator(stored_expressions.endpoints.end()));
    auxiliary.cast_replacements.insert(
        auxiliary.cast_replacements.end(),
        std::make_move_iterator(stored_expressions.cast_replacements.begin()),
        std::make_move_iterator(stored_expressions.cast_replacements.end()));
    auxiliary.string_replacements.insert(
        auxiliary.string_replacements.end(),
        std::make_move_iterator(stored_expressions.string_replacements.begin()),
        std::make_move_iterator(stored_expressions.string_replacements.end()));
    auxiliary.setting_replacements.insert(
        auxiliary.setting_replacements.end(),
        std::make_move_iterator(stored_expressions.setting_replacements.begin()),
        std::make_move_iterator(stored_expressions.setting_replacements.end()));
    PreparedViewOutputTypeBindings bindings;
    try
    {
        bindings
            = prepareViewSelectedOutputTypeBindings(object, object_schema_revision, selected_outputs, auxiliary.endpoints, view_limits);
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        rethrowViewBindingError(error);
    }
    if (!bindings.persisted_references || !bindings.bound_physical_schema || !bindings.sidecar_expectation
        || bindings.dependency_edges.empty())
        fail(Error::Code::MissingLogicalBinding, "the selected View outputs produced no indivisible logical binding package");
    result->auxiliary_cast_replacements = std::move(auxiliary.cast_replacements);
    result->auxiliary_string_replacements = std::move(auxiliary.string_replacements);
    result->auxiliary_setting_replacements = std::move(auxiliary.setting_replacements);
    result->bindings = std::move(bindings);
    return PreparedStoredObjectTypeBindingHandoff(std::move(result));
}

PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputAlterBindings(
    ASTPtr & stored_select,
    StoredObjectKind object_kind,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    const ViewOutputTypeBindingLimits & view_limits)
{
    if (object_kind != StoredObjectKind::MaterializedView || !stored_select || !stored_select->as<ASTSelectWithUnionQuery>()
        || selected_outputs.empty())
    {
        fail(Error::Code::InvalidDecision, "MaterializedView ALTER selected-output preparation has an invalid query surface");
    }
    if (!object.isValid() || object.kind != SchemaObjectKind::View || !object_schema_revision
        || object.database_uuid != authority.getDatabaseUUID() || database_name.empty())
    {
        fail(Error::Code::InvalidObject, "stored-object ALTER has an invalid or cross-authority View identity");
    }

    auto result = std::make_unique<PreparedStoredObjectTypeBindingHandoff::Impl>();
    result->object_kind = object_kind;
    result->source_mode = StoredObjectSourceMode::AsSelect;
    result->object = object;
    result->uses_selected_output_classification = true;

    UDTTypeExpressionResolutionScope resolver(String(database_name), context, authority);
    ViewAuxiliaryEndpointWalker auxiliary_walker(&resolver);
    auxiliary_walker.walk(stored_select.get());
    auto auxiliary = auxiliary_walker.releasePrepared();
    PreparedViewOutputTypeBindings bindings;
    try
    {
        bindings
            = prepareViewSelectedOutputTypeBindings(object, object_schema_revision, selected_outputs, auxiliary.endpoints, view_limits);
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        rethrowViewBindingError(error);
    }

    const bool mapped = static_cast<bool>(bindings.persisted_references);
    if (mapped != static_cast<bool>(bindings.bound_physical_schema) || mapped != static_cast<bool>(bindings.sidecar_expectation)
        || mapped != !bindings.dependency_edges.empty())
    {
        fail(Error::Code::MissingLogicalBinding, "stored-object ALTER produced a partial logical binding package");
    }
    result->auxiliary_cast_replacements = std::move(auxiliary.cast_replacements);
    result->auxiliary_string_replacements = std::move(auxiliary.string_replacements);
    result->auxiliary_setting_replacements = std::move(auxiliary.setting_replacements);
    result->bindings = std::move(bindings);
    return PreparedStoredObjectTypeBindingHandoff(std::move(result));
}

std::vector<ViewAuxiliaryPhysicalTypeBindingInput>
collectViewAuxiliaryPhysicalTypeBindings(const ASTCreateQuery & create, const ViewOutputTypeBindingLimits & limits)
{
    if (!create.isView() || !create.select)
        fail(Error::Code::InvalidObject, "auxiliary View endpoint replay requires stored View/MV SELECT metadata");
    return collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(create.select->ptr(), limits);
}

std::vector<ViewAuxiliaryPhysicalTypeBindingInput>
collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(const ASTPtr & stored_select, const ViewOutputTypeBindingLimits & limits)
{
    if (!stored_select)
        fail(Error::Code::InvalidObject, "auxiliary View endpoint replay requires an exact stored SELECT AST");
    ViewAuxiliaryEndpointWalker walker(nullptr);
    walker.walk(stored_select.get());
    auto endpoints = walker.releasePhysical();
    if (endpoints.size() > limits.maximum_outputs)
        fail(Error::Code::LimitExceeded, "View auxiliary endpoint table exceeds its endpoint limit");
    UInt64 retained_owner_key_bytes = 0;
    for (const auto & endpoint : endpoints)
    {
        if (endpoint.runtime_owner_key.size() > limits.maximum_single_runtime_owner_key_bytes
            || endpoint.runtime_owner_key.size() > limits.maximum_retained_runtime_owner_key_bytes
            || retained_owner_key_bytes > limits.maximum_retained_runtime_owner_key_bytes - endpoint.runtime_owner_key.size())
            fail(Error::Code::LimitExceeded, "View auxiliary endpoint owner keys exceed their byte limit");
        retained_owner_key_bytes += endpoint.runtime_owner_key.size();
    }
    return endpoints;
}

void physicalizeViewStoredSelectRuntimeAnnotations(const ASTPtr & stored_select)
{
    RuntimeStoredExpressionPhysicalizationWalker walker;
    walker.walk(stored_select);
    walker.apply();
}

void applyViewAuxiliaryTypePresentations(
    ASTCreateQuery & create, std::span<const ViewAuxiliaryTypePresentation> presentations, const ViewOutputTypeBindingLimits & limits)
{
    if (presentations.empty())
        return;
    if (!create.isView() || !create.select || presentations.size() > limits.maximum_outputs)
        fail(Error::Code::InvalidObject, "auxiliary View presentation requires exact stored View/MV SELECT metadata");

    ViewAuxiliaryEndpointWalker walker(presentations);
    walker.walk(create.select);
    auto replacements = walker.releasePresentations();
    for (const auto & replacement : replacements.cast_replacements)
    {
        if (!replacement.arguments || replacement.arguments->children.size() != 2 || !replacement.original_target
            || replacement.arguments->children[1].get() != replacement.original_target || !replacement.physical_target)
            fail(Error::Code::QueryChanged, "a stored structured CAST target changed before current-name rendering");
    }
    for (const auto & replacement : replacements.string_replacements)
    {
        if (!replacement.literal || replacement.literal->value.getType() != Field::Types::String
            || replacement.literal->value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty())
            fail(Error::Code::QueryChanged, "a stored type-string endpoint changed before current-name rendering");
    }
    for (const auto & replacement : replacements.setting_replacements)
    {
        if (!replacement.settings || replacement.change_ordinal >= replacement.settings->changes.size())
            fail(Error::Code::QueryChanged, "a stored setting endpoint changed before current-name rendering");
        const auto & change = replacement.settings->changes[replacement.change_ordinal];
        if (change.name != replacement.setting_name || change.value.getType() != Field::Types::String
            || change.value.safeGet<String>() != replacement.original_value || replacement.physical_value.empty())
            fail(Error::Code::QueryChanged, "a stored setting endpoint changed before current-name rendering");
    }

    for (auto & replacement : replacements.cast_replacements)
        replacement.arguments->children[1] = std::move(replacement.physical_target);
    for (auto & replacement : replacements.string_replacements)
        replacement.literal->value = replacement.physical_value;
    for (auto & replacement : replacements.setting_replacements)
        replacement.settings->changes[replacement.change_ordinal].value = replacement.physical_value;
}

PersistedTypeReferences
reconstructPersistedTableSourceReferences(const BoundObjectTypeReferences & source, const PersistedTypeReferencesLimits & limits)
{
    const auto & source_object = source.getObject();
    if (!source_object.isValid() || source_object.kind != SchemaObjectKind::Table || !source.getObjectSchemaRevision())
        fail(Error::Code::SourceSidecarMismatch, "native source binding is not a durable Table identity");

    PersistedTypeReferences references;
    references.format_version = source.getFormatVersion();
    references.object = source_object;
    references.object_schema_revision = source.getObjectSchemaRevision();
    references.physical_schema_fingerprint = source.getPhysicalSchemaFingerprint();
    references.path_dictionary_version = source.getPathDictionaryVersion();
    references.descriptors.reserve(source.getDescriptors().size());
    for (const auto & descriptor : source.getDescriptors())
    {
        if (!descriptor)
            fail(Error::Code::SourceSidecarMismatch, "native source binding contains an empty descriptor");
        references.descriptors.push_back(descriptor->getPersistedDescriptor());
    }

    references.occurrence_paths.reserve(source.getUses().size());
    references.uses.reserve(source.getUses().size());
    for (size_t index = 0; index < source.getUses().size(); ++index)
    {
        const auto & use = source.getUses()[index];
        if (use.getDescriptorIndex() >= references.descriptors.size())
            fail(Error::Code::SourceSidecarMismatch, "native source binding contains an invalid descriptor index");
        references.occurrence_paths.push_back(use.getPath());
        references.uses.push_back({
            .path_id = static_cast<UInt64>(index),
            .descriptor_id = static_cast<UInt64>(use.getDescriptorIndex()),
        });
    }

    try
    {
        static_cast<void>(encodePersistedTypeReferences(references, limits));
        if (computePersistedTypeReferencesSidecarHash(references, limits) != source.getSidecarHash())
            fail(Error::Code::SourceSidecarMismatch, "native source binding differs from its canonical sidecar hash");
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::SourceSidecarMismatch, error.what());
    }
    return references;
}

PreparedTableColumnTypeBindings prepareTableSourceSidecarCopyBindings(
    const SchemaObjectID & target,
    UInt64 target_object_schema_revision,
    const NamesAndTypesList & target_physical_columns,
    const PersistedTypeReferences & source_references,
    const TableColumnTypeBindingLimits & limits)
{
    if (!target.isValid() || target.kind != SchemaObjectKind::Table || !target_object_schema_revision
        || source_references.object.kind != SchemaObjectKind::Table || source_references.object.database_uuid != target.database_uuid)
        fail(Error::Code::CrossDatabaseReference, "native logical source cannot be retargeted across authorities or object kinds");
    if (!source_references.object_schema_revision || source_references.descriptors.empty() || source_references.occurrence_paths.empty()
        || source_references.uses.empty())
        fail(Error::Code::SourceSidecarMismatch, "native logical source sidecar is incomplete");

    PreparedTableColumnTypeBindings result;
    result.physical_columns = target_physical_columns;
    try
    {
        result.physical_schema_fingerprint = computeTableColumnPhysicalSchemaFingerprint(target_physical_columns, limits);
        auto target_references = source_references;
        target_references.object = target;
        target_references.object_schema_revision = target_object_schema_revision;
        target_references.physical_schema_fingerprint = result.physical_schema_fingerprint;
        static_cast<void>(encodePersistedTypeReferences(target_references, limits.persisted));
        result.bound_physical_schema = reconstructTableColumnPhysicalSchema(
            target, target_object_schema_revision, target_physical_columns, target_references, limits);
        result.sidecar_expectation = SidecarExpectationRecord{
            .object = target,
            .object_schema_revision = target_object_schema_revision,
            .sidecar_hash = computePersistedTypeReferencesSidecarHash(target_references, limits.persisted),
            .physical_schema_fingerprint = result.physical_schema_fingerprint,
        };

        std::set<SchemaObjectID> dependencies;
        for (const auto & descriptor : target_references.descriptors)
        {
            const auto & identity = descriptor.getDefinitionIdentity();
            if (identity.database_uuid != target.database_uuid)
                fail(Error::Code::CrossDatabaseReference, "native logical source descriptor belongs to another authority");
            dependencies.insert({
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = identity.database_uuid,
                .object_uuid = identity.type_uuid,
            });
        }
        result.dependency_edges.reserve(dependencies.size());
        for (const auto & dependency : dependencies)
        {
            result.dependency_edges.push_back({
                .dependent = target,
                .dependency = dependency,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            });
        }
        if (result.dependency_edges.empty())
            fail(Error::Code::SourceSidecarMismatch, "native logical source sidecar produced no definition dependency");
        result.persisted_references = std::move(target_references);
    }
    catch (const TableColumnTypeBindingError & error)
    {
        if (error.code == TableColumnTypeBindingError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::SourceSidecarMismatch, error.what());
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::SourceSidecarMismatch, error.what());
    }
    return result;
}

}
