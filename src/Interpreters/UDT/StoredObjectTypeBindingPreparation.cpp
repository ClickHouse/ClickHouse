;

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

void physicalizeViewStoredSelectRuntimeAnnotations(const ASTPtr & stored_select)
{
    RuntimeStoredExpressionPhysicalizationWalker walker;
    walker.walk(stored_select);
    walker.apply();
}