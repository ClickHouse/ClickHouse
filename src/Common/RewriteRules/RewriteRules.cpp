#include <Common/RewriteRules/RewriteRules.h>
#include <algorithm>
#include <array>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <Core/Settings.h>
#include <Common/FieldVisitorToString.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/forEachNonChildSemanticAST.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTDatabaseOrNone.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTDropResourceQuery.h>
#include <Parsers/ASTDropWorkloadQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTKillQueryQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/ASTShowFunctionsQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowSettingQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTCreateQuotaQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/Access/ASTShowGrantsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <base/demangle.h>

#include <typeindex>

namespace DB
{

namespace ErrorCodes
{
    extern const int REWRITE_RULE_DOESNT_EXIST;
    extern const int REWRITE_RULE_ALREADY_EXISTS;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// The placeholder types understood by the rewrite-rule matcher
    /// (`RewriteRulesASTTraversal.cpp`). A rule placeholder uses the `{name:Type}`
    /// query-parameter syntax, but `Type` is a small custom matching vocabulary
    /// rather than an ordinary ClickHouse data type: `String` and `Int` capture
    /// literals, `Expression`/`ExpressionList` capture expression subtrees and
    /// `Subquery` captures a subquery. A placeholder with any other type (e.g.
    /// `{x:UInt64}` or `{d:Date}`) would be stored but never match a literal query,
    /// silently turning the rule into a no-op, so such types are rejected at DDL time.
    bool isSupportedQueryParameterType(std::string_view type)
    {
        static constexpr std::array supported{"String", "Int", "Expression", "ExpressionList", "Subquery"};
        return std::find(supported.begin(), supported.end(), type) != supported.end();
    }

    /// Collects the names of all `{name:Type}` placeholders (`ASTQueryParameter`)
    /// reachable from `ast`, recording duplicates encountered along the way.
    void collectQueryParameters(const ASTPtr & ast, std::unordered_set<String> & names, std::vector<String> & duplicates)
    {
        if (!ast)
            return;
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            if (!names.insert(query_parameter->name).second)
                duplicates.push_back(query_parameter->name);
        }
        for (const auto & child : ast->children)
            collectQueryParameters(child, names, duplicates);
    }

    /// Returns the first placeholder in `ast` whose type is not understood by the
    /// matcher, as a `{name, type}` pair, or `std::nullopt` if all are supported.
    std::optional<std::pair<String, String>> findUnsupportedQueryParameter(const ASTPtr & ast)
    {
        if (!ast)
            return {};
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            if (!isSupportedQueryParameterType(type))
                return std::make_pair(query_parameter->name, query_parameter->type);
        }
        for (const auto & child : ast->children)
            if (auto found = findUnsupportedQueryParameter(child))
                return found;
        return {};
    }

    /// `collectQueryParameters` / `findUnsupportedQueryParameter` above and the matcher in
    /// `RewriteRulesASTTraversal.cpp` only ever descend through `IAST::children`.
    /// `ASTCreateRewriteRuleQuery` / `ASTAlterRewriteRuleQuery` keep their own
    /// `source_query` / `resulting_query` templates outside `children`, so a placeholder
    /// embedded inside a *nested* rule-DDL template (for example
    /// `CREATE RULE outer AS (CREATE RULE inner AS (SELECT {x:String}) REWRITE TO (SELECT 1))
    /// REWRITE TO (SELECT 1)`) is unreachable by the matcher: it can be neither bound nor
    /// substituted, so the rule would silently never match as intended. Returns the name of
    /// the first such placeholder so it can be rejected at DDL time. `inside_nested_template`
    /// is true once the walk has descended into a nested rule's template fields.
    std::optional<String> findQueryParameterInNestedRuleTemplate(const ASTPtr & ast, bool inside_nested_template)
    {
        if (!ast)
            return {};

        if (inside_nested_template)
            if (const auto * query_parameter = ast->as<ASTQueryParameter>())
                return query_parameter->name;

        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (auto found = findQueryParameterInNestedRuleTemplate(create_rule->source_query, true))
                return found;
            if (auto found = findQueryParameterInNestedRuleTemplate(create_rule->resulting_query, true))
                return found;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (auto found = findQueryParameterInNestedRuleTemplate(alter_rule->source_query, true))
                return found;
            if (auto found = findQueryParameterInNestedRuleTemplate(alter_rule->resulting_query, true))
                return found;
        }

        for (const auto & child : ast->children)
            if (auto found = findQueryParameterInNestedRuleTemplate(child, inside_nested_template))
                return found;

        return {};
    }

    /// Collects the declared type of every `{name:Type}` placeholder reachable from `ast`,
    /// keyed by name (whitespace-trimmed). The source template rejects duplicate names
    /// elsewhere, so its names are unique; for the result template the first occurrence is
    /// recorded, which is enough to detect a type that disagrees with the source.
    void collectQueryParameterTypes(const ASTPtr & ast, std::unordered_map<String, String> & types)
    {
        if (!ast)
            return;
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            types.emplace(query_parameter->name, type);
        }
        for (const auto & child : ast->children)
            collectQueryParameterTypes(child, types);
    }

    struct ParameterTypeMismatch
    {
        String name;
        String result_type;
        String source_type;
    };

    /// Returns the first result placeholder whose declared type disagrees with the type the
    /// same-named placeholder uses in the source template. Substitution binds captures by
    /// name and ignores the result-side type, so a mismatching type would let, say, a
    /// `String` capture land in an `{x:Int}` position, producing an AST that fails at
    /// execution time. Only names present in the source are compared (unknown names are
    /// rejected separately).
    std::optional<ParameterTypeMismatch> findResultParameterTypeMismatch(
        const ASTPtr & ast, const std::unordered_map<String, String> & source_types)
    {
        if (!ast)
            return {};
        if (const auto * query_parameter = ast->as<ASTQueryParameter>())
        {
            auto type = query_parameter->type;
            trimLeft(type);
            trimRight(type);
            auto it = source_types.find(query_parameter->name);
            if (it != source_types.end() && it->second != type)
                return ParameterTypeMismatch{query_parameter->name, type, it->second};
        }
        for (const auto & child : ast->children)
            if (auto found = findResultParameterTypeMismatch(child, source_types))
                return found;
        return {};
    }

    /// Returns true if `ast` contains an `INSERT` query carrying inline data (a `VALUES` /
    /// `FORMAT` payload). Such a template is unsafe to persist: `ASTInsertQuery::clone` copies
    /// the raw `data` / `end` pointers, which point into the buffer of the original
    /// `CREATE RULE` query; that buffer is freed long before the stored rule is used, so a
    /// later rewrite to `INSERT ... VALUES` / `INSERT ... FORMAT ...` would read dangling
    /// memory. Inline data is also not part of `children` or the tree hash, so it cannot
    /// participate in matching either. Walks ordinary `children` and the template fields of
    /// any nested rule DDL (which live outside `children`).
    bool containsInsertWithInlinedData(const ASTPtr & ast)
    {
        if (!ast)
            return false;
        if (const auto * insert = ast->as<ASTInsertQuery>(); insert && insert->hasInlinedData())
            return true;
        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (containsInsertWithInlinedData(create_rule->source_query)
                || containsInsertWithInlinedData(create_rule->resulting_query))
                return true;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (containsInsertWithInlinedData(alter_rule->source_query)
                || containsInsertWithInlinedData(alter_rule->resulting_query))
                return true;
        }
        for (const auto & child : ast->children)
            if (containsInsertWithInlinedData(child))
                return true;
        return false;
    }

    /// Returns the name of the first query parameter used as an alias (`expr AS {name:Type}`)
    /// reachable from `ast`, or `std::nullopt`. Such a parameter is stored in
    /// `ASTWithAlias::parametrised_alias`, which is NOT a child, so the placeholder walks above
    /// (and the matcher / substitution in `RewriteRulesASTTraversal.cpp`, which also follow only
    /// `children`) never see it: the rule would be stored with an alias placeholder that is
    /// never validated, bound or substituted. Walks ordinary `children` and the template fields
    /// of any nested rule DDL.
    std::optional<String> findParametrisedAlias(const ASTPtr & ast)
    {
        if (!ast)
            return {};
        if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(ast.get());
            with_alias && with_alias->parametrised_alias)
            return with_alias->parametrised_alias->name;
        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (auto found = findParametrisedAlias(create_rule->source_query))
                return found;
            if (auto found = findParametrisedAlias(create_rule->resulting_query))
                return found;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (auto found = findParametrisedAlias(alter_rule->source_query))
                return found;
            if (auto found = findParametrisedAlias(alter_rule->resulting_query))
                return found;
        }
        for (const auto & child : ast->children)
            if (auto found = findParametrisedAlias(child))
                return found;
        return {};
    }

    /// Returns the name of the first `{name:Type}` placeholder (`ASTQueryParameter`) reachable
    /// from `ast` only through an AST member kept OUTSIDE `IAST::children` (see
    /// `forEachNonChildSemanticAST`) — for example the `{n:Int}` in `SHOW TABLES LIMIT {n:Int}`,
    /// which lives in `ASTShowTablesQuery::limit_length`. The matcher and the result substitution
    /// (`RewriteRulesASTTraversal.cpp`) follow only `children`, so such a placeholder can be
    /// neither bound nor substituted: the rule would be stored but silently never work. Reject it
    /// at DDL time, like the alias / nested-template / inline-data cases above.
    /// `inside_non_child` becomes true once the walk has descended through a non-`children` member.
    std::optional<String> findQueryParameterInNonChildMember(const ASTPtr & ast, bool inside_non_child)
    {
        if (!ast)
            return {};

        if (inside_non_child)
            if (const auto * query_parameter = ast->as<ASTQueryParameter>())
                return query_parameter->name;

        std::optional<String> found;
        forEachNonChildSemanticAST(*ast, [&](const ASTPtr & member)
        {
            if (!found)
                found = findQueryParameterInNonChildMember(member, true);
        });
        if (found)
            return found;

        for (const auto & child : ast->children)
            if (auto nested = findQueryParameterInNonChildMember(child, inside_non_child))
                return nested;

        return {};
    }

    /// Returns the name of the first `{name:Type}` placeholder that a rule template flattened into
    /// a plain `String` member, and is therefore invisible as an AST node. The only such carrier is
    /// the `RENAME TO` target of `ALTER USER`: `parseRenameTo` parses it with
    /// `parseUserName(..., /*allow_query_parameter=*/true)` and stores
    /// `ASTUserNameWithHost::toString`, i.e. the text `{name:Type}` (optionally followed by
    /// `@host_pattern`), into `ASTCreateUserQuery::new_name`. No `ASTQueryParameter` survives there,
    /// so the matcher cannot bind such a placeholder (the rule would silently never match) and the
    /// substitution cannot replace it (the result template would rename the user to the literal
    /// name `{name:Type}`). Reject it at DDL time, like the other unreachable-placeholder cases.
    std::optional<String> findFlattenedQueryParameterInUserName(const ASTPtr & ast)
    {
        if (!ast)
            return {};

        if (const auto * create_user = ast->as<ASTCreateUserQuery>(); create_user && create_user->new_name)
        {
            const String & new_name = *create_user->new_name;
            const auto closing = new_name.find('}');
            const auto colon = new_name.find(':');
            /// `{` + non-empty name + `:` + type + `}`.
            if (new_name.starts_with('{') && closing != String::npos && colon > 1 && colon < closing)
                return new_name.substr(1, colon - 1);
        }

        std::optional<String> found;
        forEachNonChildSemanticAST(*ast, [&](const ASTPtr & member)
        {
            if (!found)
                found = findFlattenedQueryParameterInUserName(member);
        });
        if (found)
            return found;

        for (const auto & child : ast->children)
            if (auto nested = findFlattenedQueryParameterInUserName(child))
                return nested;

        return {};
    }

    /// Whether `node`'s AST class has been audited for complete tree-hash coverage: every data
    /// member that affects the formatted text is folded into `getTreeHash(true)` (via an
    /// `updateTreeHashImpl` override, membership in `children`, the `getID` string, or the
    /// `forEachNonChildSemanticAST` fold), so that for instances of this class an equal tree
    /// hash implies semantic equality. The rewrite-rule matcher relies on exactly that invariant
    /// when it declares a template subtree an exact match of a query subtree, so a rule template
    /// may only be built from audited classes — anything else is rejected at DDL time (fail
    /// closed) instead of silently over-matching queries that merely share a hash (e.g. a class
    /// that keeps an `IF NOT EXISTS` flag outside both `children` and its hash).
    ///
    /// The check is by exact type: a derived class does not inherit its base's audit (it can add
    /// unhashed members of its own). When auditing a new class, mind members whose subtree only
    /// the class itself knows about (see `forEachNonChildSemanticAST`) and flags whose only
    /// trace is in the formatted text.
    bool isExactMatchAuditedASTClass(const IAST & node)
    {
        static const std::unordered_set<std::type_index> audited = []
        {
            std::unordered_set<std::type_index> set;
            /// Expression / SELECT family.
            set.emplace(typeid(ASTExpressionList));
            set.emplace(typeid(ASTFunction));
            set.emplace(typeid(ASTLiteral));
            set.emplace(typeid(ASTIdentifier));
            set.emplace(typeid(ASTTableIdentifier));
            set.emplace(typeid(ASTSubquery));
            set.emplace(typeid(ASTQueryParameter));
            set.emplace(typeid(ASTAsterisk));
            set.emplace(typeid(ASTQualifiedAsterisk));
            set.emplace(typeid(ASTColumnsRegexpMatcher));
            set.emplace(typeid(ASTColumnsListMatcher));
            set.emplace(typeid(ASTQualifiedColumnsRegexpMatcher));
            set.emplace(typeid(ASTQualifiedColumnsListMatcher));
            set.emplace(typeid(ASTColumnsTransformerList));
            set.emplace(typeid(ASTColumnsApplyTransformer));
            set.emplace(typeid(ASTColumnsExceptTransformer));
            set.emplace(typeid(ASTColumnsReplaceTransformer));
            set.emplace(typeid(ASTColumnsReplaceTransformer::Replacement));
            set.emplace(typeid(ASTOrderByElement));
            set.emplace(typeid(ASTAssignment));
            set.emplace(typeid(ASTInterpolateElement));
            set.emplace(typeid(ASTWithElement));
            set.emplace(typeid(ASTSampleRatio));
            set.emplace(typeid(ASTSetQuery));
            set.emplace(typeid(ASTWindowDefinition));
            set.emplace(typeid(ASTWindowListElement));
            set.emplace(typeid(ASTSelectWithUnionQuery));
            set.emplace(typeid(ASTSelectQuery));
            set.emplace(typeid(ASTSelectIntersectExceptQuery));
            set.emplace(typeid(ASTTablesInSelectQuery));
            set.emplace(typeid(ASTTablesInSelectQueryElement));
            set.emplace(typeid(ASTTableExpression));
            set.emplace(typeid(ASTTableJoin));
            set.emplace(typeid(ASTArrayJoin));
            set.emplace(typeid(ASTInsertQuery));
            set.emplace(typeid(ASTExplainQuery));
            set.emplace(typeid(ASTUseQuery));
            set.emplace(typeid(ASTShowProcesslistQuery));
            /// Statements with an explicitly audited `updateTreeHashImpl`.
            set.emplace(typeid(ASTAlterNamedCollectionQuery));
            set.emplace(typeid(ASTBackupQuery));
            set.emplace(typeid(ASTCreateNamedCollectionQuery));
            set.emplace(typeid(ASTCreateResourceQuery));
            set.emplace(typeid(ASTCreateWorkloadQuery));
            set.emplace(typeid(ASTCreateRewriteRuleQuery));
            set.emplace(typeid(ASTAlterRewriteRuleQuery));
            set.emplace(typeid(ASTDropRewriteRuleQuery));
            set.emplace(typeid(ASTDatabaseOrNone));
            set.emplace(typeid(ASTDropFunctionQuery));
            set.emplace(typeid(ASTDropNamedCollectionQuery));
            set.emplace(typeid(ASTDropQuery));
            set.emplace(typeid(ASTDropResourceQuery));
            set.emplace(typeid(ASTDropWorkloadQuery));
            set.emplace(typeid(ASTKillQueryQuery));
            set.emplace(typeid(ASTRenameQuery));
            set.emplace(typeid(ASTShowColumnsQuery));
            set.emplace(typeid(ASTShowFunctionsQuery));
            set.emplace(typeid(ASTShowIndexesQuery));
            set.emplace(typeid(ASTShowSettingQuery));
            set.emplace(typeid(ASTShowTablesQuery));
            set.emplace(typeid(ASTSystemQuery));
            /// Access-control DDL with an explicitly audited hash, and the nested classes their
            /// trees are built from.
            set.emplace(typeid(ASTAuthenticationData));
            set.emplace(typeid(ASTCheckGrantQuery));
            set.emplace(typeid(ASTCreateMaskingPolicyQuery));
            set.emplace(typeid(ASTCreateQuotaQuery));
            set.emplace(typeid(ASTCreateRoleQuery));
            set.emplace(typeid(ASTCreateRowPolicyQuery));
            set.emplace(typeid(ASTCreateSettingsProfileQuery));
            set.emplace(typeid(ASTCreateUserQuery));
            set.emplace(typeid(ASTDropAccessEntityQuery));
            set.emplace(typeid(ASTGrantQuery));
            set.emplace(typeid(ASTMoveAccessEntityQuery));
            set.emplace(typeid(ASTRolesOrUsersSet));
            set.emplace(typeid(ASTRowPolicyName));
            set.emplace(typeid(ASTRowPolicyNames));
            set.emplace(typeid(ASTSetRoleQuery));
            set.emplace(typeid(ASTSettingsProfileElement));
            set.emplace(typeid(ASTSettingsProfileElements));
            set.emplace(typeid(ASTShowAccessEntitiesQuery));
            set.emplace(typeid(ASTShowCreateAccessEntityQuery));
            set.emplace(typeid(ASTShowGrantsQuery));
            set.emplace(typeid(ASTUserNameWithHost));
            set.emplace(typeid(ASTUserNamesWithHost));
            return set;
        }();
        return audited.contains(std::type_index(typeid(node)));
    }

    /// Returns the first node reachable from a rule's source template whose AST class is not
    /// audited for the exact-match invariant (see `isExactMatchAuditedASTClass`), or `nullptr`.
    /// Walks everything the matcher's tree hash covers: ordinary `children`, the non-`children`
    /// members of `forEachNonChildSemanticAST`, and — for nested rule DDL — both nested
    /// templates (the outer template's hash covers them through the rule-DDL node's
    /// `updateTreeHashImpl`). Only the source template needs screening: hashes are compared
    /// per class, so a false "exact match" requires the under-hashed class to be present in the
    /// template; a result template never participates in matching itself, and once substituted
    /// it is re-screened as the source side of whatever rule matches next.
    const IAST * findUnauditedTemplateNode(const ASTPtr & ast)
    {
        if (!ast)
            return nullptr;

        if (!isExactMatchAuditedASTClass(*ast))
            return ast.get();

        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            if (const auto * found = findUnauditedTemplateNode(create_rule->source_query))
                return found;
            if (const auto * found = findUnauditedTemplateNode(create_rule->resulting_query))
                return found;
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            if (const auto * found = findUnauditedTemplateNode(alter_rule->source_query))
                return found;
            if (const auto * found = findUnauditedTemplateNode(alter_rule->resulting_query))
                return found;
        }

        const IAST * found = nullptr;
        forEachNonChildSemanticAST(*ast, [&](const ASTPtr & member)
        {
            if (!found)
                found = findUnauditedTemplateNode(member);
        });
        if (found)
            return found;

        for (const auto & child : ast->children)
            if (const auto * nested = findUnauditedTemplateNode(child))
                return nested;

        return nullptr;
    }

    /// Validates a rule's source/result templates at DDL time so that invalid rule
    /// metadata is rejected on `CREATE RULE` / `ALTER RULE` instead of turning into
    /// runtime exceptions for every matching query later on.
    template <typename Query>
    void validateRuleTemplates(const Query & query)
    {
        /// The matcher declares a template subtree an exact match of a query subtree when their
        /// `getTreeHash(true)` values are equal, which is only sound for AST classes whose whole
        /// semantics are folded into that hash. Reject (fail closed) a source template containing
        /// any class that has not been audited for this invariant, instead of storing a rule that
        /// can silently fire on a query that merely shares a hash with its template (e.g.
        /// `CREATE TABLE t (...)` vs `CREATE TABLE IF NOT EXISTS t (...)` for a class that keeps
        /// the flag outside both `children` and its hash).
        if (const auto * unaudited = findUnauditedTemplateNode(query.source_query))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Rewrite rule `{}` uses a `{}` node ({}) in its source template; this construct is "
                "not supported in rewrite rule templates because exact-match checking is not "
                "guaranteed to be sound for it",
                query.rule_name, unaudited->getID(), demangle(typeid(*unaudited).name()));
        /// Placeholders inside a nested `CREATE RULE` / `ALTER RULE` template are
        /// unreachable by the matcher and the substitution (both walk only `children`),
        /// so a rule using them could never match or rewrite as intended. Reject them up
        /// front instead of silently storing a rule that does nothing.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto nested = findQueryParameterInNestedRuleTemplate(template_query, false))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` inside a nested CREATE RULE / "
                    "ALTER RULE template; placeholders in nested rule templates are not supported",
                    query.rule_name, *nested);

        /// An `INSERT` query carrying inline data (`VALUES` / `FORMAT`) is unsafe to persist:
        /// its raw `data` / `end` pointers reference the original `CREATE RULE` query buffer,
        /// which is freed before the stored rule is ever used. The parser already rejects such a
        /// template (the inline data swallows the closing parenthesis), but reject it here too so
        /// the memory-safety invariant does not depend on that parser behaviour (the inline data
        /// also cannot participate in matching, being outside `children`).
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (containsInsertWithInlinedData(template_query))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Rewrite rule `{}` uses an INSERT query with inline data (VALUES / FORMAT) in "
                    "its template; INSERT templates with inline data are not supported",
                    query.rule_name);

        /// A query parameter used as an alias (`... AS {name:Type}`) is kept in
        /// `ASTWithAlias::parametrised_alias`, outside `children`, so it is neither validated,
        /// bound nor substituted by the matcher. Reject it instead of silently storing a rule
        /// with an alias placeholder that does nothing.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto alias = findParametrisedAlias(template_query))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` as an alias; query parameters in "
                    "aliases are not supported in rewrite rule templates",
                    query.rule_name, *alias);

        /// A placeholder inside an AST member kept outside `children` — the `LIMIT` / `WHERE` of a
        /// `SHOW`, a `BACKUP` setting, a `CREATE ROW POLICY` filter, a `SETTINGS` clause value
        /// (`SETTINGS max_threads = {n:Int}`, kept as a `Field` inside `ASTSetQuery::changes`), etc.
        /// — is neither bound nor substituted by the matcher (which walks only `children`), even
        /// though the matcher's tree hash now folds those members in. The rule would be stored but
        /// silently never work, so reject it up front.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto non_child = findQueryParameterInNonChildMember(template_query, false))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` inside an AST member that the "
                    "matcher does not traverse (for example a SHOW LIMIT / WHERE, a BACKUP setting, "
                    "a SETTINGS clause value, a CREATE USER name, or a ROW POLICY filter); such "
                    "placeholders are not supported",
                    query.rule_name, *non_child);

        /// A placeholder in an `ALTER USER ... RENAME TO` clause is flattened by the parser into a
        /// plain string, leaving no AST node for the matcher to bind or for the substitution to
        /// replace, so the rule could never work as intended.
        for (const auto & template_query : {query.source_query, query.resulting_query})
            if (auto flattened = findFlattenedQueryParameterInUserName(template_query))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                    "Rewrite rule `{}` uses query parameter `{}` in an ALTER USER ... RENAME TO "
                    "clause; the parser flattens that name into a plain string, so such placeholders "
                    "are not supported",
                    query.rule_name, *flattened);

        std::unordered_set<String> source_parameters;
        std::vector<String> source_duplicates;
        collectQueryParameters(query.source_query, source_parameters, source_duplicates);

        /// A placeholder appearing more than once in the source template is accepted by
        /// the parser but later throws `REWRITE_RULE_DUPLICATED_QUERY_PARAMETER` during
        /// matching, so reject it up front.
        if (!source_duplicates.empty())
            throw Exception(
                ErrorCodes::REWRITE_RULE_DUPLICATED_QUERY_PARAMETER,
                "Rewrite rule `{}` has a duplicate query parameter `{}` in its source template",
                query.rule_name, source_duplicates.front());

        /// A placeholder in the source template whose type the matcher does not
        /// understand (anything other than `String`, `Int`, `Expression`,
        /// `ExpressionList` or `Subquery`) would be stored but never match any query,
        /// silently turning the rule into a no-op. Reject such types up front.
        if (auto unsupported = findUnsupportedQueryParameter(query.source_query))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with unsupported type `{}` in its source template. "
                "Supported placeholder types are: String, Int, Expression, ExpressionList, Subquery",
                query.rule_name, unsupported->first, unsupported->second);

        if (!query.rewrite())
            return;

        /// A placeholder in the result template whose type the substitution does not
        /// understand (anything other than `String`, `Int`, `Expression`, `ExpressionList`
        /// or `Subquery`) — for example `{t:Identifier}` — would be substituted as a raw
        /// captured node into an incompatible position, leaving a malformed AST that fails
        /// at execution time. Reject such types up front, symmetrically with the source
        /// template.
        if (auto unsupported = findUnsupportedQueryParameter(query.resulting_query))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with unsupported type `{}` in its result template. "
                "Supported placeholder types are: String, Int, Expression, ExpressionList, Subquery",
                query.rule_name, unsupported->first, unsupported->second);

        /// Every placeholder referenced by the result template must be captured by the
        /// source template, otherwise matching queries throw
        /// `REWRITE_RULE_UNKNOWN_QUERY_PARAMETER` at execution time.
        std::unordered_set<String> result_parameters;
        std::vector<String> result_duplicates;
        collectQueryParameters(query.resulting_query, result_parameters, result_duplicates);
        for (const auto & name : result_parameters)
            if (!source_parameters.contains(name))
                throw Exception(
                    ErrorCodes::REWRITE_RULE_UNKNOWN_QUERY_PARAMETER,
                    "Rewrite rule `{}` references query parameter `{}` in its result template "
                    "that is not captured by its source template",
                    query.rule_name, name);

        /// A placeholder reused in the result template must declare the same type as in the
        /// source template. Matching captures by name and ignores the result-side type, so a
        /// disagreeing type (e.g. source `{x:String}`, result `{x:Int}`) would substitute a
        /// captured string literal into an `{x:Int}` position, producing an AST that fails at
        /// execution time. Reject such mismatches up front.
        std::unordered_map<String, String> source_types;
        collectQueryParameterTypes(query.source_query, source_types);
        if (auto mismatch = findResultParameterTypeMismatch(query.resulting_query, source_types))
            throw Exception(
                ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                "Rewrite rule `{}` uses query parameter `{}` with type `{}` in its result template "
                "but type `{}` in its source template; a placeholder must use the same type in both",
                query.rule_name, mismatch->name, mismatch->result_type, mismatch->source_type);
    }
}

RewriteRules & RewriteRules::instance()
{
    static RewriteRules instance;
    return instance;
}

RewriteRules::~RewriteRules()
{
    shutdown();
}

void RewriteRules::shutdown()
{
    shutdown_called = true;
    /// Only deactivate the task here; do not reset the holder. `deactivate` waits for any
    /// in-flight `updateFunc` to return and prevents further runs, but the watcher's tail still
    /// reads `update_task` (`operator bool` / `operator->` when rescheduling). Reassigning the
    /// holder here would race with that unsynchronized read. Destruction of the holder is left
    /// to the owner (the destructor, which runs after this `shutdown`), by which point no
    /// `updateFunc` can be running.
    if (update_task)
        update_task->deactivate();
    std::lock_guard lock(mutex);
    storage.reset();
}

bool RewriteRules::exists(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return exists(rule_name, lock);
}

RewriteRuleObjectPtr RewriteRules::get(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    auto rule = tryGet(rule_name, lock);
    if (!rule)
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "There is no rewrite rule `{}`",
            rule_name);
    }
    return rule;
}

RewriteRuleObjectPtr RewriteRules::tryGet(const std::string & rule_name) const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return tryGet(rule_name, lock);
}

RewriteRuleObjectsList RewriteRules::getAll() const
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return loaded_rewrite_rules;
}

bool RewriteRules::exists(const std::string & rule_name, std::lock_guard<std::mutex> &) const
{
    return std::any_of(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == rule_name; });
}

MutableRewriteRuleObjectPtr RewriteRules::tryGet(
    const std::string & rule_name,
    std::lock_guard<std::mutex> &) const
{
    auto it = std::find_if(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == rule_name; });
    if (it != loaded_rewrite_rules.end())
        return it->second;
    return nullptr;
}

MutableRewriteRuleObjectPtr RewriteRules::getMutable(
    const std::string & rule_name,
    std::lock_guard<std::mutex> & lock) const
{
    auto rule = tryGet(rule_name, lock);
    if (!rule)
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "There is no rewrite rule `{}`",
            rule_name);
    }
    return rule;
}

void RewriteRules::add(
    const std::string & rule_name,
    MutableRewriteRuleObjectPtr rule,
    std::lock_guard<std::mutex> & lock)
{
    if (exists(rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
            "A rewrite rule `{}` already exists",
            rule_name);
    }
    loaded_rewrite_rules.emplace_back(rule_name, std::move(rule));
}

void RewriteRules::add(RewriteRuleObjectsList rules, std::lock_guard<std::mutex> & lock)
{
    for (auto & [rule_name, rule] : rules)
        add(rule_name, std::move(rule), lock);
}

void RewriteRules::remove(const std::string & rule_name, std::lock_guard<std::mutex> &)
{
    std::erase_if(
        loaded_rewrite_rules,
        [&](const auto & entry) { return entry.first == rule_name; });
}

void RewriteRules::createRule(const ASTCreateRewriteRuleQuery & query)
{
    validateRuleTemplates(query);
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    if (exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_ALREADY_EXISTS,
            "A rewrite rule `{}` already exists",
            query.rule_name);
    }
    auto ptr = RewriteRuleObject::create(query);
    storage->create(ptr);
    add(query.rule_name, std::move(ptr), lock);
}

void RewriteRules::removeRule(const ASTDropRewriteRuleQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    if (!exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "A rewrite rule `{}` doesn't exists",
            query.rule_name);
    }
    storage->remove(query.rule_name);
    remove(query.rule_name, lock);
}

void RewriteRules::updateRule(const ASTAlterRewriteRuleQuery & query)
{
    validateRuleTemplates(query);
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    if (!exists(query.rule_name, lock))
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
            "A rewrite rule `{}` doesn't exists",
            query.rule_name);
    }

    auto it = std::find_if(
        loaded_rewrite_rules.begin(), loaded_rewrite_rules.end(),
        [&](const auto & entry) { return entry.first == query.rule_name; });
    if (it == loaded_rewrite_rules.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The rewrite rule {} unexpectedly does not exist.",
            query.rule_name);
    }
    auto ptr = RewriteRuleObject::create(query);
    storage->update(ptr);
    it->second = std::move(ptr);
}

/// Rules loaded from persisted storage bypass the `CREATE RULE` / `ALTER RULE` entrypoints,
/// so a template that would be rejected there (written directly into the storage, or persisted
/// before a screening rule was introduced) could otherwise become active again after a restart
/// or reload. Re-screen on load and deactivate (fail closed) every rule that no longer passes:
/// it stays visible in `system.query_rules` and can be dropped, but the matcher refuses to
/// apply it.
void RewriteRules::screenLoadedRules(RewriteRuleObjectsList & rules) const
{
    for (auto & [rule_name, rule] : rules)
    {
        try
        {
            validateRuleTemplates(rule->getCreateQuery());
        }
        catch (const Exception & e)
        {
            rule->rejectOnLoad(e.message());
            LOG_ERROR(
                log,
                "Rewrite rule `{}` loaded from storage failed template validation and will not be "
                "applied; drop or recreate the rule to resolve this: {}",
                rule_name, e.message());
        }
    }
}

bool RewriteRules::loadIfNot(std::lock_guard<std::mutex> & lock) const
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    storage = RewriteRulesStorage::create(context);
    auto rules = storage->getAll();
    screenLoadedRules(rules);
    /// `add` is non-const but only mutates `mutable` `loaded_rewrite_rules`.
    const_cast<RewriteRules *>(this)->add(std::move(rules), lock);

    if (storage->isReplicated())
    {
        auto * self = const_cast<RewriteRules *>(this);
        update_task = context->getSchedulePool()->createTask(
            StorageID::createEmpty(),
            "RewriteRuleReplicatedStorage",
            [self]{ self->updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

bool RewriteRules::loadIfNot()
{
    std::lock_guard lock(mutex);
    return loadIfNot(lock);
}

void RewriteRules::reload()
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;
    if (!storage)
        return;
    /// For replicated storage, updates are picked up by the background watcher
    /// thread (`updateFunc`). Refreshing here would race with `waitUpdate`,
    /// which mutates storage state outside of `mutex`.
    if (storage->isReplicated())
        return;
    reloadImpl(lock);
}

void RewriteRules::reloadImpl(std::lock_guard<std::mutex> & lock)
{
    if (!storage)
        return;
    auto rules = storage->getAll();
    screenLoadedRules(rules);
    loaded_rewrite_rules.clear();
    add(std::move(rules), lock);
}

void RewriteRules::updateFunc()
{
    LOG_TRACE(log, "Rewrite/query rules background updating thread started");

    try
    {
        RewriteRulesStorage * storage_ptr = nullptr;
        {
            std::lock_guard lock(mutex);
            if (shutdown_called.load() || !storage)
                return;
            storage_ptr = storage.get();
        }

        if (storage_ptr->waitUpdate())
        {
            std::lock_guard lock(mutex);
            if (shutdown_called.load() || !storage)
                return;
            reloadImpl(lock);
        }
    }
    catch (const Coordination::Exception & e)
    {
        if (Coordination::isHardwareError(e.code))
        {
            LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}",
                    DB::getCurrentExceptionMessage(true));
        }
        else
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
        }
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false);
    }

    if (!shutdown_called.load() && update_task)
        update_task->scheduleAfter(1000);

    LOG_TRACE(log, "Rewrite/query rules background updating thread finished");
}

}
