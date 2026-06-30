#include <Interpreters/RewriteRulesASTTraversal.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Interpreters/ClientInfo.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <functional>
#include <queue>
#include <unordered_map>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsString query_rules;
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
}

namespace ErrorCodes
{
    extern const int REWRITE_RULE_REJECTION;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE;
    extern const int REWRITE_RULE_DOESNT_EXIST;
}

bool astTraversal(ASTPtr &ast, ContextPtr context, std::vector<String> & applied_rules)
{
    const auto& settings = context->getSettingsRef();
    if (!ast)
    {
        return false;
    }

    /// Rewrite rules transform the query the user submitted to the initiator. They must not
    /// be re-applied to a secondary query (a fragment sent to a shard during distributed
    /// execution): the fragment was already rewritten on the initiator, and a rule named in
    /// the `query_rules` setting may not even exist on the shard (rule storage is local by
    /// default). This is enforced on the initiator side, which strips `query_rules` from the
    /// settings it sends to shards (see `MultiplexedConnections::sendQuery` /
    /// `HedgedConnections::sendQuery`), so a genuine secondary query arrives with an empty
    /// `query_rules` and returns early at the check below.
    ///
    /// We deliberately do NOT skip here based on `ClientInfo::query_kind`: that value comes
    /// from the client and can be spoofed (`clickhouse-client --query_kind secondary_query`),
    /// so trusting it would let a user bypass a profile-enforced REWRITE/REJECT rule simply by
    /// labelling the query as a secondary one.

    /// `query_rules` lists the names of the active rewrite rules, applied in the listed
    /// order. By default it is empty and no rules are applied to the query. Check for the
    /// empty value before parsing: `parseIdentifiersOrStringLiterals` throws on an empty
    /// string rather than returning an empty list, and this runs for every query.
    const auto rules_setting = settings[Setting::query_rules].toString();
    if (rules_setting.empty())
    {
        return false;
    }
    auto active_rule_names = parseIdentifiersOrStringLiterals(rules_setting, settings);
    if (active_rule_names.empty())
    {
        return false;
    }

    /// Enforce the AST size/depth limits on the query as the user submitted it, before the
    /// matcher walks it and a rule can replace it. The generic post-rewrite limit check in
    /// `executeQuery` (`checkASTSizeLimits`) runs only after this function returns, so it sees
    /// the rewrite result, not the submitted query. Without this guard a rule could match an
    /// oversized source query and rewrite it to a tiny one, letting a user with a low
    /// `max_ast_depth` / `max_ast_elements` bypass the AST resource guard for the query they sent
    /// (and forcing the matcher to walk an unbounded tree). The rewrite result stays bounded by
    /// the post-rewrite check, and the rule templates themselves are bounded at `CREATE` / `ALTER`
    /// time (`checkRewriteRuleTemplateLimits`).
    if (const UInt64 max_ast_depth = settings[Setting::max_ast_depth])
        ast->checkDepth(max_ast_depth);
    if (const UInt64 max_ast_elements = settings[Setting::max_ast_elements])
        ast->checkSize(max_ast_elements);

    /// Build a name -> rule lookup once, then apply the requested rules in the order they
    /// are listed in `query_rules`. A listed rule that does not exist is an error, so a
    /// typo in `query_rules` fails the query instead of silently applying nothing.
    RewriteRuleObjectsList all_rules = RewriteRules::instance().getAll();
    std::unordered_map<std::string, RewriteRuleObjectPtr> rules_by_name;
    rules_by_name.reserve(all_rules.size());
    for (auto & [rule_name, rule_object] : all_rules)
        rules_by_name.emplace(rule_name, rule_object);

    for (const auto& name : active_rule_names)
    {
        auto rule_it = rules_by_name.find(name);
        if (rule_it == rules_by_name.end())
            throw Exception(
                ErrorCodes::REWRITE_RULE_DOESNT_EXIST,
                "Rewrite rule `{}` listed in the `query_rules` setting does not exist",
                name);
        const auto & rule = rule_it->second;
        const auto& query_rule = rule->getCreateQuery();
        std::queue<ASTPtr> queue_query;
        std::queue<ASTPtr> queue_rule;
        std::unordered_map<String, ASTPtr> matching_map;
        queue_query.push(ast);
        queue_rule.push(query_rule.source_query);
        bool is_template = true;
        while (!queue_query.empty())
        {
            if (queue_query.size() != queue_rule.size())
            {
                is_template = false;
                break;
            }
            auto top1 = queue_query.front();
            auto top2 = queue_rule.front();
            queue_query.pop();
            queue_rule.pop();
            if (top1->getTreeHash(true) != top2->getTreeHash(true))
            {
                auto hash1 = top1->getCurrentNodeHash(true);
                auto hash2 = top2->getCurrentNodeHash(true);
                // Second check exists because there are situations when ASTQueryParameter is embedded into ASTExpressionList
                if (hash1 != hash2 || (top2->as<ASTExpressionList>() && top2->children.size() == 1 && top2->children[0]->as<ASTQueryParameter>()))
                {
                    auto* query_parameter = top2->as<ASTQueryParameter>();
                    /// A query parameter can be embedded as the sole child of an
                    /// `ASTExpressionList` (for example the only projection in
                    /// `SELECT {x:String}`). In that case both `top1` and `top2` are the
                    /// wrapper, so unwrap `top2` to reach the parameter.
                    const bool wrapped_parameter = hash1 == hash2 && !top2->children.empty();
                    if (wrapped_parameter)
                    {
                        query_parameter = top2->children[0]->as<ASTQueryParameter>();
                    }
                    if (!query_parameter)
                    {
                        is_template = false;
                        break;
                    }
                    auto query_parameter_type = query_parameter->type;
                    trimRight(query_parameter_type);
                    trimLeft(query_parameter_type);
                    /// The query-side node to capture. For a wrapped parameter `top1` is the
                    /// `ASTExpressionList` wrapper: an `ExpressionList` placeholder captures
                    /// the whole wrapper, but a scalar (`String`/`Int`), `Expression` or
                    /// `Subquery` placeholder must capture the single inner node. Otherwise
                    /// the wrapper is bound, so either the `ASTLiteral` check below fails (and
                    /// `SELECT {x:String}` would not match `SELECT 'hello'`) or `applyRule`
                    /// substitutes an extra `ASTExpressionList` layer into the resulting query.
                    ASTPtr match_node = top1;
                    if (wrapped_parameter && query_parameter_type != "ExpressionList"
                        && top1->as<ASTExpressionList>())
                    {
                        /// A single-valued placeholder (`String`/`Int`/`Expression`/`Subquery`)
                        /// captures exactly one node. If the query side holds a multi-item
                        /// `ASTExpressionList` (for example `SELECT 1, 2`), there is no single
                        /// node to bind, so the rule must not match — otherwise `{e:Expression}`
                        /// would capture the whole `1, 2` list even though `Expression` is a
                        /// single expression (`ExpressionList` is the placeholder for a list).
                        if (top1->children.size() != 1)
                        {
                            is_template = false;
                            break;
                        }
                        match_node = top1->children[0];
                    }
                    auto add_to_matching_map = [&](ASTPtr cloned_ast)
                    {
                        if (matching_map.contains(query_parameter->name))
                        {
                            throw Exception(
                                ErrorCodes::REWRITE_RULE_DUPLICATED_QUERY_PARAMETER,
                                "Query parameter duplicate in rewrite rule template: {}\n",
                                query_parameter->name
                            );
                        }
                        matching_map.emplace(query_parameter->name, std::move(cloned_ast));
                    };
                    if (auto* literal = match_node->as<ASTLiteral>();
                        literal && ((query_parameter_type == "String" && literal->value.getType() == Field::Types::Which::String)
                        || (query_parameter_type == "Int" && Field::isDecimal(literal->value.getType()))
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt128)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt256)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt64)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int128)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int256)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int64)))
                    {
                        add_to_matching_map(literal->clone());
                    }
                    else if (query_parameter_type == "Expression")
                    {
                        /// An `Expression` placeholder captures a single arbitrary expression subtree.
                        add_to_matching_map(match_node->clone());
                    }
                    else if (auto* expression = match_node->as<ASTExpressionList>();
                        expression && query_parameter_type == "ExpressionList")
                    {
                        add_to_matching_map(expression->clone());
                    }
                    else if (auto* subquery = match_node->as<ASTSubquery>();
                        subquery && query_parameter_type == "Subquery")
                    {
                        add_to_matching_map(subquery->clone());
                    }
                    else
                    {
                        is_template = false;
                        break;
                    }
                    continue;
                }
                for (const auto& child : top1->children)
                {
                    queue_query.push(child);
                }
                for (const auto& child : top2->children)
                {
                    queue_rule.push(child);
                }
            }
            /// Otherwise the subtrees are identical (equal hash) and match as-is, with
            /// nothing to capture. Matching runs after query-parameter substitution, so the
            /// incoming query never carries a placeholder of its own — only the rule template
            /// does — and an equal-hash subtree therefore has no placeholder to bind here.
        }
        if (is_template)
        {
            applied_rules.push_back(name);
            /// `applyRule` rewrites `ast` in place, or throws `REWRITE_RULE_REJECTION` for a
            /// `REJECT` rule. `applied_rules` already records this rule (a rejecting rule is
            /// pushed before the throw), so the caller can log it in `system.query_log` even
            /// for a rejection.
            applyRule(ast, rule, matching_map);
        }
    }

    return true;
}


void checkRewriteRuleTemplateLimits(const IAST & ast, const Settings & settings)
{
    const UInt64 max_ast_depth = settings[Setting::max_ast_depth];
    const UInt64 max_ast_elements = settings[Setting::max_ast_elements];
    if (!max_ast_depth && !max_ast_elements)
        return;

    auto check_limits = [&](const ASTPtr & node)
    {
        if (!node)
            return;
        if (max_ast_depth)
            node->checkDepth(max_ast_depth);
        if (max_ast_elements)
            node->checkSize(max_ast_elements);
    };

    /// `checkDepth` / `checkSize` walk only `IAST::children`. A `CREATE RULE` / `ALTER RULE`
    /// keeps its own source and result templates outside `children`, so the generic
    /// `checkASTSizeLimits` walk in `executeQuery` never reaches them — even when the rule DDL
    /// is nested below a wrapper, e.g. `EXPLAIN AST CREATE RULE inner AS (SELECT <huge>)
    /// REWRITE TO (SELECT 1)`. Walk the whole AST through ordinary `children` (so wrappers are
    /// covered) and, whenever a rule DDL node is found, check its template fields explicitly,
    /// recursing into them since they may contain further nested rule DDL. Running this as part
    /// of the generic pre-execution AST limit gate (before access checks and interpreter
    /// dispatch) keeps an oversized or very deep template from bypassing the limits and being
    /// persisted, including via wrappers that never reach the rule interpreter.
    std::function<void(const IAST &)> walk = [&](const IAST & node)
    {
        const ASTPtr * nested_source = nullptr;
        const ASTPtr * nested_result = nullptr;
        if (const auto * create_rule = node.as<ASTCreateRewriteRuleQuery>())
        {
            nested_source = &create_rule->source_query;
            nested_result = &create_rule->resulting_query;
        }
        else if (const auto * alter_rule = node.as<ASTAlterRewriteRuleQuery>())
        {
            nested_source = &alter_rule->source_query;
            nested_result = &alter_rule->resulting_query;
        }
        if (nested_source)
        {
            check_limits(*nested_source);
            check_limits(*nested_result);
            if (*nested_source)
                walk(**nested_source);
            if (*nested_result)
                walk(**nested_result);
        }
        for (const auto & child : node.children)
            if (child)
                walk(*child);
    };

    walk(ast);
}


void applyRule(ASTPtr &ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map)
{
    const auto& query_rule = rule->getCreateQuery();
    if (query_rule.rewrite())
    {
        auto resulting_query = query_rule.resulting_query->clone();
        std::queue<ASTPtr> queue;
        queue.push(resulting_query);
        while (!queue.empty())
        {
            auto top = queue.front();
            queue.pop();

            /// Whether substituting an `ExpressionList` capture here means splicing its
            /// items into this node's child list (projection lists, function-argument
            /// lists) rather than inserting the captured list as a single child.
            const bool parent_is_list = top->as<ASTExpressionList>() != nullptr;

            ASTs new_children;
            new_children.reserve(top->children.size());
            /// Some AST nodes also keep typed pointers to a child (for example
            /// `ASTTableJoin::on_expression` or `ASTTableExpression::sample_size`). When a
            /// placeholder child is replaced by a single captured node, the corresponding
            /// typed pointer must be updated too, otherwise it keeps pointing at the old
            /// `ASTQueryParameter` and the rewritten query is still formatted/analyzed with
            /// the placeholder. Record such single-child replacements and fix the pointers
            /// after rebuilding `children`, mirroring `ReplaceQueryParameterVisitor::visitChildren`.
            std::vector<std::pair<const IAST *, ASTPtr>> replaced_children;
            for (auto & child : top->children)
            {
                auto * query_parameter = child->as<ASTQueryParameter>();
                if (!query_parameter)
                {
                    /// Not a placeholder: keep the node and descend into it so that
                    /// placeholders nested deeper are substituted too.
                    queue.push(child);
                    new_children.push_back(child);
                    continue;
                }

                auto it = matching_map.find(query_parameter->name);
                if (it == matching_map.end())
                {
                    throw Exception(
                        ErrorCodes::REWRITE_RULE_UNKNOWN_QUERY_PARAMETER,
                        "Resulting rewrite rule template contains unknown query parameter: {}\n",
                        query_parameter->name
                    );
                }

                /// Clone instead of moving so the same parameter can be substituted in
                /// multiple positions of the resulting query template.
                ASTPtr captured = it->second->clone();

                if (auto * captured_list = captured->as<ASTExpressionList>())
                {
                    /// An `ExpressionList` capture holds the matched `ASTExpressionList`.
                    /// Inside another `ASTExpressionList` its items must be spliced into
                    /// the parent list, not nested as one child — otherwise the analyzer
                    /// builds a single projection entry from the whole captured list (so
                    /// `SELECT {l:ExpressionList}, 3` over `SELECT 1, 2` would yield
                    /// `[ASTExpressionList(1, 2), 3]` rather than `[1, 2, 3]`).
                    if (!parent_is_list)
                        throw Exception(
                            ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                            "Resulting rewrite rule template substitutes the ExpressionList query "
                            "parameter `{}` where a single expression is expected\n",
                            query_parameter->name
                        );

                    /// Splicing only happens inside an `ASTExpressionList`, which keeps no
                    /// typed pointers to its items, so no pointer fix-up is needed here.
                    for (auto & item : captured_list->children)
                        new_children.push_back(item);
                }
                else
                {
                    replaced_children.emplace_back(child.get(), captured);
                    new_children.push_back(std::move(captured));
                }
            }
            top->children = std::move(new_children);
            /// `child.get()` above is still a live address: any typed pointer that referenced
            /// the replaced placeholder keeps it alive, so this only rewrites those pointers.
            for (const auto & [old_ptr, new_ptr] : replaced_children)
                top->updatePointerToChild(old_ptr, new_ptr);
        }
        ast = std::move(resulting_query);
    } else if (query_rule.reject())
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_REJECTION,
            "Query was rejected by {} with message: {}\n",
            query_rule.rule_name, query_rule.reject_message
        );
    }
}

};
