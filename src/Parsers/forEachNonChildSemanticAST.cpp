#include <Parsers/forEachNonChildSemanticAST.h>

#include <Core/Field.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowColumnsQuery.h>
#include <Parsers/ASTShowIndexesQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/IAST.h>

#include <string_view>
#include <type_traits>

namespace DB
{

namespace
{
    /// One body for both overloads: `Node` is `IAST` or `const IAST`, and the members come out
    /// with the matching constness, so the mutable overload can hand the caller a reference it
    /// may reassign (e.g. replacing an `ASTQueryParameter` member with the substituted literal).
    template <typename Node, typename Visit>
    void forEachNonChildSemanticASTImpl(Node & node, const Visit & visit)
    {
        auto visit_if = [&](auto & member)
        {
            if (!member)
                return;
            using Member = std::remove_reference_t<decltype(member)>;
            if constexpr (std::is_same_v<std::remove_const_t<Member>, ASTPtr>)
            {
                visit(member);
            }
            else
            {
                /// A member typed as a pointer to a concrete AST class (e.g. `names`). Such a
                /// node can never itself be an `ASTQueryParameter`, so handing the visitor a
                /// temporary `ASTPtr` copy is enough: the visitor descends into the shared
                /// object, and never needs to reassign the member.
                ASTPtr as_base = member;
                visit(as_base);
            }
        };

        if (auto * show_tables = node.template as<ASTShowTablesQuery>())
        {
            visit_if(show_tables->where_expression);
            visit_if(show_tables->limit_length);
        }
        else if (auto * show_columns = node.template as<ASTShowColumnsQuery>())
        {
            visit_if(show_columns->where_expression);
            visit_if(show_columns->limit_length);
        }
        else if (auto * show_indexes = node.template as<ASTShowIndexesQuery>())
        {
            visit_if(show_indexes->where_expression);
        }
        else if (auto * backup = node.template as<ASTBackupQuery>())
        {
            visit_if(backup->settings);
            visit_if(backup->cluster_host_ids);
            for (auto & element : backup->elements)
                if (element.partitions)
                    for (auto & partition : *element.partitions)
                        visit_if(partition);
        }
        else if (auto * row_policy = node.template as<ASTCreateRowPolicyQuery>())
        {
            for (auto & filter_pair : row_policy->filters)
                visit_if(filter_pair.second);
        }
        else if (auto * masking_policy = node.template as<ASTCreateMaskingPolicyQuery>())
        {
            visit_if(masking_policy->update_assignments);
            visit_if(masking_policy->where_condition);
        }
        else if (auto * create_user = node.template as<ASTCreateUserQuery>())
        {
            /// The target user names of `CREATE USER` / `ALTER USER` are kept in the `names`
            /// member, outside `children`, and `ParserCreateUserQuery` explicitly accepts a query
            /// parameter there (`ParserUserNamesWithHost(/*allow_query_parameter=*/true)`), so
            /// `CREATE USER {u:Identifier}` carries an `ASTQueryParameter` that the generic
            /// `children` walks never see. The tree hash folds `names` in, so the walks must
            /// reach it too.
            visit_if(create_user->names);
        }
        else if (auto * set_query = node.template as<ASTSetQuery>())
        {
            /// A setting value in a `SETTINGS` clause can itself be an AST — for example the
            /// `{n:Int}` in `SETTINGS max_threads = {n:Int}`. The parser stores it as a `Field`
            /// wrapping the AST (`FieldFromASTImpl`, see `ParserSetQuery`), not as a child of the
            /// `ASTSetQuery`, so the generic `children` walks never see it (compare
            /// `QueryParameterVisitor::visitSetQuery` / `ReplaceQueryParameterVisitor::visitSettingsChanges`).
            /// The `Field` payload is an immutable shared object, so only the read-only overload
            /// descends here — see the header.
            if constexpr (std::is_const_v<Node>)
            {
                for (const auto & change : set_query->changes)
                {
                    CustomType custom;
                    if (change.value.template tryGet<CustomType>(custom) && std::string_view(custom.getTypeName()) == FieldFromASTImpl::name)
                        visit_if(dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast);
                }
            }
        }
    }
}

void forEachNonChildSemanticAST(const IAST & node, const std::function<void(const ASTPtr &)> & visit)
{
    forEachNonChildSemanticASTImpl(node, visit);
}

void forEachMutableNonChildSemanticAST(IAST & node, const std::function<void(ASTPtr &)> & visit)
{
    forEachNonChildSemanticASTImpl(node, visit);
}

}
