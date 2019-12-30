#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuotaQuery.h>
#include <Parsers/ASTCreateRowPolicyQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/QuotaContext.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <ext/range.h>
#include <sstream>


namespace DB
{
BlockIO InterpreterShowCreateAccessEntityQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowCreateAccessEntityQuery::executeImpl()
{
    const auto & show_query = query_ptr->as<ASTShowCreateAccessEntityQuery &>();

    /// Build a create query.
    ASTPtr create_query = getCreateQuery(show_query);

    /// Build the result column.
    std::stringstream create_query_ss;
    formatAST(*create_query, create_query_ss, false, true);
    String create_query_str = create_query_ss.str();
    MutableColumnPtr column = ColumnString::create();
    column->insert(create_query_str);

    /// Prepare description of the result column.
    std::stringstream desc_ss;
    formatAST(show_query, desc_ss, false, true);
    String desc = desc_ss.str();
    String prefix = "SHOW ";
    if (startsWith(desc, prefix))
        desc = desc.substr(prefix.length()); /// `desc` always starts with "SHOW ", so we can trim this prefix.

    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuery(const ASTShowCreateAccessEntityQuery & show_query) const
{
    using Kind = ASTShowCreateAccessEntityQuery::Kind;
    switch (show_query.kind)
    {
        case Kind::QUOTA: return getCreateQuotaQuery(show_query);
        case Kind::ROW_POLICY: return getCreateRowPolicyQuery(show_query);
    }
    __builtin_unreachable();
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateQuotaQuery(const ASTShowCreateAccessEntityQuery & show_query) const
{
    auto & access_control = context.getAccessControlManager();

    QuotaPtr quota;
    if (show_query.current_quota)
        quota = access_control.read<Quota>(context.getQuota()->getUsageInfo().quota_id);
    else
        quota = access_control.read<Quota>(show_query.name);

    auto create_query = std::make_shared<ASTCreateQuotaQuery>();
    create_query->name = quota->getName();
    create_query->key_type = quota->key_type;
    create_query->all_limits.reserve(quota->all_limits.size());

    for (const auto & limits : quota->all_limits)
    {
        ASTCreateQuotaQuery::Limits create_query_limits;
        create_query_limits.duration = limits.duration;
        create_query_limits.randomize_interval = limits.randomize_interval;
        for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
            if (limits.max[resource_type])
                create_query_limits.max[resource_type] = limits.max[resource_type];
        create_query->all_limits.push_back(create_query_limits);
    }

    if (!quota->roles.empty() || quota->all_roles)
    {
        auto create_query_roles = std::make_shared<ASTRoleList>();
        create_query_roles->roles = quota->roles;
        create_query_roles->all_roles = quota->all_roles;
        create_query_roles->except_roles = quota->except_roles;
        create_query->roles = std::move(create_query_roles);
    }

    return create_query;
}


ASTPtr InterpreterShowCreateAccessEntityQuery::getCreateRowPolicyQuery(const ASTShowCreateAccessEntityQuery & show_query) const
{
    auto & access_control = context.getAccessControlManager();
    RowPolicyPtr policy = access_control.read<RowPolicy>(show_query.row_policy_name.getFullName(context));

    auto create_query = std::make_shared<ASTCreateRowPolicyQuery>();
    create_query->name_parts = RowPolicy::FullNameParts{policy->getDatabase(), policy->getTableName(), policy->getName()};
    if (policy->isRestrictive())
        create_query->is_restrictive = policy->isRestrictive();

    for (auto index : ext::range_with_static_cast<RowPolicy::ConditionIndex>(RowPolicy::MAX_CONDITION_INDEX))
    {
        const auto & condition = policy->conditions[index];
        if (!condition.empty())
        {
            ParserExpression parser;
            ASTPtr expr = parseQuery(parser, condition, 0);
            create_query->conditions.push_back(std::pair{index, expr});
        }
    }

    if (!policy->roles.empty() || policy->all_roles)
    {
        auto create_query_roles = std::make_shared<ASTRoleList>();
        create_query_roles->roles = policy->roles;
        create_query_roles->all_roles = policy->all_roles;
        create_query_roles->except_roles = policy->except_roles;
        create_query->roles = std::move(create_query_roles);
    }

    return create_query;
}
}
