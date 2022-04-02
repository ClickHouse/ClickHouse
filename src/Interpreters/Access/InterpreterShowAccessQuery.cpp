#include <Interpreters/Access/InterpreterShowAccessQuery.h>

#include <Parsers/formatAST.h>
#include <Parsers/Access/ASTShowAccessQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/Access/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Columns/ColumnString.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Access/Common/AccessFlags.h>
#include <Access/AccessControl.h>
#include <Access/RBACVersion.h>
#include <Parsers/ASTSetQuery.h>
#include <base/range.h>
#include <base/sort.h>
#include <base/insertAtEnd.h>


namespace DB
{

namespace
{
    ASTPtr getSetRBACVersionQuery()
    {
        auto query = std::make_shared<ASTSetQuery>();
        query->changes.emplace_back(RBACVersion::SETTING_NAME, RBACVersion::LATEST);
        return query;
    }
}

BlockIO InterpreterShowAccessQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowAccessQuery::executeImpl() const
{
    /// Build a create query.
    ASTs queries = getQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    WriteBufferFromOwnString buf;
    for (const auto & query : queries)
    {
        buf.restart();
        formatAST(*query, buf, false, true);
        column->insert(buf.str());
    }

    String desc = "ACCESS";
    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}}));
}


std::vector<AccessEntityPtr> InterpreterShowAccessQuery::getEntities() const
{
    const auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(AccessType::SHOW_ACCESS);

    std::vector<AccessEntityPtr> entities;
    for (auto type : collections::range(AccessEntityType::MAX))
    {
        auto ids = access_control.findAll(type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
                entities.push_back(entity);
        }
    }

    ::sort(entities.begin(), entities.end(), IAccessEntity::LessByTypeAndName{});
    return entities;
}


ASTs InterpreterShowAccessQuery::getQueries() const
{
    auto entities = getEntities();
    const auto & access_control = getContext()->getAccessControl();

    const auto & show_query = query_ptr->as<const ASTShowAccessQuery &>();
    bool show_rbac_version = show_query.show_rbac_version;

    ASTs queries;
    if (show_rbac_version)
        queries.emplace_back(getSetRBACVersionQuery());

    for (const auto & entity : entities)
    {
        auto new_queries = InterpreterShowCreateAccessEntityQuery::getCreateQueries(
            *entity, access_control, /* show_rbac_version = */ false, /* show_grants = */ true);
        queries.insert(queries.end(), new_queries.begin(), new_queries.end());
    }

    return queries;
}

}
