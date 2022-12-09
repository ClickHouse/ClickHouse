#include <Interpreters/Access/InterpreterShowAccessQuery.h>

#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/Access/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Columns/ColumnString.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Access/Common/AccessFlags.h>
#include <Access/AccessControl.h>
#include <base/range.h>
#include <base/sort.h>
#include <base/insertAtEnd.h>


namespace DB
{

BlockIO InterpreterShowAccessQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowAccessQuery::executeImpl() const
{
    /// Build a create query.
    ASTs queries = getCreateAndGrantQueries();

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


ASTs InterpreterShowAccessQuery::getCreateAndGrantQueries() const
{
    auto entities = getEntities();
    const auto & access_control = getContext()->getAccessControl();

    ASTs create_queries, grant_queries;
    for (const auto & entity : entities)
    {
        create_queries.push_back(InterpreterShowCreateAccessEntityQuery::getCreateQuery(*entity, access_control));
        if (entity->isTypeOf(AccessEntityType::USER) || entity->isTypeOf(AccessEntityType::ROLE))
            insertAtEnd(grant_queries, InterpreterShowGrantsQuery::getGrantQueries(*entity, access_control));
    }

    ASTs result = std::move(create_queries);
    insertAtEnd(result, std::move(grant_queries));
    return result;
}

}
