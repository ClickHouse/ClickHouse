#include <Interpreters/InterpreterShowAccessQuery.h>

#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/InterpreterShowGrantsQuery.h>
#include <Columns/ColumnString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Access/AccessFlags.h>
#include <Access/AccessControlManager.h>
#include <ext/range.h>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
using EntityType = IAccessEntity::Type;


BlockIO InterpreterShowAccessQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


BlockInputStreamPtr InterpreterShowAccessQuery::executeImpl() const
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
    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


std::vector<AccessEntityPtr> InterpreterShowAccessQuery::getEntities() const
{
    const auto & access_control = context.getAccessControlManager();
    context.checkAccess(AccessType::SHOW_ACCESS);

    std::vector<AccessEntityPtr> entities;
    for (auto type : ext::range(EntityType::MAX))
    {
        auto ids = access_control.findAll(type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
                entities.push_back(entity);
        }
    }

    boost::range::sort(entities, IAccessEntity::LessByTypeAndName{});
    return entities;
}


ASTs InterpreterShowAccessQuery::getCreateAndGrantQueries() const
{
    auto entities = getEntities();
    const auto & access_control = context.getAccessControlManager();

    ASTs create_queries, grant_queries;
    for (const auto & entity : entities)
    {
        create_queries.push_back(InterpreterShowCreateAccessEntityQuery::getCreateQuery(*entity, access_control));
        if (entity->isTypeOf(EntityType::USER) || entity->isTypeOf(EntityType::ROLE))
            boost::range::push_back(grant_queries, InterpreterShowGrantsQuery::getGrantQueries(*entity, access_control));
    }

    ASTs result = std::move(create_queries);
    boost::range::push_back(result, std::move(grant_queries));
    return result;
}

}
