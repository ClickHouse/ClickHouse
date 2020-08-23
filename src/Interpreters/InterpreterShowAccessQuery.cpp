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
#include <Access/VisibleAccessEntities.h>
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
    std::stringstream ss;
    for (const auto & query : queries)
    {
        ss.str("");
        formatAST(*query, ss, false, true);
        column->insert(ss.str());
    }

    String desc = "ACCESS";
    return std::make_shared<OneBlockInputStream>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}});
}


std::vector<AccessEntityPtr> InterpreterShowAccessQuery::getEntities() const
{
    const auto & access_control = context.getAccessControlManager();
    VisibleAccessEntities visible_entities{context.getAccess()};

    std::vector<AccessEntityPtr> entities;
    for (auto type : ext::range(EntityType::MAX))
    {
        auto ids = visible_entities.findAll(type);
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

    ASTs create_queries, grant_queries;
    for (const auto & entity : entities)
    {
        create_queries.push_back(InterpreterShowCreateAccessEntityQuery::getCreateQuery(*entity, context));
        if (entity->isTypeOf(EntityType::USER) || entity->isTypeOf(EntityType::USER))
            boost::range::push_back(grant_queries, InterpreterShowGrantsQuery::getGrantQueries(*entity, context));
    }

    ASTs result = std::move(create_queries);
    boost::range::push_back(result, std::move(grant_queries));
    return result;
}

}
