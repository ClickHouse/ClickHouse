#include "AggregateHandler.h"
#include "QueryBuilder.h"

namespace DB
{
namespace MongoDB
{

enum AggregateStageType
{
    GROUP,
    SORT,
    LIMIT,
    PROJECT,
    MATCH,
    SKIP,
    COUNT,
    UNKNOWN // the last
};

AggregateStageType stringToAggregateStageType(const std::string & value)
{
    static std::unordered_map<std::string, AggregateStageType> convetion_map{
        {"$group", GROUP},
        {"$sort", SORT},
        {"$limit", LIMIT},
        {"$project", PROJECT},
        {"$match", MATCH},
        {"$skip", SKIP},
        {"$count", COUNT}};
    auto it = convetion_map.find(value);
    if (it == convetion_map.end())
        return AggregateStageType::UNKNOWN;
    return (*it).second;
}


static void parsePipeline(QueryBuilder & builder, BSON::Array::Ptr pipeline)
{
    LOG_DEBUG(getLogger("MongoDB::parsePipeline"), "Pipeline is: {}", pipeline->toString());
    auto stages_cnt = pipeline->size();
    bool having = false;
    for (size_t i = 0; i < stages_cnt; i++)
    {
        auto doc = pipeline->get<BSON::Document::Ptr>(i);
        assert(doc->size() == 1);
        const auto & element_names = doc->elementNames();
        const auto & stage_name = element_names[0];
        LOG_DEBUG(getLogger("MongoDB::parsePipeline"), "New stage: {}", stage_name);
        switch (stringToAggregateStageType(stage_name))
        {
            case GROUP:
                having = true;
                builder.handleGroup(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case SORT:
                builder.handleSort(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case LIMIT:
                builder.setLimit(doc->get<Int32>(stage_name));
                break;
            case PROJECT:
                builder.handleProject(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case MATCH:
                builder.handleWhere(doc->get<BSON::Document::Ptr>(stage_name), having);
                break;
            case SKIP:
                builder.setOffset(doc->get<Int32>(stage_name));
                break;
            case COUNT:
                builder.handleCount(doc->get<std::string>(stage_name));
                break;
            case UNKNOWN: {
                throw Poco::NotImplementedException("Unknown command");
            }
        }
    }
}

static std::string translateQuery(const std::string & collection_name, BSON::Array::Ptr pipeline, std::vector<std::string> && columns)
{
    auto builder = QueryBuilder(collection_name, std::move(columns));
    parsePipeline(builder, pipeline);
    return std::move(builder).buildQuery();
}


BSON::Document::Ptr handleAggregate(const Command::Ptr command, ContextMutablePtr context)
{
    BSON::Array::Ptr pipeline = command->getExtra()->get<BSON::Array::Ptr>("pipeline");
    const auto & collection_name = command->getCollectionName();

    auto columns = getColumnsFromTable(context, collection_name);

    auto query = translateQuery(collection_name, pipeline, std::move(columns));
    return launchQuery(std::move(query), context, command->getDBName(), collection_name);
}

}
}
