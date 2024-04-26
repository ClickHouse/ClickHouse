#pragma once


#include <stdexcept>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Common/CurrentThread.h>
#include "../Binary.h"
#include "../Document.h"
#include "../Element.h"
#include "Commands.h"

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

class AggregateBuilder
{
public:
    AggregateBuilder(std::string collection_name_, BSON::Array::Ptr pipeline_) : collection_name(collection_name_), pipeline(pipeline_)
    {
        parsePipeline();
    }


    void parsePipeline();

    /// adders
    void addToSelect(const std::string & name, bool status);

    void addToSelect(const std::string & name, BSON::Document::Ptr value);

    void addToGroupBy(const std::string & name);

    void addToOrderBy(const std::string & expr);

    void setLimit(Int32 limit_);

    void addWhere(const std::string & expr);

    void setOffset(Int32 offset);

    /// handlers
    void handleGroup(BSON::Document::Ptr group);
    void handleSort(BSON::Document::Ptr sort);
    void handleProject(BSON::Document::Ptr project);
    void handleMatchSingleColumn(const std::string & column_name, BSON::Element::Ptr payload);
    void handleMatch(BSON::Document::Ptr match);
    void handleCount(const std::string & new_col_name);


    /// builders
    std::string buildSelect() const;
    std::string buildGroupBy() const;
    std::string buildOrderBy() const;
    std::string buildLimit() const;
    std::string buildWhere() const;
    std::string buildOffset() const;

    std::string buildQuery() const;

private:
    std::string collection_name;
    BSON::Array::Ptr pipeline;

    ProjectionMap select_map;
    std::vector<std::string> new_columns;
    bool count_expr = false;

    std::vector<std::string> group_by;
    std::unordered_map<std::string, std::string> group_name_map;

    std::vector<std::string> order_by;

    std::vector<std::string> where;

    std::optional<Int32> limit;
    std::optional<Int32> offset;
};

inline void AggregateBuilder::addToSelect(const std::string & name, bool status)
{
    select_map.add(name, status);
}

void AggregateBuilder::addToSelect(const std::string & name, BSON::Document::Ptr value)
{
    static std::vector<std::pair<std::string, std::string>> supported_opers
        = {{"$sum", "SUM"}, {"$avg", "AVG"}, {"$toLong", "toInt64"}, {"$count", "COUNT"}};

    LOG_DEBUG(getLogger("MongoDB::addToSelect"), "start addToSelect, value: {}", value->toString());
    for (const auto & operation : supported_opers)
    {
        const auto & [raw_oper_name, oper_name] = operation;
        if (value->exists(raw_oper_name))
        {
            LOG_DEBUG(getLogger("MongoDB::addToSelect"), "handling raw_oper_name: {}", raw_oper_name);
            std::string column_name;
            try
            {
                column_name = value->get<std::string>(raw_oper_name);
                if (column_name[0] == '$')
                    column_name.erase(column_name.begin()); // $
            }
            catch (const Poco::BadCastException &)
            {
                // Int32, for example SUM(1)
                column_name = std::to_string(value->get<Int32>(raw_oper_name));
            }
            if (raw_oper_name == "$count")
            {
                count_expr = true;
                new_columns.push_back(column_name);
            }
            else
                new_columns.push_back(fmt::format("{}({}) AS {}", oper_name, column_name, name));
            LOG_DEBUG(getLogger("MongoDB::addToSelect"), "added new select statement: {}", new_columns[new_columns.size() - 1]);
            return;
        }
    }
    throw Poco::NotImplementedException("Couldnt find supported operation");
}

inline void AggregateBuilder::addToGroupBy(const std::string & name)
{
    std::string name_ = name;
    if (name_[0] == '$')
        name_.erase(name_.begin());
    group_by.push_back(std::move(name_));
}

inline void AggregateBuilder::addToOrderBy(const std::string & name)
{
    std::string name_ = name;
    if (name_[0] == '$')
        name_.erase(name_.begin());
    order_by.push_back(std::move(name_));
}

inline void AggregateBuilder::setLimit(Int32 limit_)
{
    limit = limit_;
}

inline void AggregateBuilder::setOffset(Int32 offset_)
{
    offset = offset_;
}

inline void AggregateBuilder::addWhere(const std::string & expr)
{
    where.push_back(expr);
}

void AggregateBuilder::handleGroup(BSON::Document::Ptr group)
{
    LOG_DEBUG(getLogger("MongoDB::handleGroup"), "start handling group: {}", group->toString());
    // id
    BSON::Element::Ptr _id = group->get("_id");
    if (_id->getType() == BSON::ElementTraits<std::string>::TypeId)
    {
        const auto & value = group->takeValue<std::string>("_id");
        static const std::string id_prefix = "$_id";
        if (value.substr(0, id_prefix.size()) == id_prefix)
        {
            // previously already had group stage
            auto name = value.substr(id_prefix.size() + 1, value.size() - (id_prefix.size() + 1));
            addToGroupBy(group_name_map.at(name));
        }
        else
        {
            addToGroupBy(value);
            LOG_DEBUG(getLogger("MongoDB::handleGroup"), "id is string, simple groupping");
        }
    }
    else if (_id->getType() == BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
    {
        // will have group stage in future
        auto fields = group->takeValue<BSON::Document::Ptr>("_id");
        const auto & field_names = fields->elementNames();
        for (const auto & name : field_names)
            group_name_map.insert({name, fields->get<std::string>(name)});
    }


    // new columns
    const auto & column_names = group->elementNames();
    for (const auto & column_name : column_names)
    {
        LOG_DEBUG(getLogger("MongoDB::handleGroup"), "adding column_name: {}", column_name);
        if (column_name == "_id")
        {
            // in case '_id' was not removed previously
            continue;
        }
        addToSelect(column_name, group->get<BSON::Document::Ptr>(column_name));
    }
}

void AggregateBuilder::handleSort(BSON::Document::Ptr sort)
{
    const auto & element_names = sort->elementNames();
    for (const auto & name : element_names)
    {
        Int32 order = sort->get<Int32>(name);
        std::string order_str = order > 0 ? "ASC" : "DESC";
        addToOrderBy(fmt::format("{} {}", name, order_str));
    }
}

void AggregateBuilder::handleProject(BSON::Document::Ptr project)
{
    LOG_DEBUG(getLogger("MongoDB::handleProject"), "Start handling project stage: {}", project->toString());
    // TODO use ProjectionMap and exclusion of _id field
    const auto & field_names = project->elementNames();
    for (const auto & field_name : field_names)
    {
        BSON::Element::Ptr value = project->get(field_name);
        LOG_DEBUG(getLogger("MongoDB::handleProject"), "Iterating, field_name: {}, value: {}", field_name, value->toString());
        if (value->getType() == BSON::ElementTraits<Int32>::TypeId || value->getType() == BSON::ElementTraits<bool>::TypeId)
        {
            bool status;
            try
            {
                status = project->get<bool>(field_name);
            }
            catch (const Poco::BadCastException &)
            {
                status = project->get<Int32>(field_name) > 0;
            }
            LOG_DEBUG(getLogger("MongoDB::handleProject"), "Added Int, field_name: {}, status: {}", field_name, status);
            addToSelect(field_name, status);
            continue;
        }

        if (value->getType() == BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
        {
            // create new fields
            addToSelect(field_name, project->get<BSON::Document::Ptr>(field_name));
            continue;
        }

        throw Poco::NotImplementedException("Unsupported type in projection");
    }
}

// TODO rewrite to reuse in Find, currenty code is copypasted
void AggregateBuilder::handleMatchSingleColumn(const std::string & column_name, BSON::Element::Ptr payload)
{
    if (payload->getType() != BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
    {
        // == or LIKE
        std::string result;
        switch (payload->getType())
        {
            case BSON::ElementTraits<Int32>::TypeId:
            case BSON::ElementTraits<double>::TypeId:
            case BSON::ElementTraits<Int64>::TypeId:
                result = fmt::format("{} == {}", column_name, payload->getType());
                break;
            case BSON::ElementTraits<std::string>::TypeId: {
                std::string value = payload.cast<BSON::ConcreteElement<std::string>>()->getValue();
                result = fmt::format("{} LIKE {}", column_name, parseRegex(value));
                break;
            }
            default:
                throw Poco::NotImplementedException("Unsupported Match type");
        }
        addWhere(result);
        return;
    }
    BSON::Document::Ptr doc = payload.cast<BSON::ConcreteElement<BSON::Document::Ptr>>()->getValue();
    const auto & names = doc->elementNames();
    for (const auto & name : names)
    {
        auto value = doc->get(name);
        auto value_str = makeElementIntoQuery(value);
        std::string format;
        if (name == "$gt")
            format = "{} > {}";
        else if (name == "$lt")
            format = "{} < {}";
        else if (name == "$gte")
            format = "{} <= {}";
        else if (name == "$lte")
            format = "{} >= {}";
        else if (name == "$not")
            format = "{} NOT LIKE {}";
        else if (name == "$ne")
            format = "{} <> {}";
        else
        {
            LOG_WARNING(getLogger("MongoDB::MakeQuery"), "Unsupported filter; column_name: {}", column_name);
            continue;
        }
        addWhere(fmt::vformat(format, fmt::make_format_args(column_name, value_str)));
    }
}


void AggregateBuilder::handleMatch(BSON::Document::Ptr match)
{
    const auto & element_names = match->elementNames();
    for (const auto & name : element_names)
        handleMatchSingleColumn(name, match->get(name));
}

void AggregateBuilder::handleCount(const std::string & new_col_name)
{
    BSON::Document::Ptr doc = new BSON::Document();
    doc->add<std::string>("$count", new_col_name);
    addToSelect("", doc);
}

void AggregateBuilder::parsePipeline()
{
    LOG_DEBUG(getLogger("MongoDB::parsePipeline"), "Pipeline is: {}", pipeline->toString());
    auto stages_cnt = pipeline->size();
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
                handleGroup(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case SORT:
                handleSort(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case LIMIT:
                setLimit(doc->get<Int32>(stage_name));
                break;
            case PROJECT:
                handleProject(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case MATCH:
                handleMatch(doc->get<BSON::Document::Ptr>(stage_name));
                break;
            case SKIP:
                setOffset(doc->get<Int32>(stage_name));
                break;
            case COUNT:
                handleCount(doc->get<std::string>(stage_name));
                break;
            case UNKNOWN: {
                throw Poco::NotImplementedException("Unknown command");
            }
        }
    }
}

std::string AggregateBuilder::buildSelect() const
{
    // TODO include all fields if projection_map has appropriate status
    std::string query = "SELECT";
    const auto & columns = select_map.getNamesByStatus(true);
    if (count_expr)
    {
        std::string column = "*";
        if (!columns.empty())
            column = "DISTINCT " + columns[0];
        query += fmt::format(" COUNT({}) AS {}", column, new_columns[0]);
        query += " FROM " + collection_name;
        return query;
    }

    for (const auto & name : columns)
    {
        query += ' ';
        query += name + ",";
    }
    for (const auto & name : new_columns)
    {
        query += ' ';
        query += name + ",";
    }
    query.pop_back(); // ','
    query += fmt::format(" FROM {}", collection_name);
    return query;
}


std::string AggregateBuilder::buildGroupBy() const
{
    if (group_by.empty())
        return "";
    std::string query = "GROUP BY";
    for (const auto & name : group_by)
    {
        query += ' ';
        query += name;
        query += ',';
    }
    query.pop_back(); // ','
    return query;
}
std::string AggregateBuilder::buildOrderBy() const
{
    if (order_by.empty())
        return "";
    std::string query = "ORDER BY";
    for (const auto & name : group_by)
    {
        query += ' ';
        query += name;
        query += ',';
    }
    query.pop_back(); // ','
    return query;
}


inline std::string AggregateBuilder::buildLimit() const
{
    if (!limit.has_value())
        return "";
    return fmt::format("LIMIT {}", limit.value());
}
std::string AggregateBuilder::buildWhere() const
{
    if (where.empty())
        return "";
    std::string query = "WHERE";
    for (const auto & name : where)
    {
        query += ' ';
        query += name;
        query += ',';
    }
    query.pop_back(); // ','
    return query;
}

inline std::string AggregateBuilder::buildOffset() const
{
    if (!offset.has_value())
        return "";
    return fmt::format("OFFSET {}", offset.value());
}

std::string AggregateBuilder::buildQuery() const
{
    return fmt::format("{} {} {} {} {} {}", buildSelect(), buildWhere(), buildGroupBy(), buildOrderBy(), buildLimit(), buildOffset());
}


BSON::Document::Ptr handleAggregate(const Command::Ptr command, ContextMutablePtr context)
{
    LOG_DEBUG(getLogger("MongoDB::handleAggregate"), "Started Handling aggregate");
    BSON::Array::Ptr pipeline = command->getExtra()->get<BSON::Array::Ptr>("pipeline");
    LOG_DEBUG(getLogger("MongoDB::handleAggregate"), "Exctracted pipeline");
    auto build = AggregateBuilder(command->getCollectionName(), pipeline);
    LOG_DEBUG(getLogger("MongoDB::handleAggregate"), "Aggregate Builder parsed");
    auto query = build.buildQuery();
    LOG_DEBUG(getLogger("MongoDB::handleAggregate"), "Build Query");
    query += "FORMAT TabSeparatedWithNamesAndTypes";
    LOG_DEBUG(getLogger("MongoDB::handleAggregate"), "Launching Query");
    return launchQuery(std::move(query), context, command->getDBName(), command->getCollectionName());
}


}
} // namespace DB::MongoDB
