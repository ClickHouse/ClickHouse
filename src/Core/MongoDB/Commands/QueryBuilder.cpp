#include "QueryBuilder.h"


namespace DB
{
namespace MongoDB
{

std::string SelectOptions::aggregateOperatorToString(AggregationOperators oper)
{
    // NOTE: Maybe change to unordered_map?
    switch (oper)
    {
        case COUNT:
            return "COUNT";
        case SUM:
            return "SUM";
        case AVG:
            return "AVG";
        case MIN:
            return "MIN";
        case MAX:
            return "MAX";
    }
}

std::string SelectOptions::aggregateOperatorToRawString(AggregationOperators oper)
{
    switch (oper)
    {
        case COUNT:
            return "$count";
        case SUM:
            return "$sum";
        case AVG:
            return "$avg";
        case MIN:
            return "$min";
        case MAX:
            return "$max";
    }
}

SelectOptions::AggregationOperators SelectOptions::aggregateOperatorFromRawString(const std::string & str)
{
    if (str == "$count")
        return COUNT;
    else if (str == "$sum")
        return SUM;
    else if (str == "$avg")
        return AVG;
    else if (str == "$min")
        return MIN;
    else if (str == "$max")
        return MAX;
    throw Poco::RuntimeException(fmt::format("Unknown aggregate operator: {}", str));
}

// command format:  { col_name : {//aggr_oper// : expr}}
SelectOptions::SelectOptions(BSON::Element::Ptr command)
{
    LOG_DEBUG(getLogger("SelectOptions::SelectOptions"), "Making options from command: {}", command->toString());
    auto [col_name, payload] = BSON::Element::cast<BSON::Document::Ptr>(command)->deconstruct();

    if (auto cnt = payload->size(); cnt != 1)
        throw Poco::LogicException(fmt::format("Incorrect amount of commands: {}", cnt));
    BSON::Element::Ptr oper = payload->takeLast();
    auto aggr_type = aggregateOperatorFromRawString(oper->getName());
    expression = makeExpression(oper);
    setNewColumnName(col_name);
    setAggregateOper(aggr_type);
}

void QueryBuilder::addToSelect(SelectOptions && options)
{
    LOG_DEBUG(getLogger("QueryBuilder::addToSelect"), "adding options, expression: {}", options.expression);
    std::string distinct = options.distinct ? "DISTINCT" : "";
    std::string new_name = "";
    if (options.new_col_name.has_value())
    {
        new_name = std::move(options.new_col_name.value());
        new_name.insert(0, "AS ");
    }
    std::string aggregate_oper;
    if (options.aggregate_operator.has_value())
        aggregate_oper = std::move(options.aggregate_operator.value());
    select_elems.emplace_back(
        fmt::format("{}({} {}) {}", std::move(aggregate_oper), std::move(distinct), std::move(options.expression), std::move(new_name)));
}

void QueryBuilder::handleGroupIds(std::vector<BSON::Element::Ptr> && ids, BSON::Document::Ptr operations)
{
    decltype(this->group_by_map) old_map;
    std::swap(old_map, this->group_by_map);
    for (auto elem_ptr : ids)
    {
        std::string value;
        std::string name;
        if (elem_ptr->getType() != BSON::ElementTraits<std::string>::TypeId)
        {
            name = elem_ptr->getName();
            value = makeExpression(elem_ptr);
            value = fmt::format("{} AS {}", value, name);
        }
        else
        {
            auto elem = BSON::Element::cast<std::string>(elem_ptr);
            auto [name_, value_] = elem->deconstruct();
            name = std::move(name_);
            value = std::move(value_);
        }
        LOG_DEBUG(getLogger("MongoDB::handleGroupIds"), "Handling new id element, name: {}, value: {}", name, value);

        static const std::string _id_str = "$_id.";
        if (value.substr(0, _id_str.size()) != _id_str)
        {
            // just group by this field
            removeDollarFromName(value);
            if (name == "_id")
                group_by_map.emplace(value, value);
            else
                group_by_map.emplace(std::move(name), std::move(value));
            continue;
        }

        value.erase(value.begin(), value.begin() + _id_str.size());
        auto it = old_map.find(value);
        group_by_map.insert(old_map.extract(it));
    }

    if (old_map.size() > 1)
        throw Poco::LogicException("Cannot leave multiple columns in second $group operator, there should only be one left");
    else if (operations->empty())
        return;
    std::pair<std::string, std::string> pair;
    if (!old_map.empty())
        pair = std::move(*old_map.begin());

    auto operations_vec = std::move(*operations).deconstruct();
    for (auto & operation : operations_vec)
    {
        auto options = SelectOptions(operation);
        // if operation is {c : {"$sum" : 1}} translate it to COUNT(DISTINCT name)
        if (!old_map.empty())
            if (auto oper = options.getAggregateOper(); oper.has_value() && oper.value() == "SUM" && options.expression == "1")
            {
                options.expression = std::move(pair.second);
                options.setDistinct(true).setAggregateOper(SelectOptions::COUNT);
                addToSelect(std::move(options));
                continue;
            }

        if (options.expression == pair.second) // if operation over DISTINCT column
            options.setDistinct(true);
        addToSelect(std::move(options));
    }
}

void QueryBuilder::handleGroup(BSON::Document::Ptr group)
{
    has_group_operations = true;
    LOG_DEBUG(getLogger("MongoDB::handleGroup"), "start handling group: {}", group->toString());
    const char * _id_str = "_id";
    BSON::Element::Ptr _id = group->get(_id_str);
    std::vector<BSON::Element::Ptr> ids;
    // write all id pairs to vector
    if (_id->getType() == BSON::ElementTraits<std::string>::TypeId)
    {
        ids.emplace_back(group->take(_id_str));
    }
    else if (_id->getType() == BSON::ElementTraits<BSON::Document::Ptr>::TypeId)
    {
        auto fields = group->takeValue<BSON::Document::Ptr>(_id_str);
        const auto & field_names = fields->elementNames();
        for (const auto & name : field_names)
            ids.emplace_back(fields->take(name));
    }
    else
    {
        _id = group->take(_id_str);
        // no grouping
        if (_id->getType() != BSON::ElementTraits<BSON::NullValue>::TypeId)
            throw Poco::LogicException(fmt::format("Incorrect _id field with value: {}", _id->toString()));
    }
    handleGroupIds(std::move(ids), group);
}

void QueryBuilder::handleSort(BSON::Document::Ptr sort)
{
    const auto & element_names = sort->elementNames();
    for (const auto & name : element_names)
        addToOrderBy(name, sort->get<Int32>(name));
}

void QueryBuilder::handleProject(BSON::Document::Ptr project)
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
            addToSelect(std::move(SelectOptions(makeExpression(project->take(field_name))).setNewColumnName(field_name)));
            continue;
        }

        throw Poco::NotImplementedException("Unsupported type in projection");
    }
}

void QueryBuilder::handleWhereSingleColumn(const std::string & column_name, BSON::Element::Ptr payload, bool having)
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
            case BSON::ElementTraits<std::string>::TypeId:
                result = fmt::format("{} = {}", column_name, payload->toString());
                break;
            default:
                throw Poco::NotImplementedException("Unsupported Match type");
        }
        addWhere(result, having);
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
            format = "{} >= {}";
        else if (name == "$lte")
            format = "{} <= {}";
        else if (name == "$not")
            format = "{} NOT LIKE {}";
        else if (name == "$ne")
            format = "{} <> {}";
        else
        {
            LOG_WARNING(getLogger("MongoDB::MakeQuery"), "Unsupported filter; column_name: {}", column_name);
            continue;
        }
        addWhere(fmt::vformat(format, fmt::make_format_args(column_name, value_str)), having);
    }
}


void QueryBuilder::handleWhere(BSON::Document::Ptr match, bool having)
{
    const auto & element_names = match->elementNames();
    for (const auto & name : element_names)
        handleWhereSingleColumn(name, match->get(name), having);
}

void QueryBuilder::handleCount(const std::string & new_col_name)
{
    count_expr = true;
    has_group_operations = true;
    SelectOptions options("*");
    options.setAggregateOper(SelectOptions::AggregationOperators::COUNT).setNewColumnName(new_col_name);
    addToSelect(std::move(options));
}

std::string QueryBuilder::buildSelect() &&
{
    // add only grouping columns
    if (has_group_operations)
    {
        proj_map.resetStatuses();
        proj_map.setDefaultStatus(false);
        for (const auto & [_, column_name] : group_by_map)
        {
            LOG_DEBUG(getLogger("QueryBuilder::buildSelect"), "Getting column from group_by_map: {}", column_name);
            proj_map.add(column_name, true);
        }
    }
    std::vector<std::string> columns = std::move(proj_map).getNamesByStatus(true); // include columns
    return fmt::format(
        "SELECT {}{} {} FROM {}", fmt::join(columns, ", "), columns.empty() ? ' ' : ',', fmt::join(select_elems, ", "), collection_name);
}


std::string QueryBuilder::buildGroupBy() const
{
    if (group_by_map.empty())
        return "";
    std::string query = "GROUP BY";
    for (const auto & [key, column_name] : group_by_map)
        query += fmt::format(" {},", column_name);
    query.pop_back(); // ','
    return query;
}
std::string QueryBuilder::buildOrderBy() const
{
    if (order_by.empty())
        return "";
    return fmt::format("ORDER BY {}", fmt::join(order_by, ", "));
}


std::string QueryBuilder::buildWhere() const
{
    if (where.empty())
        return "";
    return fmt::format("WHERE {}", fmt::join(where, " AND "));
}

std::string QueryBuilder::buildHaving() const
{
    if (having_vec.empty())
        return "";
    return fmt::format("HAVING {}", fmt::join(having_vec, " AND "));
}

std::string QueryBuilder::buildQuery() &&
{
    return fmt::format(
        "{} {} {} {} {} {} {} FORMAT TabSeparatedWithNamesAndTypes",
        std::move(*this).buildSelect(),
        buildWhere(),
        buildGroupBy(),
        buildHaving(),
        buildOrderBy(),
        buildLimit(),
        buildOffset());
}

}
}
