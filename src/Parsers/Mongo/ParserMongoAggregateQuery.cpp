#include <Parsers/Mongo/ParserMongoAggregateQuery.h>

#include <string_view>

#include <Core/Field.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Mongo/ParserMongoAggregateExpression.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/Utils.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace Mongo
{

namespace
{

std::string_view stringView(const rapidjson::Value & value)
{
    return {value.GetString(), value.GetStringLength()};
}

ASTPtr makeLiteral(Field value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

/// Aliases the expression to the name of the field it produces, unless it already is exactly that
/// column - `SELECT UserID AS UserID` only adds noise to the generated query.
ASTPtr withAlias(ASTPtr expression, const std::string & name)
{
    const auto * identifier = expression->as<ASTIdentifier>();
    if (identifier && identifier->name() == name)
        return expression;
    expression->setAlias(name);
    return expression;
}

ASTPtr makeCollectionSource(const std::string & database, const std::string & collection)
{
    if (database.empty())
        return make_intrusive<ASTTableIdentifier>(collection);
    return make_intrusive<ASTTableIdentifier>(database, collection);
}

/// `*`, or `* EXCEPT (a, b)` when some of the columns of the source are replaced or removed.
ASTPtr makeAsterisk(const std::vector<std::string> & excluded)
{
    auto asterisk = make_intrusive<ASTAsterisk>();
    if (excluded.empty())
        return asterisk;

    /// The transformer is not strict on purpose: a `$set` that introduces a new field must not
    /// fail because the source has no column of that name yet.
    auto except_transformer = make_intrusive<ASTColumnsExceptTransformer>();
    for (const auto & name : excluded)
        except_transformer->children.push_back(make_intrusive<ASTIdentifier>(name));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(std::move(except_transformer));

    asterisk->transformers = transformers;
    asterisk->children.push_back(std::move(transformers));
    return asterisk;
}

/** The `SELECT` a pipeline is being translated into, and the ones already finished below it.
  *
  * A stage either fills a clause that is still free, or wraps everything built so far into a
  * subquery and starts a new select on top of it.
  */
class SelectChain
{
public:
    explicit SelectChain(ASTPtr source_) : source(std::move(source_)) { }

    /// `nullptr` means `SELECT *`.
    ASTPtr select_list;
    ASTPtr where;
    ASTPtr group_by;
    ASTPtr order_by;
    ASTPtr limit;
    ASTPtr offset;

    /// True when nothing but a `WHERE` has been collected, so a stage that produces the list of
    /// columns can be folded into the select being built.
    bool onlyFiltered() const { return !select_list && !group_by && !order_by && !limit && !offset; }

    void wrap()
    {
        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(build());
        union_query->list_of_selects = list_of_selects;
        union_query->children.push_back(list_of_selects);

        source = make_intrusive<ASTSubquery>(std::move(union_query));
        select_list = nullptr;
        where = nullptr;
        group_by = nullptr;
        order_by = nullptr;
        limit = nullptr;
        offset = nullptr;
    }

    /// Appends the documents of another pipeline to the stream, for `$unionWith`.
    void unionWith(ASTPtr other_select)
    {
        wrap();
        auto & union_query = source->as<ASTSubquery &>().children[0]->as<ASTSelectWithUnionQuery &>();
        union_query.list_of_selects->children.push_back(std::move(other_select));
        /// The mode has to be spelled out for every select but the first: the interpreter reads
        /// `list_of_modes`, and only the formatter falls back to `union_mode`.
        union_query.union_mode = SelectUnionMode::UNION_ALL;
        union_query.list_of_modes.push_back(SelectUnionMode::UNION_ALL);
        union_query.set_of_modes.insert(SelectUnionMode::UNION_ALL);
    }

    ASTPtr build() const
    {
        auto select = make_intrusive<ASTSelectQuery>();

        ASTPtr projection = select_list;
        if (!projection)
        {
            projection = make_intrusive<ASTExpressionList>();
            projection->children.push_back(makeAsterisk({}));
        }
        select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection));

        auto table_expression = make_intrusive<ASTTableExpression>();
        if (source->as<ASTTableIdentifier>())
            table_expression->database_and_table_name = source;
        else
            table_expression->subquery = source;
        table_expression->children.push_back(source);

        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        element->table_expression = table_expression;
        element->children.push_back(std::move(table_expression));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(element));
        select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        if (where)
            select->setExpression(ASTSelectQuery::Expression::WHERE, where->clone());
        if (group_by)
            select->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by->clone());
        if (order_by)
            select->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by->clone());
        if (limit)
            select->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, limit->clone());
        if (offset)
            select->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, offset->clone());

        return select;
    }

private:
    ASTPtr source;
};

UInt64 parseNonNegativeInteger(const rapidjson::Value & value, std::string_view stage)
{
    if (!value.IsInt64() || value.GetInt64() < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a non negative integer", stage);
    return static_cast<UInt64>(value.GetInt64());
}

ASTPtr translatePipeline(const rapidjson::Value & pipeline, ASTPtr source, const std::shared_ptr<QueryMetadata> & metadata);

void translateMatch(SelectChain & chain, const rapidjson::Value & stage, const std::shared_ptr<QueryMetadata> & metadata)
{
    if (!stage.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$match' must be a document");

    /// A `$match` that follows a stage which builds new documents filters those documents, so it
    /// has to run on top of them rather than on the collection.
    if (!chain.onlyFiltered())
        chain.wrap();

    ASTPtr condition;
    ParserMongoFilter filter(copyValue(stage, metadata->getAllocator()), metadata, "");
    if (!filter.parseImpl(condition))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot translate the filter of '$match'");

    /// An empty `$match` matches everything.
    if (!condition)
        return;

    chain.where = chain.where ? makeASTFunction("and", chain.where, condition) : condition;
}

void translateGroup(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$group' must be a document");
    auto id_it = stage.FindMember("_id");
    if (id_it == stage.MemberEnd())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '$group' stage must have an '_id'");

    if (!chain.onlyFiltered())
        chain.wrap();

    auto select_list = make_intrusive<ASTExpressionList>();
    auto group_by = make_intrusive<ASTExpressionList>();

    if (id_it->value.IsNull())
    {
        /// `{"_id": null}` aggregates the whole stream into one document.
        select_list->children.push_back(withAlias(makeLiteral(Field()), "_id"));
    }
    else
    {
        std::vector<MongoProjectedField> key_fields;
        expandMongoProjectedField("_id", id_it->value, key_fields);
        for (auto & field : key_fields)
        {
            group_by->children.push_back(field.expression->clone());
            select_list->children.push_back(withAlias(field.expression, field.name));
        }
    }

    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        std::string name(stringView(it->name));
        if (name == "_id")
            continue;
        select_list->children.push_back(withAlias(parseMongoAccumulator(it->value), name));
    }

    chain.select_list = std::move(select_list);
    if (!group_by->children.empty())
        chain.group_by = std::move(group_by);
}

void translateProject(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject() || stage.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$project' must be a non empty document");

    std::vector<MongoProjectedField> fields;
    std::vector<std::string> excluded;

    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        std::string name(stringView(it->name));
        const auto & value = it->value;

        /// A number or a boolean says whether to keep the field; only a document or a `$` prefixed
        /// expression computes one.
        const bool is_flag = value.IsBool() || value.IsNumber();
        if (is_flag)
        {
            const bool included = value.IsBool() ? value.GetBool() : value.GetDouble() != 0;
            if (included)
                fields.push_back({name, make_intrusive<ASTIdentifier>(name)});
            else
                excluded.push_back(std::move(name));
            continue;
        }

        expandMongoProjectedField(name, value, fields);
    }

    if (!chain.onlyFiltered())
        chain.wrap();

    auto select_list = make_intrusive<ASTExpressionList>();

    if (fields.empty())
    {
        /// Nothing is included, so the stage only removes fields.
        select_list->children.push_back(makeAsterisk(excluded));
    }
    else
    {
        for (auto & field : fields)
            select_list->children.push_back(withAlias(field.expression, field.name));
    }

    chain.select_list = std::move(select_list);
}

void translateSet(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject() || stage.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$set' must be a non empty document");

    std::vector<MongoProjectedField> fields;
    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
        expandMongoProjectedField(std::string(stringView(it->name)), it->value, fields);

    /// `$set` keeps every field of the document and adds its own on top, so the select it produces
    /// starts from `*`; a field it replaces is dropped from that `*` first.
    if (chain.select_list || chain.group_by || chain.order_by || chain.limit || chain.offset)
        chain.wrap();

    std::vector<std::string> replaced;
    replaced.reserve(fields.size());
    for (const auto & field : fields)
        replaced.push_back(field.name);

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(makeAsterisk(replaced));
    for (auto & field : fields)
        select_list->children.push_back(withAlias(field.expression, field.name));

    chain.select_list = std::move(select_list);
}

void translateSort(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject() || stage.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$sort' must be a non empty document");

    if (chain.order_by || chain.limit || chain.offset)
        chain.wrap();

    auto order_by = make_intrusive<ASTExpressionList>();
    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        if (!it->value.IsInt64() || (it->value.GetInt64() != 1 && it->value.GetInt64() != -1))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The direction of '$sort' on '{}' must be 1 or -1", stringView(it->name));

        const int direction = static_cast<int>(it->value.GetInt64());
        auto element = make_intrusive<ASTOrderByElement>();
        element->children.push_back(make_intrusive<ASTIdentifier>(String(stringView(it->name))));
        element->direction = direction;
        element->nulls_direction = direction;
        order_by->children.push_back(std::move(element));
    }

    chain.order_by = std::move(order_by);
}

void translateCount(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsString() || stage.GetStringLength() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$count' must be the name of the resulting field");

    if (!chain.onlyFiltered())
        chain.wrap();

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(withAlias(makeASTFunction("count"), std::string(stringView(stage))));
    chain.select_list = std::move(select_list);
}

void translateUnionWith(SelectChain & chain, const rapidjson::Value & stage, const std::shared_ptr<QueryMetadata> & metadata)
{
    std::string collection;
    const rapidjson::Value * sub_pipeline = nullptr;

    if (stage.IsString())
        collection = stringView(stage);
    else if (stage.IsObject())
    {
        auto collection_it = stage.FindMember("coll");
        if (collection_it == stage.MemberEnd() || !collection_it->value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$unionWith' must name a collection in 'coll'");
        collection = stringView(collection_it->value);
        if (auto pipeline_it = stage.FindMember("pipeline"); pipeline_it != stage.MemberEnd())
            sub_pipeline = &pipeline_it->value;
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$unionWith' must be a collection name or a document");

    auto source = makeCollectionSource(metadata->getDatabaseName(), collection);
    ASTPtr other_select;
    if (sub_pipeline)
        other_select = translatePipeline(*sub_pipeline, std::move(source), metadata);
    else
        other_select = SelectChain(std::move(source)).build();

    chain.unionWith(std::move(other_select));
}

ASTPtr translatePipeline(const rapidjson::Value & pipeline, ASTPtr source, const std::shared_ptr<QueryMetadata> & metadata)
{
    if (!pipeline.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A pipeline must be an array of stages");

    SelectChain chain(std::move(source));

    for (const auto & stage : pipeline.GetArray())
    {
        if (!stage.IsObject() || stage.MemberCount() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A stage of a pipeline must be a document holding one operator");

        const auto & member = *stage.MemberBegin();
        const auto name = stringView(member.name);

        if (name == "$match")
            translateMatch(chain, member.value, metadata);
        else if (name == "$group")
            translateGroup(chain, member.value);
        else if (name == "$project")
            translateProject(chain, member.value);
        else if (name == "$set" || name == "$addFields")
            translateSet(chain, member.value);
        else if (name == "$sort")
            translateSort(chain, member.value);
        else if (name == "$count")
            translateCount(chain, member.value);
        else if (name == "$unionWith")
            translateUnionWith(chain, member.value, metadata);
        else if (name == "$limit")
        {
            if (chain.limit)
                chain.wrap();
            chain.limit = makeLiteral(Field(parseNonNegativeInteger(member.value, name)));
        }
        else if (name == "$skip")
        {
            /// `$limit` before `$skip` takes the first documents and only then drops some of them,
            /// which is not what `LIMIT ... OFFSET ...` does.
            if (chain.limit || chain.offset)
                chain.wrap();
            chain.offset = makeLiteral(Field(parseNonNegativeInteger(member.value, name)));
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The aggregation stage '{}' is not supported", name);
    }

    return chain.build();
}

}

bool ParserMongoAggregateQuery::parseImpl(ASTPtr & node)
{
    auto select = translatePipeline(data, makeCollectionSource(metadata->getDatabaseName(), metadata->getCollectionName()), metadata);

    /** A field path inside a stage always names a field of the document the stage receives, while
      * a stage folded into the same select - a `$sort` after a `$group` - names a field of the
      * document it produces. A `$group` that carries a field through with `$first` has both under
      * the same name, as `{"_id": ..., "SearchPhrase": {"$first": "$SearchPhrase"}}` does, and
      * without this setting the alias would shadow the column the group is computed from.
      */
    auto settings = make_intrusive<ASTSetQuery>();
    settings->is_standalone = false;
    settings->changes.emplace_back("prefer_column_name_to_alias", Field(UInt64(1)));
    /// A `$sort` on a `JSON` or a `Dynamic` column is a sort on a suspicious type, and a collection
    /// created by `createCollection` is a single `JSON` column.
    settings->changes.emplace_back("allow_suspicious_types_in_order_by", Field(UInt64(1)));
    select->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));

    node = std::move(select);
    return true;
}

}

}
