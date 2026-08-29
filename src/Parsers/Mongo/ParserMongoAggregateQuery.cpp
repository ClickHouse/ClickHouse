#include <Parsers/Mongo/ParserMongoAggregateQuery.h>

#include <cmath>
#include <limits>
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
    /// fail because the source has no column of that name yet. It matches by pattern, so that a
    /// field takes the fields below it with it: the nested document of a table is a set of columns
    /// whose names are the dotted paths of its fields, and a stage that removes or replaces a field
    /// removes or replaces the whole subdocument it names.
    auto except_transformer = make_intrusive<ASTColumnsExceptTransformer>();
    String pattern;
    for (const auto & name : excluded)
        pattern += (pattern.empty() ? "" : "|") + fieldSubtreePattern(name);
    except_transformer->setPattern(std::move(pattern));

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
    ASTPtr array_join;

    /// The order the documents of the stream are in, which the accumulators of a `$group` that
    /// depend on it are lowered through (see `MongoGroupOrder`).
    MongoGroupOrder order;

    /// True when nothing but a `WHERE` has been collected, so a stage that produces the list of
    /// columns can be folded into the select being built.
    bool onlyFiltered() const { return !select_list && !group_by && !order_by && !limit && !offset; }

    void wrap()
    {
        /// A select that builds documents of its own - a projection, a grouping - does not have to
        /// carry the sort keys into them, so they are no longer fields the next stage can name.
        if (select_list)
            order.keys_in_scope = false;

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
        array_join = nullptr;
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

        if (array_join)
        {
            /// An `ARRAY JOIN` is an element of its own, following the table it applies to.
            auto array_join_element = make_intrusive<ASTTablesInSelectQueryElement>();
            array_join_element->array_join = array_join->clone();
            array_join_element->children.push_back(array_join_element->array_join);
            tables->children.push_back(std::move(array_join_element));
        }

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

/// A required member of the document of a stage.
const rapidjson::Value & requireStageMember(const rapidjson::Value & stage, const char * name, std::string_view stage_name)
{
    if (!stage.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a document", stage_name);
    auto it = stage.FindMember(name);
    if (it == stage.MemberEnd())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must have a '{}' field", stage_name, name);
    return it->value;
}

/// A count of documents: `$limit`, `$skip` and the size of `$sample`. A driver may send a whole
/// number as a double, and Extended JSON in its relaxed form does so as well.
UInt64 parseCount(const rapidjson::Value & value, std::string_view stage, bool positive)
{
    if (!value.IsNumber() || value.GetDouble() != std::floor(value.GetDouble()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a whole number", stage);

    const double count = value.GetDouble();
    if (count < (positive ? 1 : 0))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a {} number", stage, positive ? "positive" : "non negative");
    if (count > double(std::numeric_limits<Int64>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' is too large", stage);

    return static_cast<UInt64>(count);
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

    /// The order of the documents a `$group` produces is undefined in Mongo, so the stream has no
    /// order after it; the one it consumed is what its accumulators are lowered through.
    const MongoGroupOrder order = std::move(chain.order);
    chain.order = {};

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
        select_list->children.push_back(withAlias(parseMongoAccumulator(it->value, order), name));
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

        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of a projection must not be empty");

        /// A number or a boolean says whether to keep the field; only a document or a `$` prefixed
        /// expression computes one.
        const bool is_flag = value.IsBool() || value.IsNumber();
        if (is_flag)
        {
            const bool included = value.IsBool() ? value.GetBool() : value.GetDouble() != 0;
            /// A kept field is kept with the fields below it: the nested document of a table is a
            /// set of columns whose names are the dotted paths of its fields, so `{"profile": 1}`
            /// keeps `profile` and every `profile.<...>` there is.
            if (included)
                fields.push_back({name, makeFieldSubtreeMatcher(name)});
            else
                excluded.push_back(std::move(name));
            continue;
        }

        expandMongoProjectedField(name, value, fields);
    }

    if (!fields.empty() && !excluded.empty())
    {
        /// Mongo rejects an exclusion inside an inclusion projection, with one exception: the
        /// implicit `_id` may always be suppressed. This dialect never adds an implicit `_id`,
        /// so the exclusion has nothing left to do and is simply dropped; any other exclusion
        /// is an error rather than being silently ignored.
        std::erase(excluded, "_id");
        if (!excluded.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The argument of '$project' must not mix inclusion and exclusion of fields, except an exclusion of '_id'");
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
        {
            /// A matcher of a subtree keeps the names of the columns it selects, and carries no
            /// alias: it stands for however many columns the subdocument holds.
            if (fieldOfSubtreeMatcher(*field.expression))
                select_list->children.push_back(field.expression);
            else
                select_list->children.push_back(withAlias(field.expression, field.name));
        }
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
    MongoGroupOrder order;
    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        if (!it->value.IsInt64() || (it->value.GetInt64() != 1 && it->value.GetInt64() != -1))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "The direction of '$sort' on '{}' must be 1 or -1", stringView(it->name));

        const int direction = static_cast<int>(it->value.GetInt64());
        auto key = make_intrusive<ASTIdentifier>(String(stringView(it->name)));
        auto element = make_intrusive<ASTOrderByElement>();
        element->children.push_back(key);
        element->direction = direction;
        element->nulls_direction = direction;
        order_by->children.push_back(std::move(element));
        order.keys.emplace_back(std::move(key), direction);
    }

    chain.order_by = std::move(order_by);
    /// A `$group` that follows reads its documents in this order, which is what Mongo defines the
    /// value of `$first`, `$last`, `$push`, `$firstN` and `$lastN` by.
    chain.order = std::move(order);
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

void translateUnset(SelectChain & chain, const rapidjson::Value & stage)
{
    std::vector<std::string> removed;
    if (stage.IsString())
        removed.emplace_back(stringView(stage));
    else if (stage.IsArray())
    {
        for (const auto & name : stage.GetArray())
        {
            if (!name.IsString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$unset' must name fields");
            removed.emplace_back(stringView(name));
        }
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$unset' must be a field name or an array of them");

    if (removed.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$unset' must name at least one field");

    for (const auto & name : removed)
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'$unset' must not name an empty field");

    if (!chain.onlyFiltered())
        chain.wrap();

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(makeAsterisk(removed));
    chain.select_list = std::move(select_list);
}

void translateSortByCount(SelectChain & chain, const rapidjson::Value & stage)
{
    /// `$sortByCount` is a `$group` on the expression followed by a descending `$sort` on the size
    /// of each group.
    if (!chain.onlyFiltered())
        chain.wrap();

    auto key = parseMongoAggregateExpression(stage);

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(withAlias(key->clone(), "_id"));
    select_list->children.push_back(withAlias(makeASTFunction("count"), "count"));
    chain.select_list = std::move(select_list);

    auto group_by = make_intrusive<ASTExpressionList>();
    group_by->children.push_back(std::move(key));
    chain.group_by = std::move(group_by);

    auto element = make_intrusive<ASTOrderByElement>();
    element->children.push_back(make_intrusive<ASTIdentifier>("count"));
    element->direction = -1;
    element->nulls_direction = -1;
    /// Mongo leaves the order of the groups with equal counts unspecified; sorting them by the
    /// key keeps the result deterministic.
    auto tiebreak = make_intrusive<ASTOrderByElement>();
    tiebreak->children.push_back(make_intrusive<ASTIdentifier>("_id"));
    tiebreak->direction = 1;
    tiebreak->nulls_direction = 1;
    auto order_by = make_intrusive<ASTExpressionList>();
    order_by->children.push_back(std::move(element));
    order_by->children.push_back(std::move(tiebreak));
    chain.order_by = std::move(order_by);

    /// The documents this stage produces are ordered by the size of the group, which is the order
    /// a `$group` after it reads them in. The tiebreak on `_id` is not part of what Mongo defines,
    /// so it is left out of the order the accumulators are lowered through.
    chain.order = {};
    chain.order.keys.emplace_back(make_intrusive<ASTIdentifier>("count"), -1);
}

void translateSample(SelectChain & chain, const rapidjson::Value & stage)
{
    const auto & size = requireStageMember(stage, "size", "$sample");

    if (chain.order_by || chain.limit || chain.offset)
        chain.wrap();

    /// Mongo picks the documents at random, which is a sort on a random value.
    auto element = make_intrusive<ASTOrderByElement>();
    element->children.push_back(makeASTFunction("rand"));
    element->direction = 1;
    element->nulls_direction = 1;
    auto order_by = make_intrusive<ASTExpressionList>();
    order_by->children.push_back(std::move(element));

    chain.order_by = std::move(order_by);
    chain.limit = makeLiteral(Field(parseCount(size, "$sample", /* positive = */ false)));
    /// The documents come out in a random order, which Mongo leaves unspecified as well, so the
    /// accumulators that depend on the order have none to be lowered through.
    chain.order = {};
}

void translateUnwind(SelectChain & chain, const rapidjson::Value & stage)
{
    std::string path;
    bool preserve_empty = false;
    std::string index_name;

    if (stage.IsString())
        path = stringView(stage);
    else if (stage.IsObject())
    {
        const auto & path_value = requireStageMember(stage, "path", "$unwind");
        if (!path_value.IsString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'path' of '$unwind' must be a field path");
        path = stringView(path_value);

        if (auto it = stage.FindMember("preserveNullAndEmptyArrays"); it != stage.MemberEnd())
            preserve_empty = it->value.IsBool() && it->value.GetBool();
        if (auto it = stage.FindMember("includeArrayIndex"); it != stage.MemberEnd() && !it->value.IsNull())
        {
            if (!it->value.IsString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'includeArrayIndex' of '$unwind' must be a field name");
            index_name = stringView(it->value);
        }
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$unwind' must be a field path or a document");

    if (!path.starts_with("$") || path.size() == 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The path of '$unwind' must be a non-empty field name starting with '$'");
    path = path.substr(1);

    /** An `ARRAY JOIN` applies to the rows the select reads, so everything collected so far has to
      * be below it - including a `WHERE`, which in the pipeline filters the documents before they
      * are unwound and afterwards would see the element in place of the array.
      */
    if (chain.where || !chain.onlyFiltered() || chain.array_join)
        chain.wrap();

    /** The element of a preserved row is not the element of an array: a `LEFT ARRAY JOIN` of an
      * empty array produces the default value of the element type, while Mongo keeps the document
      * with the field empty. So the element is joined under a name of its own - which keeps the
      * array itself readable next to it - and the field is answered as `NULL` for the rows the
      * array of which holds nothing, rather than as a value no document ever held.
      */
    static constexpr std::string_view unwound_element = "__mongo_unwound";
    static constexpr std::string_view unwound_index = "__mongo_unwound_index";

    auto expressions = make_intrusive<ASTExpressionList>();
    if (preserve_empty)
        expressions->children.push_back(withAlias(make_intrusive<ASTIdentifier>(path), std::string(unwound_element)));
    else
        expressions->children.push_back(make_intrusive<ASTIdentifier>(path));

    if (!index_name.empty())
    {
        /// A second array of the same length is joined element by element with the first one.
        /// `arrayEnumerate` counts from one and Mongo's index from zero.
        auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>("__mongo_index"));
        auto body = makeASTFunction("minus", make_intrusive<ASTIdentifier>("__mongo_index"), makeLiteral(Field(UInt64(1))));
        auto indexes = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", std::move(parameters), std::move(body)),
            makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(path)));
        expressions->children.push_back(withAlias(std::move(indexes), preserve_empty ? std::string(unwound_index) : index_name));
    }

    auto unwind = make_intrusive<ASTArrayJoin>();
    /// Mongo drops a document whose array is empty or missing unless it is asked to keep it, which
    /// is exactly the difference between an inner and a left `ARRAY JOIN`.
    unwind->kind = preserve_empty ? ASTArrayJoin::Kind::Left : ASTArrayJoin::Kind::Inner;
    unwind->expression_list = expressions;
    unwind->children.push_back(std::move(expressions));

    chain.array_join = std::move(unwind);

    if (preserve_empty)
    {
        /** The field takes the place of the array it was unwound from, and a row that was kept
          * although its array held nothing answers with no value at all.
          */
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(makeAsterisk({path}));

        auto preserved = [&](const std::string & name)
        {
            return makeASTFunction(
                "if",
                makeASTFunction("empty", make_intrusive<ASTIdentifier>(path)),
                makeLiteral(Field()),
                make_intrusive<ASTIdentifier>(name));
        };

        select_list->children.push_back(withAlias(preserved(std::string(unwound_element)), path));
        if (!index_name.empty())
            select_list->children.push_back(withAlias(preserved(std::string(unwound_index)), index_name));
        chain.select_list = std::move(select_list);
    }
    else if (!index_name.empty())
    {
        /// A `*` does not expand to the columns an `ARRAY JOIN` introduces, so the index has to be
        /// named in the list of the select that produces it.
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(makeAsterisk({}));
        select_list->children.push_back(make_intrusive<ASTIdentifier>(index_name));
        chain.select_list = std::move(select_list);
    }

    /** The stages that follow see documents in which the field is one element of the array, so the
      * unwinding is closed off into a subquery of its own. Referring to the field by name in the
      * same select would otherwise be ambiguous between the array and the element.
      */
    chain.wrap();
}

void translateReplaceRoot(SelectChain & chain, const rapidjson::Value & new_root, std::string_view stage_name)
{
    if (!new_root.IsObject() || new_root.MemberCount() == 0)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Only a document is supported as the new root of '{}', because a field path names a column rather than a subdocument",
            stage_name);
    if (stringView(new_root.MemberBegin()->name).starts_with("$"))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a document is supported as the new root of '{}'", stage_name);

    std::vector<MongoProjectedField> fields;
    for (auto it = new_root.MemberBegin(); it != new_root.MemberEnd(); ++it)
        expandMongoProjectedField(std::string(stringView(it->name)), it->value, fields);

    if (!chain.onlyFiltered())
        chain.wrap();

    auto select_list = make_intrusive<ASTExpressionList>();
    for (auto & field : fields)
        select_list->children.push_back(withAlias(field.expression, field.name));
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
        else if (name == "$unset")
            translateUnset(chain, member.value);
        else if (name == "$sortByCount")
            translateSortByCount(chain, member.value);
        else if (name == "$sample")
            translateSample(chain, member.value);
        else if (name == "$unwind")
            translateUnwind(chain, member.value);
        else if (name == "$replaceRoot")
            translateReplaceRoot(chain, requireStageMember(member.value, "newRoot", name), name);
        else if (name == "$replaceWith")
            translateReplaceRoot(chain, member.value, name);
        else if (name == "$unionWith")
            translateUnionWith(chain, member.value, metadata);
        else if (name == "$limit")
        {
            if (chain.limit)
                chain.wrap();
            chain.limit = makeLiteral(Field(parseCount(member.value, name, /* positive = */ true)));
        }
        else if (name == "$skip")
        {
            /// `$limit` before `$skip` takes the first documents and only then drops some of them,
            /// which is not what `LIMIT ... OFFSET ...` does.
            if (chain.limit || chain.offset)
                chain.wrap();
            chain.offset = makeLiteral(Field(parseCount(member.value, name, /* positive = */ false)));
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
    /// A `$sort` or a `$group` on a path of a document is a sort or a group on a `Dynamic` value,
    /// which the collections this endpoint creates are made of.
    settings->changes.emplace_back("allow_suspicious_types_in_order_by", Field(UInt64(1)));
    settings->changes.emplace_back("allow_suspicious_types_in_group_by", Field(UInt64(1)));
    select->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));

    node = std::move(select);
    return true;
}

}

}
