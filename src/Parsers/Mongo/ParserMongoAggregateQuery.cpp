#include <Parsers/Mongo/ParserMongoAggregateQuery.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <string_view>

#include <Core/Field.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
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

/** `*`, or `* EXCEPT '<pattern>'` when some of the fields of the source are replaced or removed.
  * A nested document is stored as one column per leaf, so a field is removed together with
  * everything under it: replacing or removing `profile` drops `profile.name` and `profile.age`
  * (see `makeFieldSubtreePattern`).
  */
ASTPtr makeAsterisk(const std::vector<std::string> & excluded)
{
    auto asterisk = make_intrusive<ASTAsterisk>();
    if (excluded.empty())
        return asterisk;

    /// The transformer is not strict on purpose: a `$set` that introduces a new field must not
    /// fail because the source has no column of that name yet.
    auto except_transformer = make_intrusive<ASTColumnsExceptTransformer>();
    except_transformer->setPattern(makeFieldSubtreePattern(excluded));

    auto transformers = make_intrusive<ASTColumnsTransformerList>();
    transformers->children.push_back(std::move(except_transformer));

    asterisk->transformers = transformers;
    asterisk->children.push_back(std::move(transformers));
    return asterisk;
}

/// Whether a field of a projection is a field of the source kept as itself, which keeps everything
/// under it as well, rather than a value computed under that name.
bool isKeptAsItself(const MongoProjectedField & field)
{
    const auto * identifier = field.expression->as<ASTIdentifier>();
    return identifier && identifier->name() == field.name;
}

/// Whether the value of a member of a projection is a nested document of further members, such as
/// `{"age": 1}` in `{"profile": {"age": 1}}`, rather than an expression or a constant.
bool isNestedProjectionDocument(const rapidjson::Value & value)
{
    if (!value.IsObject())
        return false;
    if (value.MemberCount() == 0)
        return true;
    /// An operator, and an Extended JSON constant such as `{"$date": ...}`, start with `$`.
    return !stringView(value.MemberBegin()->name).starts_with("$");
}

/// Whether a select of `* EXCEPT (excluded)` keeps every field the stream is ordered by, so a later
/// order-sensitive stage can still name them.
bool exclusionPreservesOrderKeys(const MongoGroupOrder & order, const std::vector<std::string> & excluded)
{
    for (const auto & [key, _] : order.keys)
    {
        const auto * identifier = key->as<ASTIdentifier>();
        if (!identifier
            || std::any_of(
                excluded.begin(), excluded.end(), [&](const std::string & path) { return isInFieldSubtree(identifier->name(), path); }))
            return false;
    }
    return true;
}

/// `(<select>)`, a subquery in the `FROM` of a select or a scalar one in an expression.
ASTPtr makeSubquery(ASTPtr select)
{
    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select));
    union_query->list_of_selects = list_of_selects;
    union_query->children.push_back(list_of_selects);
    return make_intrusive<ASTSubquery>(std::move(union_query));
}

/** The names of the fields of the documents of a stream, where the pipeline determines them.
  *
  * The branches of a SQL `UNION ALL` are matched by position and named after the first one, while
  * `$unionWith` appends documents with their own field names. So a union is only translated when
  * both of its sides are known to produce the same fields (see `translateUnionWith`), and this is
  * what that is decided by.
  */
struct StreamFields
{
    enum class Kind : uint8_t
    {
        /// The columns of the collection `database`.`collection`, in its order.
        Collection,
        /// Exactly the columns in `names`, in that order.
        Names,
        /// Depends on which columns the source has, which the translation does not know.
        Unknown,
    };

    Kind kind = Kind::Unknown;
    std::string database;
    std::string collection;
    std::vector<std::string> names;

    static StreamFields ofCollection(const std::string & database, const std::string & collection)
    {
        StreamFields fields;
        fields.kind = Kind::Collection;
        fields.database = database;
        fields.collection = collection;
        return fields;
    }

    static StreamFields ofNames(std::vector<std::string> names)
    {
        StreamFields fields;
        fields.kind = Kind::Names;
        fields.names = std::move(names);
        return fields;
    }

    static StreamFields ofProjectedFields(const std::vector<MongoProjectedField> & projected)
    {
        std::vector<std::string> names;
        names.reserve(projected.size());
        for (const auto & field : projected)
            names.push_back(field.name);
        return ofNames(std::move(names));
    }

    /// The fields that remain after a `* EXCEPT` of the subtrees under `paths`.
    void removeSubtrees(const std::vector<std::string> & paths)
    {
        if (paths.empty())
            return;
        if (kind != Kind::Names)
        {
            *this = {};
            return;
        }
        std::erase_if(
            names,
            [&](const std::string & name)
            { return std::any_of(paths.begin(), paths.end(), [&](const std::string & path) { return isInFieldSubtree(name, path); }); });
    }

    /// The fields under `path`, in their order, which is what a select of `path` as itself keeps.
    std::optional<std::vector<std::string>> subtree(const std::string & path) const
    {
        if (kind != Kind::Names)
            return std::nullopt;
        std::vector<std::string> result;
        for (const auto & name : names)
            if (isInFieldSubtree(name, path))
                result.push_back(name);
        return result;
    }
};

/** The `SELECT` a pipeline is being translated into, and the ones already finished below it.
  *
  * A stage either fills a clause that is still free, or wraps everything built so far into a
  * subquery and starts a new select on top of it.
  */
class SelectChain
{
public:
    explicit SelectChain(ASTPtr source_) : source(std::move(source_)) { }

    /// The chain that reads a collection as it is.
    static SelectChain ofCollection(const std::string & database, const std::string & collection)
    {
        SelectChain chain(makeCollectionSource(database, collection));
        chain.fields = StreamFields::ofCollection(database, collection);
        return chain;
    }

    /// `nullptr` means `SELECT *`.
    ASTPtr select_list;
    ASTPtr where;
    ASTPtr group_by;
    ASTPtr having;
    ASTPtr order_by;
    ASTPtr limit;
    ASTPtr offset;
    ASTPtr array_join;

    /// The order the documents of the stream are in, which the accumulators of a `$group` that
    /// depend on it are lowered through (see `MongoGroupOrder`).
    MongoGroupOrder order;
    /// Whether `select_list` preserves every field named by `order`. `$set` starts from `*` and
    /// has that property unless it replaces a sort key; `$project` and `$group` do not in general.
    bool select_list_preserves_order_keys = false;

    /// The fields of the documents the chain produces. A stage that changes them updates this.
    StreamFields fields;

    /// True when nothing but a `WHERE` has been collected, so a stage that produces the list of
    /// columns can be folded into the select being built.
    bool onlyFiltered() const { return !select_list && !group_by && !having && !order_by && !limit && !offset; }

    void wrap()
    {
        /// A select that builds documents of its own - a projection, a grouping - does not have to
        /// carry the sort keys into them, so they are no longer fields the next stage can name.
        if (select_list && !select_list_preserves_order_keys)
            order.keys_in_scope = false;

        source = makeSubquery(build());
        select_list = nullptr;
        where = nullptr;
        group_by = nullptr;
        having = nullptr;
        order_by = nullptr;
        limit = nullptr;
        offset = nullptr;
        array_join = nullptr;
        select_list_preserves_order_keys = false;
    }

    /// Appends the documents of another pipeline to the stream, for `$unionWith`. The other
    /// pipeline must produce the same fields in the same order (see `translateUnionWith`).
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
        /// A union appends another stream, so the order established before it no longer describes
        /// the combined documents. A later order-sensitive `$group` must not reuse that key.
        order = {};
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
        if (having)
            select->setExpression(ASTSelectQuery::Expression::HAVING, having->clone());
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

SelectChain translatePipeline(
    const rapidjson::Value & pipeline, const std::string & collection, const std::shared_ptr<QueryMetadata> & metadata);

/** The aggregation of the whole stream into one document - a `$group` with a `_id` of `null`, a
  * `$count` - produces no document at all when the stream is empty, while an aggregation without
  * keys in ClickHouse produces one row of the default values. The `HAVING` drops that row, and it
  * also makes the select an aggregation when it computes nothing but constants, so that a
  * nonempty stream becomes one document rather than one per row.
  */
ASTPtr makeNonEmptyGroupCondition()
{
    return makeASTFunction("greater", makeASTFunction("count"), makeLiteral(Field(UInt64(0))));
}

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
    std::vector<std::string> names;

    if (id_it->value.IsNull())
    {
        /// `{"_id": null}` aggregates the whole stream into one document.
        select_list->children.push_back(withAlias(makeLiteral(Field()), "_id"));
        names.emplace_back("_id");
        chain.having = makeNonEmptyGroupCondition();
    }
    else
    {
        /// A constant key, such as `{"_id": 42}`, groups by that value: the generated query
        /// disables positional arguments, so `GROUP BY 42` is not the 42nd column of the select
        /// (see `ParserMongoAggregateQuery::parseImpl`).
        std::vector<MongoProjectedField> key_fields;
        expandMongoProjectedField("_id", id_it->value, key_fields);
        for (auto & field : key_fields)
        {
            group_by->children.push_back(field.expression->clone());
            names.push_back(field.name);
            select_list->children.push_back(withAlias(field.expression, field.name));
        }
    }

    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        std::string name(stringView(it->name));
        if (name == "_id")
            continue;
        select_list->children.push_back(withAlias(parseMongoAccumulator(it->value, order), name));
        names.push_back(std::move(name));
    }

    chain.fields = StreamFields::ofNames(std::move(names));

    chain.select_list = std::move(select_list);
    if (!group_by->children.empty())
        chain.group_by = std::move(group_by);
}

/** One member of a `$project` document: a field to keep, a field to remove, or a field to compute.
  *
  * A number or a boolean says whether to keep the field, at any depth: `{"profile": {"age": 1}}`
  * keeps `profile.age` and `{"profile": {"age": 0}}` removes it, as in Mongo, where a literal value
  * has to be spelled as `$literal` inside a projection. A `$` prefixed expression, or any other value,
  * computes the field. This is what tells a projection from `$set`, where every value is computed.
  */
void collectProjectionMember(
    const std::string & name,
    const rapidjson::Value & value,
    std::vector<MongoProjectedField> & fields,
    std::vector<std::string> & excluded)
{
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of a projection must not be empty");

    if (value.IsBool() || value.IsNumber())
    {
        const bool included = value.IsBool() ? value.GetBool() : value.GetDouble() != 0;
        if (included)
            fields.push_back({name, make_intrusive<ASTIdentifier>(name)});
        else
            excluded.push_back(name);
        return;
    }

    if (isNestedProjectionDocument(value))
    {
        if (value.MemberCount() == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The projection of '{}' must not be an empty document", name);
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it)
            collectProjectionMember(name + "." + std::string(stringView(it->name)), it->value, fields, excluded);
        return;
    }

    expandMongoProjectedField(name, value, fields);
}

void translateProject(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject() || stage.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$project' must be a non empty document");

    std::vector<MongoProjectedField> fields;
    std::vector<std::string> excluded;

    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
        collectProjectionMember(std::string(stringView(it->name)), it->value, fields, excluded);

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
            /// A field kept as itself is kept together with everything under it, the same way as
            /// in the projection of a `find`: `{"profile": 1}` keeps `profile.name` and `profile.age`.
            if (isKeptAsItself(field))
            {
                auto matcher = make_intrusive<ASTColumnsRegexpMatcher>();
                matcher->setPattern(makeFieldSubtreePattern({field.name}));
                select_list->children.push_back(std::move(matcher));
            }
            else
                select_list->children.push_back(withAlias(field.expression, field.name));
        }
    }

    /// A projection carries a sort key into the documents it builds when it keeps that field as
    /// itself: an exclusion projection keeps everything it does not name, and an inclusion
    /// projection keeps the fields it names, but only where the name is the field rather than an
    /// expression computed under it.
    if (fields.empty())
    {
        chain.select_list_preserves_order_keys = exclusionPreservesOrderKeys(chain.order, excluded);
        chain.select_list = std::move(select_list);
        chain.fields.removeSubtrees(excluded);
        return;
    }

    std::vector<std::string> names;
    bool names_known = true;
    for (const auto & field : fields)
    {
        if (!isKeptAsItself(field))
        {
            names.push_back(field.name);
            continue;
        }
        auto subtree = chain.fields.subtree(field.name);
        if (!subtree)
        {
            names_known = false;
            break;
        }
        names.insert(names.end(), subtree->begin(), subtree->end());
    }
    chain.fields = names_known ? StreamFields::ofNames(std::move(names)) : StreamFields{};

    chain.select_list_preserves_order_keys = true;
    for (const auto & [key, _] : chain.order.keys)
    {
        const auto * identifier = key->as<ASTIdentifier>();
        if (!identifier)
        {
            chain.select_list_preserves_order_keys = false;
            break;
        }

        /// A field kept as itself keeps everything under it, so keeping `profile` keeps the sort
        /// key `profile.age` as well.
        const auto & name = identifier->name();
        const bool preserved = std::any_of(
            fields.begin(),
            fields.end(),
            [&](const MongoProjectedField & field) { return isKeptAsItself(field) && isInFieldSubtree(name, field.name); });

        if (!preserved)
        {
            chain.select_list_preserves_order_keys = false;
            break;
        }
    }

    chain.select_list = std::move(select_list);
}

void translateSet(SelectChain & chain, const rapidjson::Value & stage)
{
    if (!stage.IsObject() || stage.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$set' must be a non empty document");

    /// A field the stage assigns is replaced as a whole: `{"profile": {"name": "y"}}` produces the
    /// column `profile.name` and drops every other column under `profile`, as Mongo replaces the
    /// whole subdocument. So the fields dropped from `*` are the names of the stage, not the leaves
    /// they expand into.
    std::vector<MongoProjectedField> fields;
    std::vector<std::string> replaced;
    for (auto it = stage.MemberBegin(); it != stage.MemberEnd(); ++it)
    {
        replaced.emplace_back(stringView(it->name));
        expandMongoProjectedField(replaced.back(), it->value, fields);
    }

    /// `$set` keeps every field of the document and adds its own on top, so the select it produces
    /// starts from `*`; a field it replaces is dropped from that `*` first.
    if (!chain.onlyFiltered())
        chain.wrap();

    chain.select_list_preserves_order_keys = true;
    for (const auto & [key, _] : chain.order.keys)
    {
        const auto * identifier = key->as<ASTIdentifier>();
        if (identifier
            && std::any_of(
                replaced.begin(), replaced.end(), [&](const std::string & path) { return isInFieldSubtree(identifier->name(), path); }))
        {
            chain.select_list_preserves_order_keys = false;
            break;
        }
    }

    if (chain.fields.kind == StreamFields::Kind::Names)
    {
        chain.fields.removeSubtrees(replaced);
        for (const auto & field : fields)
            chain.fields.names.push_back(field.name);
    }
    else
        chain.fields = {};

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

    /** The sort orders the documents the preceding stage produced. A stage that builds the list of
      * columns - `$project`, `$set`, `$group` - may compute a field under the name of a column of
      * its source, and in the same select the `ORDER BY` would name the source column, because the
      * generated query prefers a column name to an alias (see `ParserMongoAggregateQuery::parseImpl`).
      * So the sort runs on top of such a stage rather than next to it.
      */
    if (!chain.onlyFiltered())
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
        order_by->children.push_back(makeMongoOrderByElement(key, direction));
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
    chain.having = makeNonEmptyGroupCondition();
    chain.fields = StreamFields::ofNames({std::string(stringView(stage))});
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
    /// `$unset` is the exclusion form of `$project`, so it carries the sort keys it does not remove.
    chain.select_list_preserves_order_keys = exclusionPreservesOrderKeys(chain.order, removed);
    chain.select_list = std::move(select_list);
    chain.fields.removeSubtrees(removed);
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
    chain.fields = StreamFields::ofNames({"_id", "count"});

    /** The sort runs on top of the grouping rather than next to it. In the same select `count` and
      * `_id` would name the columns of the source, if it has columns of these names, because the
      * generated query prefers a column name to an alias (see `ParserMongoAggregateQuery::parseImpl`).
      */
    chain.wrap();

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

ASTPtr makeArrayJoin(ASTPtr expressions)
{
    auto array_join = make_intrusive<ASTArrayJoin>();
    array_join->kind = ASTArrayJoin::Kind::Inner;
    array_join->expression_list = expressions;
    array_join->children.push_back(std::move(expressions));
    return array_join;
}

/** `if(notEmpty(array), <elements>, [NULL])`: an array with one null element in place of an empty one.
  * The type of the result is the common one of both arrays: an array of `Nullable` elements, or of the
  * elements themselves when they hold a null on their own, as a `Dynamic` and a `Variant` do.
  */
ASTPtr makeNullIfEmpty(const std::string & path, ASTPtr elements)
{
    auto null_array = makeASTFunction("array", makeLiteral(Field()));
    auto is_not_empty = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(path));
    return makeASTFunction("if", std::move(is_not_empty), std::move(elements), std::move(null_array));
}

/// `arrayMap(name -> body, array)`.
ASTPtr makeArrayMap(const std::string & parameter, ASTPtr body, ASTPtr array)
{
    auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(parameter));
    return makeASTFunction("arrayMap", makeASTFunction("lambda", std::move(parameters), std::move(body)), std::move(array));
}

void addUnwindIndexField(SelectChain & chain, const std::string & index_name)
{
    if (chain.fields.kind == StreamFields::Kind::Names)
        chain.fields.names.push_back(index_name);
    else
        chain.fields = {};
}

/** `$unwind` with `preserveNullAndEmptyArrays`, which keeps a document whose array is empty, with
  * the field missing and the index, if asked for, null. A left `ARRAY JOIN` would keep it with the
  * default value of the element type in place of the field, which is a value the document does not
  * have, so the empty array is replaced with an array of one null element before the join instead:
  *
  *     SELECT * EXCEPT (__mongo_unwind_element, __mongo_unwind_index)
  *            REPLACE (__mongo_unwind_element AS a), __mongo_unwind_index AS index
  *     FROM (SELECT *, if(notEmpty(a), a, [NULL]) AS __mongo_unwind_element,
  *                     if(notEmpty(a), arrayMap(i -> i - 1, arrayEnumerate(a)), [NULL]) AS __mongo_unwind_index
  *           FROM ...)
  *     ARRAY JOIN __mongo_unwind_element, __mongo_unwind_index
  *
  * The arrays are computed under names of their own, below the join, because both are computed from
  * the array itself. An array of elements that can hold no null, such as an array of arrays, has no
  * array type in common with `[NULL]`, so the query is rejected rather than unwound into fabricated
  * values.
  */
void translateUnwindPreservingEmpty(SelectChain & chain, const std::string & path, const std::string & index_name)
{
    static constexpr auto element_column = "__mongo_unwind_element";
    static constexpr auto index_column = "__mongo_unwind_index";

    auto lower_select_list = make_intrusive<ASTExpressionList>();
    lower_select_list->children.push_back(makeAsterisk({}));
    lower_select_list->children.push_back(withAlias(makeNullIfEmpty(path, make_intrusive<ASTIdentifier>(path)), element_column));

    auto join_expressions = make_intrusive<ASTExpressionList>();
    join_expressions->children.push_back(make_intrusive<ASTIdentifier>(element_column));

    std::vector<std::string> helper_columns{element_column};
    if (!index_name.empty())
    {
        /// `arrayEnumerate` counts from one and Mongo's index from zero.
        auto index = makeASTFunction("minus", make_intrusive<ASTIdentifier>("__mongo_index"), makeLiteral(Field(UInt64(1))));
        auto positions = makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(path));
        lower_select_list->children.push_back(
            withAlias(makeNullIfEmpty(path, makeArrayMap("__mongo_index", std::move(index), std::move(positions))), index_column));
        join_expressions->children.push_back(make_intrusive<ASTIdentifier>(index_column));
        helper_columns.emplace_back(index_column);
    }

    chain.select_list = std::move(lower_select_list);
    chain.select_list_preserves_order_keys = true;
    chain.wrap();

    chain.array_join = makeArrayJoin(std::move(join_expressions));

    /// The element takes the place of the array, in the position of its column.
    auto replacement = make_intrusive<ASTColumnsReplaceTransformer::Replacement>();
    replacement->name = path;
    replacement->children.push_back(make_intrusive<ASTIdentifier>(element_column));
    auto replace_transformer = make_intrusive<ASTColumnsReplaceTransformer>();
    replace_transformer->children.push_back(std::move(replacement));

    auto asterisk = makeAsterisk(helper_columns);
    asterisk->as<ASTAsterisk &>().transformers->children.push_back(std::move(replace_transformer));

    auto select_list = make_intrusive<ASTExpressionList>();
    select_list->children.push_back(std::move(asterisk));
    if (!index_name.empty())
    {
        select_list->children.push_back(withAlias(make_intrusive<ASTIdentifier>(index_column), index_name));
        addUnwindIndexField(chain, index_name);
    }
    chain.select_list = std::move(select_list);
    /// The join keeps the order of the documents, and every field but the unwound one keeps its value.
    chain.select_list_preserves_order_keys = exclusionPreservesOrderKeys(chain.order, {path});

    /// The stages that follow see documents in which the field is one element of the array.
    chain.wrap();
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

        if (auto it = stage.FindMember("preserveNullAndEmptyArrays"); it != stage.MemberEnd() && !it->value.IsNull())
        {
            /// Reading another type as `false` would silently drop the documents the client asked to keep.
            if (!it->value.IsBool())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'preserveNullAndEmptyArrays' of '$unwind' must be a boolean");
            preserve_empty = it->value.GetBool();
        }
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

    if (preserve_empty)
    {
        translateUnwindPreservingEmpty(chain, path, index_name);
        return;
    }

    auto expressions = make_intrusive<ASTExpressionList>();
    expressions->children.push_back(make_intrusive<ASTIdentifier>(path));

    if (!index_name.empty())
    {
        /// A second array of the same length is joined element by element with the first one.
        /// `arrayEnumerate` counts from one and Mongo's index from zero.
        auto body = makeASTFunction("minus", make_intrusive<ASTIdentifier>("__mongo_index"), makeLiteral(Field(UInt64(1))));
        auto positions = makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(path));
        auto indexes = makeArrayMap("__mongo_index", std::move(body), std::move(positions));
        expressions->children.push_back(withAlias(std::move(indexes), index_name));
    }

    /// Mongo drops a document whose array is empty or missing, which is what an inner `ARRAY JOIN` does.
    chain.array_join = makeArrayJoin(std::move(expressions));

    if (!index_name.empty())
    {
        /// A `*` does not expand to the columns an `ARRAY JOIN` introduces, so the index has to be
        /// named in the list of the select that produces it.
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(makeAsterisk({}));
        select_list->children.push_back(make_intrusive<ASTIdentifier>(index_name));
        chain.select_list = std::move(select_list);
        addUnwindIndexField(chain, index_name);
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

    chain.fields = StreamFields::ofProjectedFields(fields);

    auto select_list = make_intrusive<ASTExpressionList>();
    for (auto & field : fields)
        select_list->children.push_back(withAlias(field.expression, field.name));
    chain.select_list = std::move(select_list);
}

/** The names of the columns a `*` of a collection expands to, in order, as a scalar subquery:
  *
  *     (SELECT arrayMap(c -> c.2, arraySort(groupArray((position, name)))) FROM system.columns
  *      WHERE database = '<database>' AND table = '<collection>' AND default_kind != 'EPHEMERAL'
  *          AND (default_kind != 'MATERIALIZED' OR getSetting('asterisk_include_materialized_columns'))
  *          AND (default_kind != 'ALIAS' OR getSetting('asterisk_include_alias_columns')))
  */
ASTPtr makeCollectionColumnNames(const std::string & database, const std::string & collection)
{
    auto lambda = makeASTFunction(
        "lambda",
        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("__mongo_column")),
        makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("__mongo_column"), makeLiteral(Field(UInt64(2)))));
    auto names = makeASTFunction(
        "arrayMap",
        std::move(lambda),
        makeASTFunction(
            "arraySort",
            makeASTFunction(
                "groupArray",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("position"), make_intrusive<ASTIdentifier>("name")))));

    /// A `*` never expands to an `EPHEMERAL` column, and expands to a `MATERIALIZED` or an `ALIAS` one
    /// only when `asterisk_include_materialized_columns` or `asterisk_include_alias_columns` says
    /// so. The scalar subquery runs with the settings of the query, so it reads the same ones.
    auto computed_kind_is_included = [](const String & kind, const String & setting)
    {
        return makeASTFunction(
            "or",
            makeASTFunction("notEquals", make_intrusive<ASTIdentifier>("default_kind"), makeLiteral(Field(kind))),
            makeASTFunction("getSetting", makeLiteral(Field(setting))));
    };
    auto condition = makeASTFunction(
        "and",
        makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>("database"),
            database.empty() ? makeASTFunction("currentDatabase") : makeLiteral(Field(database))),
        makeASTFunction("equals", make_intrusive<ASTIdentifier>("table"), makeLiteral(Field(collection))),
        makeASTFunction("notEquals", make_intrusive<ASTIdentifier>("default_kind"), makeLiteral(Field(String("EPHEMERAL")))),
        computed_kind_is_included("MATERIALIZED", "asterisk_include_materialized_columns"),
        computed_kind_is_included("ALIAS", "asterisk_include_alias_columns"));

    SelectChain columns(makeCollectionSource("system", "columns"));
    columns.select_list = make_intrusive<ASTExpressionList>();
    columns.select_list->children.push_back(std::move(names));
    columns.where = std::move(condition);
    return makeSubquery(columns.build());
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

    SelectChain other = sub_pipeline ? translatePipeline(*sub_pipeline, collection, metadata)
                                     : SelectChain::ofCollection(metadata->getDatabaseName(), collection);

    /** A SQL `UNION ALL` matches the columns of its branches by position and names them after the
      * first branch, while `$unionWith` appends documents with their own field names. So the union
      * is translated only when both sides produce the same fields in the same order: both read a
      * collection with its columns as they are, and the columns of the two collections are checked
      * to be the same when the query runs, or both produce the same set of fields named by the
      * pipeline, which the other side is then reordered to. Otherwise a field could be silently
      * renamed, or the values of two fields swapped, so the query is rejected.
      */
    const auto & ours = chain.fields;
    const auto & theirs = other.fields;

    using Kind = StreamFields::Kind;
    if (ours.kind == Kind::Collection && theirs.kind == Kind::Collection)
    {
        /// Two different collections may have the same fields, which only the server knows, so the
        /// query checks that they do: the names of the columns of both, in order, must be equal.
        if (ours.database != theirs.database || ours.collection != theirs.collection)
        {
            auto check = makeASTFunction(
                "equals",
                makeASTFunction(
                    "throwIf",
                    makeASTFunction(
                        "notEquals",
                        makeCollectionColumnNames(ours.database, ours.collection),
                        makeCollectionColumnNames(theirs.database, theirs.collection)),
                    makeLiteral(Field(fmt::format(
                        "'$unionWith' of the collection '{}' requires it to have the same fields in the same order as the collection "
                        "'{}', because the documents of a union are matched by the position of a field rather than by its name",
                        theirs.collection,
                        ours.collection)))),
                makeLiteral(Field(UInt64(0))));

            if (!other.onlyFiltered())
                other.wrap();
            other.where = other.where ? makeASTFunction("and", other.where, std::move(check)) : std::move(check);
        }

        chain.unionWith(other.build());
        return;
    }

    auto has_duplicates = [](std::vector<std::string> names)
    {
        std::sort(names.begin(), names.end());
        return std::adjacent_find(names.begin(), names.end()) != names.end();
    };

    auto same_set = [](std::vector<std::string> lhs, std::vector<std::string> rhs)
    {
        std::sort(lhs.begin(), lhs.end());
        std::sort(rhs.begin(), rhs.end());
        return lhs == rhs;
    };

    if (ours.kind != Kind::Names || theirs.kind != Kind::Names || has_duplicates(ours.names) || has_duplicates(theirs.names)
        || !same_set(ours.names, theirs.names))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "'$unionWith' is supported only when both pipelines produce the same fields: either both read a collection with "
            "its fields as they are, or both end with stages that name every field they produce, such as '$group' or a "
            "'$project' of computed fields, with the same names on both sides");

    if (theirs.names != ours.names)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        for (const auto & name : ours.names)
            select_list->children.push_back(make_intrusive<ASTIdentifier>(name));
        other.wrap();
        other.select_list = std::move(select_list);
    }

    chain.unionWith(other.build());
}

SelectChain translatePipeline(
    const rapidjson::Value & pipeline, const std::string & collection, const std::shared_ptr<QueryMetadata> & metadata)
{
    if (!pipeline.IsArray())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A pipeline must be an array of stages");

    auto chain = SelectChain::ofCollection(metadata->getDatabaseName(), collection);

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

    return chain;
}

}

bool ParserMongoAggregateQuery::parseImpl(ASTPtr & node)
{
    auto select = translatePipeline(data, metadata->getCollectionName(), metadata).build();

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
    /// A number in a stage is a value, never the position of a column: `{"$group": {"_id": 42}}`
    /// groups every document under the key 42, which is `GROUP BY 42` without positional arguments.
    settings->changes.emplace_back("enable_positional_arguments", Field(UInt64(0)));
    select->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));

    node = std::move(select);
    return true;
}

}

}
