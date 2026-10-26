#include <Parsers/Mongo/DocumentCollection.h>

#include <unordered_set>
#include <vector>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace Mongo
{

namespace
{

bool readsDocuments(const ASTSelectQuery & select);

/** The fields an asterisk removes from what it selects, or nothing when its transformers are not
  * the exclusion of whole fields this translation builds - a `REPLACE`, an `APPLY` or a pattern
  * written by hand says nothing about which fields of a document survive it.
  */
std::optional<std::vector<std::string>> excludedFieldsOfAsterisk(const ASTAsterisk & asterisk)
{
    if (!asterisk.transformers || asterisk.transformers->children.empty())
        return std::vector<std::string>{};

    if (asterisk.transformers->children.size() != 1)
        return std::nullopt;

    const auto * except = asterisk.transformers->children.front()->as<ASTColumnsExceptTransformer>();
    if (!except || !except->getPattern())
        return std::nullopt;

    return fieldsOfSubtreePattern(*except->getPattern());
}

/** Whether an asterisk hands every stored document on unchanged. The object id is the only column
  * of a collection of documents besides the document itself, so an exclusion of it still leaves
  * every field of every document where it was; an exclusion of anything else asks for a document
  * the column does not hold.
  */
bool selectsEveryDocument(const ASTAsterisk & asterisk)
{
    const auto excluded = excludedFieldsOfAsterisk(asterisk);
    if (!excluded)
        return false;
    return excluded->empty() || (excluded->size() == 1 && excluded->front() == OBJECT_ID_COLUMN);
}

[[noreturn]] void throwCannotExcludeFieldOfDocument()
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "A collection created by this endpoint keeps whole documents in one column, and a field of a stored document cannot be left "
        "out of it: ask for the fields to keep instead of the ones to remove, or keep the collection in a table created in "
        "ClickHouse, whose fields are columns of their own");
}

/// Whether the select list is the one asterisk that hands the stored document on unchanged, so that
/// a field of a document is still a path of the `json` column of the rows the select produces.
bool selectsEveryColumn(const ASTSelectQuery & select)
{
    const auto select_list = select.select();
    if (!select_list || select_list->children.size() != 1)
        return false;

    const auto * asterisk = select_list->children.front()->as<ASTAsterisk>();
    return asterisk && selectsEveryDocument(*asterisk);
}

/// Whether a query - the one inside a subquery - produces the stored documents themselves.
bool producesDocuments(const ASTPtr & query)
{
    if (!query)
        return false;

    if (const auto * union_query = query->as<ASTSelectWithUnionQuery>())
    {
        if (!union_query->list_of_selects || union_query->list_of_selects->children.empty())
            return false;
        for (const auto & child : union_query->list_of_selects->children)
            if (!producesDocuments(child))
                return false;
        return true;
    }

    const auto * select = query->as<ASTSelectQuery>();
    return select && readsDocuments(*select) && selectsEveryColumn(*select);
}

/** Whether the rows a select reads still have the object id of the stored documents. An exclusion of
  * `_id` in a stage below - `[{$project: {_id: 0}}, {$match: ...}]` - hands the documents on without
  * it, and a select over that stage must not ask for it again.
  */
bool inputKeepsObjectId(const ASTSelectQuery & select)
{
    const auto tables = select.tables();
    if (!tables || tables->children.empty())
        return false;

    const auto * element = tables->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression)
        return false;

    const auto * table_expression = element->table_expression->as<ASTTableExpression>();
    if (!table_expression)
        return false;
    if (table_expression->database_and_table_name)
        return true;

    const auto * subquery = table_expression->subquery ? table_expression->subquery->as<ASTSubquery>() : nullptr;
    if (!subquery || subquery->children.empty())
        return false;

    auto keeps_object_id = [](const ASTPtr & query)
    {
        const auto * inner = query->as<ASTSelectQuery>();
        if (!inner)
            return false;
        const auto select_list = inner->select();
        if (!select_list || select_list->children.size() != 1)
            return false;
        const auto * asterisk = select_list->children.front()->as<ASTAsterisk>();
        if (!asterisk)
            return false;
        const auto excluded = excludedFieldsOfAsterisk(*asterisk);
        return excluded && excluded->empty() && inputKeepsObjectId(*inner);
    };

    const auto & query = subquery->children.front();
    if (const auto * union_query = query->as<ASTSelectWithUnionQuery>())
    {
        if (!union_query->list_of_selects || union_query->list_of_selects->children.empty())
            return false;
        for (const auto & child : union_query->list_of_selects->children)
            if (!keeps_object_id(child))
                return false;
        return true;
    }
    return keeps_object_id(query);
}

/** Whether the select reads the stored documents rather than the ones another select builds. Only
  * such a select reads the collection, and only there does a field name a path of a stored document:
  * the fields of the documents a stage builds are the aliases of that stage.
  *
  * A stage of a pipeline reads what the stage before it produced, so a select over a subquery that
  * hands the documents on unchanged - the `SELECT *` of a `$match`, of a `$sort`, of the branches a
  * `$unionWith` reads - still names their fields.
  */
bool readsDocuments(const ASTSelectQuery & select)
{
    const auto tables = select.tables();
    if (!tables || tables->children.empty())
        return false;

    const auto * element = tables->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression)
        return false;

    const auto * table_expression = element->table_expression->as<ASTTableExpression>();
    if (!table_expression)
        return false;
    if (table_expression->database_and_table_name)
        return true;

    const auto * subquery = table_expression->subquery ? table_expression->subquery->as<ASTSubquery>() : nullptr;
    if (!subquery || subquery->children.empty())
        return false;
    return producesDocuments(subquery->children.front());
}

/// The names the select list of a select binds. An identifier of one of them names that expression
/// rather than a field of the collection, which is how a `$sortByCount` orders by its own `count`.
void collectAliases(const ASTPtr & node, std::unordered_set<String> & aliases)
{
    if (!node)
        return;
    for (const auto & child : node->children)
    {
        if (const auto * with_alias = dynamic_cast<const ASTWithAlias *>(child.get()); with_alias && !with_alias->alias.empty())
            aliases.insert(with_alias->alias);
    }
}

/// The parameters a lambda binds, which name its element rather than a field: `$map` and `$filter`
/// bind one, and so does the array a `$push` after a `$sort` is collected into.
void collectLambdaParameters(const ASTFunction & lambda, std::unordered_set<String> & bound)
{
    if (!lambda.arguments || lambda.arguments->children.empty())
        return;
    const auto & parameters = lambda.arguments->children.front();
    if (const auto * tuple = parameters->as<ASTFunction>(); tuple && tuple->name == "tuple" && tuple->arguments)
    {
        for (const auto & parameter : tuple->arguments->children)
            if (const auto * identifier = parameter->as<ASTIdentifier>())
                bound.insert(identifier->name());
    }
    else if (const auto * identifier = parameters->as<ASTIdentifier>())
        bound.insert(identifier->name());
}

/// `json.<path>` for a field: the parts of the path are the parts of the identifier, so that the
/// dotted name of a nested field addresses the nested path of the document. A non-empty
/// `input_table` qualifies the path with the table it is read from - see `rewriteField`.
ASTPtr makeDocumentPath(const ASTIdentifier & identifier, const String & input_table = {})
{
    std::vector<String> parts;
    if (!input_table.empty())
        parts.push_back(input_table);
    parts.emplace_back(DOCUMENT_COLUMN);
    for (const auto & part : identifier.name_parts)
    {
        size_t begin = 0;
        while (begin <= part.size())
        {
            auto dot = part.find('.', begin);
            parts.push_back(part.substr(begin, dot == String::npos ? String::npos : dot - begin));
            if (dot == String::npos)
                break;
            begin = dot + 1;
        }
    }

    auto path = make_intrusive<ASTIdentifier>(std::move(parts));
    path->setAlias(identifier.tryGetAlias());
    return path;
}

/** The field of `has(flatten(array(<field>)), <value>)` and of `hasAny(<values>, flatten(array(<field>)))`,
  * which is how the filters spell Mongo's equality and its `$in`: both are tests over the elements
  * of a field that holds an array, and over a scalar field over the one element it is.
  *
  * The elements of a path of a document are `Dynamic` values, which the array functions do not
  * accept, so those two conditions are answered over the value of the path instead - see
  * `rewriteElementWiseCondition`.
  */
const ASTIdentifier * elementWiseField(const ASTPtr & node)
{
    const auto * elements = node->as<ASTFunction>();
    if (!elements || elements->name != "flatten" || !elements->arguments || elements->arguments->children.size() != 1)
        return nullptr;

    const auto * array = elements->arguments->children.front()->as<ASTFunction>();
    if (!array || array->name != "array" || !array->arguments || array->arguments->children.size() != 1)
        return nullptr;

    return array->arguments->children.front()->as<ASTIdentifier>();
}

/** Rewrites a condition over the elements of a field into one over the value of its path. A document
  * collection therefore compares the value a field holds rather than the elements of an array it
  * holds, which is the one thing of Mongo's equality it cannot express - and it is a narrower
  * answer, never a wrong one.
  */
ASTPtr rewriteElementWiseCondition(const ASTFunction & function, const String & input_table)
{
    /** Mongo's `null` equals both a `null` and a missing field, and the path of a missing field is
      * `NULL` as well. `equals` with `NULL` is `NULL` for every document rather than true, which
      * would match nothing - and under a negation, which takes a `NULL` condition as not holding,
      * everything - so the comparison with a `null` is a test of `NULL` instead.
      */
    auto make_equality = [](ASTPtr path, const ASTPtr & value) -> ASTPtr
    {
        if (const auto * literal = value->as<ASTLiteral>(); literal && literal->value.isNull())
            return makeASTFunction("isNull", std::move(path));
        return makeASTFunction("equals", std::move(path), value->clone());
    };

    if (!function.arguments || function.arguments->children.size() != 2)
        return nullptr;

    const auto & left = function.arguments->children[0];
    const auto & right = function.arguments->children[1];

    /// The object id is a column of the table rather than a path of the document, and it holds one
    /// value, so the condition over its elements is the condition over it. It is qualified with the
    /// table it is read from the same way as everywhere else - see `rewriteField`.
    auto field_or_path = [&](const ASTIdentifier & field) -> ASTPtr
    {
        if (field.name() == OBJECT_ID_COLUMN)
        {
            if (input_table.empty())
                return make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
            return make_intrusive<ASTIdentifier>(std::vector<String>{input_table, String(OBJECT_ID_COLUMN)});
        }
        return makeDocumentPath(field, input_table);
    };

    if (function.name == "has")
    {
        if (const auto * field = elementWiseField(left))
            return make_equality(field_or_path(*field), right);
        return nullptr;
    }

    if (function.name == "hasAny")
    {
        const auto * field = elementWiseField(right);
        if (!field)
            return nullptr;

        /** The candidates are compared one by one rather than by `IN`: the value of a path is a
          * `Dynamic`, which `IN` does not accept, while equality does - the same equality the
          * bare-constant filter is answered by, so `$in` and `=` agree on what they match.
          */
        const auto * candidates = left->as<ASTFunction>();
        if (!candidates || candidates->name != "array" || !candidates->arguments)
            return nullptr;

        auto path = field_or_path(*field);
        if (candidates->arguments->children.empty())
            return make_intrusive<ASTLiteral>(Field(UInt64(0)));

        /// One `or` of all of them rather than a chain of binary ones, which would be as deep as
        /// there are candidates: this runs after the depth of the query has been checked, and the
        /// tree is walked recursively afterwards.
        if (candidates->arguments->children.size() == 1)
            return make_equality(std::move(path), candidates->arguments->children.front());
        auto matches = makeASTFunction("or");
        for (const auto & candidate : candidates->arguments->children)
            matches->arguments->children.push_back(make_equality(path->clone(), candidate));
        return matches;
    }

    return nullptr;
}

/** Rewrites one expression: a field becomes the path of the document that holds it, a condition over
  * the elements of a field becomes one over the value of its path, and anything else is walked into.
  *
  * `keep_name_as_alias` is for the select list, where the column of the result must keep the name of
  * the field: `json.name` as a column name would be read back as the `name` member of a `json`
  * document, because a dotted column name is the nested document it names.
  *
  * `input_table` is the alias of the table the select reads, which the object id and the paths of
  * the document are qualified with when it is not empty, so that they name the columns of the input
  * even where the select list has an alias `_id` or `json` of its own - see `rewriteSelect`.
  */
void rewriteField(ASTPtr & node, std::unordered_set<String> bound, bool keep_name_as_alias = false, const String & input_table = {})
{
    /// The object id of the input: a column of the table rather than a path of the document.
    auto make_object_id = [&](const String & alias) -> ASTPtr
    {
        if (input_table.empty())
        {
            auto object_id = make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
            object_id->setAlias(alias);
            return object_id;
        }
        auto object_id = make_intrusive<ASTIdentifier>(std::vector<String>{input_table, String(OBJECT_ID_COLUMN)});
        /// A qualified name would also be the name of the column of the result.
        if (!alias.empty())
            object_id->setAlias(alias);
        else if (keep_name_as_alias)
            object_id->setAlias(String(OBJECT_ID_COLUMN));
        return object_id;
    };

    if (!node)
        return;

    /// A subquery reads what the select inside it produces, and that select is rewritten on its own.
    if (node->as<ASTSubquery>() || node->as<ASTSelectQuery>() || node->as<ASTSelectWithUnionQuery>())
        return;

    /** A projection of a field asks for the columns of its whole subtree, which a document has none
      * of: every field of it is a path of the one document column, and the path of a field is the
      * path of its subtree as well.
      */
    if (auto field = fieldOfSubtreeMatcher(*node))
    {
        /// A name the query binds itself is a column of the result of the select below, not a field.
        if (bound.contains(*field))
            return;

        if (*field == OBJECT_ID_COLUMN)
        {
            node = make_object_id({});
            return;
        }

        ASTIdentifier identifier(*field);
        node = makeDocumentPath(identifier, input_table);
        if (keep_name_as_alias)
            node->setAlias(*field);
        return;
    }

    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        /// The object id is a column of the table, and a name the query binds itself - an alias of
        /// the select list, the element of a lambda - is not a field of a document.
        if (bound.contains(identifier->name()))
            return;
        if (identifier->name() == OBJECT_ID_COLUMN)
        {
            node = make_object_id(identifier->tryGetAlias());
            return;
        }

        auto field_name = identifier->name();
        const bool named_by_itself = identifier->tryGetAlias().empty();
        node = makeDocumentPath(*identifier, input_table);
        if (keep_name_as_alias && named_by_itself)
            node->setAlias(field_name);
        return;
    }

    if (const auto * function = node->as<ASTFunction>())
    {
        if (auto rewritten = rewriteElementWiseCondition(*function, input_table))
        {
            rewritten->setAlias(node->tryGetAlias());
            node = std::move(rewritten);
            return;
        }

        if (function->name == "lambda")
            collectLambdaParameters(*function, bound);
    }

    for (auto & child : node->children)
        rewriteField(child, bound, /* keep_name_as_alias = */ false, input_table);
}

/** Gives the table a select reads an alias, unless it has one already, and returns that alias. The
  * fields of the input are qualified with it where a name of the select list could capture them.
  */
String aliasInputTable(ASTSelectQuery & select)
{
    static constexpr std::string_view input_alias = "__mongo_input";

    const auto tables = select.tables();
    if (!tables || tables->children.empty())
        return {};
    auto * element = tables->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression)
        return {};
    auto * table_expression = element->table_expression->as<ASTTableExpression>();
    if (!table_expression)
        return {};

    ASTPtr table = table_expression->database_and_table_name ? table_expression->database_and_table_name : table_expression->subquery;
    auto * with_alias = dynamic_cast<ASTWithAlias *>(table.get());
    if (!with_alias)
        return {};
    if (with_alias->alias.empty())
        with_alias->setAlias(String(input_alias));
    return with_alias->alias;
}

void rewriteSelect(ASTSelectQuery & select)
{
    /** The expressions of a stage are computed from the document the stage receives, never from the
      * fields it produces itself: `{$project: {value: {$toString: "$value"}}}` reads the stored
      * `value`. So the select list, the `WHERE` - a `$match` folded under a projection filters the
      * documents before it - the `GROUP BY` and the `ARRAY JOIN` name the fields of the documents,
      * even where an alias of the select list has the same name. Only the clauses that a stage
      * folded on top of the select fills - the `ORDER BY` of a `$sortByCount`, the `HAVING` of an
      * aggregation - name the columns the select produces.
      */
    std::unordered_set<String> aliases;
    collectAliases(select.select(), aliases);
    const std::unordered_set<String> input_scope;

    /** An asterisk next to fields of its own - what `$set` and `$addFields` are lowered into - keeps
      * every column of the input and adds the fields as columns beside it. The stored document is one
      * column, so the result would be that column and the new fields next to it rather than one
      * document with the fields replaced or added in it, which is refused rather than answered.
      */
    const auto select_expressions = select.select();
    if (select_expressions && select_expressions->children.size() > 1)
    {
        for (const auto & child : select_expressions->children)
        {
            if (child->as<ASTAsterisk>())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "A collection created by this endpoint keeps whole documents in one column, and a stage that adds fields to the "
                    "documents it reads, such as '$set' or '$addFields', cannot change a field of a stored document: name the fields to "
                    "return in a '$project' instead, or keep the collection in a table created in ClickHouse, whose fields are columns "
                    "of their own");
        }
    }

    /** An asterisk that removes fields - what an exclusion projection, a `$unset` and a `$project`
      * of nothing but exclusions are lowered into - cannot leave a path of the stored document out
      * of the column that holds it, and only the object id is a column of its own. This is checked
      * on every select that reads the stored documents rather than only on the outermost one: a
      * later stage - `[{$unset: "secret"}, {$match: ...}]` - wraps the exclusion in a subquery,
      * and the result would be the physical columns of the collection with the field still in.
      */
    if (select_expressions && select_expressions->children.size() == 1)
    {
        const auto * asterisk = select_expressions->children.front()->as<ASTAsterisk>();
        if (asterisk && !selectsEveryDocument(*asterisk))
            throwCannotExcludeFieldOfDocument();
    }

    /** The object id and the document are columns of the table, and an alias wins over a column of
      * the same name here (see `preferAliasesToColumns`). `{$group: {_id: null, ids: {$push: "$_id"}}}`
      * would collect its own key rather than the object ids of the documents it groups, and a
      * `$match` under `{$project: {json: ...}}` would filter on the paths of the projected `json`
      * rather than of the stored document. So the columns of the input are qualified with the table
      * they are read from when the select binds either name.
      */
    const String input_table = aliases.contains(String(OBJECT_ID_COLUMN)) || aliases.contains(String(DOCUMENT_COLUMN))
        ? aliasInputTable(select)
        : String{};

    /// A field of the select list becomes a path that keeps the name of the field as its alias.
    if (const auto select_list = select.select())
    {
        for (auto & child : select_list->children)
            rewriteField(child, input_scope, /* keep_name_as_alias = */ true, input_table);
    }

    /// A clause whose whole expression may be a field of its own is taken by reference, so that the
    /// field can be replaced. The rest are lists, whose children are replaced one by one.
    if (select.where())
        rewriteField(select.refWhere(), input_scope, /* keep_name_as_alias = */ false, input_table);
    if (select.prewhere())
        rewriteField(select.refPrewhere(), input_scope, /* keep_name_as_alias = */ false, input_table);
    if (select.having())
        rewriteField(select.refHaving(), aliases);

    if (const auto group_by = select.groupBy())
    {
        for (auto & child : group_by->children)
            rewriteField(child, input_scope, /* keep_name_as_alias = */ false, input_table);
    }

    for (const auto & list : {select.orderBy(), select.limitBy()})
    {
        if (!list)
            continue;
        for (auto & child : list->children)
            rewriteField(child, aliases);
    }

    /// An `ARRAY JOIN` of a `$unwind` names the array it walks, which is a field as well.
    if (const auto tables = select.tables())
    {
        for (auto & child : tables->children)
        {
            auto * element = child->as<ASTTablesInSelectQueryElement>();
            if (element && element->array_join)
                rewriteField(element->array_join, input_scope, /* keep_name_as_alias = */ false, input_table);
        }
    }
}

/** The predicate of a mutation - the filter of an `update` or a `delete` - names the fields of the
  * documents it matches, the same way the `WHERE` of a select does.
  */
void rewriteAlterCommands(ASTAlterQuery & alter)
{
    if (!alter.command_list)
        return;

    for (auto & command_node : alter.command_list->children)
    {
        auto * command = command_node->as<ASTAlterCommand>();
        if (!command || !command->predicate)
            continue;

        for (auto & child : command->children)
        {
            if (child.get() != command->predicate)
                continue;
            rewriteField(child, {});
            /// The command holds a raw pointer into its children, so it follows a replaced node.
            command->predicate = child.get();
        }
    }
}

/** A pipeline asks for a column of the table to win over an alias of the same name, because a stage
  * may name the field it computes after the one it reads. A document has no column of its own, so
  * there is nothing to prefer - while the object id is a column, and `_id` is also the name a
  * `$group` gives to its key, which the group has to be ordered and filtered by.
  */
void preferAliasesToColumns(ASTSelectQuery & select)
{
    const auto settings = select.settings();
    if (!settings)
        return;

    auto * set_query = settings->as<ASTSetQuery>();
    if (!set_query)
        return;

    for (auto & change : set_query->changes)
    {
        if (change.name == "prefer_column_name_to_alias")
            change.value = Field(UInt64(0));
    }
}

/// Walks every select of a query, rewriting the ones that read the collection.
void rewriteSelects(const ASTPtr & node)
{
    if (!node)
        return;

    if (auto * select = node->as<ASTSelectQuery>())
    {
        preferAliasesToColumns(*select);
        if (readsDocuments(*select))
            rewriteSelect(*select);
    }

    if (auto * alter = node->as<ASTAlterQuery>())
        rewriteAlterCommands(*alter);

    /// A `deleteMany` becomes a `DELETE FROM`, whose predicate names the fields the same way.
    if (auto * delete_query = node->as<ASTDeleteQuery>(); delete_query && delete_query->predicate)
        rewriteField(delete_query->predicate, {});

    for (const auto & child : node->children)
        rewriteSelects(child);
}

/// The outermost select of a query, which produces the rows a read returns.
ASTSelectQuery * outermostSelect(const ASTPtr & query)
{
    if (auto * select = query->as<ASTSelectQuery>())
        return select;

    if (auto * union_query = query->as<ASTSelectWithUnionQuery>();
        union_query && union_query->list_of_selects && !union_query->list_of_selects->children.empty())
        return union_query->list_of_selects->children.front()->as<ASTSelectQuery>();

    return nullptr;
}

}

void rewriteFieldsAsDocumentPaths(const ASTPtr & query)
{
    rewriteSelects(query);
}

bool selectDocumentsOfCollection(const ASTPtr & query)
{
    auto * select = outermostSelect(query);
    if (!select)
        return false;

    /// An asterisk over the result of a stage that builds documents of its own - a `$sort` after a
    /// `$group` - selects the columns of that result, not the stored documents.
    if (!readsDocuments(*select))
        return false;

    const auto select_list = select->select();
    if (!select_list || select_list->children.empty())
        return false;

    /** Only a read of the documents as they are stored is answered with them: a projection and the
      * stages of a pipeline build documents of their own, whose fields are the columns of the
      * result and are turned into a reply the way the columns of any other table are.
      */
    if (select_list->children.size() != 1)
        return false;

    const auto * asterisk = select_list->children.front()->as<ASTAsterisk>();
    if (!asterisk)
        return false;

    /** An asterisk that removes fields - what an exclusion projection, a `$unset` and a `$project`
      * of nothing but exclusions are lowered into - cannot be answered with the stored document:
      * the fields of a document are the paths of one column, and a path of it cannot be left out
      * of the value the column holds. Only the object id is a column of its own, so only its
      * exclusion is honoured, and the rest is refused rather than answered with the whole document.
      */
    const auto excluded = excludedFieldsOfAsterisk(*asterisk);
    if (!excluded || !selectsEveryDocument(*asterisk))
        throwCannotExcludeFieldOfDocument();

    /// The object id is also gone when a stage below has excluded it already.
    const bool keeps_object_id = excluded->empty() && inputKeepsObjectId(*select);

    ASTPtr document = make_intrusive<ASTIdentifier>(String(DOCUMENT_COLUMN));

    auto new_select_list = make_intrusive<ASTExpressionList>();

    /// The object id of the document, which Mongo returns as the `_id` field of it. A projection
    /// that excludes `_id` - the usual way of asking for a document without it - leaves it out.
    if (keeps_object_id)
    {
        auto object_id = make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
        new_select_list->children.push_back(std::move(object_id));
    }

    auto returned_document = document->clone();
    returned_document->setAlias(String(RETURNED_DOCUMENT_ALIAS));
    new_select_list->children.push_back(std::move(returned_document));

    auto types = makeASTFunction("JSONAllPathsWithTypes", document->clone());
    types->setAlias(String(RETURNED_TYPES_ALIAS));
    new_select_list->children.push_back(std::move(types));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(new_select_list));
    return true;
}

}

}
