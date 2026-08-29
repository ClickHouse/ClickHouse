#include <Parsers/Mongo/DocumentCollection.h>

#include <unordered_set>
#include <vector>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
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

namespace Mongo
{

namespace
{

bool readsDocuments(const ASTSelectQuery & select);

/// Whether the select list is the one asterisk that hands the stored document on unchanged, so that
/// a field of a document is still a path of the `json` column of the rows the select produces.
bool selectsEveryColumn(const ASTSelectQuery & select)
{
    const auto select_list = select.select();
    return select_list && select_list->children.size() == 1 && select_list->children.front()->as<ASTAsterisk>();
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
/// dotted name of a nested field addresses the nested path of the document.
ASTPtr makeDocumentPath(const ASTIdentifier & identifier)
{
    std::vector<String> parts;
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
ASTPtr rewriteElementWiseCondition(const ASTFunction & function)
{
    if (!function.arguments || function.arguments->children.size() != 2)
        return nullptr;

    const auto & left = function.arguments->children[0];
    const auto & right = function.arguments->children[1];

    /// The object id is a column of the table rather than a path of the document, and it holds one
    /// value, so the condition over its elements is the condition over it.
    auto field_or_path = [](const ASTIdentifier & field) -> ASTPtr
    {
        if (field.name() == OBJECT_ID_COLUMN)
            return make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
        return makeDocumentPath(field);
    };

    if (function.name == "has")
    {
        if (const auto * field = elementWiseField(left))
            return makeASTFunction("equals", field_or_path(*field), right->clone());
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

        ASTPtr matches;
        for (const auto & candidate : candidates->arguments->children)
        {
            auto match = makeASTFunction("equals", path->clone(), candidate->clone());
            matches = matches ? makeASTFunction("or", std::move(matches), std::move(match)) : std::move(match);
        }
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
  */
void rewriteField(ASTPtr & node, std::unordered_set<String> bound, bool keep_name_as_alias = false)
{
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
            node = make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
            return;
        }

        ASTIdentifier identifier(*field);
        node = makeDocumentPath(identifier);
        if (keep_name_as_alias)
            node->setAlias(*field);
        return;
    }

    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        /// The object id is a column of the table, and a name the query binds itself - an alias of
        /// the select list, the element of a lambda - is not a field of a document.
        if (identifier->name() == OBJECT_ID_COLUMN || bound.contains(identifier->name()))
            return;

        auto field_name = identifier->name();
        const bool named_by_itself = identifier->tryGetAlias().empty();
        node = makeDocumentPath(*identifier);
        if (keep_name_as_alias && named_by_itself)
            node->setAlias(field_name);
        return;
    }

    if (const auto * function = node->as<ASTFunction>())
    {
        if (auto rewritten = rewriteElementWiseCondition(*function))
        {
            rewritten->setAlias(node->tryGetAlias());
            node = std::move(rewritten);
            return;
        }

        if (function->name == "lambda")
            collectLambdaParameters(*function, bound);
    }

    for (auto & child : node->children)
        rewriteField(child, bound);
}

void rewriteSelect(ASTSelectQuery & select)
{
    std::unordered_set<String> aliases;
    collectAliases(select.select(), aliases);

    /** The clauses whose whole expression may be an identifier of its own are taken by reference, so
      * that such an identifier can be replaced. The rest are lists, and a list is never replaced -
      * only the identifiers inside it are, which a copy of the pointer to it reaches all the same.
      */
    /// A field of the select list becomes a path that keeps the name of the field as its alias.
    if (const auto select_list = select.select())
    {
        for (auto & child : select_list->children)
            rewriteField(child, aliases, /* keep_name_as_alias = */ true);
    }

    /// A clause whose whole expression may be a field of its own is taken by reference, so that the
    /// field can be replaced. The rest are lists, whose children are replaced one by one.
    if (select.where())
        rewriteField(select.refWhere(), aliases);
    if (select.prewhere())
        rewriteField(select.refPrewhere(), aliases);
    if (select.having())
        rewriteField(select.refHaving(), aliases);

    for (const auto & list : {select.groupBy(), select.orderBy(), select.limitBy()})
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
                rewriteField(element->array_join, aliases);
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

    const auto select_list = select->select();
    if (!select_list || select_list->children.empty())
        return false;

    /** Only a read of the documents as they are stored is answered with them: a projection and the
      * stages of a pipeline build documents of their own, whose fields are the columns of the
      * result and are turned into a reply the way the columns of any other table are.
      */
    if (select_list->children.size() != 1 || !select_list->children.front()->as<ASTAsterisk>())
        return false;

    ASTPtr document = make_intrusive<ASTIdentifier>(String(DOCUMENT_COLUMN));

    auto new_select_list = make_intrusive<ASTExpressionList>();

    /// The object id of the document, which Mongo returns as the `_id` field of it.
    auto object_id = make_intrusive<ASTIdentifier>(String(OBJECT_ID_COLUMN));
    new_select_list->children.push_back(std::move(object_id));

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
