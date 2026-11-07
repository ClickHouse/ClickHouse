#pragma once

#include <string_view>

#include <base/types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace Mongo
{

/** A collection the Mongo endpoint creates keeps whole documents in one `JSON` column instead of a
  * column per field, because a Mongo collection has no schema: a document may hold a field that no
  * document before it had, and asking whether it holds one at all is a question about the document
  * rather than about the table.
  *
  * A table that was created in ClickHouse keeps its own columns, and a field of a query names the
  * column of the same name there, so both shapes are read through the same translation: the query
  * of a document collection is rewritten to address the fields as the paths of its `JSON` column.
  */

/// The column that holds the document of a row.
inline constexpr std::string_view DOCUMENT_COLUMN = "json";

/// The column that holds the Mongo object id of the document, which is the primary key of the table.
inline constexpr std::string_view OBJECT_ID_COLUMN = "_id";

/// A table comment marks a collection created by the Mongo endpoint. The column names alone do
/// not distinguish it from a ClickHouse table that happens to use the same names and types.
inline constexpr std::string_view DOCUMENT_COLLECTION_COMMENT = "Created by the MongoDB protocol";

/** The name the document that a read returns is selected under. It must not be the name of the
  * document column itself: an alias shadows a column of the same name, so a filter over a field
  * would read the document the projection built rather than the stored one.
  */
inline constexpr std::string_view RETURNED_DOCUMENT_ALIAS = "__mongo_document";

/// The name the types of the paths of the returned document are selected under. The text of a
/// result carries no types, so they are what a date, a decimal or an integer is restored from.
inline constexpr std::string_view RETURNED_TYPES_ALIAS = "__mongo_types";

/** Rewrites the queries translated from Mongo that read a collection of whole documents, so that
  * every field names a path of the document column: `name` becomes `json.name` and `profile.name`
  * becomes `json.profile.name`, while `_id` stays the column of the same name.
  *
  * Only the selects that read the collection itself are rewritten. The outer selects of a
  * translated aggregation pipeline read the documents that the stages below them build, whose
  * fields are the aliases of those stages rather than paths of a stored document.
  */
void rewriteFieldsAsDocumentPaths(const ASTPtr & query);

/** Replaces the select list of a read that asks for every field of the documents of a collection by
  * the stored document itself, with the object id and the types of its paths next to it. A query
  * that projects fields or aggregates them builds documents of its own, and those are turned into a
  * reply out of the columns of the result, like the columns of any other table - so such a query is
  * left alone.
  *
  * Returns whether the read answers with the stored documents, which is how the reply encoder
  * learns what the rows of the result hold - the name of the alias is not a signal of its own.
  */
bool selectDocumentsOfCollection(const ASTPtr & query);

}

}
