#pragma once

#include <vector>

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

/** Appends the document of one row of a result that holds the documents of a collection which keeps
  * them in one `JSON` column, rather than a column per field: such a result has the document of each
  * row in one column (see `Mongo::RETURNED_DOCUMENT_ALIAS`), the types of its paths next to it, and
  * the object id before them. Whether a result is of that shape is what the rewrite of the query
  * answered, not anything the result itself carries. to `document`.
  *
  * The text of a result carries no types, so the type of every path - which is what tells a date
  * from the string it is written as, and an integer from a number - is taken from the map of the
  * paths of the document that is selected next to it.
  */
void appendDocumentOfRow(bson_t * document, const rapidjson::Value & row);

}
