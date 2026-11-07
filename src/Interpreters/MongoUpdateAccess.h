#pragma once

#include "config.h"

#if USE_RAPIDJSON

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <base/types.h>

#include <vector>

namespace DB
{

/** All the columns of the table that an update of the Mongo dialect writes to, in the order of the
  * table, or nothing when there is no such table. These are all of them, not only the ones the user
  * may see: `$unset`, `$rename` and `$set` of a subdocument write the whole of it, see
  * `Mongo::expandMongoSubtreeAssignments`, and narrowing it to the visible columns would turn the
  * write of a subdocument into the write of a part of it. `expandMongoSubtreeAssignmentsCheckingAccess` then makes sure
  * the user may write all of it.
  */
std::vector<String> getMongoCollectionColumns(const String & database, const String & table, const ContextPtr & context);

/** `Mongo::expandMongoSubtreeAssignments` of the `ALTER TABLE ... UPDATE` translated from the Mongo
  * dialect over `columns`, which checks that the user may update every column the expanded update
  * writes. A column the user may see is left to the interpreter, whose error names it. A column the
  * user may not see fails the update with an error that does not name it, before any other error
  * of the expansion could, so that the update reveals nothing about the columns hidden from the user.
  */
void expandMongoSubtreeAssignmentsCheckingAccess(ASTPtr & ast, const std::vector<String> & columns, const ContextPtr & context);

}

#endif
