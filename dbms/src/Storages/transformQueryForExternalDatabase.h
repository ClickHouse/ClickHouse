#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

class IAST;
class Context;

/** For given ClickHouse query,
  * creates another query in a form of
  *
  * SELECT columns... FROM db.table WHERE ...
  *
  * where 'columns' are all required columns to read from "left" table of original query,
  * and WHERE contains subset of (AND-ed) conditions from original query,
  * that contain only compatible expressions.
  *
  * Compatible expressions are comparisons of identifiers, constants, and logical operations on them.
  *
  * NOTE There are concerns with proper quoting of identifiers for remote database.
  * Some databases use `quotes` and other use "quotes".
  */
String transformQueryForExternalDatabase(
    const IAST & query,
    const NamesAndTypesList & available_columns,
    const String & database,
    const String & table,
    const Context & context);

}
