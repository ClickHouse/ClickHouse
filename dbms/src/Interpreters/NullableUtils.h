#include <Columns/ColumnNullable.h>


namespace DB
{

/** Replace Nullable key_columns to corresponding nested columns.
  * In 'null_map' return a map of positions where at least one column was NULL.
  * null_map_holder could take ownership of null_map, if required.
  */
void extractNestedColumnsAndNullMap(ColumnRawPtrs & key_columns, ColumnPtr & null_map_holder, ConstNullMapPtr & null_map);

}
