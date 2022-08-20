#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * BACKUP { TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_expr [,...]] |
  *          DICTIONARY [db.]dictionary_name [AS [db.]dictionary_name_in_backup] |
  *          DATABASE database_name [AS database_name_in_backup] [EXCEPT TABLES ...] |
  *          TEMPORARY TABLE table_name [AS table_name_in_backup] |
  *          ALL [EXCEPT {TABLES|DATABASES}...] } [,...]
  *        [ON CLUSTER 'cluster_name']
  *        TO { File('path/') |
  *             Disk('disk_name', 'path/') }
  *        [SETTINGS ...]
  *
  * RESTORE { TABLE [db.]table_name_in_backup [AS [db.]table_name] [PARTITION[S] partition_expr [,...]] |
  *          DICTIONARY [db.]dictionary_name_in_backup [AS [db.]dictionary_name] |
  *          DATABASE database_name_in_backup [AS database_name] [EXCEPT TABLES ...] |
  *          TEMPORARY TABLE table_name_in_backup [AS table_name] |
  *          ALL [EXCEPT {TABLES|DATABASES} ...] } [,...]
  *         [ON CLUSTER 'cluster_name']
  *         FROM { File('path/') |
  *                Disk('disk_name', 'path/') }
  *        [SETTINGS ...]
  */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
