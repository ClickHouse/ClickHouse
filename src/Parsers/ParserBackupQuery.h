#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * BACKUP { TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_expr [,...]] |
  *          DICTIONARY [db.]dictionary_name [AS [db.]dictionary_name_in_backup] |
  *          DATABASE database_name [AS database_name_in_backup] |
  *          ALL DATABASES |
  *          TEMPORARY TABLE table_name [AS table_name_in_backup]
  *          ALL TEMPORARY TABLES |
  *          EVERYTHING } [,...]
  *        TO 'backup_name'
  *        [SETTINGS base_backup = 'base_backup_name']
  *
  * RESTORE { TABLE [db.]table_name_in_backup [INTO [db.]table_name] [PARTITION[S] partition_expr [,...]] |
  *           DICTIONARY [db.]dictionary_name_in_backup [INTO [db.]dictionary_name] |
  *           DATABASE database_name_in_backup [INTO database_name] |
  *           ALL DATABASES |
  *           TEMPORARY TABLE table_name_in_backup [INTO table_name] |
  *           ALL TEMPORARY TABLES |
  *           EVERYTHING } [,...]
  *         FROM 'backup_name'
  */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
