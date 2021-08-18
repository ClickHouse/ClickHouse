#pragma once

#include <Parsers/IAST.h>


namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;


/** BACKUP { TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_expr [,...]] |
  *          DICTIONARY [db.]dictionary_name [AS [db.]dictionary_name_in_backup] |
  *          DATABASE database_name [AS database_name_in_backup] |
  *          ALL DATABASES |
  *          TEMPORARY TABLE table_name [AS table_name_in_backup]
  *          ALL TEMPORARY TABLES |
  *          EVERYTHING } [,...]
  *        TO 'backup_name'
  *        SETTINGS base_backup='base_backup_name'
  *
  * RESTORE { TABLE [db.]table_name_in_backup [INTO [db.]table_name] [PARTITION[S] partition_expr [,...]] |
  *           DICTIONARY [db.]dictionary_name_in_backup [INTO [db.]dictionary_name] |
  *           DATABASE database_name_in_backup [INTO database_name] |
  *           ALL DATABASES |
  *           TEMPORARY TABLE table_name_in_backup [INTO table_name] |
  *           ALL TEMPORARY TABLES |
  *           EVERYTHING } [,...]
  *         FROM 'backup_name'
  *
  * Notes:
  * RESTORE doesn't drop any data, it either creates a table or appends an existing table with restored data.
  * This behaviour can cause data duplication.
  * If appending isn't possible because the existing table has incompatible format then RESTORE will throw an exception.
  *
  * The "UNDER NAME" clause is useful to backup or restore under another name.
  * For the BACKUP command this clause allows to set the name which an object will have inside the backup.
  * And for the RESTORE command this clause allows to set the name which an object will have after RESTORE has finished.
  *
  * "ALL DATABASES" means all databases except the system database and the internal database containing temporary tables.
  * "EVERYTHING" works exactly as "ALL DATABASES, ALL TEMPORARY TABLES"
  *
  * The "WITH BASE" clause allows to set a base backup. Only differences made after the base backup will be
  * included in a newly created backup, so this option allows to make an incremental backup.
  */
class ASTBackupQuery : public IAST
{
public:
    enum Kind
    {
        BACKUP,
        RESTORE,
    };
    Kind kind = Kind::BACKUP;

    enum ElementType
    {
        TABLE,
        DICTIONARY,
        DATABASE,
        ALL_DATABASES,
        TEMPORARY_TABLE,
        ALL_TEMPORARY_TABLES,
        EVERYTHING,
    };

    struct Element
    {
        ElementType type;
        DatabaseAndTableName name;
        DatabaseAndTableName new_name;
        ASTs partitions;
        std::set<String> except_list;
    };

    using Elements = std::vector<Element>;
    Elements elements;

    String backup_name;

    ASTPtr settings;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
