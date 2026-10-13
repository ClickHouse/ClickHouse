#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace Poco::JSON { class Object; }

namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;
class ASTFunction;
class ASTSnapshotQuery;


/** BACKUP { TABLE [db.]table_name [AS [db.]table_name_in_backup] [PARTITION[S] partition_expr [,...]] [EXCEPT DATA FROM TABLE [db.]table_name] |
  *          DICTIONARY [db.]dictionary_name [AS [db.]dictionary_name_in_backup] [EXCEPT DATA FROM TABLE [db.]dictionary_name] |
  *          DATABASE database_name [AS database_name_in_backup] [EXCEPT TABLES ...] [EXCEPT DATA FROM {TABLE|TABLES} ...] |
  *          TEMPORARY TABLE table_name [AS table_name_in_backup] [EXCEPT DATA FROM TABLE table_name] |
  *          ALL [EXCEPT {TABLES|DATABASES}...] [EXCEPT DATA FROM {TABLE|TABLES} ...] } [,...]
  *        [ON CLUSTER 'cluster_name']
  *        TO { File('path/') |
  *             Disk('disk_name', 'path/') }
  *        [SETTINGS ...]
  *
  * RESTORE { TABLE [db.]table_name_in_backup [AS [db.]table_name] [PARTITION[S] partition_expr [,...]] |
  *           DICTIONARY [db.]dictionary_name_in_backup [AS [db.]dictionary_name] |
  *           DATABASE database_name_in_backup [AS database_name] [EXCEPT TABLES ...] |
  *           TEMPORARY TABLE table_name_in_backup [AS table_name] |
  *           ALL [EXCEPT {TABLES|DATABASES} ...] } [,...]
  *         [ON CLUSTER 'cluster_name']
  *         FROM { File('path/') |
  *                Disk('disk_name', 'path/') }
  *        [SETTINGS ...]
  *
  * Notes:
  * RESTORE doesn't drop any data, it either creates a table or appends an existing table with restored data.
  * This behaviour can cause data duplication.
  * If appending isn't possible because the existing table has incompatible format then RESTORE will throw an exception.
  *
  * The "AS" clause is useful to backup or restore under another name.
  * For the BACKUP command this clause allows to set the name which an object will have inside the backup.
  * And for the RESTORE command this clause allows to set the name which an object will have after RESTORE has finished.
  *
  * The "EXCEPT DATA FROM {TABLE|TABLES}" clause (BACKUP only) puts a table's definition in the backup without
  * its data, so the table is restored empty. The clause is scoped to the element it is written on:
  * on a DATABASE or ALL element it may name any table selected by that element, while on a single-object
  * element (TABLE, DICTIONARY, VIEW, TEMPORARY TABLE) the only object in scope is the element's own one,
  * so it is represented by the `except_data` flag rather than by a list of names.
  */
class ASTBackupQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
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
        TEMPORARY_TABLE,
        DATABASE,
        ALL,
    };

    struct Element
    {
        ElementType type{};
        String table_name;
        String database_name;
        String new_table_name; /// usually the same as `table_name`, can be different in case of using AS <new_name>
        String new_database_name; /// usually the same as `database_name`, can be different in case of using AS <new_name>
        std::optional<ASTs> partitions;
        std::set<DatabaseAndTableName> except_tables;

        /// Tables of this element whose data must not be put in the backup (EXCEPT DATA FROM TABLE/TABLES).
        /// Only DATABASE and ALL elements can carry it: those elements select many tables, so the excluded
        /// ones have to be named. A single-object element selects exactly one object, and the clause written
        /// on it can only refer to that object, which `except_data` below expresses. Keeping the two cases in
        /// separate fields is what makes an exclusion element-scoped: a name written on one element cannot
        /// reach the tables selected by another element of the same query.
        std::set<DatabaseAndTableName> except_data_tables;

        /// TABLE / DICTIONARY / VIEW / TEMPORARY TABLE elements only: the data of this element's own object
        /// must not be put in the backup, i.e. `EXCEPT DATA FROM TABLE <this element's object>` was written.
        bool except_data = false;

        /// TABLE elements only, and only while `database_name` is still unresolved: the database name written
        /// in the `EXCEPT DATA FROM TABLE` clause, which the parser could not yet compare with the element's
        /// own database. An unqualified element takes its database from the current database, which the parser
        /// does not know, so `BACKUP TABLE t EXCEPT DATA FROM TABLE db.t` cannot be decided at parse time.
        /// `setCurrentDatabase` performs that comparison once the element's database is known, and clears this
        /// field. Empty when the clause named no database, or named one the parser could already verify.
        String except_data_database_name;

        std::set<String> except_databases;

        /// Only member functions are declared below: `Element` is aggregate-initialized (see
        /// `fromSnapshotQuery`), so it must keep no constructors and no non-public data members.

        /// Resolves the element against the current database: fills in the database names it left out, and
        /// then performs the `EXCEPT DATA FROM TABLE` comparison the parser had to defer. Throws
        /// `SYNTAX_ERROR` if that comparison fails. Idempotent.
        void setCurrentDatabase(const String & current_database);

        /// Substitutes `current_database` for every database name this element left out.
        void fillEmptyDatabaseNames(const String & current_database);

        /// Checks `except_data_database_name` against the element's own (now resolved) database name, and
        /// clears it once checked. See the field's comment for why the check cannot happen at parse time.
        void checkExceptDataDatabaseName();
    };

    using Elements = std::vector<Element>;
    static void setCurrentDatabase(Elements & elements, const String & current_database);
    void setCurrentDatabase(const String & current_database) { setCurrentDatabase(elements, current_database); }

    static ASTPtr fromSnapshotQuery(const ASTSnapshotQuery & query);

    Elements elements;

    ASTFunction * backup_name = nullptr;

    ASTPtr settings;

    /// Base backup. Only differences made after the base backup will be included in a newly created backup,
    /// so this setting allows to make an incremental backup.
    ASTFunction * base_backup_name = nullptr;

    /// Base snapshot for lightweight snapshot-based backups. Specified using the FROM_SNAPSHOT clause.
    ASTFunction * base_snapshot_name = nullptr;

    /// List of cluster's hosts' IDs if this is a BACKUP/RESTORE ON CLUSTER command.
    ASTPtr cluster_host_ids;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & fs, FormatState &, FormatStateStacked) const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override;
    QueryKind getQueryKind() const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(reinterpret_cast<IAST **>(&backup_name), nullptr);
        f(reinterpret_cast<IAST **>(&base_backup_name), nullptr);
        f(reinterpret_cast<IAST **>(&base_snapshot_name), nullptr);
    }
};

}
