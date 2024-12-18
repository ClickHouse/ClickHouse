#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <IO/Operators.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    void formatPartitions(const ASTs & partitions, const IAST::FormatSettings & format)
    {
        format.ostr << " " << (format.hilite ? IAST::hilite_keyword : "") << ((partitions.size() == 1) ? "PARTITION" : "PARTITIONS") << " "
                    << (format.hilite ? IAST::hilite_none : "");
        bool need_comma = false;
        for (const auto & partition : partitions)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            format.ostr << " ";
            partition->format(format);
        }
    }

    void formatExceptDatabases(const std::set<String> & except_databases, const IAST::FormatSettings & format)
    {
        if (except_databases.empty())
            return;

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " EXCEPT "
                    << (except_databases.size() == 1 ? "DATABASE" : "DATABASES") << " " << (format.hilite ? IAST::hilite_none : "");

        bool need_comma = false;
        for (const auto & database_name : except_databases)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            format.ostr << backQuoteIfNeed(database_name);
        }
    }

    void formatExceptTables(const std::set<DatabaseAndTableName> & except_tables, const IAST::FormatSettings & format)
    {
        if (except_tables.empty())
            return;

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " EXCEPT " << (except_tables.size() == 1 ? "TABLE" : "TABLES") << " "
                    << (format.hilite ? IAST::hilite_none : "");

        bool need_comma = false;
        for (const auto & table_name : except_tables)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ", ";

            if (!table_name.first.empty())
                format.ostr << backQuoteIfNeed(table_name.first) << ".";
            format.ostr << backQuoteIfNeed(table_name.second);
        }
    }

    void formatElement(const Element & element, const IAST::FormatSettings & format)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "TABLE " << (format.hilite ? IAST::hilite_none : "");

                if (!element.database_name.empty())
                    format.ostr << backQuoteIfNeed(element.database_name) << ".";
                format.ostr << backQuoteIfNeed(element.table_name);

                if ((element.new_table_name != element.table_name) || (element.new_database_name != element.database_name))
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                    if (!element.new_database_name.empty())
                        format.ostr << backQuoteIfNeed(element.new_database_name) << ".";
                    format.ostr << backQuoteIfNeed(element.new_table_name);
                }

                if (element.partitions)
                    formatPartitions(*element.partitions, format);
                break;
            }

            case ElementType::TEMPORARY_TABLE:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "TEMPORARY TABLE " << (format.hilite ? IAST::hilite_none : "");
                format.ostr << backQuoteIfNeed(element.table_name);

                if (element.new_table_name != element.table_name)
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                    format.ostr << backQuoteIfNeed(element.new_table_name);
                }
                break;
            }

            case ElementType::DATABASE:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "");
                format.ostr << "DATABASE ";
                format.ostr << (format.hilite ? IAST::hilite_none : "");
                format.ostr << backQuoteIfNeed(element.database_name);

                if (element.new_database_name != element.database_name)
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " AS " << (format.hilite ? IAST::hilite_none : "");
                    format.ostr << backQuoteIfNeed(element.new_database_name);
                }

                formatExceptTables(element.except_tables, format);
                break;
            }

            case ElementType::ALL:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "ALL" << (format.hilite ? IAST::hilite_none : "");
                formatExceptDatabases(element.except_databases, format);
                formatExceptTables(element.except_tables, format);
                break;
            }
        }
    }

    void formatElements(const std::vector<Element> & elements, const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ", ";
            formatElement(element, format);
        }
    }

    void formatSettings(const ASTPtr & settings, const ASTFunction * base_backup_name, const ASTPtr & cluster_host_ids, const IAST::FormatSettings & format)
    {
        if (!settings && !base_backup_name && !cluster_host_ids)
            return;

        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " SETTINGS " << (format.hilite ? IAST::hilite_none : "");
        bool empty = true;

        if (base_backup_name)
        {
            format.ostr << "base_backup = ";
            base_backup_name->format(format);
            empty = false;
        }

        if (settings)
        {
            if (!empty)
                format.ostr << ", ";
            settings->format(format);
            empty = false;
        }

        if (cluster_host_ids)
        {
            if (!empty)
                format.ostr << ", ";
            format.ostr << "cluster_host_ids = ";
            cluster_host_ids->format(format);
        }
    }

    ASTPtr rewriteSettingsWithoutOnCluster(ASTPtr settings, const WithoutOnClusterASTRewriteParams & params)
    {
        SettingsChanges changes;
        if (settings)
            changes = assert_cast<ASTSetQuery *>(settings.get())->changes;

        std::erase_if(
            changes,
            [](const SettingChange & change)
            {
                const String & name = change.name;
                return (name == "internal") || (name == "async") || (name == "host_id");
            });

        changes.emplace_back("internal", true);
        changes.emplace_back("async", true);
        changes.emplace_back("host_id", params.host_id);

        auto out_settings = std::make_shared<ASTSetQuery>();
        out_settings->changes = std::move(changes);
        out_settings->is_standalone = false;
        return out_settings;
    }
}


void ASTBackupQuery::Element::setCurrentDatabase(const String & current_database)
{
    if (current_database.empty())
        return;

    if (type == ASTBackupQuery::TABLE)
    {
        if (database_name.empty())
            database_name = current_database;
        if (new_database_name.empty())
            new_database_name = current_database;
    }
    else if (type == ASTBackupQuery::ALL)
    {
        for (auto it = except_tables.begin(); it != except_tables.end();)
        {
            const auto & except_table = *it;
            if (except_table.first.empty())
            {
                except_tables.emplace(DatabaseAndTableName{current_database, except_table.second});
                it = except_tables.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}


void ASTBackupQuery::setCurrentDatabase(ASTBackupQuery::Elements & elements, const String & current_database)
{
    for (auto & element : elements)
        element.setCurrentDatabase(current_database);
}


String ASTBackupQuery::getID(char) const
{
    return (kind == Kind::BACKUP) ? "BackupQuery" : "RestoreQuery";
}


ASTPtr ASTBackupQuery::clone() const
{
    auto res = std::make_shared<ASTBackupQuery>(*this);
    res->children.clear();

    if (backup_name)
        res->set(res->backup_name, backup_name->clone());

    if (base_backup_name)
        res->set(res->base_backup_name, base_backup_name->clone());

    if (cluster_host_ids)
        res->cluster_host_ids = cluster_host_ids->clone();

    if (settings)
        res->settings = settings->clone();

    cloneOutputOptions(*res);

    return res;
}


void ASTBackupQuery::formatQueryImpl(const FormatSettings & fs, FormatState &, FormatStateStacked) const
{
    fs.ostr << (fs.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? "BACKUP " : "RESTORE ") << (fs.hilite ? hilite_none : "");

    formatElements(elements, fs);
    formatOnCluster(fs);

    fs.ostr << (fs.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO " : " FROM ") << (fs.hilite ? hilite_none : "");
    backup_name->format(fs);

    if (settings || base_backup_name)
        formatSettings(settings, base_backup_name, cluster_host_ids, fs);
}

ASTPtr ASTBackupQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto new_query = std::static_pointer_cast<ASTBackupQuery>(clone());
    new_query->cluster.clear();
    new_query->settings = rewriteSettingsWithoutOnCluster(new_query->settings, params);
    new_query->setCurrentDatabase(new_query->elements, params.default_database);
    return new_query;
}

IAST::QueryKind ASTBackupQuery::getQueryKind() const
{
    return kind == Kind::BACKUP ? QueryKind::Backup : QueryKind::Restore;
}

}
