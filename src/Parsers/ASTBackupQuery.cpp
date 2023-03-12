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

    void formatPartitions(const ASTs & partitions, const IAST::FormattingBuffer & out)
    {
        out.writeKeyword((partitions.size() == 1) ? "PARTITION " : "PARTITIONS ");
        bool need_comma = false;
        for (const auto & partition : partitions)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ",";
            out.ostr << " ";
            partition->format(out.copy());
        }
    }

    void formatExceptDatabases(const std::set<String> & except_databases, const IAST::FormattingBuffer & out)
    {
        if (except_databases.empty())
            return;

        out.writeKeyword(" EXCEPT ");
        out.writeKeyword(except_databases.size() == 1 ? "DATABASE " : "DATABASES ");

        bool need_comma = false;
        for (const auto & database_name : except_databases)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ",";
            out.ostr << backQuoteIfNeed(database_name);
        }
    }

    void formatExceptTables(const std::set<DatabaseAndTableName> & except_tables, const IAST::FormattingBuffer & out)
    {
        if (except_tables.empty())
            return;

        out.writeKeyword(" EXCEPT ");
        out.writeKeyword(except_tables.size() == 1 ? "TABLE " : "TABLES ");

        bool need_comma = false;
        for (const auto & table_name : except_tables)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";

            if (!table_name.first.empty())
                out.ostr << backQuoteIfNeed(table_name.first) << ".";
            out.ostr << backQuoteIfNeed(table_name.second);
        }
    }

    void formatElement(const Element & element, const IAST::FormattingBuffer & out)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                out.writeKeyword("TABLE ");

                if (!element.database_name.empty())
                    out.ostr << backQuoteIfNeed(element.database_name) << ".";
                out.ostr << backQuoteIfNeed(element.table_name);

                if ((element.new_table_name != element.table_name) || (element.new_database_name != element.database_name))
                {
                    out.writeKeyword(" AS ");
                    if (!element.new_database_name.empty())
                        out.ostr << backQuoteIfNeed(element.new_database_name) << ".";
                    out.ostr << backQuoteIfNeed(element.new_table_name);
                }

                if (element.partitions)
                    formatPartitions(*element.partitions, out.copy());
                break;
            }

            case ElementType::TEMPORARY_TABLE:
            {
                out.writeKeyword("TEMPORARY TABLE ");
                out.ostr << backQuoteIfNeed(element.table_name);

                if (element.new_table_name != element.table_name)
                {
                    out.writeKeyword(" AS ");
                    out.ostr << backQuoteIfNeed(element.new_table_name);
                }
                break;
            }

            case ElementType::DATABASE:
            {
                out.writeKeyword("DATABASE ");
                out.ostr << backQuoteIfNeed(element.database_name);

                if (element.new_database_name != element.database_name)
                {
                    out.writeKeyword(" AS ");
                    out.ostr << backQuoteIfNeed(element.new_database_name);
                }

                formatExceptTables(element.except_tables, out.copy());
                break;
            }

            case ElementType::ALL:
            {
                out.writeKeyword("ALL");
                formatExceptDatabases(element.except_databases, out.copy());
                formatExceptTables(element.except_tables, out.copy());
                break;
            }
        }
    }

    void formatElements(const std::vector<Element> & elements, const IAST::FormattingBuffer & out)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ", ";
            formatElement(element, out.copy());
        }
    }

    void formatSettings(const ASTPtr & settings, const ASTFunction * base_backup_name, const ASTPtr & cluster_host_ids, const IAST::FormattingBuffer & out)
    {
        if (!settings && !base_backup_name && !cluster_host_ids)
            return;

        out.writeKeyword(" SETTINGS ");
        bool empty = true;

        if (base_backup_name)
        {
            out.ostr << "base_backup = ";
            base_backup_name->format(out.copy());
            empty = false;
        }

        if (settings)
        {
            if (!empty)
                out.ostr << ", ";
            settings->format(out.copy());
            empty = false;
        }

        if (cluster_host_ids)
        {
            if (!empty)
                out.ostr << ", ";
            out.ostr << "cluster_host_ids = ";
            cluster_host_ids->format(out.copy());
        }
    }

    ASTPtr rewriteSettingsWithoutOnCluster(ASTPtr settings, const WithoutOnClusterASTRewriteParams & params)
    {
        SettingsChanges changes;
        if (settings)
            changes = assert_cast<ASTSetQuery *>(settings.get())->changes;

        boost::remove_erase_if(
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

    return res;
}


void ASTBackupQuery::formatImpl(const FormattingBuffer & out) const
{
    out.writeKeyword((kind == Kind::BACKUP) ? "BACKUP " : "RESTORE ");

    formatElements(elements, out.copy());
    formatOnCluster(out);

    out.writeKeyword((kind == Kind::BACKUP) ? " TO " : " FROM ");
    backup_name->format(out);

    if (settings || base_backup_name)
        formatSettings(settings, base_backup_name, cluster_host_ids, out.copy());
}

ASTPtr ASTBackupQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto new_query = std::static_pointer_cast<ASTBackupQuery>(clone());
    new_query->cluster.clear();
    new_query->settings = rewriteSettingsWithoutOnCluster(new_query->settings, params);
    new_query->setCurrentDatabase(new_query->elements, params.default_database);
    return new_query;
}

}
