#include <Parsers/ASTBackupQuery.h>
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

    void formatExceptList(const char * except_keyword, const std::set<String> & except_list, const IAST::FormatSettings & format)
    {
        if (except_list.empty())
            return;

        format.ostr << " " << (format.hilite ? IAST::hilite_keyword : "") << except_keyword << (format.hilite ? IAST::hilite_none : "") << " ";

        bool need_comma = false;
        for (const auto & item : except_list)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            format.ostr << " " << backQuoteIfNeed(item);
        }
    }

    void formatElement(const Element & element, const IAST::FormatSettings & format)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "");
                if (element.is_temporary_database)
                    format.ostr << "TEMPORARY TABLE ";
                else
                    format.ostr << "TABLE ";
                format.ostr << (format.hilite ? IAST::hilite_none : "");

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

                if (!element.except_list.empty())
                    formatExceptList("EXCEPT TABLES", element.except_list, format);
                break;
            }

            case ElementType::ALL_DATABASES:
            {
                format.ostr << (format.hilite ? IAST::hilite_keyword : "") << "ALL DATABASES" << (format.hilite ? IAST::hilite_none : "");

                if (!element.except_list.empty())
                    formatExceptList("EXCEPT", element.except_list, format);
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

    void formatSettings(const ASTPtr & settings, const ASTPtr & base_backup_name, const ASTPtr & cluster_host_ids, const IAST::FormatSettings & format)
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

        boost::remove_erase_if(
            changes,
            [](const SettingChange & change)
            {
                const String & name = change.name;
                return (name == "internal") || (name == "async") || (name == "host_id");
            });

        changes.emplace_back("internal", true);
        changes.emplace_back("async", false);
        changes.emplace_back("host_id", params.host_id);

        auto out_settings = std::make_shared<ASTSetQuery>();
        out_settings->changes = std::move(changes);
        out_settings->is_standalone = false;
        return out_settings;
    }
}


void ASTBackupQuery::Element::setCurrentDatabase(const String & current_database)
{
    if ((type == ASTBackupQuery::TABLE) && !is_temporary_database)
    {
        if (database_name.empty())
            database_name = current_database;
        if (new_database_name.empty())
            new_database_name = current_database;
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
    return std::make_shared<ASTBackupQuery>(*this);
}


void ASTBackupQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? "BACKUP " : "RESTORE ")
                << (format.hilite ? hilite_none : "");

    formatElements(elements, format);
    formatOnCluster(format);

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO " : " FROM ") << (format.hilite ? hilite_none : "");
    backup_name->format(format);

    if (settings || base_backup_name)
        formatSettings(settings, base_backup_name, cluster_host_ids, format);
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
