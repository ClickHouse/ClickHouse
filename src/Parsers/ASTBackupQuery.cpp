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

    void formatTypeWithName(const DatabaseAndTableName & name, bool name_is_in_temp_db, ElementType type, bool show_type, const IAST::FormatSettings & format)
    {
        switch (type)
        {
            case ElementType::TABLE:
            {
                if (show_type)
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "");
                    if (name_is_in_temp_db)
                        format.ostr << " TEMPORARY TABLE";
                    else
                        format.ostr << " TABLE";
                    format.ostr << (format.hilite ? IAST::hilite_none : "");
                }

                format.ostr << " ";
                if (!name_is_in_temp_db && !name.first.empty())
                    format.ostr << backQuoteIfNeed(name.first) << ".";
                format.ostr << backQuoteIfNeed(name.second);
                break;
            }
            case ElementType::DATABASE:
            {
                if (show_type)
                {
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "");
                    if (name_is_in_temp_db)
                        format.ostr << " ALL TEMPORARY TABLES";
                    else
                        format.ostr << " DATABASE";
                    format.ostr << (format.hilite ? IAST::hilite_none : "");
                }

                if (!name_is_in_temp_db)
                    format.ostr << " " << backQuoteIfNeed(name.first);
                break;
            }
            case ElementType::ALL_DATABASES:
            {
                if (show_type)
                    format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ALL DATABASES" << (format.hilite ? IAST::hilite_none : "");
                break;
            }
        }
    }

    void formatPartitions(const ASTs & partitions, const IAST::FormatSettings & format)
    {
        if (partitions.empty())
            return;
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " " << ((partitions.size() == 1) ? "PARTITION" : "PARTITIONS") << " "
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

    void formatExceptList(const std::set<String> & except_list, const IAST::FormatSettings & format)
    {
        if (except_list.empty())
            return;
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " EXCEPT "
                    << (format.hilite ? IAST::hilite_none : "");
        bool need_comma = false;
        for (const auto & item : except_list)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            format.ostr << " " << backQuoteIfNeed(item);
        }
    }

    void formatElement(const Element & element, Kind kind, const IAST::FormatSettings & format)
    {
        formatTypeWithName(element.name, element.name_is_in_temp_db, element.type, true, format);

        formatPartitions(element.partitions, format);
        formatExceptList(element.except_list, format);

        bool new_name_is_different = (element.new_name != element.name) || (element.new_name_is_in_temp_db != element.name_is_in_temp_db);
        if (new_name_is_different)
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " " << ((kind == Kind::BACKUP) ? "AS" : "INTO")
                        << (format.hilite ? IAST::hilite_none : "");
            bool show_type = (element.new_name_is_in_temp_db != element.name_is_in_temp_db);
            formatTypeWithName(element.new_name, element.new_name_is_in_temp_db, element.type, show_type, format);
        }
    }

    void formatElements(const std::vector<Element> & elements, Kind kind, const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                format.ostr << ",";
            formatElement(element, kind, format);
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


void ASTBackupQuery::Element::setDatabase(const String & new_database)
{
    if (type == ASTBackupQuery::TABLE)
    {
        if (name.first.empty() && !name.second.empty() && !name_is_in_temp_db)
            name.first = new_database;
        if (new_name.first.empty() && !name.second.empty() && !name_is_in_temp_db)
            new_name.first = new_database;
    }
}


void ASTBackupQuery::setDatabase(ASTBackupQuery::Elements & elements, const String & new_database)
{
    for (auto & element : elements)
        element.setDatabase(new_database);
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
    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? "BACKUP" : "RESTORE")
                << (format.hilite ? hilite_none : "");

    formatElements(elements, kind, format);
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
    new_query->setDatabase(new_query->elements, params.default_database);
    return new_query;
}

}
