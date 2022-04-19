#include <Parsers/ASTBackupQuery.h>
#include <IO/Operators.h>
#include <Common/quoteString.h>


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

    void formatSettings(const ASTPtr & settings, const ASTPtr & base_backup_name, const IAST::FormatSettings & format)
    {
        if (!settings && !base_backup_name)
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
        }

    }
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

    format.ostr << (format.hilite ? hilite_keyword : "") << ((kind == Kind::BACKUP) ? " TO " : " FROM ") << (format.hilite ? hilite_none : "");
    backup_name->format(format);

    if (settings || base_backup_name)
        formatSettings(settings, base_backup_name, format);
}

}
