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

    void formatName(const DatabaseAndTableName & name, ElementType type, const IAST::FormatSettings & format)
    {
        switch (type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                format.ostr << " ";
                if (!name.first.empty())
                    format.ostr << backQuoteIfNeed(name.first) << ".";
                format.ostr << backQuoteIfNeed(name.second);
                break;
            }
            case ElementType::DATABASE:
            {
                format.ostr << " " << backQuoteIfNeed(name.first);
                break;
            }
            case ElementType::TEMPORARY_TABLE:
            {
                format.ostr << " " << backQuoteIfNeed(name.second);
                break;
            }
            default:
                break;
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

    void formatElement(const Element & element, Kind kind, const IAST::FormatSettings & format)
    {
        format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " ";
        switch (element.type)
        {
            case ElementType::TABLE: format.ostr << "TABLE"; break;
            case ElementType::DICTIONARY: format.ostr << "DICTIONARY"; break;
            case ElementType::DATABASE: format.ostr << "DATABASE"; break;
            case ElementType::ALL_DATABASES: format.ostr << "ALL DATABASES"; break;
            case ElementType::TEMPORARY_TABLE: format.ostr << "TEMPORARY TABLE"; break;
            case ElementType::ALL_TEMPORARY_TABLES: format.ostr << "ALL TEMPORARY TABLES"; break;
            case ElementType::EVERYTHING: format.ostr << "EVERYTHING"; break;
        }
        format.ostr << (format.hilite ? IAST::hilite_none : "");

        formatName(element.name, element.type, format);

        bool under_another_name = !element.new_name.first.empty() || !element.new_name.second.empty();
        if (under_another_name)
        {
            format.ostr << (format.hilite ? IAST::hilite_keyword : "") << " " << ((kind == Kind::BACKUP) ? "AS" : "INTO")
                        << (format.hilite ? IAST::hilite_none : "");
            formatName(element.new_name, element.type, format);
        }

        formatPartitions(element.partitions, format);
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
