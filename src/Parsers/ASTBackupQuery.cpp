#include <IO/Operators.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSnapshotQuery.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using Kind = ASTBackupQuery::Kind;
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    void formatPartitions(const ASTs & partitions, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        ostr << " " << ((partitions.size() == 1) ? "PARTITION" : "PARTITIONS") << " ";
        bool need_comma = false;
        for (const auto & partition : partitions)
        {
            if (std::exchange(need_comma, true))
                ostr << ",";
            ostr << " ";
            partition->format(ostr, format);
        }
    }

    void formatExceptDatabases(const std::set<String> & except_databases, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        if (except_databases.empty())
            return;

        ostr << " EXCEPT " << (except_databases.size() == 1 ? "DATABASE" : "DATABASES") << " ";

        bool need_comma = false;
        for (const auto & database_name : except_databases)
        {
            if (std::exchange(need_comma, true))
                ostr << ",";
            ostr << backQuoteIfNeed(database_name);
        }
    }

    void formatExceptTables(const std::set<DatabaseAndTableName> & except_tables, WriteBuffer & ostr, const IAST::FormatSettings &, bool only_table_names=false)
    {
        if (except_tables.empty())
            return;

        ostr << " EXCEPT " << (except_tables.size() == 1 ? "TABLE" : "TABLES") << " ";

        bool need_comma = false;
        for (const auto & table_name : except_tables)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";

            if (!table_name.first.empty() && !only_table_names)
                ostr << backQuoteIfNeed(table_name.first) << ".";
            ostr << backQuoteIfNeed(table_name.second);
        }
    }

    void formatElement(const Element & element, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        switch (element.type)
        {
            case ElementType::TABLE:
            {
                ostr << "TABLE ";

                if (!element.database_name.empty())
                    ostr << backQuoteIfNeed(element.database_name) << ".";
                ostr << backQuoteIfNeed(element.table_name);

                if ((element.new_table_name != element.table_name) || (element.new_database_name != element.database_name))
                {
                    ostr << " AS ";
                    if (!element.new_database_name.empty())
                        ostr << backQuoteIfNeed(element.new_database_name) << ".";
                    ostr << backQuoteIfNeed(element.new_table_name);
                }

                if (element.partitions)
                    formatPartitions(*element.partitions, ostr, format);
                break;
            }

            case ElementType::TEMPORARY_TABLE:
            {
                ostr << "TEMPORARY TABLE ";
                ostr << backQuoteIfNeed(element.table_name);

                if (element.new_table_name != element.table_name)
                {
                    ostr << " AS ";
                    ostr << backQuoteIfNeed(element.new_table_name);
                }
                break;
            }

            case ElementType::DATABASE:
            {
                ostr << "DATABASE ";
                ostr << backQuoteIfNeed(element.database_name);

                if (element.new_database_name != element.database_name)
                {
                    ostr << " AS ";
                    ostr << backQuoteIfNeed(element.new_database_name);
                }

                formatExceptTables(element.except_tables, ostr, format, /*only_table_names*/true);
                break;
            }

            case ElementType::ALL:
            {
                ostr << "ALL";
                formatExceptDatabases(element.except_databases, ostr, format);
                formatExceptTables(element.except_tables, ostr, format);
                break;
            }
        }
    }

    void formatElements(const std::vector<Element> & elements, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        bool need_comma = false;
        for (const auto & element : elements)
        {
            if (std::exchange(need_comma, true))
                ostr << ", ";
            formatElement(element, ostr, format);
        }
    }

    void formatSettings(const ASTPtr & settings, const ASTFunction * base_backup_name, const ASTPtr & cluster_host_ids, WriteBuffer & ostr, const IAST::FormatSettings & format)
    {
        if (!settings && !base_backup_name && !cluster_host_ids)
            return;

        ostr << " SETTINGS ";
        bool empty = true;

        if (base_backup_name)
        {
            ostr << "base_backup = ";
            base_backup_name->format(ostr, format);
            empty = false;
        }

        if (settings)
        {
            if (!empty)
                ostr << ", ";
            settings->format(ostr, format);
            empty = false;
        }

        if (cluster_host_ids)
        {
            if (!empty)
                ostr << ", ";
            ostr << "cluster_host_ids = ";
            cluster_host_ids->format(ostr, format);
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

        auto out_settings = make_intrusive<ASTSetQuery>();
        out_settings->changes = std::move(changes);
        out_settings->is_standalone = false;
        return out_settings;
    }

    constexpr ASTBackupQuery::ElementType toBackupElementType(ASTSnapshotQuery::ElementType snapshot_type)
    {
        switch (snapshot_type)
        {
            case ASTSnapshotQuery::ElementType::TABLE:
                return ASTBackupQuery::ElementType::TABLE;
            case ASTSnapshotQuery::ElementType::ALL:
                return ASTBackupQuery::ElementType::ALL;
        }
        std::unreachable();
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

ASTPtr ASTBackupQuery::fromSnapshotQuery(const ASTSnapshotQuery & query)
{
    auto res = make_intrusive<ASTBackupQuery>();
    res->children.clear();

    const auto & element = query.element;
    res->elements.push_back(
        ASTBackupQuery::Element{
            toBackupElementType(element.type),
            element.table_name,
            element.database_name,
            element.table_name,
            element.database_name,
            /*partitions*/ {},
            element.except_tables,
            element.except_databases});
    if (query.snapshot_destination)
        res->set(res->backup_name, query.snapshot_destination->clone());

    SettingsChanges changes;
    changes.emplace_back("experimental_lightweight_snapshot", true);
    changes.emplace_back("snapshot", true);
    auto settings = make_intrusive<ASTSetQuery>();
    settings->changes = std::move(changes);
    settings->is_standalone = false;
    res->settings = settings;

    return res;
};

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
    auto res = make_intrusive<ASTBackupQuery>(*this);
    res->children.clear();

    if (backup_name)
        res->set(res->backup_name, backup_name->clone());

    if (base_backup_name)
        res->set(res->base_backup_name, base_backup_name->clone());

    if (base_snapshot_name)
        res->set(res->base_snapshot_name, base_snapshot_name->clone());

    if (cluster_host_ids)
        res->cluster_host_ids = cluster_host_ids->clone();

    if (settings)
        res->settings = settings->clone();

    cloneOutputOptions(*res);

    return res;
}


void ASTBackupQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & fs, FormatState &, FormatStateStacked) const
{
    ostr << ((kind == Kind::BACKUP) ? "BACKUP " : "RESTORE ");

    if (base_snapshot_name)
    {
        /// BACKUP FROM SNAPSHOT <snapshot_name> [ON CLUSTER ...] TO <backup_name>
        ostr << "FROM SNAPSHOT ";
        base_snapshot_name->format(ostr, fs);
    }
    else
    {
        formatElements(elements, ostr, fs);
    }

    formatOnCluster(ostr, fs);

    ostr << ((kind == Kind::BACKUP) ? " TO " : " FROM ");
    backup_name->format(ostr, fs);

    if (settings || base_backup_name)
        formatSettings(settings, base_backup_name, cluster_host_ids, ostr, fs);
}

ASTPtr ASTBackupQuery::getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const
{
    auto new_query = boost::static_pointer_cast<ASTBackupQuery>(clone());
    new_query->cluster.clear();
    new_query->settings = rewriteSettingsWithoutOnCluster(new_query->settings, params);
    ASTBackupQuery::setCurrentDatabase(new_query->elements, params.default_database);
    return new_query;
}

IAST::QueryKind ASTBackupQuery::getQueryKind() const
{
    return kind == Kind::BACKUP ? QueryKind::Backup : QueryKind::Restore;
}

namespace
{
    using Element = ASTBackupQuery::Element;
    using ElementType = ASTBackupQuery::ElementType;

    void writeElementJSON(const Element & e, JSONObjectWriter & w)
    {
        WriteBuffer & out = w.getOut();
        const FormatSettings & fs = w.getFormatSettings();
        out << "{\"type\":" << static_cast<Int64>(e.type);
        if (!e.table_name.empty())
        {
            out << ",\"table_name\":";
            writeJSONString(e.table_name, out, fs);
        }
        if (!e.database_name.empty())
        {
            out << ",\"database_name\":";
            writeJSONString(e.database_name, out, fs);
        }
        if (!e.new_table_name.empty())
        {
            out << ",\"new_table_name\":";
            writeJSONString(e.new_table_name, out, fs);
        }
        if (!e.new_database_name.empty())
        {
            out << ",\"new_database_name\":";
            writeJSONString(e.new_database_name, out, fs);
        }
        if (e.partitions)
        {
            out << ",\"partitions\":[";
            bool first = true;
            for (const auto & p : *e.partitions)
            {
                if (!first) out << ',';
                first = false;
                p->writeJSON(out);
            }
            out << ']';
        }
        if (!e.except_tables.empty())
        {
            out << ",\"except_tables\":[";
            bool first = true;
            for (const auto & [db, tbl] : e.except_tables)
            {
                if (!first) out << ',';
                first = false;
                out << "{\"database\":";
                writeJSONString(db, out, fs);
                out << ",\"table\":";
                writeJSONString(tbl, out, fs);
                out << '}';
            }
            out << ']';
        }
        if (!e.except_databases.empty())
        {
            out << ",\"except_databases\":[";
            bool first = true;
            for (const auto & db : e.except_databases)
            {
                if (!first) out << ',';
                first = false;
                writeJSONString(db, out, fs);
            }
            out << ']';
        }
        out << '}';
    }

    Element readElementJSON(const Poco::JSON::Object & elem_obj, size_t element_index)
    {
        Element e;
        Int64 type_value = elem_obj.getValue<Poco::Int64>("type");
        auto type_opt = magic_enum::enum_cast<ElementType>(static_cast<std::underlying_type_t<ElementType>>(type_value));
        if (!type_opt || static_cast<Int64>(*type_opt) != type_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown BACKUP/RESTORE element type at index {}: {}", element_index, type_value);
        e.type = *type_opt;
        if (elem_obj.has("table_name"))
            e.table_name = elem_obj.getValue<String>("table_name");
        if (elem_obj.has("database_name"))
            e.database_name = elem_obj.getValue<String>("database_name");
        e.new_table_name = elem_obj.has("new_table_name") ? elem_obj.getValue<String>("new_table_name") : e.table_name;
        e.new_database_name = elem_obj.has("new_database_name") ? elem_obj.getValue<String>("new_database_name") : e.database_name;
        if (elem_obj.has("partitions"))
        {
            auto arr = elem_obj.getArray("partitions");
            if (!arr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'partitions' is not a JSON array at element index {} during AST JSON deserialization", element_index);
            ASTs partitions;
            partitions.reserve(arr->size());
            for (unsigned int i = 0; i < arr->size(); ++i)
            {
                auto p_obj = arr->getObject(i);
                if (!p_obj)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'partitions' array at element index {} during AST JSON deserialization", i, element_index);
                partitions.push_back(IAST::createFromJSON(*p_obj));
            }
            e.partitions = std::move(partitions);
        }
        if (elem_obj.has("except_tables"))
        {
            auto arr = elem_obj.getArray("except_tables");
            if (!arr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'except_tables' is not a JSON array at element index {} during AST JSON deserialization", element_index);
            for (unsigned int i = 0; i < arr->size(); ++i)
            {
                auto t_obj = arr->getObject(i);
                if (!t_obj)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'except_tables' array at element index {} during AST JSON deserialization", i, element_index);
                String db = t_obj->has("database") ? t_obj->getValue<String>("database") : String();
                String tbl = t_obj->getValue<String>("table");
                e.except_tables.emplace(std::move(db), std::move(tbl));
            }
        }
        if (elem_obj.has("except_databases"))
        {
            auto arr = elem_obj.getArray("except_databases");
            if (!arr)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "'except_databases' is not a JSON array at element index {} during AST JSON deserialization", element_index);
            for (unsigned int i = 0; i < arr->size(); ++i)
            {
                auto var = arr->get(i);
                if (!var.isString())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} of 'except_databases' at element index {} is not a string during AST JSON deserialization", i, element_index);
                e.except_databases.insert(var.extract<String>());
            }
        }
        return e;
    }
}

void ASTBackupQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "BackupQuery");
    w.writeInt("kind", static_cast<Int64>(kind));
    w.writeChild("backup_name", backup_name);
    w.writeChild("base_backup_name", base_backup_name);
    w.writeChild("base_snapshot_name", base_snapshot_name);
    w.writeChild("settings", settings);
    w.writeChild("cluster_host_ids", cluster_host_ids);
    if (!cluster.empty())
        w.writeString("cluster", cluster);
    if (!elements.empty())
    {
        w.writeKey("elements");
        WriteBuffer & buf = w.getOut();
        buf << '[';
        for (size_t i = 0; i < elements.size(); ++i)
        {
            if (i > 0) buf << ',';
            writeElementJSON(elements[i], w);
        }
        buf << ']';
    }
    w.writeChildren(children);
}

void ASTBackupQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    if (!r.has("kind"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'kind' field in `BackupQuery` during AST JSON deserialization");
    Int64 kind_value = r.getInt("kind");
    auto kind_opt = magic_enum::enum_cast<Kind>(static_cast<std::underlying_type_t<Kind>>(kind_value));
    if (!kind_opt || static_cast<Int64>(*kind_opt) != kind_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown BACKUP/RESTORE kind: {}", kind_value);
    kind = *kind_opt;
    auto backup_name_child = r.readChild("backup_name");
    if (!backup_name_child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'backup_name' for `BackupQuery` during AST JSON deserialization");
    set(backup_name, backup_name_child);
    auto base_backup_name_child = r.readChild("base_backup_name");
    if (base_backup_name_child)
        set(base_backup_name, base_backup_name_child);
    auto base_snapshot_name_child = r.readChild("base_snapshot_name");
    if (base_snapshot_name_child)
        set(base_snapshot_name, base_snapshot_name_child);
    settings = r.readChild("settings");
    if (settings)
        children.push_back(settings);
    cluster_host_ids = r.readChild("cluster_host_ids");
    if (cluster_host_ids)
        children.push_back(cluster_host_ids);
    cluster = r.getString("cluster");
    if (r.has("elements"))
    {
        auto arr = r.getArray("elements");
        if (!arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'elements' is not a JSON array during AST JSON deserialization");
        elements.reserve(arr->size());
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto elem_obj = arr->getObject(i);
            if (!elem_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'elements' array during AST JSON deserialization", i);
            elements.push_back(readElementJSON(*elem_obj, i));
        }
    }
}

}
