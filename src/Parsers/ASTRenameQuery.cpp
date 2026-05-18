#include <Parsers/ASTRenameQuery.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTRenameQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    exchange = r.getBool("exchange");
    database = r.getBool("database");
    dictionary = r.getBool("dictionary");
    rename_if_cannot_exchange = r.getBool("rename_if_cannot_exchange");

    cluster = r.getString("cluster");

    elements.clear();
    children.clear();

    auto arr = r.getArray("elements");
    if (arr)
    {
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto elem_obj = arr->getObject(i);
            if (!elem_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null element at index {} in 'elements' array during AST JSON deserialization", i);
            Element elem;

            String from_db = elem_obj->getValue<String>("from_database");
            String from_tbl = elem_obj->getValue<String>("from_table");
            String to_db = elem_obj->getValue<String>("to_database");
            String to_tbl = elem_obj->getValue<String>("to_table");
            elem.if_exists = elem_obj->getValue<bool>("if_exists");

            /// `formatQueryImpl` unconditionally dereferences these pointers,
            /// so require the names the chosen rename form actually needs.
            if (database)
            {
                if (from_db.empty() || to_db.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} in `RenameQuery` must specify non-empty 'from_database' and 'to_database' for `RENAME DATABASE` during AST JSON deserialization", i);
            }
            else
            {
                if (from_tbl.empty() || to_tbl.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} in `RenameQuery` must specify non-empty 'from_table' and 'to_table' during AST JSON deserialization", i);
            }

            if (!from_db.empty())
            {
                elem.from.database = make_intrusive<ASTIdentifier>(from_db);
                children.push_back(elem.from.database);
            }
            if (!from_tbl.empty())
            {
                elem.from.table = make_intrusive<ASTIdentifier>(from_tbl);
                children.push_back(elem.from.table);
            }
            if (!to_db.empty())
            {
                elem.to.database = make_intrusive<ASTIdentifier>(to_db);
                children.push_back(elem.to.database);
            }
            if (!to_tbl.empty())
            {
                elem.to.table = make_intrusive<ASTIdentifier>(to_tbl);
                children.push_back(elem.to.table);
            }

            elements.push_back(std::move(elem));
        }
    }

    /// `RENAME DATABASE` form indexes `elements.at(0)`, so we must have at least one element.
    if (database && elements.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`RENAME DATABASE` requires at least one element during AST JSON deserialization");
}

void ASTRenameQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "RenameQuery");

    w.writeBool("exchange", exchange);
    w.writeBool("database", database);
    w.writeBool("dictionary", dictionary);
    w.writeBool("rename_if_cannot_exchange", rename_if_cannot_exchange);

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    w.writeKey("elements");
    out << '[';
    for (size_t i = 0; i < elements.size(); ++i)
    {
        if (i > 0)
            out << ',';
        out << '{';
        out << "\"from_database\":";
        writeJSONString(elements[i].from.getDatabase(), out, w.getFormatSettings());
        out << ",\"from_table\":";
        writeJSONString(elements[i].from.getTable(), out, w.getFormatSettings());
        out << ",\"to_database\":";
        writeJSONString(elements[i].to.getDatabase(), out, w.getFormatSettings());
        out << ",\"to_table\":";
        writeJSONString(elements[i].to.getTable(), out, w.getFormatSettings());
        out << ",\"if_exists\":";
        out << (elements[i].if_exists ? "true" : "false");
        out << '}';
    }
    out << ']';
}

}
