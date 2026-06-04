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

            /// Prefer the full identifier ASTs when present (they preserve parameterized
            /// names like `{tbl:Identifier}` that the string fields above cannot represent),
            /// and fall back to building plain identifiers from the strings otherwise.
            JSONObjectReader elem_reader(*elem_obj);

            if (auto ast = elem_reader.readChild("from_database_ast"))
                elem.from.database = ast;
            else if (!from_db.empty())
                elem.from.database = make_intrusive<ASTIdentifier>(from_db);
            if (elem.from.database)
                children.push_back(elem.from.database);

            if (auto ast = elem_reader.readChild("from_table_ast"))
                elem.from.table = ast;
            else if (!from_tbl.empty())
                elem.from.table = make_intrusive<ASTIdentifier>(from_tbl);
            if (elem.from.table)
                children.push_back(elem.from.table);

            if (auto ast = elem_reader.readChild("to_database_ast"))
                elem.to.database = ast;
            else if (!to_db.empty())
                elem.to.database = make_intrusive<ASTIdentifier>(to_db);
            if (elem.to.database)
                children.push_back(elem.to.database);

            if (auto ast = elem_reader.readChild("to_table_ast"))
                elem.to.table = ast;
            else if (!to_tbl.empty())
                elem.to.table = make_intrusive<ASTIdentifier>(to_tbl);
            if (elem.to.table)
                children.push_back(elem.to.table);

            /// `formatQueryImpl` unconditionally dereferences these pointers, so require the
            /// identifier ASTs the chosen rename form actually needs. We validate the pointers
            /// (not the strings) because a parameterized identifier like `{tbl:Identifier}`
            /// stringifies to an empty name but is still a valid, non-null target.
            if (database)
            {
                if (!elem.from.database || !elem.to.database)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} in `RenameQuery` must specify 'from_database' and 'to_database' for `RENAME DATABASE` during AST JSON deserialization", i);
            }
            else
            {
                if (!elem.from.table || !elem.to.table)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element at index {} in `RenameQuery` must specify 'from_table' and 'to_table' during AST JSON deserialization", i);
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

        /// Also serialize the identifier ASTs so that parameterized names like
        /// `{tbl:Identifier}` survive the round-trip. The string fields above stringify
        /// such identifiers to empty, so on read we prefer these AST fields when present.
        if (elements[i].from.database)
        {
            out << ",\"from_database_ast\":";
            elements[i].from.database->writeJSON(out);
        }
        if (elements[i].from.table)
        {
            out << ",\"from_table_ast\":";
            elements[i].from.table->writeJSON(out);
        }
        if (elements[i].to.database)
        {
            out << ",\"to_database_ast\":";
            elements[i].to.database->writeJSON(out);
        }
        if (elements[i].to.table)
        {
            out << ",\"to_table_ast\":";
            elements[i].to.table->writeJSON(out);
        }

        out << '}';
    }
    out << ']';
}

}
