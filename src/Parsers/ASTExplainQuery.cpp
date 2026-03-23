#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

void ASTExplainQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ExplainQuery");
    w.writeString("kind", toString(kind));
    w.writeChild("settings", ast_settings);
    w.writeChild("query", query);
    w.writeChild("table_function", table_function);
    w.writeChild("table_override", table_override);
    w.writeChild("out_file", out_file);
    w.writeChild("format_ast", format_ast);
    w.writeChild("settings_ast", settings_ast);
    w.writeChild("compression", compression);
    w.writeChild("compression_level", compression_level);
}

void ASTExplainQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    kind = fromString(r.getString("kind"));

    auto settings_child = r.readChild("settings");
    if (settings_child)
        setSettings(std::move(settings_child));

    auto query_child = r.readChild("query");
    if (query_child)
        setExplainedQuery(std::move(query_child));

    auto table_function_child = r.readChild("table_function");
    if (table_function_child)
        setTableFunction(std::move(table_function_child));

    auto table_override_child = r.readChild("table_override");
    if (table_override_child)
        setTableOverride(std::move(table_override_child));

    out_file = r.readChild("out_file");
    if (out_file)
        children.push_back(out_file);

    format_ast = r.readChild("format_ast");
    if (format_ast)
        children.push_back(format_ast);

    settings_ast = r.readChild("settings_ast");
    if (settings_ast)
        children.push_back(settings_ast);

    compression = r.readChild("compression");
    if (compression)
        children.push_back(compression);

    compression_level = r.readChild("compression_level");
    if (compression_level)
        children.push_back(compression_level);
}

}
