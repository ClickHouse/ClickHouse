#include <Functions/FunctionBaseLLM.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionLLMGenerateSQL final : public FunctionBaseLLM
{
public:
    static constexpr auto name = "llmGenerateSQL";
    static FunctionPtr create(ContextPtr ctx) { return std::make_shared<FunctionLLMGenerateSQL>(std::move(ctx)); }
    explicit FunctionLLMGenerateSQL(ContextPtr ctx) : FunctionBaseLLM(std::move(ctx)) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1-3 arguments: [collection,] query[, temperature]", name);

        if (hasNamedCollectionArg(arguments) && arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} with a named collection as first argument requires at least a query argument", name);

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

protected:
    String functionName() const override { return name; }
    float defaultTemperature() const override { return 0.1f; }

    String resolveSchemaForDatabase(const String & db_name) const
    {
        auto database = DatabaseCatalog::instance().getDatabase(db_name, getContext());
        String schema;
        auto iter = database->getTablesIterator(getContext());
        while (iter->isValid())
        {
            auto table_name = iter->name();
            auto storage = iter->table();
            if (!storage) { iter->next(); continue; }

            auto metadata = storage->getInMemoryMetadataPtr();
            if (!metadata) { iter->next(); continue; }

            schema += "Table: " + db_name + "." + table_name + "\nColumns:\n";

            const auto & columns_desc = metadata->getColumns();
            for (const auto & col : columns_desc.getAll())
                schema += "  " + col.name + " " + col.type->getName() + "\n";

            auto primary_key = metadata->getPrimaryKey();
            if (!primary_key.column_names.empty())
            {
                schema += "ORDER BY: ";
                for (size_t i = 0; i < primary_key.column_names.size(); ++i)
                {
                    if (i > 0) schema += ", ";
                    schema += primary_key.column_names[i];
                }
                schema += "\n";
            }
            schema += "\n";
            iter->next();
        }
        return schema;
    }

    String resolveSchema() const
    {
        String schema;
        auto databases = DatabaseCatalog::instance().getDatabases({});
        for (const auto & [db_name, db] : databases)
        {
            if (db_name == "system" || db_name == "INFORMATION_SCHEMA"
                || db_name == "information_schema" || db_name == "default")
                continue;
            String db_schema = resolveSchemaForDatabase(db_name);
            if (!db_schema.empty())
                schema += db_schema;
        }

        String current_db = getContext()->getCurrentDatabase();
        if (!current_db.empty() && current_db != "system" && current_db != "INFORMATION_SCHEMA"
            && current_db != "information_schema")
        {
            String current_schema = resolveSchemaForDatabase(current_db);
            if (!current_schema.empty() && schema.find(current_db + ".") == String::npos)
                schema += current_schema;
        }

        return schema;
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        String schema = resolveSchema();
        return "You are a ClickHouse SQL expert. Generate a valid ClickHouse SQL query.\n"
               "Rules:\n"
               "- ALWAYS use fully qualified table names (database.table).\n"
               "- Use only the exact column names from the schema below.\n"
               "- Use ClickHouse-specific syntax and functions.\n"
               "- Return ONLY the raw SQL query. No markdown, no code fences, no explanation.\n\n"
               "Available schema:\n" + schema;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        return String(arguments[idx].column->getDataAt(row));
    }

    String postProcessResponse(const String & raw) const override
    {
        String result = raw;

        auto strip_prefix = [](String & s, const String & prefix)
        {
            size_t pos = s.find(prefix);
            if (pos != String::npos)
                s = s.substr(pos + prefix.size());
        };

        if (result.find("```sql") != String::npos)
            strip_prefix(result, "```sql");
        else if (result.find("```SQL") != String::npos)
            strip_prefix(result, "```SQL");
        else if (result.find("```") != String::npos)
            strip_prefix(result, "```");

        size_t end_fence = result.rfind("```");
        if (end_fence != String::npos)
            result = result.substr(0, end_fence);

        while (!result.empty() && (result.front() == '\n' || result.front() == '\r' || result.front() == ' '))
            result.erase(result.begin());
        while (!result.empty() && (result.back() == '\n' || result.back() == '\r' || result.back() == ' ' || result.back() == ';'))
            result.pop_back();

        return result;
    }
};

}

REGISTER_FUNCTION(llmGenerateSQL)
{
    factory.registerFunction<FunctionLLMGenerateSQL>(FunctionDocumentation{
        .description = "Generates a ClickHouse SQL query from a natural language description using an LLM.",
        .syntax = "llmGenerateSQL([collection,] query[, temperature])",
        .arguments = {
            {"collection", "Optional named collection with LLM provider configuration"},
            {"query", "Natural language description of the desired query"},
            {"temperature", "Optional sampling temperature (default: 0.1)"}},
        .returned_value = {"Generated SQL query as String.", {"String"}},
        .examples = {{"basic", "SELECT llmGenerateSQL('top 10 users by revenue')", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
