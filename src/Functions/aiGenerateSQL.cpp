#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Access/ContextAccess.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Exception.h>

namespace DB
{

namespace
{

class FunctionAiGenerateSQL final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiGenerateSQL";

    explicit FunctionAiGenerateSQL(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiGenerateSQL>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"query", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };
        FunctionArgumentDescriptors optional_args{
            {"temperature", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), &isColumnConst, "const Number"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t prompt_arg_index = 1;
    static constexpr size_t temp_arg_idx = 2;

    String functionName() const override { return name; }

    float defaultTemperature() const override { return default_temp; }
    size_t promptArgumentIndex() const override { return prompt_arg_index; }
    size_t temperatureArgumentIndex() const override { return temp_arg_idx; }

    /// Only tables/columns visible to the current user via `SHOW TABLES` / `SHOW COLUMNS` grants are included.
    /// This matches the filtering performed by `system.tables` and `system.columns` — see StorageSystemTables.cpp
    /// and StorageSystemColumns.cpp. The generated schema is sent to a third-party LLM endpoint, so skipping this
    /// check would leak schemas of tables the user is not permitted to see.
    ///
    /// `SHOW_TABLES` and `SHOW_COLUMNS` are tracked independently because a user can have one without the other
    /// (e.g. `GRANT SHOW TABLES ON *.*` without `SHOW COLUMNS`). The `check_tables_in_db` / `check_columns_in_db`
    /// flags are pre-computed once per database so the hot loop skips grants checks it can prove are redundant.
    String resolveSchemaForDatabase(
        const String & db_name,
        const ContextPtr & context,
        const std::shared_ptr<const ContextAccessWrapper> & access,
        bool check_tables_in_db,
        bool check_columns_in_db) const
    {
        auto database = DatabaseCatalog::instance().getDatabase(db_name, context);
        String schema;
        auto iter = database->getTablesIterator(context);
        while (iter->isValid())
        {
            auto table_name = iter->name();
            auto storage = iter->table();
            if (!storage)
            {
                iter->next();
                continue;
            }

            if (check_tables_in_db && !access->isGranted(AccessType::SHOW_TABLES, db_name, table_name))
            {
                iter->next();
                continue;
            }

            auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
            if (!metadata)
            {
                iter->next();
                continue;
            }

            bool check_columns_in_table = check_columns_in_db
                && !access->isGranted(AccessType::SHOW_COLUMNS, db_name, table_name);

            String columns_section;
            for (const auto & col : metadata->getColumns().getAll())
            {
                if (check_columns_in_table
                    && !access->isGranted(AccessType::SHOW_COLUMNS, db_name, table_name, col.name))
                    continue;
                columns_section += "  " + col.name + " " + col.type->getName() + "\n";
            }

            /// If every column was filtered out, don't emit the table at all — an empty table definition is noise.
            if (columns_section.empty())
            {
                iter->next();
                continue;
            }

            schema += "Table: " + db_name + "." + table_name + "\nColumns:\n";
            schema += columns_section;

            auto primary_key = metadata->getPrimaryKey();
            if (!primary_key.column_names.empty())
            {
                schema += "ORDER BY: ";
                for (size_t i = 0; i < primary_key.column_names.size(); ++i)
                {
                    if (i > 0)
                        schema += ", ";
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
        auto context = getContext();
        auto access = context->getAccess();

        /// Short-circuit: if the user has a grant globally, no per-object check is needed.
        bool check_tables = !access->isGranted(AccessType::SHOW_TABLES);
        bool check_columns = !access->isGranted(AccessType::SHOW_COLUMNS);

        auto build_for_db = [&](const String & db_name) -> String
        {
            bool check_tables_in_db = check_tables && !access->isGranted(AccessType::SHOW_TABLES, db_name);
            bool check_columns_in_db = check_columns && !access->isGranted(AccessType::SHOW_COLUMNS, db_name);
            return resolveSchemaForDatabase(db_name, context, access, check_tables_in_db, check_columns_in_db);
        };

        String schema;
        auto databases = DatabaseCatalog::instance().getDatabases({});
        for (const auto & [db_name, db] : databases)
        {
            if (db_name == "system" || db_name == "INFORMATION_SCHEMA" || db_name == "information_schema" || db_name == "default")
                continue;
            schema += build_for_db(db_name);
        }

        String current_db = context->getCurrentDatabase();
        if (!current_db.empty() && current_db != "system" && current_db != "INFORMATION_SCHEMA" && current_db != "information_schema"
            && schema.find("Table: " + current_db + ".") == String::npos)
        {
            schema += build_for_db(current_db);
        }

        return schema;
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return "You are a ClickHouse SQL expert. Generate a valid ClickHouse SQL query.\n"
               "Rules:\n"
               "- ALWAYS use fully qualified table names (database.table).\n"
               "- Use only the exact column names from the schema below.\n"
               "- Use ClickHouse-specific syntax and functions.\n"
               "- Return ONLY the raw SQL query. No markdown, no code fences, no explanation.\n\n"
               "Available schema:\n"
            + resolveSchema();
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return String(arguments[prompt_arg_index].column->getDataAt(row));
    }

    /// Strip markdown code fences and trailing whitespace/semicolon that some models add despite being asked not to.
    String postProcessResponse(const String & raw) const override
    {
        String result = raw;

        auto strip_leading_fence = [&](std::string_view fence)
        {
            size_t pos = result.find(fence);
            if (pos == String::npos)
                return false;
            result.erase(0, pos + fence.size());
            return true;
        };

        strip_leading_fence("```sql") || strip_leading_fence("```SQL") || strip_leading_fence("```");

        size_t end_fence = result.rfind("```");
        if (end_fence != String::npos)
            result.resize(end_fence);

        static constexpr std::string_view leading_trim = " \t\n\r";
        static constexpr std::string_view trailing_trim = " \t\n\r;";

        size_t first = result.find_first_not_of(leading_trim);
        if (first == String::npos)
            return {};
        size_t last = result.find_last_not_of(trailing_trim);
        result.erase(last + 1);
        result.erase(0, first);

        return result;
    }
};

}

REGISTER_FUNCTION(AiGenerateSQL)
{
    factory.registerFunction<FunctionAiGenerateSQL>(FunctionDocumentation{
        .description = R"(
Generates a ClickHouse SQL query from a natural-language description using an LLM provider.

The function introspects the current server schema (all user databases plus the current database, excluding `system`,
`INFORMATION_SCHEMA`, `information_schema`, and `default`) and includes the table definitions — names, column types
and `ORDER BY` keys — in the system prompt so the model produces a query referencing real tables and columns.

Markdown code fences and trailing semicolons that some models add despite being instructed not to are stripped
from the response. The generated SQL is NOT executed — it is returned as text.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiGenerateSQL(collection, query[, temperature])",
        .arguments = {
            {"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
            {"query", "Natural-language description of the desired query.", {"String"}},
            {"temperature", "Sampling temperature controlling randomness. Default: `0.0`.", {"Float64"}},
        },
        .returned_value = {"The generated SQL query as text, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Simple request", "SELECT aiGenerateSQL('ai_credentials', 'top 10 users by revenue')", ""},
            {"Aggregation over a table", "SELECT aiGenerateSQL('ai_credentials', 'count of orders per month in 2025 from the orders table')", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
