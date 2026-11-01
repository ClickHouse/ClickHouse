#include <Storages/System/StorageSystemTemplateQueries.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/System/TemplateQueriesBuiltin.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <ctime>
#include <filesystem>
#include <optional>
#include <set>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int ACCESS_DENIED;
    extern const int ATOMIC_RENAME_FAIL;
}

namespace
{
    constexpr size_t MAX_NAME_BYTES = 256;
    constexpr size_t MAX_CATEGORY_BYTES = 256;
    constexpr size_t MAX_DESCRIPTION_BYTES = 4096;
    constexpr size_t MAX_QUERY_BYTES = 1024 * 1024;
    constexpr size_t MAX_USER_QUERIES = 10000;
}

ColumnsDescription StorageSystemTemplateQueries::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Unique name of the template query."},
        {"category", std::make_shared<DataTypeString>(), "Category used to group template queries."},
        {"description", std::make_shared<DataTypeString>(), "Human-readable description of what the query does."},
        {"query", std::make_shared<DataTypeString>(), "The text of the query."},
        {"is_builtin", std::make_shared<DataTypeUInt8>(), "1 for queries shipped with the server (read-only), 0 for queries added by users. Ignored on INSERT: inserted rows are always user rows."},
        {"created_at", std::make_shared<DataTypeDateTime>(), "Time when a user query was added. Ignored on INSERT (set by the server). Zero for built-in queries."},
    };
}

StorageSystemTemplateQueries::StorageSystemTemplateQueries(const StorageID & table_id_, ColumnsDescription columns_description_, String file_path_)
    : IStorageSystemOneBlock(table_id_, std::move(columns_description_))
    , file_path(std::move(file_path_))
{
}

static bool isBuiltinName(const String & name)
{
    for (const auto & builtin : getBuiltinTemplateQueries())
        if (builtin.name == name)
            return true;
    return false;
}

void StorageSystemTemplateQueries::loadIfNeeded() const
{
    if (loaded)
        return;

    std::vector<UserQuery> rows;
    std::error_code ec;
    if (fs::exists(file_path, ec))
    {
        ReadBufferFromFile in(file_path);
        while (!in.eof())
        {
            UserQuery row;
            readEscapedString(row.name, in);
            assertChar('\t', in);
            readEscapedString(row.category, in);
            assertChar('\t', in);
            readEscapedString(row.description, in);
            assertChar('\t', in);
            readEscapedString(row.query, in);
            assertChar('\t', in);
            readIntText(row.created_at, in);
            if (!in.eof())
                assertChar('\n', in);
            rows.push_back(std::move(row));
        }
    }

    user_queries = std::move(rows);
    loaded = true;
}

void StorageSystemTemplateQueries::saveToFile(const std::vector<UserQuery> & rows) const
{
    const String tmp_path = file_path + ".tmp";
    {
        WriteBufferFromFile out(tmp_path);
        for (const auto & row : rows)
        {
            writeEscapedString(row.name, out);
            writeChar('\t', out);
            writeEscapedString(row.category, out);
            writeChar('\t', out);
            writeEscapedString(row.description, out);
            writeChar('\t', out);
            writeEscapedString(row.query, out);
            writeChar('\t', out);
            writeIntText(row.created_at, out);
            writeChar('\n', out);
        }
        out.finalize();
        out.sync();
    }
    std::error_code ec;
    fs::rename(tmp_path, file_path, ec);
    if (ec)
        throw Exception(ErrorCodes::ATOMIC_RENAME_FAIL, "Cannot rename '{}' to '{}': {}", tmp_path, file_path, ec.message());
}

void StorageSystemTemplateQueries::addUserQueries(std::vector<UserQuery> new_rows)
{
    std::lock_guard lock(mutex);
    loadIfNeeded();

    std::set<String> names;
    for (const auto & row : user_queries)
        names.insert(row.name);

    auto updated = user_queries;
    for (auto & row : new_rows)
    {
        if (row.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Template query name must not be empty");
        if (row.name.size() > MAX_NAME_BYTES)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Template query name is too long: {} bytes, the maximum is {}", row.name.size(), MAX_NAME_BYTES);
        if (row.category.size() > MAX_CATEGORY_BYTES)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Template query category is too long: {} bytes, the maximum is {}", row.category.size(), MAX_CATEGORY_BYTES);
        if (row.description.size() > MAX_DESCRIPTION_BYTES)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Template query description is too long: {} bytes, the maximum is {}", row.description.size(), MAX_DESCRIPTION_BYTES);
        if (row.query.size() > MAX_QUERY_BYTES)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Template query text is too long: {} bytes, the maximum is {}", row.query.size(), MAX_QUERY_BYTES);
        if (isBuiltinName(row.name))
            throw Exception(ErrorCodes::ACCESS_DENIED, "Template query '{}' is built-in and cannot be overridden", row.name);
        if (!names.insert(row.name).second)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Template query '{}' already exists", row.name);
        updated.push_back(std::move(row));
    }

    if (updated.size() > MAX_USER_QUERIES)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many user template queries: {}, the maximum is {}", updated.size(), MAX_USER_QUERIES);

    saveToFile(updated);
    user_queries = std::move(updated);
}

void StorageSystemTemplateQueries::deleteUserQuery(const String & name)
{
    if (isBuiltinName(name))
        throw Exception(ErrorCodes::ACCESS_DENIED, "Template query '{}' is built-in and cannot be deleted", name);

    std::lock_guard lock(mutex);
    loadIfNeeded();

    auto updated = user_queries;
    std::erase_if(updated, [&](const UserQuery & row) { return row.name == name; });
    if (updated.size() == user_queries.size())
        return;

    saveToFile(updated);
    user_queries = std::move(updated);
}

class TemplateQueriesSink final : public SinkToStorage
{
public:
    TemplateQueriesSink(SharedHeader header_, StorageSystemTemplateQueries & storage_)
        : SinkToStorage(std::move(header_))
        , storage(storage_)
    {}

    String getName() const override { return "TemplateQueriesSink"; }

    void consume(Chunk & chunk) override
    {
        const auto & columns = chunk.getColumns();
        if (columns.size() < 4)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Expected at least 4 columns, got {}", columns.size());

        const UInt32 now = static_cast<UInt32>(std::time(nullptr));
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            StorageSystemTemplateQueries::UserQuery row;
            row.name = String(columns[0]->getDataAt(i));
            row.category = String(columns[1]->getDataAt(i));
            row.description = String(columns[2]->getDataAt(i));
            row.query = String(columns[3]->getDataAt(i));
            row.created_at = now;
            rows.push_back(std::move(row));
        }
    }

    /// Apply the whole INSERT atomically (all rows or none) once all data is received.
    void onFinish() override
    {
        if (!rows.empty())
            storage.addUserQueries(std::move(rows));
    }

private:
    StorageSystemTemplateQueries & storage;
    std::vector<StorageSystemTemplateQueries::UserQuery> rows;
};

SinkToStoragePtr StorageSystemTemplateQueries::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool)
{
    return std::make_shared<TemplateQueriesSink>(std::make_shared<const Block>(metadata_snapshot->getSampleBlock()), *this);
}

void StorageSystemTemplateQueries::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & builtin : getBuiltinTemplateQueries())
    {
        res_columns[0]->insert(builtin.name);
        res_columns[1]->insert(builtin.category);
        res_columns[2]->insert(builtin.description);
        res_columns[3]->insert(builtin.query);
        res_columns[4]->insert(UInt8(1));
        res_columns[5]->insert(UInt32(0));
    }

    std::lock_guard lock(mutex);
    loadIfNeeded();
    for (const auto & row : user_queries)
    {
        res_columns[0]->insert(row.name);
        res_columns[1]->insert(row.category);
        res_columns[2]->insert(row.description);
        res_columns[3]->insert(row.query);
        res_columns[4]->insert(UInt8(0));
        res_columns[5]->insert(row.created_at);
    }
}

void StorageSystemTemplateQueries::alter(const AlterCommands &, ContextPtr, AlterLockHolder &, DDLGuardPtr &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER is not supported by storage {}", getName());
}

void StorageSystemTemplateQueries::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER is not supported by storage {}", getName());
}

/// Extracts `X` from a single `DELETE ... WHERE name = 'X'` command.
static std::optional<String> getDeleteNameFromCommands(const MutationCommands & commands)
{
    if (commands.size() != 1)
        return {};

    const auto & command = commands.front();
    auto alter = command.ast();
    if (command.type != MutationCommand::DELETE || !alter || alter->partition || alter->partitions || !alter->predicate)
        return {};

    const auto * func = alter->predicate->as<ASTFunction>();
    if (!func || func->name != "equals" || !func->arguments || func->arguments->children.size() != 2)
        return {};

    const auto & args = func->arguments->children;
    const auto * ident = args[0]->as<ASTIdentifier>();
    if (!ident || ident->full_name != "name")
        return {};

    const auto * literal = args[1]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return {};

    return literal->value.safeGet<String>();
}

void StorageSystemTemplateQueries::checkMutationIsPossible(const MutationCommands & commands, const Settings &) const
{
    if (getDeleteNameFromCommands(commands))
        return;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Only deletion by name is supported on {}. The WHERE clause must be `name = 'template_name'`.",
        getStorageID().getFullTableName());
}

void StorageSystemTemplateQueries::mutate(const MutationCommands & commands, ContextPtr)
{
    auto name = getDeleteNameFromCommands(commands);
    if (!name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected single DELETE command with WHERE name = '...', got {}", commands.toString(/* with_pure_metadata_commands */ true));
    deleteUserQuery(*name);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTemplateQueries) }
