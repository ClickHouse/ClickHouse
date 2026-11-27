#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Databases/DatabaseMemory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
}

/// The interpreter refuses `CREATE` and a full `ATTACH` over a detached name before `createTable`, so only a direct call reaches this check.
TEST(DatabaseMemory, CreateTableRefusesDetachedName)
{
    const ContextPtr context = getContext().context;
    const String database_name = "gtest_database_memory_detached_name";
    const String table_name = "t";
    auto database = std::make_shared<DatabaseMemory>(database_name, context);
    auto make_storage = [&]
    {
        return std::make_shared<StorageMemory>(
            StorageID(database_name, table_name),
            ColumnsDescription{{"x", std::make_shared<DataTypeUInt64>()}},
            ConstraintsDescription{},
            String{},
            MemorySettings{});
    };

    database->createTable(context, table_name, make_storage(), nullptr);
    database->detachTable(context, table_name);

    try
    {
        database->createTable(context, table_name, make_storage(), nullptr);
        FAIL() << "createTable over a detached name must throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TABLE_ALREADY_EXISTS);
    }
    EXPECT_FALSE(database->isTableExist(table_name, context));

    auto short_attach = make_intrusive<ASTCreateQuery>();
    short_attach->attach = true;
    short_attach->attach_short_syntax = true;
    database->createTable(context, table_name, make_storage(), short_attach);
    EXPECT_TRUE(database->isTableExist(table_name, context));
}
