#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOverlay.h>
#include <Databases/IDatabase.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageMemory.h>
#include <Storages/MemorySettings.h>

using namespace DB;

/// Regression test for the DatabaseOverlay detached-tables fix: the outer
/// overlay's own database name must be reported in
/// getDetachedTablesIterator(), even when the underlying member databases
/// were constructed with a different name than the overlay itself.
/// This cannot be reproduced via SQL, because clickhouse-local always
/// constructs the overlay and its member databases with the same name.
TEST(DatabaseOverlay, DetachedTablesReportOverlayNameNotMemberName)
{
    const ContextPtr context = getContext().context;

    const String overlay_name = "outer_overlay_name";
    const String member_db_name = "different_member_db_name";

    auto member_database = std::make_shared<DatabaseMemory>(member_db_name, context);

    DatabaseOverlay overlay(overlay_name, context);
    overlay.registerNextDatabase(member_database);

    const String table_name = "t_overlay_test_table";
    auto columns = ColumnsDescription{{"x", std::make_shared<DataTypeUInt64>()}};

    auto storage = std::make_shared<StorageMemory>(
        StorageID(member_db_name, table_name), columns, ConstraintsDescription{}, String{}, MemorySettings{});

    member_database->createTable(context, table_name, storage, nullptr);
    member_database->detachTable(context, table_name);

    auto it = overlay.getDetachedTablesIterator(context, {}, false);
    bool found = false;
    for (; it->isValid(); it->next())
    {
        if (it->table() == table_name)
        {
            found = true;
            EXPECT_EQ(it->database(), overlay_name)
                << "getDetachedTablesIterator() must report the overlay's own name, "
                << "not the differently-named member database's name";
        }
    }
    EXPECT_TRUE(found) << "Detached table was not found via the overlay's iterator";
}
