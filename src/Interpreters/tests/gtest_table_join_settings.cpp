#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/TableJoin.h>

using namespace DB;

namespace DB::Setting
{
extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
}

TEST(TableJoin, PreservesTemporaryFilesBufferSizeFromSettings)
{
    Settings settings;
    settings[Setting::temporary_files_buffer_size] = 123456;

    TableJoin table_join(settings, JoinAnalyzeMode::None, nullptr, nullptr);

    EXPECT_EQ(table_join.temporaryFilesBufferSize(), 123456);
}

TEST(JoinActionRef, KeepsExpressionActionsAlive)
{
    JoinActionRef action = nullptr;
    {
        JoinExpressionActions expression_actions;
        action = expression_actions.addInput("x", std::make_shared<DataTypeUInt8>(), /*source_relation=*/0);
    }

    EXPECT_TRUE(action.fromLeft());
}
