#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

/// Mimics the `(in file/uri ...)` context `IInputFormat::generate` appends to what it catches.
std::string callerSuffix(size_t caller)
{
    return fmt::format("(in file/uri caller{}.parquet)", caller);
}

/// Calls the shared init `num_callers` times; each caller must see its own message only.
/// `std::call_once` does not consume its flag when the callable throws, so every call after the
/// first re-enters and takes the `init_exception` rethrow branch. Three callers pin both rethrow
/// sites: caller 1 the `catch` block, caller 2 the `if (init_exception)` branch, and caller 3
/// observes whether caller 2 mutated the shared object.
void expectNoCallerContextLeak(const std::function<void()> & init, size_t num_callers, const std::string & expected_own_message)
{
    for (size_t caller = 1; caller <= num_callers; ++caller)
    {
        try
        {
            init();
            FAIL() << "caller " << caller << ": expected the shared init to throw";
        }
        catch (Exception & e)
        {
            const std::string message = e.message();
            EXPECT_NE(message.find(expected_own_message), std::string::npos)
                << "caller " << caller << " lost the original init error: " << message;

            for (size_t other = 1; other < caller; ++other)
                EXPECT_EQ(message.find(callerSuffix(other)), std::string::npos)
                    << "caller " << caller << " inherited caller " << other << "'s appended context: " << message;

            e.addMessage(callerSuffix(caller));
        }
    }
}

}

/// `FormatParserSharedResources::initOnce` takes the init callable from the caller, so a
/// failing shared init is expressed directly.
TEST(FormatSharedInit, ParserSharedResourcesDoesNotLeakCallerContext)
{
    Settings settings;
    FormatParserSharedResources resources(settings, /* num_streams_ */ 3);

    size_t init_attempts = 0;
    auto failing_init = [&]
    {
        ++init_attempts;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "shared init failed");
    };

    expectNoCallerContextLeak([&] { resources.initOnce(failing_init); }, /* num_callers */ 3, "shared init failed");

    /// The init body must run exactly once; later callers are served from `init_exception`.
    EXPECT_EQ(init_attempts, 1u);
}

/// The reported stack fails inside `FormatFilterInfo::initKeyConditionOnce`: building the
/// `KeyCondition` converts the filter's constant to the key type and an unparseable
/// `DateTime64` string throws out of `extractAtomFromTree`.
TEST(FormatSharedInit, FilterInfoDoesNotLeakCallerContext)
{
    tryRegisterFunctions();

    auto context = getContext().context;
    const DataTypePtr key_type = std::make_shared<DataTypeDateTime64>(3);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();

    /// `dt = 'not-a-datetime'`, i.e. the filter shape from the report.
    ActionsDAG dag;
    const auto & key_node = dag.addInput("dt", key_type);
    const auto & const_node = dag.addColumn(
        DataTypeString().createColumnConst(1, "not-a-datetime"), string_type, "'not-a-datetime'_String");
    auto equals = FunctionFactory::instance().get("equals", context)->build(
        {{key_node.column, key_node.result_type, key_node.result_name},
         {const_node.column, const_node.result_type, const_node.result_name}});
    const auto & predicate = dag.addFunction(equals, {&key_node, &const_node}, "equals(dt, 'not-a-datetime')");
    dag.getOutputs() = {&predicate};

    /// A row-level filter over a column outside `keys` makes the init append to
    /// `additional_columns`, which is how a caller observes that the init body ran: unlike the
    /// sibling test the init callable is internal here, so there is no attempt counter to check.
    ActionsDAG row_level_dag;
    const auto & extra_node = row_level_dag.addInput("extra", key_type);
    row_level_dag.getOutputs() = {&extra_node};
    auto row_level_filter = std::make_shared<FilterDAGInfo>(
        FilterDAGInfo{std::move(row_level_dag), extra_node.result_name, false});

    auto filter_info = std::make_shared<FormatFilterInfo>(
        std::make_shared<const ActionsDAG>(std::move(dag)), context, nullptr, row_level_filter, nullptr);

    Block keys;
    keys.insert({key_type->createColumn(), key_type, "dt"});

    expectNoCallerContextLeak(
        [&] { filter_info->initKeyConditionOnce(keys); }, /* num_callers */ 3, "not-a-datetime");

    /// The init body must run exactly once; later callers are served from `init_exception`. If
    /// the failure were not cached, every caller would re-run the body and append `extra` again.
    EXPECT_EQ(filter_info->additional_columns.columns(), 1u);
}

}
