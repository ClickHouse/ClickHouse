#include <Parsers/obfuscateQueries.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/StorageFactory.h>
#include <Storages/registerStorages.h>
#include <DataTypes/DataTypeFactory.h>

#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>


using namespace DB;


TEST(ObfuscateQueries, Test1)
{
    WordMap obfuscated_words_map;
    WordSet used_nouns;
    SipHash hash_func;

    std::string salt = "Hello, world";
    hash_func.update(salt);

    SharedContextHolder shared_context;
    const ContextHolder & context_holder = getContext();

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    std::unordered_set<std::string> additional_names;

    auto all_known_storage_names = StorageFactory::instance().getAllRegisteredNames();
    auto all_known_data_type_names = DataTypeFactory::instance().getAllRegisteredNames();

    additional_names.insert(all_known_storage_names.begin(), all_known_storage_names.end());
    additional_names.insert(all_known_data_type_names.begin(), all_known_data_type_names.end());

    KnownIdentifierFunc is_known_identifier = [&](std::string_view name)
    {
        std::string what(name);

        return FunctionFactory::instance().tryGet(what, context_holder.context) != nullptr
            || AggregateFunctionFactory::instance().isAggregateFunctionName(what)
            || TableFunctionFactory::instance().isTableFunctionName(what)
            || additional_names.count(what);
    };

    WriteBufferFromOwnString out;

    obfuscateQueries(
        R"(
SELECT
    VisitID,
    Goals.ID, Goals.EventTime,
    WatchIDs,
    EAction.ProductName, EAction.ProductPrice, EAction.ProductCurrency, EAction.ProductQuantity, EAction.EventTime, EAction.Type
FROM merge.visits_v2
WHERE
    StartDate >= '2020-09-17' AND StartDate <= '2020-09-25'
    AND CounterID = 24226447
    AND intHash32(UserID) = 416638616 AND intHash64(UserID) = 13269091395366875299
    AND VisitID IN (5653048135597886819, 5556254872710352304, 5516214175671455313, 5476714937521999313, 5464051549483503043)
    AND Sign = 1
)",
        out, obfuscated_words_map, used_nouns, hash_func, is_known_identifier);

    EXPECT_EQ(out.str(), R"(
SELECT
    CorduroyID,
    Steel.ID, Steel.AcornSidestream,
    WealthBRANCH,
    GOVERNMENT.SedimentName, GOVERNMENT.SedimentExhaustion, GOVERNMENT.SedimentFencing, GOVERNMENT.SedimentOpossum, GOVERNMENT.AcornSidestream, GOVERNMENT.Lute
FROM merge.luncheonette_pants
WHERE
    GovernanceCreche >= '2021-04-16' AND GovernanceCreche <= '2021-04-24'
    AND StarboardID = 26446940
    AND intHash32(MessyID) = 474525514 AND intHash64(MessyID) = 13916317227779800149
    AND CorduroyID IN (5223158832904664474, 5605365157729463108, 7543250143731591192, 8715842063486405567, 7837015536326316923)
    AND Tea = 1
)");
}

