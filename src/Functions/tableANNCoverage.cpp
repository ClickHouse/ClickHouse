#include "config.h"

#if USE_DISKANN

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/StorageMergeTree.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_TABLE;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Inspect the ANN (DiskANN) coverage state of a non-replicated MergeTree table without
/// blocking on a build. Returns a two-element tuple:
///   - `total`:   number of currently active parts (matches `system.parts WHERE active`).
///   - `covered`: number of those parts that are already covered by some active ANN group.
///
/// `SYSTEM BUILD ANN INDEX` is fire-and-forget; tests and admin scripts that want to wait
/// for a build to observe its results poll this function:
///
///     SELECT tupleElement(tableANNCoverage('db', 't'), 'total')
///          = tupleElement(tableANNCoverage('db', 't'), 'covered');
///
/// The function is non-deterministic and recomputes on every call — it observes a live
/// snapshot of the table's state rather than caching.
class FunctionTableANNCoverage : public IFunction, WithContext
{
public:
    static constexpr auto name = "tableANNCoverage";

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionTableANNCoverage>(context_->getGlobalContext());
    }

    explicit FunctionTableANNCoverage(ContextPtr global_context_) : WithContext(global_context_)
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!checkColumnConst<ColumnString>(arguments[i].column.get()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} for function {} must be a const String", i + 1, getName());
        }
        return std::make_shared<DataTypeTuple>(
            DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()},
            Names{"total", "covered"});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const String database_name = checkAndGetColumnConst<ColumnString>(arguments[0].column.get())->getValue<String>();
        const String table_name = checkAndGetColumnConst<ColumnString>(arguments[1].column.get())->getValue<String>();

        auto table = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, getContext());
        if (!table)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist", database_name, table_name);

        auto * merge_tree = dynamic_cast<StorageMergeTree *>(table.get());
        if (!merge_tree)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "tableANNCoverage: {}.{} is not a non-replicated MergeTree table (got {})",
                database_name, table_name, table->getName());

        auto manager_ptr = merge_tree->getANNIndexManager();
        if (!manager_ptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "tableANNCoverage: table {}.{} has no `ann` index", database_name, table_name);

        const auto parts = merge_tree->getDataPartsVectorForInternalUsage();
        UInt64 total = 0;
        UInt64 covered = 0;
        for (const auto & part : parts)
        {
            ++total;
            if (manager_ptr->isPartCovered(part))
                ++covered;
        }

        /// Constant tuple broadcast to `input_rows_count` rows — the function is itself const-folding
        /// friendly but a caller might apply it row-wise over a dummy stream.
        auto col_total = ColumnUInt64::create();
        auto col_covered = ColumnUInt64::create();
        col_total->insertValue(total);
        col_covered->insertValue(covered);
        auto tuple_col = ColumnTuple::create(Columns{std::move(col_total), std::move(col_covered)});
        return ColumnConst::create(std::move(tuple_col), input_rows_count);
    }
};

}

REGISTER_FUNCTION(TableANNCoverage)
{
    FunctionDocumentation::Description description = "Returns (total, covered) for the ANN (DiskANN) index coverage of a non-replicated MergeTree table.";
    FunctionDocumentation::Syntax syntax = "tableANNCoverage(database, table)";
    FunctionDocumentation::Arguments arguments = {
        {"database", "Database name.", {"const String"}},
        {"table", "Table name.", {"const String"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Tuple(total UInt64, covered UInt64) with the number of active parts and the subset covered by some active ANN group."};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTableANNCoverage>(documentation);
}

}

#endif
