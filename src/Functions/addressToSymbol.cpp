#if defined(__ELF__) && !defined(OS_FREEBSD)
#   define HAS_SYMBOL_INDEX 1
#elif defined(OS_DARWIN)
#   define HAS_SYMBOL_INDEX 1
#endif

#if defined(HAS_SYMBOL_INDEX)

#include <Common/SymbolIndex.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionAddressToSymbol final : public IFunction
{
public:
    static constexpr auto name = "addressToSymbol";
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToSymbol);
        return std::make_shared<FunctionAddressToSymbol>();
    }

    [[nodiscard]] String getName() const override
    {
        return name;
    }

    [[nodiscard]] size_t getNumberOfArguments() const override
    {
        return 1;
    }

    /// Resolved against the executing node's own binary, like `buildId`.
    [[nodiscard]] bool isDeterministic() const override { return false; }
    [[nodiscard]] bool isServerConstant() const override { return true; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(UInt64) -> String";
    }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column->getName(), getName());

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        auto result_column = ColumnString::create();

        const SymbolIndex & symbol_index = SymbolIndex::instance();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (const auto * symbol = symbol_index.findSymbol(reinterpret_cast<const void *>(data[i])))
            {
                if (std::string_view symbol_name = symbol_index.getSymbolName(*symbol); !symbol_name.empty())
                    result_column->insertData(symbol_name.data(), symbol_name.size());
                else
                    result_column->insertDefault();
            }
            else
                result_column->insertDefault();
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(AddressToSymbol)
{
    FunctionDocumentation::Description description = R"(
Converts an address of an instruction inside the main ClickHouse executable to a symbol from ClickHouse's object files.

The address must be an offset inside the main ClickHouse executable, the form in which addresses are stored in the `trace` columns of `system.trace_log`, `system.stack_trace`, and `system.crash_log`, and the `last_error_trace` column of `system.error_log`.
Runtime (virtual memory) addresses are not accepted, so that the function cannot be used to learn the memory layout of the server process.
Frames outside the main executable are stored as `0`, and the function returns an empty result for them.
    )";
    FunctionDocumentation::Syntax syntax = "addressToSymbol(address_of_binary_instruction)";
    FunctionDocumentation::Arguments arguments = {
        {"address_of_binary_instruction", "Offset of an instruction inside the main ClickHouse executable, as stored in the `trace` column of `system.trace_log` and similar tables.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the symbol from ClickHouse object files or an empty string, if the address is not valid or is `0`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Selecting the first string from the `trace_log` system table",
        R"(
SET allow_introspection_functions=1;
SELECT * FROM system.trace_log LIMIT 1 FORMAT Vertical;
        )",
        R"(
-- The `trace` field contains the stack trace at the moment of sampling.
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [0,0,0,0,631010292,205306916,447260096,447257472,447067860,447068840,443102728,443098700,442466608,442461536,442451604,442504896,442512004,442579276,290968008,555786732,291036904,205189920,0,0]
        )"
    },
    {
        "Getting a symbol for a single address",
        R"(
SET allow_introspection_functions=1;
SELECT addressToSymbol(447257472::UInt64) AS symbol FORMAT Vertical;
        )",
        R"(
Row 1:
──────
symbol: _ZN2DB16LazyOutputFormat8getChunkEm
        )"
    },
    {
        "Applying the function to the whole stack trace",
        R"(
SET allow_introspection_functions=1;

-- The arrayMap function allows to process each individual element of the trace array by the addressToSymbols function.
-- The result of this processing is shown in the trace_symbols column of output.

SELECT
    arrayStringConcat(arrayMap(x -> addressToSymbol(x), trace), '\n') AS trace_symbols
FROM system.trace_log
LIMIT 1
FORMAT Vertical;
        )",
        R"(
Row 1:
──────
trace_symbols:


_ZNSt3__118condition_variable15__do_timed_waitERNS_11unique_lockINS_5mutexEEENS_6chrono10time_pointINS5_12system_clockENS5_8durationIxNS_5ratioILl1ELl1000000000EEEEEEE
_ZNSt3__118condition_variable10wait_untilB9nqe220101INS_6chrono12steady_clockENS2_8durationIxNS_5ratioILl1ELl1000000000EEEEEEENS_9cv_statusERNS_11unique_lockINS_5mutexEEERKNS2_10time_pointIT_T0_EE
_ZN22ConcurrentBoundedQueueIN2DB5ChunkEE7popImplILb1EEEbRS1_NSt3__18optionalImEE
_ZN2DB16LazyOutputFormat8getChunkEm
_ZN2DB28PullingAsyncPipelineExecutor4pullERNS_5ChunkEm
_ZN2DB28PullingAsyncPipelineExecutor4pullERNS_5BlockEm
_ZN2DB15LocalConnection8pollImplEv
_ZN2DB15LocalConnection4pollEm
_ZN2DB10ClientBase13receiveResultEN5boost13intrusive_ptrINS_4IASTEEEib
_ZN2DB10ClientBase20processOrdinaryQueryENSt3__112basic_stringIcNS1_11char_traitsIcEENS1_9allocatorIcEEEEN5boost13intrusive_ptrINS_4IASTEEE
_ZN2DB10ClientBase24processParsedSingleQueryENSt3__117basic_string_viewIcNS1_11char_traitsIcEEEEN5boost13intrusive_ptrINS_4IASTEEERbm
_ZN2DB10ClientBase17executeMultiQueryERKNSt3__112basic_stringIcNS1_11char_traitsIcEENS1_9allocatorIcEEEE
_ZN2DB10ClientBase16processQueryTextERKNSt3__112basic_stringIcNS1_11char_traitsIcEENS1_9allocatorIcEEEE
_ZN2DB10ClientBase17runNonInteractiveEv
_ZN2DB11LocalServer4mainERKNSt3__16vectorINS1_12basic_stringIcNS1_11char_traitsIcEENS1_9allocatorIcEEEENS6_IS8_EEEE
_ZN4Poco4Util11Application3runEv
_Z24mainEntryClickHouseLocaliPPc
main


        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddressToSymbol>(documentation);
}

}

#endif
