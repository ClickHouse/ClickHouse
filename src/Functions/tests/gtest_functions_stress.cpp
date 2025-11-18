#include <gtest/gtest.h>
#include <Common/logger_useful.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/thread_local_rng.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <Storages/StorageGenerateRandom.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int DICTIONARIES_WAS_NOT_LOADED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNSUPPORTED_METHOD;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int DECIMAL_OVERFLOW;
}

namespace
{
const std::unordered_set<String> excluded_functions = {
    "transactionLatestSnapshot",
    "transactionOldestSnapshot",
    "showCertificate",
    "getClientHTTPHeader",
    "serverUUID",
    "synonyms",
    "sleep",
    "sleepEachRow",
    "file",
};

size_t generateRandomNumberOfArgs()
{
    uint64_t n = thread_local_rng() % 16;
    //// Usually 1 or 2 args, sometimes 0, sometimes 3-7.
    if (n < 5)
        return 1;
    else if (n < 10)
        return 2;
    else if (n < 11)
        return 0;
    else
        return n - 11 + 3;
}

struct ArgConstraints
{
    auto toTuple() const { return std::make_tuple(); }
    bool operator<(const ArgConstraints & rhs) const { return toTuple() < rhs.toTuple(); }
};

struct FunctionsStressTest
{
    LoggerPtr log;
    ContextMutablePtr context;

    /// Function that we might be able to test. At this point, we don't know what the types of
    /// arguments are.
    struct Function
    {
        FunctionOverloadResolverPtr overload_resolver;

        /// Stats.
        size_t overload_attempts {0};
        size_t overload_successes {0};
    };

    std::vector<Function> functions;

    /// Function + argument types and constness + values of const args.
    struct Overload
    {
        FunctionBasePtr resolved_function;
        ColumnsWithTypeAndName args;
    };

    std::map<ArgConstraints, std::vector<ColumnWithTypeAndName>> random_values;
    /// Some outputs of previously executed functions, to be used as inputs later.
    /// E.g. to sometimes pass the output of toString(DateTime) as input to toDateTime(String),
    /// because toDateTime(String) would ~never succeed on randomly generated strings.
    std::vector<ColumnWithTypeAndName> additional_random_values;

    void run()
    {
        log = getLogger("stress");
        tryRegisterFunctions();
        ContextPtr global_context = getContext().context;
        context = Context::createCopy(global_context);
        context->setSetting("allow_suspicious_low_cardinality_types", 1);
        context->setSetting("allow_experimental_nlp_functions", 1);
        context->setSetting("allow_deprecated_error_prone_window_functions", 1);
        context->setSetting("allow_not_comparable_types_in_comparison_functions", 1);
        context->setSetting("allow_experimental_time_time64_type", 1);
        context->setSetting("allow_introspection_functions", 1);
        context->setSetting("allow_experimental_full_text_index", 1);

        listTestableFunctions();

        mainLoop();
    }

    void listTestableFunctions()
    {
        Strings all_names = FunctionFactory::instance().getAllNames();

        size_t num_dict = 0;
        size_t num_excluded = 0;
        for (const String & name : all_names)
        {
            if (excluded_functions.contains(name))
            {
                ++num_excluded;
                continue;
            }

            FunctionOverloadResolverPtr resolver;
            try
            {
                resolver = FunctionFactory::instance().get(name, context);
            }
            catch (Exception & e)
            {
                if (e.code() == ErrorCodes::DICTIONARIES_WAS_NOT_LOADED)
                {
                    ++num_dict;
                    continue;
                }
                /// We could also ignore things like DEPRECATED_FUNCTION, SUPPORT_IS_DISABLED,
                /// NOT_IMPLEMENTED. But for now we explicitly list such functions in `excluded_functions`
                /// instead, to make sure we don't unintentionally skip a function that e.g. just
                /// requires a setting change.

                LOG_ERROR(log, "Failed to create function {}. Please either make it work with this test or add it to `excluded_functions`.", name);
                throw;
            }

            functions.push_back(Function {.overload_resolver = resolver});
        }

        LOG_INFO(log, "testable functions: {} / {}; skipped dictionary functions: {}, excluded functions: {}, ", functions.size(), all_names.size(), num_dict, num_excluded);
    }

    /// Random type and a column with plenty of random values of that type.
    ColumnWithTypeAndName generateRandomTypeAndColumn(const ArgConstraints &)
    {
        ColumnWithTypeAndName res;
        res.name = fmt::format("c{}", thread_local_rng());

        /// Generate simple types more often because most functions don't support complex types.
        bool allow_complex_types = thread_local_rng() % 2 == 0;
        String type_name = FunctionGenerateRandomStructure::generateRandomDataType(thread_local_rng, /*allow_suspicious_lc_types=*/ true, allow_complex_types);
        res.type = DataTypeFactory::instance().get(type_name);

        res.column = fillColumnWithRandomData(res.type, 128, /*max_array_length=*/ 4, /*max_string_length=*/ 80, thread_local_rng, /*fuzzy=*/ true);

        return res;
    }

    /// Random type and maybe constant value.
    ColumnWithTypeAndName pickRandomArg(const ArgConstraints & constraints, bool always_const)
    {
        ColumnWithTypeAndName res;
        if (!additional_random_values.empty() && thread_local_rng() % 8 == 0)
        {
            res = additional_random_values[thread_local_rng() % additional_random_values.size()];
        }
        else
        {
            /// Cache generated types and values for speed. (I didn't check whether this helps.)
            auto & candidates = random_values[constraints];
            if (candidates.size() < 128)
                candidates.push_back(generateRandomTypeAndColumn(constraints));
            auto & candidate_ref = candidates[thread_local_rng() % candidates.size()];
            /// Sometimes update cached values with freshly generated ones.
            if (thread_local_rng() % 16 == 0)
                candidate_ref = generateRandomTypeAndColumn(constraints);
            res = candidate_ref;
        }

        if (always_const || thread_local_rng() % 2 == 0)
        {
            /// Const value.
            auto new_col = res.column->cloneEmpty();
            new_col->insertFrom(*res.column, thread_local_rng() % res.column->size());
            res.column = std::move(new_col);
        }
        else
        {
            /// Non-const value.
            res.column = nullptr;
        }
        return res;
    }

    std::optional<Overload> tryGenerateRandomOverload(const IFunctionOverloadResolver & resolver)
    {
        ColumnsWithTypeAndName args;
        size_t num_args = resolver.isVariadic() ? generateRandomNumberOfArgs() : resolver.getNumberOfArguments();
        ColumnNumbers always_const_args = resolver.getArgumentsThatAreAlwaysConstant();
        for (size_t i = 0; i < num_args; ++i)
        {
            bool always_const = std::find(always_const_args.begin(), always_const_args.end(), i) != always_const_args.end();
            ArgConstraints constraints; // TODO: lookup from hardcoded table or something
            args.push_back(pickRandomArg(constraints, always_const));
        }

        FunctionBasePtr resolved_function;
        try
        {
            resolved_function = resolver.build(args);
        }
        catch (Exception & e)
        {
            static const int allowed_error_codes[] = {
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                ErrorCodes::ILLEGAL_COLUMN,
                ErrorCodes::BAD_ARGUMENTS,
                ErrorCodes::NO_COMMON_TYPE,
                ErrorCodes::NOT_IMPLEMENTED,
                ErrorCodes::UNSUPPORTED_METHOD,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                ErrorCodes::DECIMAL_OVERFLOW,
            };
            const int * allowed_error_codes_end = allowed_error_codes + sizeof(allowed_error_codes) / sizeof(allowed_error_codes[0]);
            if (std::find(allowed_error_codes, allowed_error_codes_end, e.code()) != allowed_error_codes_end)
                return std::nullopt;
            if ((e.code() == ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH ||
                 e.code() == ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION ||
                 e.code() == ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION) &&
                resolver.isVariadic())
                return std::nullopt;
            throw;
        }

        return Overload{.resolved_function = resolved_function, .args = std::move(args)};
    }

    void mainLoop()
    {
        for (size_t iteration = 0; iteration < 1000000; ++iteration)
        {
            /// TODO: bias
            Function & function = functions[thread_local_rng() % functions.size()];
            auto overload = tryGenerateRandomOverload(*function.overload_resolver);
            ++function.overload_attempts;
            if (!overload.has_value())
                continue;

            ++function.overload_successes;
        }
    }
};
}

TEST(FunctionsStress, stress)
{
    try
    {
        /// TODO: threads
        FunctionsStressTest().run();
    }
    catch (...)
    {
        tryLogCurrentException("test");
        throw;
    }
}

//asdqwe:
// * monotonicity check on endpoints or inside; filter non-throwing values for monotonicity check
// * query monotonicity on full or half-infinite range sometimes
// * save results as value candidates for future use, maybe mutate them randomly for simple types
// * print repro query or something
// * CAST, _CAST, accurateCast, accurateCastOrNull, to*
// * maybe try all functions, not just casts
// * detect and blacklist slow functions
// * maybe test constant folding
// * sanity check that the toDate monotonicity bug is found
// * randomize settings: decimal_check_overflow, cast_string_to_date_time_mode, enable_extended_results_for_datetime_functions, allow_nonconst_timezone_arguments, use_legacy_to_time, function_locate_has_mysql_compatible_argument_order, allow_simdjson, splitby_max_substrings_includes_remaining_string, least_greatest_legacy_null_behavior, h3togeo_lon_lat_result_order, geotoh3_argument_order, cast_keep_nullable, cast_ipv4_ipv6_default_on_conversion_error, enable_named_columns_in_function_tuple, function_visible_width_behavior, function_json_value_return_type_allow_nullable, function_json_value_return_type_allow_complex, use_variant_as_common_type, geo_distance_returns_float64_on_float64_arguments, session_timezone, function_date_trunc_return_type_behavior, date_time_input_format, date_time_output_format, date_time_overflow_behavior
