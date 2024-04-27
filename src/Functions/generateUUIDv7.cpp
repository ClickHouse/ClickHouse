#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/* Bit layouts of the UUIDv7

without counter:
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |       rand_a          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                        rand_b                             |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘

with counter:
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
*/

// bit counts
constexpr auto rand_a_bits_count = 12;
constexpr auto rand_b_bits_count = 62;
constexpr auto rand_b_low_bits_count = 32;
constexpr auto counter_high_bits_count = rand_a_bits_count;
constexpr auto counter_low_bits_count = 30;
constexpr auto bits_in_counter = counter_high_bits_count + counter_low_bits_count;
constexpr uint64_t counter_limit = (1ull << bits_in_counter);

// bit masks for UUIDv7 parts
constexpr uint64_t variant_2_mask  = (2ull << rand_b_bits_count);
constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;
constexpr uint64_t rand_b_with_counter_bits_mask = (1ull << rand_b_low_bits_count) - 1;
constexpr uint64_t counter_low_bits_mask = (1ull << counter_low_bits_count) - 1;
constexpr auto counter_high_bits_mask = rand_a_bits_mask;

inline uint64_t getTimestampMillisecond()
{
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    const uint64_t sec = tp.tv_sec;
    return sec * 1000 + tp.tv_nsec / 1000000;
}

inline void setTimestamp(UUID & uuid, uint64_t timestamp)
{
    UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & rand_a_bits_mask) | ( timestamp << 16) | 0x7000; // version 7
}

inline void setVariant(UUID & uuid)
{
    UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_bits_mask) | variant_2_mask;
}

struct FillAllRandomPolicy
{
    static constexpr auto name = "generateUUIDv7NonMonotonic";
    static constexpr auto doc_description = "Generates a UUID of version 7 containing the current Unix timestamp in milliseconds (48 bits), followed by version \"7\" (4 bits), and a random field (74 bit) to distinguish UUIDs within a millisecond (including a variant field \"2\", 2 bit). It is the fastest version of generateUUIDv7* functions family.";
    struct Data
    {
        Data() {}

        void generate(UUID & uuid, uint64_t ts)
        {
            setTimestamp(uuid, ts);
            setVariant(uuid);
        }
    };
};

struct CounterFields
{
    uint64_t timestamp = 0;
    uint64_t counter = 0;

    void resetCounter(const UUID& uuid)
    {
        const uint64_t counterLowBits = (UUIDHelpers::getLowBytes(uuid) >> rand_b_low_bits_count) & counter_low_bits_mask;
        const uint64_t counterHighBits = UUIDHelpers::getHighBytes(uuid) & counter_high_bits_mask;
        counter = (counterHighBits << 30) | counterLowBits;
    }

    void incrementCounter(UUID& uuid)
    {
        if (++counter == counter_limit) [[unlikely]]
        {
            ++timestamp;
            resetCounter(uuid);
            setTimestamp(uuid, timestamp);
            setVariant(uuid);
        }
        else
        {
            UUIDHelpers::getHighBytes(uuid) = (timestamp << 16) | 0x7000 | (counter >> counter_low_bits_count);
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_with_counter_bits_mask) | variant_2_mask | ((counter & counter_low_bits_mask) << rand_b_low_bits_count);
        }
    }

    void generate(UUID& new_uuid, uint64_t unix_time_ms)
    {
        const bool need_to_increment_counter = (timestamp == unix_time_ms) || ((timestamp > unix_time_ms) & (timestamp < unix_time_ms + 10000));
        if (need_to_increment_counter)
        {
            incrementCounter(new_uuid);
        }
        else
        {
            timestamp = unix_time_ms;
            resetCounter(new_uuid);
            setTimestamp(new_uuid, timestamp);
            setVariant(new_uuid);
        }
    }
};


struct CounterDataCommon
{
    CounterFields& fields;
    explicit CounterDataCommon(CounterFields & f)
        : fields(f)
    {}

    void generate(UUID& uuid, uint64_t ts)
    {
        fields.generate(uuid, ts);
    }
};

struct ThreadLocalCounter
{
    static constexpr auto name = "generateUUIDv7WithFastCounter";
    static constexpr auto doc_description = "Generates a UUID of version 7 containing the current Unix timestamp in milliseconds (48 bits), followed by version \"7\" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field \"2\", 2 bit), and a random field (32 bits). Counter increment monotony at one timestamp is guaraneed only within one thread running generateUUIDv7 function.";

    struct Data : CounterDataCommon
    {
        // Implement counter monotony only within one thread so function doesn't require mutexes and doesn't affect performance of the same function running simultenaously on other threads
        static inline thread_local CounterFields thread_local_fields;
        Data()
            : CounterDataCommon(thread_local_fields)
        {
        }
    };
};

struct GlobalCounter
{
    static constexpr auto name = "generateUUIDv7";
    static constexpr auto doc_description = "Generates a UUID of version 7 containing the current Unix timestamp in milliseconds (48 bits), followed by version \"7\" (4 bits), a counter (42 bit) to distinguish UUIDs within a millisecond (including a variant field \"2\", 2 bit), and a random field (32 bits). Counter increment monotony at one timestamp is guaraneed across all generateUUIDv7 functions running simultaneously.";

    struct Data : CounterDataCommon
    {
        // Implement counter monotony within one timestamp across all threads generating UUIDv7 with counter simultaneously
        static inline CounterFields static_fields;
        static inline SharedMutex mtx;
        std::lock_guard<SharedMutex> guard; // SharedMutex works a little bit faster than std::mutex here
        Data()
            : CounterDataCommon(static_fields)
            , guard(mtx)
        {
        }
    };
};
}

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

template <typename FillPolicy>
class FunctionGenerateUUIDv7Base : public IFunction,  public FillPolicy {
public:
    using FillPolicy::name;
    using FillPolicyData = typename FillPolicy::Data;

    String getName() const final {  return name; }

    size_t getNumberOfArguments() const final { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const final { return false; }
    bool useDefaultImplementationForNulls() const final { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const final { return false; }
    bool isVariadic() const final { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                getName(),
                arguments.size());

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        if (size)
        {
            vec_to.resize(size);
          
            /// Not all random bytes produced here are required for the UUIDv7 but it's the simplest way to get the required number of them by using RandImpl
            RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));
            auto ts = getTimestampMillisecond();
            for (UUID & new_uuid : vec_to)
            {
                FillPolicyData data;
                data.generate(new_uuid, ts);
            }
        }
        return col_res;
    }
};
) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

template <typename FillPolicy>
class FunctionGenerateUUIDv7Base : public TargetSpecific::Default::FunctionGenerateUUIDv7Base<FillPolicy>
{
public:
    using Self = FunctionGenerateUUIDv7Base<FillPolicy>;
    using Parent = TargetSpecific::Default::FunctionGenerateUUIDv7Base<FillPolicy>;

    explicit FunctionGenerateUUIDv7Base(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, Parent>();

    #if USE_MULTITARGET_CODE
        using ParentAVX2 = TargetSpecific::AVX2::FunctionGenerateUUIDv7Base<FillPolicy>;
        selector.registerImplementation<TargetArch::AVX2, ParentAVX2>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<Self>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

template<typename FillPolicy>
void registerUUIDv7Generator(auto& factory) {
    static constexpr auto doc_syntax_format = "{}([expression])";
    static constexpr auto example_format = "SELECT {}()";
    static constexpr auto multiple_example_format = "SELECT {f}(1), {f}(2)";

    FunctionDocumentation::Description doc_description = FillPolicy::doc_description;
    FunctionDocumentation::Syntax doc_syntax = fmt::format(doc_syntax_format, FillPolicy::name);
    FunctionDocumentation::Arguments doc_arguments = {{"expression", "The expression is used to bypass common subexpression elimination if the function is called multiple times in a query but otherwise ignored. Optional."}};
    FunctionDocumentation::ReturnedValue doc_returned_value = "A value of type UUID version 7.";
    FunctionDocumentation::Examples doc_examples = {{"uuid", fmt::format(example_format, FillPolicy::name), ""}, {"multiple", fmt::format(multiple_example_format, fmt::arg("f", FillPolicy::name)), ""}};
    FunctionDocumentation::Categories doc_categories = {"UUID"};
    factory.template registerFunction<FunctionGenerateUUIDv7Base<FillPolicy>>({doc_description, doc_syntax, doc_arguments, doc_returned_value, doc_examples, doc_categories}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(GenerateUUIDv7)
{
    registerUUIDv7Generator<GlobalCounter>(factory);
    registerUUIDv7Generator<FillAllRandomPolicy>(factory);
    registerUUIDv7Generator<ThreadLocalCounter>(factory);
}

}
