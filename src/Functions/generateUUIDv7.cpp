#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

namespace
{

/* Bit layouts of UUIDv7

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

/// bit counts
constexpr auto rand_a_bits_count = 12;
constexpr auto rand_b_bits_count = 62;
constexpr auto rand_b_low_bits_count = 32;
constexpr auto counter_high_bits_count = rand_a_bits_count;
constexpr auto counter_low_bits_count = 30;
constexpr auto bits_in_counter = counter_high_bits_count + counter_low_bits_count;
constexpr uint64_t counter_limit = (1ull << bits_in_counter);

/// bit masks for UUIDv7 components
constexpr uint64_t variant_2_mask  = (2ull << rand_b_bits_count);
constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;
constexpr uint64_t rand_b_with_counter_bits_mask = (1ull << rand_b_low_bits_count) - 1;
constexpr uint64_t counter_low_bits_mask = (1ull << counter_low_bits_count) - 1;
constexpr uint64_t counter_high_bits_mask = rand_a_bits_mask;

uint64_t getTimestampMillisecond()
{
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    const uint64_t sec = tp.tv_sec;
    return sec * 1000 + tp.tv_nsec / 1000000;
}

void setTimestampAndVersion(UUID & uuid, uint64_t timestamp)
{
    UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & rand_a_bits_mask) | (timestamp << 16) | 0x7000;
}

void setVariant(UUID & uuid)
{
    UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_bits_mask) | variant_2_mask;
}

struct FillAllRandomPolicy
{
    static constexpr auto name = "generateUUIDv7NonMonotonic";
    static constexpr auto doc_description = R"(Generates a UUID of version 7. The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), and a random field (74 bit, including a 2-bit variant field "2") to distinguish UUIDs within a millisecond. This function is the fastest generateUUIDv7* function but it gives no monotonicity guarantees within a timestamp.)";
    struct Data
    {
        void generate(UUID & uuid, uint64_t ts)
        {
            setTimestampAndVersion(uuid, ts);
            setVariant(uuid);
        }
    };
};

struct CounterFields
{
    uint64_t last_timestamp = 0;
    uint64_t counter = 0;

    void resetCounter(const UUID & uuid)
    {
        const uint64_t counter_low_bits = (UUIDHelpers::getLowBytes(uuid) >> rand_b_low_bits_count) & counter_low_bits_mask;
        const uint64_t counter_high_bits = UUIDHelpers::getHighBytes(uuid) & counter_high_bits_mask;
        counter = (counter_high_bits << 30) | counter_low_bits;
    }

    void incrementCounter(UUID & uuid)
    {
        if (++counter == counter_limit) [[unlikely]]
        {
            ++last_timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
        else
        {
            UUIDHelpers::getHighBytes(uuid) = (last_timestamp << 16) | 0x7000 | (counter >> counter_low_bits_count);
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_with_counter_bits_mask) | variant_2_mask | ((counter & counter_low_bits_mask) << rand_b_low_bits_count);
        }
    }

    void generate(UUID & uuid, uint64_t timestamp)
    {
        const bool need_to_increment_counter = (last_timestamp == timestamp) || ((last_timestamp > timestamp) & (last_timestamp < timestamp + 10000));
        if (need_to_increment_counter)
        {
            incrementCounter(uuid);
        }
        else
        {
            last_timestamp = timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
    }
};


struct GlobalCounterPolicy
{
    static constexpr auto name = "generateUUIDv7";
    static constexpr auto doc_description = R"(Generates a UUID of version 7. The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit, including a variant field "2", 2 bit) to distinguish UUIDs within a millisecond, and a random field (32 bits). For any given timestamp (unix_ts_ms), the counter starts at a random value and is incremented by 1 for each new UUID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to a random new start value. Function generateUUIDv7 guarantees that the counter field within a timestamp increments monotonically across all function invocations in concurrently running threads and queries.)";

    /// Guarantee counter monotonicity within one timestamp across all threads generating UUIDv7 simultaneously.
    struct Data
    {
        static inline CounterFields fields;
        static inline SharedMutex mutex; /// works a little bit faster than std::mutex here
        std::lock_guard<SharedMutex> guard;

        Data()
            : guard(mutex)
        {}

        void generate(UUID & uuid, uint64_t timestamp)
        {
            fields.generate(uuid, timestamp);
        }
    };
};

struct ThreadLocalCounterPolicy
{
    static constexpr auto name = "generateUUIDv7ThreadMonotonic";
    static constexpr auto doc_description = R"(Generates a UUID of version 7. The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit, including a variant field "2", 2 bit) to distinguish UUIDs within a millisecond, and a random field (32 bits). For any given timestamp (unix_ts_ms), the counter starts at a random value and is incremented by 1 for each new UUID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to a random new start value. This function behaves like generateUUIDv7 but gives no guarantee on counter monotony across different simultaneous requests. Monotonicity within one timestamp is guaranteed only within the same thread calling this function to generate UUIDs.)";

    /// Guarantee counter monotonicity within one timestamp within the same thread. Faster than GlobalCounterPolicy if a query uses multiple threads.
    struct Data
    {
        static inline thread_local CounterFields fields;

        void generate(UUID & uuid, uint64_t timestamp)
        {
            fields.generate(uuid, timestamp);
        }
    };
};

}

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

template <typename FillPolicy>
class FunctionGenerateUUIDv7Base : public IFunction, public FillPolicy
{
public:
    String getName() const final {  return FillPolicy::name; }

    size_t getNumberOfArguments() const final { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const final { return false; }
    bool useDefaultImplementationForNulls() const final { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const final { return false; }
    bool isVariadic() const final { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args;
        FunctionArgumentDescriptors optional_args{
            {"expr", nullptr, nullptr, "Arbitrary Expression"}
        };
        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        if (input_rows_count)
        {
            vec_to.resize(input_rows_count);

            /// Not all random bytes produced here are required for the UUIDv7 but it's the simplest way to get the required number of them by using RandImpl
            RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

            /// Note: For performance reasons, clock_gettime is called once per chunk instead of once per UUID. This reduces precision but
            /// it still complies with the UUID standard.
            uint64_t timestamp = getTimestampMillisecond();
            for (UUID & uuid : vec_to)
            {
                typename FillPolicy::Data data;
                data.generate(uuid, timestamp);
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
void registerUUIDv7Generator(auto& factory)
{
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
    registerUUIDv7Generator<GlobalCounterPolicy>(factory);
    registerUUIDv7Generator<ThreadLocalCounterPolicy>(factory);
    registerUUIDv7Generator<FillAllRandomPolicy>(factory);
}
}
