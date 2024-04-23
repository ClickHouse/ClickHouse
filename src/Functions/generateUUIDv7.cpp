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
constexpr auto bits_in_counter = 42;
constexpr uint64_t counter_limit = (uint64_t{1} << bits_in_counter);
constexpr uint8_t random_data_offset = 6;
constexpr uint8_t random_data_count = 10;
constexpr uint8_t next_count_random_data_offset = 12;
constexpr uint8_t next_count_random_data_count = 4;

using UUIDAsArray = std::array<uint8_t, 16>;

inline uint64_t getTimestampMs()
{
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    const uint64_t sec = tp.tv_sec;
    return sec * 1000 + tp.tv_nsec / 1000000;
}

inline void fillTimestamp(UUIDAsArray & uuid, uint64_t timestamp)
{
    uuid[0] = (timestamp >> 40) & 0xFF;
    uuid[1] = (timestamp >> 32) & 0xFF;
    uuid[2] = (timestamp >> 24) & 0xFF;
    uuid[3] = (timestamp >> 16) & 0xFF;
    uuid[4] = (timestamp >> 8) & 0xFF;
    uuid[5] = (timestamp)&0xFF;
}
}

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
    DECLARE_DEFAULT_CODE(__VA_ARGS__) \
    DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

    namespace UUIDv7Impl
    {
        inline void store(UUID & new_uuid, UUIDAsArray & uuid)
        {
            uuid[6] = (uuid[6] & 0x0f) | 0x70; // version 7
            uuid[8] = (uuid[8] & 0x3f) | 0x80; // variant 2

            DB::UUIDHelpers::getHighBytes(new_uuid) = unalignedLoadBigEndian<uint64_t>(uuid.data());
            DB::UUIDHelpers::getLowBytes(new_uuid) = unalignedLoadBigEndian<uint64_t>(uuid.data() + 8);
        }

        struct UUIDv7Base
        {
            UUIDAsArray & uuid;
            UUIDv7Base(UUIDAsArray & u) : uuid(u) { }
        };

        struct RandomData
        {
            static constexpr auto name = "generateUUIDv7";
            struct Data : UUIDv7Base
            {
                UUIDAsArray uuid_data;

                Data() : UUIDv7Base(uuid_data) { }

                void generate(UUID & new_uuid)
                {
                    fillTimestamp(uuid, getTimestampMs());
                    memcpy(uuid.data() + random_data_offset, &new_uuid, random_data_count);
                    store(new_uuid, uuid);
                }
            };
        };

        struct CounterDataCommon : UUIDv7Base
        {
            CounterDataCommon(UUIDAsArray & u) : UUIDv7Base(u) { }

            uint64_t getCounter()
            {
                uint64_t counter = uuid[6] & 0x0f;
                counter = (counter << 8) | uuid[7];
                counter = (counter << 6) | (uuid[8] & 0x3f);
                counter = (counter << 8) | uuid[9];
                counter = (counter << 8) | uuid[10];
                counter = (counter << 8) | uuid[11];
                return counter;
            }

            void generate(UUID & newUUID)
            {
                uint64_t timestamp = 0;
                /// Get timestamp of the previous uuid
                for (int i = 0; i != 6; ++i)
                {
                    timestamp = (timestamp << 8) | uuid[i];
                }

                const uint64_t unix_time_ms = getTimestampMs();
                // continue incrementing counter when clock slightly goes back or when counter overflow happened during the previous UUID generation
                bool need_to_increment_counter = (timestamp == unix_time_ms || timestamp < unix_time_ms + 10000);
                uint64_t counter = 0;
                if (need_to_increment_counter)
                {
                    counter = getCounter();
                }
                else
                {
                    timestamp = unix_time_ms;
                }

                bool counter_incremented = false;
                if (need_to_increment_counter)
                {
                    if (++counter == counter_limit)
                    {
                        ++timestamp;
                        // counter bytes will be filled by the random data
                    }
                    else
                    {
                        uuid[6] = counter >> 38;
                        uuid[7] = counter >> 30;
                        uuid[8] = counter >> 24;
                        uuid[9] = counter >> 16;
                        uuid[10] = counter >> 8;
                        uuid[11] = counter;
                        counter_incremented = true;
                    }
                }

                fillTimestamp(uuid, timestamp);

                // Get the required number of random bytes: 4 in the case of incrementing existing counter, 10 in the case of renewing counter
                memcpy(
                    uuid.data() + (counter_incremented ? next_count_random_data_offset : random_data_offset),
                    &newUUID,
                    counter_incremented ? next_count_random_data_count : random_data_count);

                store(newUUID, uuid);
            }
        };

        struct ThreadLocalCounter
        {
            static constexpr auto name = "generateUUIDv7WithFastCounter";
            struct Data : CounterDataCommon
            {
                // Implement counter monotony only within one thread so function doesn't require mutexes and doesn't affect performance of the same function running simultenaously on other threads
                static inline thread_local UUIDAsArray uuid_data;
                Data() : CounterDataCommon(uuid_data) { }
            };
        };

        struct GlobalCounter
        {
            static constexpr auto name = "generateUUIDv7WithCounter";
            struct Data : std::lock_guard<std::mutex>, CounterDataCommon
            {
                // Implement counter monotony within one timestamp across all threads generating UUIDv7 with counter simultaneously
                static inline UUIDAsArray uuid_data;
                static inline std::mutex mtx;
                Data() : std::lock_guard<std::mutex>(mtx), CounterDataCommon(uuid_data) { }
            };
        };
    } // namespace UUIDv7Impl


    template <typename FillPolicy>
    class FunctionGenerateUUIDv7Base
    : public IFunction,
      public FillPolicy {
      public:
          using FillPolicy::name;
          using FillPolicyData = typename FillPolicy::Data;

          FunctionGenerateUUIDv7Base() = default;

          String getName() const final
          {
              return name;
          }

          size_t getNumberOfArguments() const final
          {
              return 0;
          }

          bool isDeterministicInScopeOfQuery() const final
          {
              return false;
          }
          bool useDefaultImplementationForNulls() const final
          {
              return false;
          }
          bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const final
          {
              return false;
          }
          bool isVariadic() const final
          {
              return true;
          }

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

          bool isDeterministic() const override
          {
              return false;
          }

          ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
          {
              auto col_res = ColumnVector<UUID>::create();
              typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

              size_t size = input_rows_count;
              vec_to.resize(size);

              /// RandImpl is target-dependent and is not the same in different TargetSpecific namespaces.
              /// Not all random bytes produced here are required for the UUIDv7 but it's the simplest way to get the required number of them by using RandImpl
              RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

              for (UUID & new_uuid : vec_to)
              {
                  FillPolicyData data;
                  data.generate(new_uuid);
              }

              return col_res;
          }
      };

    using FunctionGenerateUUIDv7 = FunctionGenerateUUIDv7Base<UUIDv7Impl::RandomData>;
    using FunctionGenerateUUIDv7WithCounter = FunctionGenerateUUIDv7Base<UUIDv7Impl::GlobalCounter>;
    using FunctionGenerateUUIDv7WithFastCounter = FunctionGenerateUUIDv7Base<UUIDv7Impl::ThreadLocalCounter>;

    ) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS


class FunctionGenerateUUIDv7 : public TargetSpecific::Default::FunctionGenerateUUIDv7
{
public:
    explicit FunctionGenerateUUIDv7(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionGenerateUUIDv7>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2, TargetSpecific::AVX2::FunctionGenerateUUIDv7>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGenerateUUIDv7>(context); }

private:
    ImplementationSelector<IFunction> selector;
};

class FunctionGenerateUUIDv7WithCounter : public TargetSpecific::Default::FunctionGenerateUUIDv7WithCounter
{
public:
    explicit FunctionGenerateUUIDv7WithCounter(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionGenerateUUIDv7WithCounter>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2, TargetSpecific::AVX2::FunctionGenerateUUIDv7WithCounter>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGenerateUUIDv7WithCounter>(context); }

private:
    ImplementationSelector<IFunction> selector;
};


class FunctionGenerateUUIDv7WithFastCounter : public TargetSpecific::Default::FunctionGenerateUUIDv7WithFastCounter
{
public:
    explicit FunctionGenerateUUIDv7WithFastCounter(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionGenerateUUIDv7WithFastCounter>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2, TargetSpecific::AVX2::FunctionGenerateUUIDv7WithFastCounter>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGenerateUUIDv7WithFastCounter>(context); }

private:
    ImplementationSelector<IFunction> selector;
};


REGISTER_FUNCTION(GenerateUUIDv7)
{
    factory.registerFunction<FunctionGenerateUUIDv7>(
        FunctionDocumentation{
            .description = R"(
Generates a UUID of version 7 with current Unix time having milliseconds precision followed by random data.
This function takes an optional argument, the value of which is discarded to generate different values in case the function is called multiple times.
The function returns a value of type UUID.
)",
            .examples{{"uuid", "SELECT generateUUIDv7()", ""}, {"multiple", "SELECT generateUUIDv7(1), generateUUIDv7(2)", ""}},
            .categories{"UUID"}},
        FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionGenerateUUIDv7WithCounter>(
        FunctionDocumentation{
            .description = R"(
Generates a UUID of version 7 with current Unix time having milliseconds precision, a monotonic counter within the same timestamp starting from the random value, and followed by 4 random bytes.
This function takes an optional argument, the value of which is discarded to generate different values in case the function is called multiple times.
The function returns a value of type UUID.
)",
            .examples{
                {"uuid", "SELECT generateUUIDv7WithCounter()", ""},
                {"multiple", "SELECT generateUUIDv7WithCounter(1), generateUUIDv7WithCounter(2)", ""}},
            .categories{"UUID"}},
        FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionGenerateUUIDv7WithFastCounter>(
        FunctionDocumentation{
            .description = R"(
Generates a UUID of version 7 with current Unix time having milliseconds precision, a monotonic counter within the same timestamp and the same request starting from the random value, and followed by 4 random bytes.
This function takes an optional argument, the value of which is discarded to generate different values in case the function is called multiple times.
This function is a little bit faster version of the function GenerateUUIDv7WithCounter. It doesn't guarantee the counter monotony within the same timestamp across different requests.
The function returns a value of type UUID.
)",
            .examples{
                {"uuid", "SELECT generateUUIDv7WithFastCounter()", ""},
                {"multiple", "SELECT generateUUIDv7WithFastCounter(1), generateUUIDv7WithFastCounter(2)", ""}},
            .categories{"UUID"}},
        FunctionFactory::CaseSensitive);
}

}
