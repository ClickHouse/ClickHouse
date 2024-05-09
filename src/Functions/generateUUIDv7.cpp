#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

class FunctionGenerateUUIDv7 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv7";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                getName(), arguments.size());

        return std::make_shared<DataTypeUUID>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);

        /// RandImpl is target-dependent and is not the same in different TargetSpecific namespaces.
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

        for (UUID & uuid : vec_to)
        {
            ///  https://www.ietf.org/archive/id/draft-peabody-dispatch-new-uuid-format-04.html#section-5.2

            const auto tm_point = std::chrono::system_clock::now();
            UInt64 unix_ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                   tm_point.time_since_epoch()).count();

            UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & 0x0000000000000fffull) | 0x0000000000007000ull | (unix_ts_ms << 16);
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        return col_res;
    }
};

) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

class FunctionGenerateUUIDv7 : public TargetSpecific::Default::FunctionGenerateUUIDv7
{
public:
    explicit FunctionGenerateUUIDv7(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionGenerateUUIDv7>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionGenerateUUIDv7>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionGenerateUUIDv7>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

REGISTER_FUNCTION(GenerateUUIDv7)
{
    factory.registerFunction<FunctionGenerateUUIDv7>();
}

}


