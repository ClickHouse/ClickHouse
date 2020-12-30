#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

class FunctionGenerateUUIDv4 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv4";

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUUID>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UInt128>::create();
        typename ColumnVector<UInt128>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);

        /// RandImpl is target-dependent and is not the same in different TargetSpecific namespaces.
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UInt128));

        for (UInt128 & uuid: vec_to)
        {
            /** https://tools.ietf.org/html/rfc4122#section-4.4
             */
            uuid.low = (uuid.low & 0xffffffffffff0fffull) | 0x0000000000004000ull;
            uuid.high = (uuid.high & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        return col_res;
    }
};

) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

class FunctionGenerateUUIDv4 : public TargetSpecific::Default::FunctionGenerateUUIDv4
{
public:
    explicit FunctionGenerateUUIDv4(const Context & context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionGenerateUUIDv4>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionGenerateUUIDv4>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionGenerateUUIDv4>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

void registerFunctionGenerateUUIDv4(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGenerateUUIDv4>();
}

}


