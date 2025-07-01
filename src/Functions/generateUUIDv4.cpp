#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <Common/TargetSpecific.h>

namespace DB
{

namespace
{

void generateUUID4Generic(ColumnVector<UUID>::Container & vec_to)
{
    RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));
    for (UUID & uuid : vec_to)
    {
        /// https://tools.ietf.org/html/rfc4122#section-4.4
        UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
    }
}

#if USE_MULTITARGET_CODE

AVX2_FUNCTION_SPECIFIC_ATTRIBUTE void NO_INLINE generateUUID4AVX2(ColumnVector<UUID>::Container & vec_to)
{
    RandImpl::executeAVX2(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));
    for (UUID & uuid : vec_to)
    {
        UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
    }
}

AVX512BW_FUNCTION_SPECIFIC_ATTRIBUTE void NO_INLINE generateUUID4AVX512BW(ColumnVector<UUID>::Container & vec_to)
{
    RandImpl::executeAVX512BW(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));
    for (UUID & uuid : vec_to)
    {
        UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
    }
}

#endif
}

class FunctionGenerateUUIDv4 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv4";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args;
        FunctionArgumentDescriptors optional_args{
            {"expr", nullptr, nullptr, "any type"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);
        if (!size)
            return col_res;

#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::AVX512BW))
        {
            generateUUID4AVX512BW(vec_to);
            return col_res;
        }

        if (isArchSupported(TargetArch::AVX2))
        {
            generateUUID4AVX2(vec_to);
            return col_res;
        }
#endif
        generateUUID4Generic(vec_to);
        return col_res;
    }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGenerateUUIDv4>(); }
};

REGISTER_FUNCTION(GenerateUUIDv4)
{
    factory.registerFunction<FunctionGenerateUUIDv4>();
}

}


