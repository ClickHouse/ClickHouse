#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

class FunctionGenerateUUIDv4 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv4";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGenerateUUIDv4>(); }

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

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        auto col_res = ColumnVector<UInt128>::create();
        typename ColumnVector<UInt128>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UInt128));

        for (UInt128 & uuid: vec_to)
        {
            /** https://tools.ietf.org/html/rfc4122#section-4.4
             */
            uuid.low = (uuid.low & 0xffffffffffff0fffull) | 0x0000000000004000ull;
            uuid.high = (uuid.high & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        block.getByPosition(result).column = std::move(col_res);
    }
};

void registerFunctionGenerateUUIDv4(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGenerateUUIDv4>();
}

}


