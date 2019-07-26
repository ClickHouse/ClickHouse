#include <dlfcn.h>
#include <unordered_map>
#include <optional>
#include <common/unaligned.h>
#include <common/demangle.h>
#include <common/SimpleCache.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionSymbolizeAddress : public IFunction
{
public:
    static constexpr auto name = "symbolizeAddress";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSymbolizeAddress>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception("Function " + getName() + " needs exactly one argument; passed "
                + toString(arguments.size()) + ".", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isUInt64())
            throw Exception("The only argument for function " + getName() + " must be UInt64. Found "
                + type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    static std::string addressToSymbol(UInt64 uint_address)
    {
        void * addr = unalignedLoad<void *>(&uint_address);

        /// This is extremely slow.
        Dl_info info;
        if (dladdr(addr, &info) && info.dli_sname)
        {
            int demangling_status = 0;
            return demangle(info.dli_sname, demangling_status);
        }
        else
        {
            return {};
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception("Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        auto result_column = ColumnString::create();

        static SimpleCache<decltype(addressToSymbol), &addressToSymbol> func_cached;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string symbol = func_cached(data[i]);
            result_column->insertDataWithTerminatingZero(symbol.data(), symbol.size() + 1);
        }

        block.getByPosition(result).column = std::move(result_column);

        /// Do not let our cache to grow indefinitely (simply drop it)
        if (func_cached.size() > 1000000)
            func_cached.drop();
    }
};

void registerFunctionSymbolizeAddress(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSymbolizeAddress>();
}

}
