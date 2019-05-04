#include <DataTypes/DataTypeDateTime.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

template<typename DT>
struct TypeGetter;

template<>
struct TypeGetter<DataTypeDateTime64> {
    using Type = DataTypeDateTime64;
    static constexpr auto name = "now64";

    static DateTime64::Type now() {
        long int ns;
        time_t sec;
        timespec spec;
        clock_gettime(CLOCK_REALTIME, &spec);
        sec = spec.tv_sec;
        ns = spec.tv_nsec;
        return 1000 * 1000 * 1000 * sec + ns;
    }
};

template<>
struct TypeGetter<DataTypeDateTime> {
    using Type = DataTypeDateTime;
    static constexpr auto name = "now";

    static UInt64 now() {
        return static_cast<UInt64>(time(nullptr));
    }
};


/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
template<typename TG>
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = TypeGetter<TG>::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow<TG>>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<typename TypeGetter<TG>::Type>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = typename TypeGetter<TG>::Type().createColumnConst(input_rows_count, TypeGetter<TG>::now());
    }
};

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNow<DataTypeDateTime64>>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionNow<DataTypeDateTime>>(FunctionFactory::CaseInsensitive);
}

}
