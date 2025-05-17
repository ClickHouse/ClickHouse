#if defined(MEMORY_ACCESS_TRACING)

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#include <base/memory_access_tracing.h>


namespace DB
{

namespace
{
// TODO: make only one counter value

/** If ClickHouse is build with memory access tracing, returns information about memory accesses
  * in form of a tuple (access_type, thread_id, address, return_address)
  * Currently just returns counter as placeholder in all columns.
  */
class FunctionMemoryAccesses : public IFunction
{
public:
    String getName() const override
    {
        return "memoryAccesses";
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        // Возвращаем массив кортежей с 4 полями: type, thread_id, address, return_address
        DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeUInt32>(), // тип доступа
            std::make_shared<DataTypeUInt64>(), // поток
            std::make_shared<DataTypeUInt64>(), // адрес, к которому был доступ
            std::make_shared<DataTypeUInt64>()  // return address
        });

        return std::make_shared<DataTypeArray>(tuple_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        // Получаем счетчик доступов к памяти
        uint64_t counter = getMemoryAccessCount();

        // В будущем здесь будет полноценная информация о доступах к памяти
        // Пока просто возвращаем счетчик во всех колонках как placeholder

        // Создаем колонки для каждого поля кортежа
        auto column_access_type = ColumnUInt32::create(1, counter);
        auto column_thread_id = ColumnUInt64::create(1, counter);
        auto column_address = ColumnUInt64::create(1, counter);
        auto column_return_address = ColumnUInt64::create(1, counter);

        // Создаем кортеж из этих колонок
        Columns tuple_elements;
        tuple_elements.emplace_back(std::move(column_access_type));
        tuple_elements.emplace_back(std::move(column_thread_id));
        tuple_elements.emplace_back(std::move(column_address));
        tuple_elements.emplace_back(std::move(column_return_address));

        auto column_tuple = ColumnTuple::create(std::move(tuple_elements));

        // Оборачиваем в массив
        auto column_array = ColumnArray::create(
            std::move(column_tuple),
            ColumnArray::ColumnOffsets::create(1, 1)); // Создаем массив из одного элемента

        // Возвращаем константный столбец с этим массивом
        return ColumnConst::create(std::move(column_array), input_rows_count);
    }
};

}

REGISTER_FUNCTION(MemoryAccesses)
{
    factory.registerFunction("memoryAccesses", [](ContextPtr){ return std::make_shared<FunctionMemoryAccesses>(); },
        FunctionDocumentation
        {
            .description=R"(
This function is only available if ClickHouse was built with the MEMORY_ACCESS_TRACING=1 option.

It returns information about memory accesses in the form of tuples with 4 fields:
- access_type: type of memory access (1 for load, 2 for store)
- thread_id: thread identifier that performed the access
- address: memory address that was accessed
- return_address: address of the instruction that performed the access

Example usage:
SELECT 
    access_type, 
    thread_id, 
    demangle(addressToSymbol(address)), 
    demangle(addressToSymbol(return_address))
FROM 
    (SELECT * FROM arrayJoin(memoryAccesses()));

Note: This function is experimental and currently only returns a placeholder value.
)",
            .examples{
                {"basic", "SELECT count() FROM arrayJoin(memoryAccesses())", ""}},
            .category{"Introspection"}
        });
}

}

#endif
