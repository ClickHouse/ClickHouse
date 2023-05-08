#pragma once

#ifndef A6580F72_7317_47C2_B0B2_4EA564CB6BF4
#define A6580F72_7317_47C2_B0B2_4EA564CB6BF4



#endif /* A6580F72_7317_47C2_B0B2_4EA564CB6BF4 */


// что это такое?) ахахах
// ... мб это коммит какой-то мой? или хеш ветки

#include <Columns/ColumnConst.h>
#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>

// я буду использовать за основу файл Functions/FunctionsTestClassification.h
// Там используется почти все то, что мне уже нужно
// Очень быстро разберусь со структурой context и прочими полями, очень удобно :)

namespace DB 
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
} // взято из FunctionsTestClassification.h

template <typename Impl, typename Name>
class NgramTextClassification : public IFunction
{
public:
    static constexpr auto name = Name::name;

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<NgramTextClassification>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
        // return nullptr;
    }

    ColumnPtr executeImpl([[maybe_unused]] const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;
        // throw Exception("Not Implemented", ErrorCodes::SUPPORT_IS_DISABLED);
        auto col_res = ColumnVector<ResultType>::create();
        
        return col_res;
    }

};


}
