#pragma once

#include <cstdio>
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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Must be String.",
                arguments[0]->getName(), getName());

        return arguments[0];
    }

    ColumnPtr executeImpl([[maybe_unused]] const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        std::cout << "\nExecute Implementation started\n";

        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & column_needle = arguments[1].column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if (col_haystack_const && col_needle_const) {
            // нужно вызывать имплементацию, когда надо проклассифицировать только одно слово одним срезом
            const String &text = col_needle_const->getValue<String>();
            const String &class_name = col_haystack_const->getValue<String>();
            ResultType res{};
            Impl::constant(text, class_name, res);
            return result_type->createColumnConst(col_haystack_const->size(), toField(res));
        }

        // в противном случае у нас классификация целого столбца (то есть вектора), и надо смотреть от этого
        // auto col_res = ColumnVector<ResultType>::create();
        auto col_res = ColumnString::create();
        // typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        // vec_res.resize(column_haystack->size());
        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const auto &chars = col_haystack_vector->getChars();
        const auto &offsets = col_haystack_vector->getOffsets();
        const String &class_name = col_haystack_const->getValue<String>();


        // vec_res.resize(column_haystack->size());
        // auto col_res = ColumnString::create();


        Impl::vector(chars, offsets, class_name, col_res->getChars(), col_res->getOffsets());
        return col_res;
    }

};


}
