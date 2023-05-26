#pragma once

#include <cstdio>
#include "DataTypes/DataTypeString.h"
#include "DataTypes/IDataType.h"
#ifndef A6580F72_7317_47C2_B0B2_4EA564CB6BF4
#define A6580F72_7317_47C2_B0B2_4EA564CB6BF4



#endif /* A6580F72_7317_47C2_B0B2_4EA564CB6BF4 */


#include <Columns/ColumnConst.h>
#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionsStringSimilarity.h>


namespace DB 
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SUPPORT_IS_DISABLED;
}

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
        if (!isString(arguments[0])) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Must be String.",
                arguments[0]->getName(), getName());
        }

        if (!isString(arguments[1]) /* Тут еще нужно проверить, что это не колонка строк */) {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument of function {}. Must be String or Column of Strings.",
                arguments[1]->getName(), getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        std::cout << "\nExecute Implementation started\n";

        using ResultType = typename Impl::ResultType;

        const ColumnPtr & slice_name = arguments[0].column;
        const ColumnPtr & texts = arguments[1].column;

        const ColumnConst * slice_name_const = typeid_cast<const ColumnConst *>(&*slice_name);
        const ColumnConst * texts_const = typeid_cast<const ColumnConst *>(&*texts);

        if (!slice_name_const) { // пока что мы поддерживаем только классивикацию одного одним или многих одним, но НЕ многих многими. Чуть позже добавлю это
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of fist argument of function. Must be String.", arguments[0].column->getName());
        }

        if (texts_const) {
            // нужно вызывать имплементацию, когда надо проклассифицировать только одно слово одним срезом
            const String &text = texts_const->getValue<String>();
            const String &slice = slice_name_const->getValue<String>();
            ResultType res{};
            // Impl::constant(text, slice, res);
            std::cout << "Constant Constant\n";
            return result_type->createColumnConst(texts_const->size(), toField(res));
        }

        auto col_res = ColumnString::create();

        // const ColumnString * col_vector = checkAndGetColumn<ColumnString>(&*arguments[1].column);

        // const auto &chars = col_vector->getChars();
        // const auto &offsets = col_vector->getOffsets();

        // const String &slice = slice_name_const->getValue<String>();

        // Impl::vector(chars, offsets, slice, col_res->getChars(), col_res->getOffsets());
        return col_res;
    }

};


}
