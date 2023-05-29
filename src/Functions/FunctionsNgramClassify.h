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

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const DataTypePtr & result_type, [[maybe_unused]] size_t input_rows_count) const override
    {
        std::cout << "\nExecute Implementation started\n";

        // using ResultType = typename Impl::ResultType;

        const ColumnPtr & slice_name = arguments[0].column;
        const ColumnPtr & texts = arguments[1].column;

        const ColumnConst * slice_name_const = typeid_cast<const ColumnConst *>(&*slice_name);
        const ColumnConst * texts_const = typeid_cast<const ColumnConst *>(&*texts);
        
        const ColumnString * texts_vector = checkAndGetColumn<ColumnString>(&*texts);

        if (!slice_name_const) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of fist argument of function. Must be String.", arguments[0].column->getName());
            return nullptr;
        }

        if (texts_const) {
            const String &text = texts_const->getValue<String>();
            std::cout << "text is: " << text << '\n';
            const String &slice = slice_name_const->getValue<String>();
            std::cout << "slice is: " << slice << '\n';
            String res{implementation->classify(slice, text)};
            std::cout << "Constant Constant\n";
            std::cout << texts_const->size() << '\n';
            std::cout << res << '\n';
            return result_type->createColumnConst(texts_const->size(), toField(res));
        }

        const String & slice_name_string = slice_name_const->getValue<String>();

        std::cout << "Slice name in executeImpl: " << slice_name_string << '\n';

        if (!texts_vector) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of fist argument of function. Must be String or Column of Strings.", arguments[0].column->getName());
            return nullptr;
        }

        std::cout << "All Checks Passed. Ready to execute implementation\n";

        auto result = ColumnString::create();

        const auto & chars = texts_vector->getChars();
        const auto & offsets = texts_vector->getOffsets();

        IColumn::Offset current_offset = 0;

        for (size_t index = 0; index < (*texts_vector).size(); ++index) {
            String text = String(reinterpret_cast<const char *>(&chars[current_offset]));
            result->insert(implementation->classify(slice_name_string, text));
            current_offset = offsets[index];
        }
        return result;

    }
private:
    std::shared_ptr<Impl> implementation{new Impl()};
};


}
