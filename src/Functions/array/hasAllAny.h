#pragma once

#include <common/range.h>
#include <common/map.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionArrayHasAllAny : public IFunction
{
public:
    FunctionArrayHasAllAny(GatherUtils::ArraySearchType search_type_, const char * name_)
        : search_type(search_type_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (auto i : collections::range(0, arguments.size()))
        {
            auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
            if (!array_type)
                throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                                + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_args = arguments.size();

        DataTypePtr common_type = getLeastSupertype(collections::map(arguments, [](auto & arg) { return arg.type; }));

        Columns preprocessed_columns(num_args);
        for (size_t i = 0; i < num_args; ++i)
            preprocessed_columns[i] = castColumn(arguments[i], common_type);

        std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;

        for (auto & argument_column : preprocessed_columns)
        {
            bool is_const = false;

            if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
            {
                is_const = true;
                argument_column = argument_column_const->getDataColumnPtr();
            }

            if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
                sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
            else
                throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
        }

        auto result_column = ColumnUInt8::create(input_rows_count);
        auto * result_column_ptr = typeid_cast<ColumnUInt8 *>(result_column.get());
        GatherUtils::sliceHas(*sources[0], *sources[1], search_type, *result_column_ptr);

        return result_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    GatherUtils::ArraySearchType search_type;
    const char * name;
};

}
