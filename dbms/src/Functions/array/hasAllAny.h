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
#include <ext/range.h>


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
    FunctionArrayHasAllAny(const Context & context, bool all, const char * name)
        : context(context), all(all), name(name) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (auto i : ext::range(0, arguments.size()))
        {
            auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
            if (!array_type)
                throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                                + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t rows = input_rows_count;
        size_t num_args = arguments.size();

        DataTypePtr common_type = nullptr;
        auto commonType = [&common_type, &block, &arguments]()
        {
            if (common_type == nullptr)
            {
                DataTypes data_types;
                data_types.reserve(arguments.size());
                for (const auto & argument : arguments)
                    data_types.push_back(block.getByPosition(argument).type);

                common_type = getLeastSupertype(data_types);
            }

            return common_type;
        };

        Columns preprocessed_columns(num_args);

        for (size_t i = 0; i < num_args; ++i)
        {
            const auto & argument = block.getByPosition(arguments[i]);
            ColumnPtr preprocessed_column = argument.column;

            const auto argument_type = typeid_cast<const DataTypeArray *>(argument.type.get());
            const auto & nested_type = argument_type->getNestedType();

            /// Converts Array(Nothing) or Array(Nullable(Nothing) to common type. Example: hasAll([Null, 1], [Null]) -> 1
            if (typeid_cast<const DataTypeNothing *>(removeNullable(nested_type).get()))
                preprocessed_column = castColumn(argument, commonType(), context);

            preprocessed_columns[i] = std::move(preprocessed_column);
        }

        std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;

        for (auto & argument_column : preprocessed_columns)
        {
            bool is_const = false;

            if (auto argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
            {
                is_const = true;
                argument_column = argument_column_const->getDataColumnPtr();
            }

            if (auto argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
                sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, rows));
            else
                throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
        }

        auto result_column = ColumnUInt8::create(rows);
        auto result_column_ptr = typeid_cast<ColumnUInt8 *>(result_column.get());
        GatherUtils::sliceHas(*sources[0], *sources[1], all, *result_column_ptr);

        block.getByPosition(result).column = std::move(result_column);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    const Context & context;
    bool all;
    const char * name;
};

}
