#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** tuple(x, y, ...) is a function that allows you to group several columns
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */

class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTuple>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes types;
        Strings names;

        for (const auto & argument : arguments)
        {
            types.emplace_back(argument.type);
            names.emplace_back(argument.name);
        }

        /// Create named tuple if possible. We don't print tuple element names
        /// because they are bad anyway -- aliases are not used, e.g. tuple(1 a)
        /// will have element name '1' and not 'a'. If we ever change this, and
        /// add the ability to access tuple elements by name, like tuple(1 a).a,
        /// we should probably enable printing for better discoverability.
        if (DataTypeTuple::canBeCreatedWithNames(names))
            return std::make_shared<DataTypeTuple>(types, names, false /*print names*/);

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        size_t tuple_size = arguments.size();
        Columns tuple_columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            /** If tuple is mixed of constant and not constant columns,
              *  convert all to non-constant columns,
              *  because many places in code expect all non-constant columns in non-constant tuple.
              */
            tuple_columns[i] = arguments[i].column->convertToFullColumnIfConst();
        }
        return ColumnTuple::create(tuple_columns);
    }
};

}

void registerFunctionTuple(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTuple>();
}

}
