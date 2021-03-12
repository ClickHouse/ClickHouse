#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, init_accum) - apply the expression to each element of the array (or set of parallel arrays).
  */
struct ArrayFoldImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }
    static bool isFolding() { return true; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & accum_type)
    {
        return accum_type;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        std::cerr << " **** ArrayFoldImpl **** " << std::endl;
        std::cerr << "     array: "  << array.dumpStructure()   << std::endl;
        std::cerr << "     mapped: " << mapped->dumpStructure() << std::endl;
        std::cerr << "     mapped[0]: " << (*mapped)[0].dump() << std::endl;
        // std::cerr << "     mapped[1]: " << (*mapped)[1].dump() << std::endl;
        //

        ColumnPtr res;
        if (mapped->size() == 0)
        {
            res = mapped;
        }
        else
        {
            res = mapped->cut(0, 1);
        }
        std::cerr << " ^^^^ ArrayFoldImpl ^^^^" << std::endl;
        return res;

        // return ColumnArray::create(mapped->convertToFullColumnIfConst(), array.getOffsetsPtr());
        // return ColumnArray::create(mapped->convertToFullColumnIfConst(), array.getOffsetsPtr());
    }
};

struct NameArrayFold { static constexpr auto name = "arrayFold"; };
using FunctionArrayFold = FunctionArrayMapped<ArrayFoldImpl, NameArrayFold>;

void registerFunctionArrayFold(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFold>();
}

}


