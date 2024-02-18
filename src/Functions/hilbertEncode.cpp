#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionSpaceFillingCurveEncode.h>
#include <Functions/PerformanceAdaptors.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}


class FunctionHilbertEncode : public FunctionSpaceFillingCurveEncode
{
public:
    static constexpr auto name = "hilbertEncode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHilbertEncode>();
    }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_dimensions = arguments.size();
        if (num_dimensions < 1 || num_dimensions > 2) {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal number of UInt arguments of function {}: should be at least 1 and not more than 2",
                getName());
        }

        size_t vector_start_index = 0;
        const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const ColumnTuple * mask;
        if (const_col)
            mask = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get());
        else
            mask = typeid_cast<const ColumnTuple *>(arguments[0].column.get());
        if (mask)
        {
            num_dimensions = mask->tupleSize();
            vector_start_index = 1;
            for (size_t i = 0; i < num_dimensions; i++)
            {
                auto ratio = mask->getColumn(i).getUInt(0);
                if (ratio > 8 || ratio < 1)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Illegal argument {} of function {}, should be a number in range 1-8",
                                    arguments[0].column->getName(), getName());
            }
        }

        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        auto col_res = ColumnUInt64::create();
        ColumnUInt64::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const ColumnPtr & col0 = non_const_arguments[0 + vector_start_index].column;
        if (num_dimensions == 1) {
            for (size_t i = 0; i < input_rows_count; i++)
            {
                vec_res[i] = col0->getUInt(i);
            }
            return col_res;
        }

        return nullptr;
    }
};


REGISTER_FUNCTION(HilbertEncode)
{
    factory.registerFunction<FunctionHilbertEncode>(FunctionDocumentation{
    .description=R"(

)",
        .examples{
            },
        .categories {}
    });
}

}
