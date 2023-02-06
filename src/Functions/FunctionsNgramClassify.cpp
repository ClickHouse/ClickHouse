#include <Functions/FunctionsStringSimilarity.h>
#include <Functions/FunctionsNgramClassify.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct NgramTextClassificationImpl
{
    using ResultType = Float32;
};

struct NgramClassificationName
{
    static constexpr auto name = "NgramClassification";
};

using FunctionNgramTextClassification = NgramTextClassification<NgramTextClassificationImpl, NgramClassificationName>;

REGISTER_FUNCTION(NgramClassify)
{
    factory.registerFunction<FunctionNgramTextClassification>();
}

}

