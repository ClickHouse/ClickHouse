#include "config.h"

#if USE_H3

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/VectorWithMemoryTracking.h>

#include <h3api.h>


namespace DB
{
namespace
{

class FunctionH3GetRes0Indexes final : public IFunction
{
public:
    static constexpr auto name = "h3GetRes0Indexes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetRes0Indexes>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        VectorWithMemoryTracking<H3Index> res0_indexes;
        const auto cell_count = res0CellCount();
        res0_indexes.resize(cell_count);
        getRes0Cells(res0_indexes.data());

        Array res_indexes;
        res_indexes.insert(res_indexes.end(), res0_indexes.begin(), res0_indexes.end());

        return result_type->createColumnConst(input_rows_count, res_indexes);
    }
};

}

REGISTER_FUNCTION(H3GetRes0Indexes)
{
    FunctionDocumentation::Description description = R"(
Returns an array of all the resolution 0 [H3](#h3-index) indices.
    )";
    FunctionDocumentation::Syntax syntax = "h3GetRes0Indexes()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of all resolution 0 H3 indices.",
        {"Array(UInt64)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get all resolution 0 H3 indices",
            "SELECT h3GetRes0Indexes() AS indexes",
            R"(
┌─indexes────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [576495936675512319,576531121047601151,576566305419689983,576601489791778815,576636674163867647,576671858535956479,576707042908045311,576742227280134143,576777411652222975,576812596024311807,576847780396400639,576882964768489471,576918149140578303,576953333512667135,576988517884755967,577023702256844799,577058886628933631,577094071001022463,577129255373111295,577164439745200127,577199624117288959,577234808489377791,577269992861466623,577305177233555455,577340361605644287,577375545977733119,577410730349821951,577445914721910783,577481099093999615,577516283466088447,577551467838177279,577586652210266111,577621836582354943,577657020954443775,577692205326532607,577727389698621439,577762574070710271,577797758442799103,577832942814887935,577868127186976767,577903311559065599,577938495931154431,577973680303243263,578008864675332095,578044049047420927,578079233419509759,578114417791598591,578149602163687423,578184786535776255,578219970907865087,578255155279953919,578290339652042751,578325524024131583,578360708396220415,578395892768309247,578431077140398079,578466261512486911,578501445884575743,578536630256664575,578571814628753407,578606999000842239,578642183372931071,578677367745019903,578712552117108735,578747736489197567,578782920861286399,578818105233375231,578853289605464063,578888473977552895,578923658349641727,578958842721730559,578994027093819391,579029211465908223,579064395837997055,579099580210085887,579134764582174719,579169948954263551,579205133326352383,579240317698441215,579275502070530047,579310686442618879,579345870814707711,579381055186796543,579416239558885375,579451423930974207,579486608303063039,579521792675151871,579556977047240703,579592161419329535,579627345791418367,579662530163507199,579697714535596031,579732898907684863,579768083279773695,579803267651862527,579838452023951359,579873636396040191,579908820768129023,579944005140217855,579979189512306687,580014373884395519,580049558256484351,580084742628573183,580119927000662015,580155111372750847,580190295744839679,580225480116928511,580260664489017343,580295848861106175,580331033233195007,580366217605283839,580401401977372671,580436586349461503,580471770721550335,580506955093639167,580542139465727999,580577323837816831,580612508209905663,580647692581994495,580682876954083327,580718061326172159,580753245698260991] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3GetRes0Indexes>(documentation);
}

}

#endif
