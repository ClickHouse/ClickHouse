#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct HorizontalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::HORIZONTAL;
    static constexpr auto Name = "extractAllGroupsHorizontal";
};

}

namespace DB
{

void registerFunctionExtractAllGroupsHorizontal(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAllGroups<HorizontalImpl>>();
}

}
