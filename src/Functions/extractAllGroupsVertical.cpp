#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct VerticalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::VERTICAL;
    static constexpr auto Name = "extractAllGroupsVertical";
};

}

namespace DB
{

REGISTER_FUNCTION(ExtractAllGroupsVertical)
{
    factory.registerFunction<FunctionExtractAllGroups<VerticalImpl>>();
    factory.registerAlias("extractAllGroups", VerticalImpl::Name, FunctionFactory::CaseSensitive);
}

}
