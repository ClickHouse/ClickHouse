#include <Processors/Executors/Runtime/Wyrd/Testing/ExclusiveScope.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Log.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>

#include <fmt/format.h>

namespace DB::Runtime::Wyrd
{

ExclusiveScope::Section::Section(ExclusiveScope & scope_, std::string section_name_)
    : scope(scope_)
    , section_name(std::move(section_name_))
{
    if (scope.inside.fetch_add(1) != 0)
        abort("{}::{} was entered while another thread was inside the scope", scope.name, section_name);
}

ExclusiveScope::Section::~Section()
{
    scope.inside.fetch_sub(1);
    log("{}::{}: leave", scope.name, section_name);
}

ExclusiveScope::ExclusiveScope(std::string name_)
    : name(std::move(name_))
{
    setName(inside, "{}", name);
}

ExclusiveScope::Section ExclusiveScope::enter(const std::string & section_name)
{
    log("{}::{}: enter", name, section_name);
    return Section(*this, section_name);
}

}
