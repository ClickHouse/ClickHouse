#include <Formats/registerWithNamesAndTypes.h>

namespace DB
{

void registerWithNamesAndTypes(const std::string & base_format_name, RegisterWithNamesAndTypesFunc register_func)
{
    register_func(base_format_name, false, false);
    register_func(base_format_name + "WithNames", true, false);
    register_func(base_format_name + "WithNamesAndTypes", true, true);
}

}
