#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeEnum.h>

namespace DB
{

std::shared_ptr<DataTypeEnum8> getSettingsTierEnum()
{
    return std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"Production",      static_cast<Int8>(SettingsTierType::PRODUCTION)},
            {"Obsolete",        static_cast<Int8>(SettingsTierType::OBSOLETE)},
            {"Experimental",    static_cast<Int8>(SettingsTierType::EXPERIMENTAL)},
            {"Beta",            static_cast<Int8>(SettingsTierType::BETA)}
        });
}

}
