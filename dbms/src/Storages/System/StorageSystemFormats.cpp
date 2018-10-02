#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Storages/System/StorageSystemFormats.h>

namespace DB
{

NamesAndTypesList StorageSystemFormats::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"is_input", std::make_shared<DataTypeUInt8>()},
        {"is_output", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemFormats::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    const auto & formats = FormatFactory::instance().getAllFormats();
    for (const auto & pair : formats)
    {
        const auto & [name, creator_pair] = pair;
        UInt64 has_input_format(creator_pair.first != nullptr);
        UInt64 has_output_format(creator_pair.second != nullptr);
        res_columns[0]->insert(name);
        res_columns[1]->insert(has_input_format);
        res_columns[2]->insert(has_output_format);
    }
}

}
