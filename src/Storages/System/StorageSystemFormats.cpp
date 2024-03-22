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
        {"supports_parallel_parsing", std::make_shared<DataTypeUInt8>()},
        {"supports_parallel_formatting", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemFormats::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    const auto & formats = FormatFactory::instance().getAllFormats();
    for (const auto & pair : formats)
    {
        const auto & [format_name, creators] = pair;
        UInt64 has_input_format(creators.input_creator != nullptr || creators.random_access_input_creator != nullptr);
        UInt64 has_output_format(creators.output_creator != nullptr);
        UInt64 supports_parallel_parsing(creators.file_segmentation_engine != nullptr || creators.random_access_input_creator != nullptr);
        UInt64 supports_parallel_formatting(creators.supports_parallel_formatting);

        res_columns[0]->insert(format_name);
        res_columns[1]->insert(has_input_format);
        res_columns[2]->insert(has_output_format);
        res_columns[3]->insert(supports_parallel_parsing);
        res_columns[4]->insert(supports_parallel_formatting);
    }
}

}
