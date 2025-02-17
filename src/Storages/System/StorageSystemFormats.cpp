#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatFactory.h>
#include <Storages/System/StorageSystemFormats.h>

namespace DB
{

ColumnsDescription StorageSystemFormats::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Format name."},
        {"is_input", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format is suitable for data input."},
        {"is_output", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format is suitable for data output."},
        {"supports_parallel_parsing", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format supports parallel parsing."},
        {"supports_parallel_formatting", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format supports parallel formatting."},
        {"is_tty_friendly", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format usually displays fine in the terminal. For other formats, CLI will ask before output."},
    };
}

void StorageSystemFormats::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & formats = FormatFactory::instance().getAllFormats();
    for (const auto & pair : formats)
    {
        const auto & [name, creators] = pair;
        String format_name = creators.name;
        bool has_input_format(creators.input_creator != nullptr || creators.random_access_input_creator != nullptr);
        bool has_output_format(creators.output_creator != nullptr);
        bool supports_parallel_parsing(creators.file_segmentation_engine_creator != nullptr || creators.random_access_input_creator != nullptr);

        res_columns[0]->insert(format_name);
        res_columns[1]->insert(has_input_format);
        res_columns[2]->insert(has_output_format);
        res_columns[3]->insert(supports_parallel_parsing);
        res_columns[4]->insert(creators.supports_parallel_formatting);
        res_columns[5]->insert(creators.is_tty_friendly);
    }
}

}
