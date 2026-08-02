#include <Columns/IColumn.h>
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
        {"content_type", std::make_shared<DataTypeString>(), "HTTP Content-Type corresponding to the output format. May depend on the current format settings."},
        {"supports_random_access", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the format supports random access in the input."},
        {"has_schema_inference", std::make_shared<DataTypeUInt8>(), "The format can dynamically determine the schema from the data (either from embedded header/metadata or from the piece of data)."},
        {"has_external_schema", std::make_shared<DataTypeUInt8>(), "The format either has a fixed schema or accepts a predefined schema in its own format."},
        {"prefers_large_blocks", std::make_shared<DataTypeUInt8>(), "The format will write larger blocks into output and generate larger blocks on input."},
        {"supports_append", std::make_shared<DataTypeUInt8>(), "It's possible to append into a single file with this format."},
        {"supports_subsets_of_columns", std::make_shared<DataTypeUInt8>(), "The input format can recognize when certain columns are omitted."},
    };
}

void StorageSystemFormats::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & formats = FormatFactory::instance().getAllFormats();
    for (const auto & pair : formats)
    {
        FormatSettings settings = getFormatSettings(context);

        const auto & [name, creators] = pair;
        String format_name = creators.name;
        bool has_input_format(creators.input_creator != nullptr || creators.random_access_input_creator != nullptr);
        bool has_output_format(creators.output_creator != nullptr);
        bool supports_parallel_parsing(creators.file_segmentation_engine_creator != nullptr || creators.random_access_input_creator != nullptr);
        String content_type = has_output_format ? FormatFactory::instance().getContentType(format_name, settings) : "";
        bool supports_random_access = creators.random_access_input_creator != nullptr;
        bool has_schema_inference = creators.schema_reader_creator != nullptr;
        bool has_external_schema = creators.external_schema_reader_creator != nullptr;
        bool prefers_large_blocks = creators.prefers_large_blocks;
        bool supports_append = creators.append_support_checker && creators.append_support_checker(settings);
        bool supports_subset_of_columns = creators.subset_of_columns_support_checker && creators.subset_of_columns_support_checker(settings);

        res_columns[0]->insert(format_name);
        res_columns[1]->insert(has_input_format);
        res_columns[2]->insert(has_output_format);
        res_columns[3]->insert(supports_parallel_parsing);
        res_columns[4]->insert(creators.supports_parallel_formatting);
        res_columns[5]->insert(creators.is_tty_friendly);
        res_columns[6]->insert(content_type);
        res_columns[7]->insert(supports_random_access);
        res_columns[8]->insert(has_schema_inference);
        res_columns[9]->insert(has_external_schema);
        res_columns[10]->insert(prefers_large_blocks);
        res_columns[11]->insert(supports_append);
        res_columns[12]->insert(supports_subset_of_columns);
    }
}

}
