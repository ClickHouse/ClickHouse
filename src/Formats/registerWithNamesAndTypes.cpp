#include <Formats/registerWithNamesAndTypes.h>

namespace DB
{

void registerInputFormatWithNamesAndTypes(FormatFactory & factory, const String & base_format_name, GetInputCreatorWithNamesAndTypesFunc get_input_creator)
{
    factory.registerInputFormat(base_format_name, get_input_creator(false, false));
    factory.registerInputFormat(base_format_name + "WithNames", get_input_creator(true, false));
    factory.registerInputFormat(base_format_name + "WithNamesAndTypes", get_input_creator(true, true));
}

void registerOutputFormatWithNamesAndTypes(
    FormatFactory & factory,
    const String & base_format_name,
    GetOutputCreatorWithNamesAndTypesFunc get_output_creator,
    bool supports_parallel_formatting)
{
    factory.registerOutputFormat(base_format_name, get_output_creator(false, false));
    factory.registerOutputFormat(base_format_name + "WithNames", get_output_creator(true, false));
    factory.registerOutputFormat(base_format_name + "WithNamesAndTypes", get_output_creator(true, true));

    if (supports_parallel_formatting)
    {
        factory.markOutputFormatSupportsParallelFormatting(base_format_name);
        factory.markOutputFormatSupportsParallelFormatting(base_format_name + "WithNames");
        factory.markOutputFormatSupportsParallelFormatting(base_format_name + "WithNamesAndTypes");
    }
}

void registerFileSegmentationEngineForFormatWithNamesAndTypes(
    FormatFactory & factory, const String & base_format_name, GetFileSegmentationEngineWithNamesAndTypesFunc get_file_segmentation_engine)
{
    factory.registerFileSegmentationEngine(base_format_name, get_file_segmentation_engine(1));
    factory.registerFileSegmentationEngine(base_format_name + "WithNames", get_file_segmentation_engine(2));
    factory.registerFileSegmentationEngine(base_format_name + "WithNamesAndTypes", get_file_segmentation_engine(3));
}

}
