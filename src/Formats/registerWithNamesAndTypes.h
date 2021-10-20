#pragma once

#include <Formats/FormatFactory.h>

namespace DB
{

using GetInputCreatorWithNamesAndTypesFunc = std::function<FormatFactory::InputCreator(bool with_names, bool with_types)>;
void registerInputFormatWithNamesAndTypes(
    FormatFactory & factory, const String & base_format_name, GetInputCreatorWithNamesAndTypesFunc get_input_creator);

using GetOutputCreatorWithNamesAndTypesFunc = std::function<FormatFactory::OutputCreator(bool with_names, bool with_types)>;
void registerOutputFormatWithNamesAndTypes(
    FormatFactory & factory,
    const String & base_format_name,
    GetOutputCreatorWithNamesAndTypesFunc get_output_creator,
    bool supports_parallel_formatting = false);

using GetFileSegmentationEngineWithNamesAndTypesFunc = std::function<FormatFactory::FileSegmentationEngine(size_t min_rows)>;
void registerFileSegmentationEngineForFormatWithNamesAndTypes(
    FormatFactory & factory, const String & base_format_name, GetFileSegmentationEngineWithNamesAndTypesFunc get_file_segmentation_engine);

}
