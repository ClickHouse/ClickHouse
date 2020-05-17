#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>

namespace DB
{

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergeTreeDataPartWriter(data_part_, columns_list_,
           indices_to_recalc_, marks_file_extension_,
           default_codec_, settings_, index_granularity_)
{
}

}
