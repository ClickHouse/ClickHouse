#include <DataTypes/Serializations/getSubcolumnsDeserializationOrder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::vector<size_t> getSubcolumnsDeserializationOrder(
    const String & column_name,
    const std::vector<ISerialization::SubstreamData> & subcolumns_data,
    const std::vector<String> & substreams_in_serialization_order,
    ISerialization::EnumerateStreamsSettings & enumerate_settings,
    const ISerialization::StreamFileNameSettings & stream_file_name_settings)
{
    /// Create map (substream) -> (pos in serialization order).
    std::unordered_map<std::string_view, size_t> substream_to_pos;
    substream_to_pos.reserve(substreams_in_serialization_order.size());
    for (size_t i = 0; i != substreams_in_serialization_order.size(); ++i)
        substream_to_pos[substreams_in_serialization_order[i]] = i;

    /// For each subcolumn use enumerateStreams to find all streams required for its deserialization
    /// and collect all their positions.
    std::vector<std::vector<size_t>> subcolumns_substreams_positions;
    subcolumns_substreams_positions.reserve(subcolumns_data.size());
    for (const auto & data : subcolumns_data)
    {
        auto & substreams_positions = subcolumns_substreams_positions.emplace_back();
        auto callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            String substream_name = ISerialization::getFileNameForStream(column_name, substream_path, stream_file_name_settings);
            auto it = substream_to_pos.find(substream_name);
            if (it == substream_to_pos.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected substream {} for column {}", substream_name, column_name);
            substreams_positions.push_back(it->second);
        };

        data.serialization->enumerateStreams(enumerate_settings, callback, data);
        /// Sort substream positions, because enumerateStreams may enumerate them not in serialization order.
        std::sort(substreams_positions.begin(), substreams_positions.end());
    }

    /// Now for each subcolumn we have a list of required substreams positions.
    /// To determine the order of deserialization we can just sort these lists in lexicographical order,
    /// so we first deserialize subcolumn with smallest substreams positions.
    std::vector<size_t> subcolumns_positions(subcolumns_data.size());
    for (size_t i = 0; i != subcolumns_positions.size(); ++i)
        subcolumns_positions[i] = i;
    std::sort(subcolumns_positions.begin(), subcolumns_positions.end(), [&](size_t left, size_t right){ return subcolumns_substreams_positions[left] < subcolumns_substreams_positions[right]; });
    return subcolumns_positions;
}

}

