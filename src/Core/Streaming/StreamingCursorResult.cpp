#include <Core/Streaming/StreamingCursorResult.h>
#include <Core/Streaming/CursorTree.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace DB
{

String serializeStreamingCursor(const StreamingCursorResult::PartitionCursors & cursors)
{
    WriteBufferFromOwnString out;
    writeVarUInt(cursors.size(), out);
    for (const auto & [partition_id, fields] : cursors)
    {
        writeStringBinary(partition_id, out);
        writeVarUInt(fields.size(), out);
        for (const auto & [field, value] : fields)
        {
            writeStringBinary(field, out);
            writeVarInt(value, out);
        }
    }
    return out.str();
}

StreamingCursorResult::PartitionCursors deserializeStreamingCursor(const String & data)
{
    StreamingCursorResult::PartitionCursors cursors;
    if (data.empty())
        return cursors;

    ReadBufferFromString in(data);
    size_t partitions_count = 0;
    readVarUInt(partitions_count, in);
    for (size_t i = 0; i < partitions_count; ++i)
    {
        String partition_id;
        readStringBinary(partition_id, in);

        size_t fields_count = 0;
        readVarUInt(fields_count, in);

        auto & fields = cursors[partition_id];
        for (size_t j = 0; j < fields_count; ++j)
        {
            String field;
            readStringBinary(field, in);
            Int64 value = 0;
            readVarInt(value, in);
            fields[field] = value;
        }
    }
    return cursors;
}

CursorTreeNodePtr streamingCursorToTree(const StreamingCursorResult::PartitionCursors & cursors)
{
    auto root = std::make_shared<CursorTreeNode>();
    for (const auto & [partition_id, fields] : cursors)
    {
        auto & subtree = root->getSubtreeOrCreate(partition_id);
        for (const auto & [field, value] : fields)
            subtree->setValue(field, value);
    }
    return root;
}

}
