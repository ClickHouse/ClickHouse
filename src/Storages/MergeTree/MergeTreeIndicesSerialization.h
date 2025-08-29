#pragma once
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

struct IndexSubstream
{
    enum class Type
    {
        Regular,
        TextIndexDictionary,
        TextIndexPostings,
    };

    Type type;
    String suffix;
    String extension;
};

using IndexSubstreams = std::vector<IndexSubstream>;

using IndexWriterStream = MergeTreeWriterStream<false>;
using IndexOutputStreams = std::map<IndexSubstream::Type, IndexWriterStream *>;

using IndexReaderStream = MergeTreeReaderStream;
using IndexInputStreams = std::map<IndexSubstream::Type, IndexReaderStream *>;

}
