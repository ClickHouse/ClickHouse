#pragma once
#include <Storages/MergeTree/MergeTreeWriterStream.h>

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

}
