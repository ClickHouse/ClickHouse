#pragma once
#include <memory>

namespace DB
{
    class IMergeTreeDataPart;
    class IMergeTreeReader;
    class IMergeTreeWriter;

    using MergeTreeMutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
    using MergeTreeDataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    using MergeTreeWriterPtr = std::unique_ptr<IMergeTreeWriter>;
}
