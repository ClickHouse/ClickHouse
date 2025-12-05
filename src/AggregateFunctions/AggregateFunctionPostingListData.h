#include <Common/RadixSort.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Storages/MergeTree/PostingsContainer.h>

#pragma clang optimize off
namespace DB {
struct Settings;

namespace
{
struct PostingListDataSorted
{
    using PostingsContainerType = PostingsContainer64;
    using PostingListDataType = PostingsContainerType::ValueType;

    PostingsContainer64 container;

    static constexpr auto name() { return "postingList"; }
    ALWAYS_INLINE void add(PostingListDataType && element, Arena *)
    {
        container.add(element);
    }

    ALWAYS_INLINE void merge(const PostingListDataSorted & rhs, Arena *)
    {
        PostingsContainer64 out_container;
        mergePostingsContainers(out_container, container, rhs.container);
        container.swap(out_container);
    }
    ALWAYS_INLINE void insertResultInto(IColumn & to)
    {
        auto & result_array = assert_cast<ColumnArray &>(to);
        auto & result_array_offsets = result_array.getOffsets();
        result_array_offsets.push_back(result_array_offsets.back() + container.size());

        if (container.empty())
            return;

        auto & result_array_data = assert_cast<ColumnVector<PostingListDataType> &>(result_array.getData()).getData();

        size_t result_array_data_insert_begin = result_array_data.size();
        result_array_data.resize(result_array_data_insert_begin + container.size());

        size_t idx = 0;
        for (auto it = container.begin(); it != container.end(); ++it, idx++)
        {
            result_array_data[result_array_data_insert_begin + idx] = *it;
        }
    }

    void serialize(WriteBuffer & buf, std::optional<size_t> /* version */) const
    {
        container.serialize(buf);
    }

    void deserialize(ReadBuffer & buf, std::optional<size_t> /* version */, Arena *)
    {
        container.deserialize(buf);
    }
};

using PostingListData = PostingListDataSorted;

}
}
