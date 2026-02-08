#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

namespace DB
{

class AggregateFunctionPostingList final : public IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>
{
    using Data = PostingListData;

public:
    explicit AggregateFunctionPostingList(const MergeTreeIndexTextParams & index_params_, size_t version_)
        : IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>({}, {}, createResultType())
        , index_params(index_params_)
        , version(version_)
    {
    }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()); }

    String getName() const override { return "postingList"; }

    void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override;

    /// TODO(amos): Currently `rhs` is const because its state is allocated in an Arena and will be
    /// deallocated along with the Arena. This prevents moving its contents directly into `place`.
    /// Investigate whether we can support move-merge by either:
    ///   1) avoiding Arena allocation and managing memory with unique_ptr, or
    ///   2) designing a separate move-safe memory pool for aggregate states.
    ///
    /// Current workaround: using const_cast on std::unique_ptr to move. This works but may be unsafe; needs review to
    /// ensure correctness and avoid undefined behavior.
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * /* arena */) const override;

    void serialize(ConstAggregateDataPtr, WriteBuffer &, std::optional<size_t>) const override;

    void deserialize(AggregateDataPtr, ReadBuffer &, std::optional<size_t>, Arena *) const override;

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /* arena */) const override;

    bool allocatesMemoryInArena() const override { return false; }

    /// Returns the data type without non-persistent parameters to resolve type mismatches caused by DataTypePostingList
    /// having runtime arguments not present in stored data.
    // DataTypePtr getNormalizedStateType() const override;

    MergeTreeIndexTextParams index_params;

    size_t version;
};

struct DataTypePostingList : public IDataTypeCustomName
{
    DataTypePostingList(const Array & parameters_, size_t version_)
        : parameters(parameters_)
        , version(version_)
    {
    }

    String getName() const override;

    const Array parameters;
    size_t version;
};

static constexpr auto POSTING_LIST_FORMAT_VERSION_INITIAL = 0;

/// Construct PostingList DataType from index definition using the specified posting list format version. This is used
/// in metadata construction paths (projection definition, merge, and output parts) to control the on-disk format.
DataTypePtr createPostingListType(const ASTPtr & text_index_definition, size_t format_version);

/// Reconstruct PostingList DataType from on-disk part metadata. The format version is inferred from stored fields and
/// all historical posting list format versions are supported.
DataTypePtr createPostingListTypeFromPartMetadata(const ASTPtr & parsed_fields);

}
