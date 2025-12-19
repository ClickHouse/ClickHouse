#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

namespace DB
{

class AggregateFunctionPostingList final : public IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>
{
    using Data = PostingListData;

public:
    explicit AggregateFunctionPostingList(const Array & params, const MergeTreeIndexTextParams & index_params_)
        : IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>({}, params, createResultType())
        , index_params(index_params_)
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
    DataTypePtr getNormalizedStateType() const override;

    /// Build index parameters from data type arguments. This path is only valid when the type is created via
    /// getPostingListType() and is used exclusively for metadata, not for the columns.txt persisted in data parts.
    ///
    /// This is safe because these parameters are only needed when creating new data parts (during INSERT or MERGE),
    /// where metadata is used as the source of truth.
    MergeTreeIndexTextParams index_params;
};

DataTypePtr getPostingListType(const ASTPtr & arguments);

}
