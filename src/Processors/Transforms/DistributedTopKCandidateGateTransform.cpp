#include <Processors/Transforms/DistributedTopKCandidateGateTransform.h>

#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Port.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <unordered_set>

namespace ProfileEvents
{
extern const Event DistributedTopKFallbackRows;
}

namespace DB
{

namespace FailPoints
{
extern const char distributed_top_k_force_fallback[];
extern const char distributed_top_k_pause_before_candidate_submission[];
}

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

DistributedTopKCandidateGateTransform::DistributedTopKCandidateGateTransform(
    SharedHeader header_,
    UInt64 limit_,
    SortDescription sort_description_,
    QueryCoordinationCallback coordination_callback_)
    : IProcessor(InputPorts{header_}, OutputPorts{header_})
    , header(std::move(header_))
    , limit(limit_)
    , sort_description(std::move(sort_description_))
    , coordination_callback(std::move(coordination_callback_))
{
    if (!limit)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed TopK candidate gate requires a positive limit");
    if (sort_description.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed TopK candidate gate requires a sort description");
    if (!coordination_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Distributed TopK candidate gate requires a query coordination callback");

    std::unordered_set<String> key_names;
    for (const auto & column : sort_description)
    {
        if (!key_names.emplace(column.column_name).second)
            continue;

        const auto position = header->getPositionByName(column.column_name);
        sort_key_positions.push_back(position);
        candidate_columns.push_back(header->getByPosition(position).type->createColumn());
    }
}

IProcessor::Status DistributedTopKCandidateGateTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        current_chunk.clear();
        retained_chunks.clear();
        output_chunks.clear();
        candidate_columns.clear();
        return Status::Finished;
    }

    if (!output_chunks.empty())
    {
        if (!output.canPush())
            return Status::PortFull;

        output.push(std::move(output_chunks.front()));
        output_chunks.pop_front();
        return Status::PortFull;
    }

    if (coordination_complete)
    {
        output.finish();
        return Status::Finished;
    }

    if (current_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        if (!coordination_complete)
            return Status::Ready;

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull();
    return Status::Ready;
}

void DistributedTopKCandidateGateTransform::work()
{
    if (current_chunk)
    {
        retainChunk(std::move(current_chunk));
        current_chunk.clear();
        return;
    }

    coordinate();
}

void DistributedTopKCandidateGateTransform::validateRowCount(UInt64 rows) const
{
    if (retained_rows > limit || rows > limit - retained_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Distributed TopK candidate gate received more than {} rows from its marked limit", limit);
}

void DistributedTopKCandidateGateTransform::retainChunk(Chunk chunk)
{
    const UInt64 rows = chunk.getNumRows();
    validateRowCount(rows);
    if (!rows)
        return;

    const auto & columns = chunk.getColumns();
    for (size_t index = 0; index < sort_key_positions.size(); ++index)
    {
        const auto source = columns[sort_key_positions[index]]->convertToFullColumnIfConst();
        candidate_columns[index]->insertRangeFrom(*source, 0, rows);
    }

    retained_rows += rows;
    retained_chunks.push_back(std::move(chunk));
}

void DistributedTopKCandidateGateTransform::coordinate()
{
    Block candidates;
    for (size_t index = 0; index < sort_key_positions.size(); ++index)
    {
        const auto & source = header->getByPosition(sort_key_positions[index]);
        candidates.insert(ColumnWithTypeAndName(std::move(candidate_columns[index]), source.type, source.name));
    }

    FailPointInjection::pauseFailPoint(FailPoints::distributed_top_k_pause_before_candidate_submission);

    bool force_fallback = false;
    fiu_do_on(FailPoints::distributed_top_k_force_fallback, { force_fallback = true; });

    auto response = coordination_callback(
        QueryCoordinationRequest{
            .kind = QueryCoordinationRequestKind::DistributedTopKCandidates,
            .mode = force_fallback ? QueryCoordinationRequestMode::FallbackAll : QueryCoordinationRequestMode::Candidates,
            .payload = force_fallback ? Block{} : std::move(candidates),
        });

    if (force_fallback && response.mode != QueryCoordinationResponseMode::FallbackAll)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Expected FallbackAll response to a fallback request");
    if (static_cast<UInt64>(response.mode) > static_cast<UInt64>(QueryCoordinationResponseMode::MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown query coordination response mode");

    switch (response.mode)
    {
        case QueryCoordinationResponseMode::Selected: selectRows(response.selected_ordinals); break;
        case QueryCoordinationResponseMode::FallbackAll:
            if (!response.selected_ordinals.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "FallbackAll query coordination response contains selected ordinals");
            ProfileEvents::increment(ProfileEvents::DistributedTopKFallbackRows, retained_rows);
            output_chunks = std::move(retained_chunks);
            break;
    }

    coordination_complete = true;
}

void DistributedTopKCandidateGateTransform::selectRows(const std::vector<UInt64> & selected_ordinals)
{
    IColumn::Filter selected(retained_rows, 0);
    for (const UInt64 ordinal : selected_ordinals)
    {
        if (ordinal >= retained_rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Distributed TopK candidate ordinal {} is outside the candidate range [0, {})",
                ordinal,
                retained_rows);
        if (selected[ordinal])
            throw Exception(ErrorCodes::INCORRECT_DATA, "Distributed TopK candidate ordinal {} is duplicated", ordinal);
        selected[ordinal] = 1;
    }

    UInt64 chunk_begin = 0;
    while (!retained_chunks.empty())
    {
        auto chunk = std::move(retained_chunks.front());
        retained_chunks.pop_front();

        const UInt64 chunk_rows = chunk.getNumRows();
        const auto selected_begin = selected.begin() + chunk_begin;
        const size_t selected_rows = static_cast<size_t>(std::count(selected_begin, selected_begin + chunk_rows, UInt8{1}));
        chunk_begin += chunk_rows;

        if (!selected_rows)
            continue;

        if (selected_rows != chunk_rows)
        {
            IColumn::Filter chunk_filter(selected_begin, selected_begin + chunk_rows);
            auto columns = chunk.detachColumns();
            for (auto & column : columns)
                column = column->filter(chunk_filter, selected_rows);
            chunk.setColumns(std::move(columns), selected_rows);
        }

        output_chunks.push_back(std::move(chunk));
    }
}

}
