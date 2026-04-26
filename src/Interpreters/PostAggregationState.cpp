#include <Interpreters/PostAggregationState.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

/// ===== PostAggregationState: TOTALS constructor =====

PostAggregationState::PostAggregationState(
    size_t aggregate_index_,
    const IAggregateFunction * aggregate_function_,
    size_t state_offset_in_group_)
    : aggregate_index(aggregate_index_)
    , aggregate_function(aggregate_function_)
    , state_offset_in_group(state_offset_in_group_)
    , kind(Kind::Totals)
    , arena(std::make_unique<Arena>())
{
    /// One global state for TOTALS.
    global_state = arena->alignedAlloc(
        aggregate_function->sizeOfData(),
        aggregate_function->alignOfData());
    aggregate_function->create(global_state);
}

/// ===== PostAggregationState: BY constructor =====

PostAggregationState::PostAggregationState(
    size_t aggregate_index_,
    const IAggregateFunction * aggregate_function_,
    size_t state_offset_in_group_,
    std::vector<size_t> by_column_positions_)
    : aggregate_index(aggregate_index_)
    , aggregate_function(aggregate_function_)
    , state_offset_in_group(state_offset_in_group_)
    , kind(Kind::By)
    , arena(std::make_unique<Arena>())
    , by_column_positions(std::move(by_column_positions_))
{
    /// States for BY are created lazily in findOrCreateByState() as keys appear.
}

/// ===== Destructor =====

PostAggregationState::~PostAggregationState()
{
    if (kind == Kind::Totals)
    {
        if (global_state)
        {
            aggregate_function->destroy(global_state);
            global_state = nullptr;
        }
    }
    else if (kind == Kind::By)
    {
        for (auto & cell : by_states)
            if (cell.getMapped())
                aggregate_function->destroy(cell.getMapped());
    }
}

/// ===== Helpers =====

std::string_view PostAggregationState::serializeByKey(
    const IColumn ** key_columns, size_t row_num, Arena * keys_arena)
{
    IColumn::SerializationSettings settings{};

    const char * begin = nullptr;
    size_t total_size = 0;
    for (size_t pos : by_column_positions)
    {
        auto part = key_columns[pos]->serializeValueIntoArena(row_num, *keys_arena, begin, &settings);
        total_size += part.size();
    }
    return std::string_view{begin, total_size};
}

AggregateDataPtr PostAggregationState::findOrCreateByState(std::string_view key)
{
    ByStates::LookupResult it;
    bool inserted;
    by_states.emplace(key, it, inserted);

    if (inserted)
    {
        AggregateDataPtr place = arena->alignedAlloc(
            aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(place);
        it->getMapped() = place;
    }

    return it->getMapped();
}

/// ===== absorb / finalizeInto =====

void PostAggregationState::absorb(
    AggregateDataPtr per_group_place,
    const IColumn ** key_columns,
    size_t row_num,
    Arena * keys_arena)
{
    AggregateDataPtr our_state_in_group = per_group_place + state_offset_in_group;

    if (kind == Kind::Totals)
    {
        aggregate_function->merge(global_state, our_state_in_group, arena.get());
    }
    else if (kind == Kind::By)
    {
        if (!key_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "BY post-state: key_columns must be provided during absorb");

        auto key = serializeByKey(key_columns, row_num, keys_arena);
        AggregateDataPtr by_state = findOrCreateByState(key);
        aggregate_function->merge(by_state, our_state_in_group, arena.get());
    }
}

void PostAggregationState::finalizeInto(
    IColumn & target_column,
    const IColumn ** key_columns,
    size_t row_num,
    Arena * keys_arena)
{
    if (kind == Kind::Totals)
    {
        aggregate_function->insertResultInto(global_state, target_column, arena.get());
    }
    else if (kind == Kind::By)
    {
        if (!key_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "BY post-state: key_columns must be provided during finalize");

        auto key = serializeByKey(key_columns, row_num, keys_arena);

        auto *it = by_states.find(key);
        if (!it)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "BY post-state: key not found during finalize (absorb was incomplete)");

        aggregate_function->insertResultInto(it->getMapped(), target_column, arena.get());
    }
}

/// ===== PostAggregationManager =====

void PostAggregationManager::init(
    const AggregateDescriptions & aggregates,
    const std::vector<const IAggregateFunction *> & aggregate_functions,
    const Sizes & offsets_of_aggregate_states,
    const Names & group_by_key_names)
{
    states.clear();
    computed = false;

    for (size_t i = 0; i < aggregates.size(); ++i)
    {
        const auto & descr = aggregates[i];

        if (descr.totals_combinator)
        {
            states.push_back(std::make_unique<PostAggregationState>(
                i,
                aggregate_functions[i],
                offsets_of_aggregate_states[i]));
        }
        else if (descr.by_columns.has_value())
        {
            /// Resolve BY-column names to positions in group_by_key_names.
            std::vector<size_t> positions;
            positions.reserve(descr.by_columns->size());

            for (const auto & by_name : *descr.by_columns)
            {
                auto it = std::find(group_by_key_names.begin(), group_by_key_names.end(), by_name);
                if (it == group_by_key_names.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "BY column '{}' in aggregate '{}' is not a GROUP BY key",
                        by_name, descr.column_name);
                positions.push_back(static_cast<size_t>(it - group_by_key_names.begin()));
            }

            states.push_back(std::make_unique<PostAggregationState>(
                i,
                aggregate_functions[i],
                offsets_of_aggregate_states[i],
                std::move(positions)));
        }
    }
}

PostAggregationState * PostAggregationManager::find(size_t aggregate_index) const
{
    for (const auto & state : states)
        if (state->getAggregateIndex() == aggregate_index)
            return state.get();
    return nullptr;
}

bool PostAggregationManager::hasByStates() const
{
    for (const auto & state : states)
        if (state->getKind() == PostAggregationState::Kind::By)
            return true;
    return false;
}

}
