#pragma once

#include <Core/Block_fwd.h>
#include <Core/Range.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SelectQueryInfo.h>

#include <cstddef>
#include <optional>

namespace DB
{

class KeyCondition;
class Pipe;

struct Settings;

namespace NumbersLikeUtils
{

struct ExtractedRanges
{
    enum class Kind
    {
        ExactRanges,
        ConservativeRanges
    };

    Kind kind = Kind::ConservativeRanges;
    Ranges ranges;
};

/// Return true if extracted ranges represent an unsatisfiable condition.
inline bool isAlwaysFalse(const Ranges & ranges) { return ranges.empty(); }

/// Return true if extracted ranges carry no restrictions (i.e. the whole universe).
inline bool isUniverse(const Ranges & ranges) { return ranges.size() == 1 && ranges.front().isInfinite(); }

/// Try to extract exact ranges from the condition, otherwise fall back to conservative ranges extraction.
ExtractedRanges extractRanges(const KeyCondition & condition);

/// Apply query LIMIT to an effective (storage) limit, if possible.
void applyQueryLimit(std::optional<UInt64> & effective_limit, const std::optional<size_t> & query_limit);

void addNullSource(Pipe & pipe, SharedHeader header);

/// TODO: This is ideologically wrong. We should only get it from the query plan optimization.
std::optional<size_t> getLimitFromQueryInfo(const SelectQueryInfo & query_info, const ContextPtr & context);

/// Fail fast if estimated number of rows to read exceeds the limit.
void checkLimits(const Settings & settings, size_t rows);

}

}
