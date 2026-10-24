#pragma once

#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/levenshteinDistance.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <queue>
#include <utility>

namespace DB
{

/// Throws if the current query has been cancelled. Out of line so that this header does not pull in
/// CurrentThread and ProcessList.
void checkPromptingNotCancelled();

template <size_t MaxNumHints>
class NamePrompter
{
public:
    using DistanceIndex = std::pair<size_t, size_t>;
    using DistanceIndexQueue = std::priority_queue<DistanceIndex>;

    /// Scoring one candidate costs the product of the two name lengths, and both the number of
    /// candidates and the length of a name are query-controlled, so the limit is cumulative over the
    /// whole call. Exhausting it yields no hint at all: the nearest candidate is only known once every
    /// candidate has been scored, so a partial scan cannot answer the question this function asks.
    static constexpr size_t max_prompting_work = 50'000'000;

    static VectorWithMemoryTracking<String> getHints(const String & name, const VectorWithMemoryTracking<String> & prompting_strings)
    {
        if (name.empty())
            return {};

        checkPromptingNotCancelled();

        size_t work_left = max_prompting_work;
        DistanceIndexQueue queue;
        for (size_t i = 0; i < prompting_strings.size(); ++i)
        {
            if (!appendToQueue(i, name, queue, prompting_strings, work_left))
            {
                checkPromptingNotCancelled();
                return {};
            }
        }
        /// Cancellation arriving during the scoring above has to be observed on both exit paths: a
        /// query-analysis caller runs this while building an unresolved-name exception, which would
        /// otherwise be reported in place of the cancellation.
        checkPromptingNotCancelled();
        return release(queue, prompting_strings);
    }

private:
    static bool appendToQueue(size_t ind, const String & name, DistanceIndexQueue & queue, const VectorWithMemoryTracking<String> & prompting_strings, size_t & work_left)
    {
        const String & prompt = prompting_strings[ind];

        /// Clang SimpleTypoCorrector logic
        const size_t min_possible_edit_distance = std::abs(static_cast<int64_t>(name.size()) - static_cast<int64_t>(prompt.size()));
        const size_t mistake_factor = (name.size() + 2) / 3;
        if (min_possible_edit_distance > 0 && name.size() / min_possible_edit_distance < 3)
            return true;

        if (prompt.size() <= name.size() + mistake_factor && prompt.size() + mistake_factor >= name.size())
        {
            /// Compare by division so the product never has to be formed before it is known to fit.
            /// The divisor is non-zero: an empty name yields no hints and never reaches here.
            if (prompt.size() > work_left / name.size())
                return false;
            work_left -= prompt.size() * name.size();

            size_t distance = levenshteinDistanceCaseInsensitive(prompt, name);
            if (distance <= mistake_factor)
            {
                queue.emplace(distance, ind);
                if (queue.size() > MaxNumHints)
                    queue.pop();
            }
        }
        return true;
    }

    static VectorWithMemoryTracking<String> release(DistanceIndexQueue & queue, const VectorWithMemoryTracking<String> & prompting_strings)
    {
        VectorWithMemoryTracking<String> answer;
        answer.reserve(queue.size());
        while (!queue.empty())
        {
            auto top = queue.top();
            queue.pop();
            answer.push_back(prompting_strings[top.second]);
        }
        std::reverse(answer.begin(), answer.end());
        return answer;
    }
};

String getHintsErrorMessageSuffix(const VectorWithMemoryTracking<String> & hints);

void appendHintsMessage(String & error_message, const VectorWithMemoryTracking<String> & hints);

template <size_t MaxNumHints = 1>
class IHints
{
public:
    virtual VectorWithMemoryTracking<String> getAllRegisteredNames() const = 0;

    VectorWithMemoryTracking<String> getHints(const String & name) const
    {
        return prompter.getHints(name, getAllRegisteredNames());
    }

    VectorWithMemoryTracking<String> getHints(const String & name, const VectorWithMemoryTracking<String> & prompting_strings) const
    {
        return prompter.getHints(name, prompting_strings);
    }

    void appendHintsMessage(String & error_message, const String & name) const
    {
        auto hints = getHints(name);
        DB::appendHintsMessage(error_message, hints);
    }

    String getHintsMessage(const String & name) const
    {
        return getHintsErrorMessageSuffix(getHints(name));
    }

    IHints() = default;

    IHints(const IHints &) = default;
    IHints(IHints &&) noexcept = default;
    IHints & operator=(const IHints &) = default;
    IHints & operator=(IHints &&) noexcept = default;

    virtual ~IHints() = default;

private:
    NamePrompter<MaxNumHints> prompter;
};
}
