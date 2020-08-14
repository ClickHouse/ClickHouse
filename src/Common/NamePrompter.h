#pragma once

#include <Core/Types.h>
#include <Common/PODArray.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <memory>
#include <queue>
#include <utility>

namespace DB
{
template <size_t MaxNumHints>
class NamePrompter
{
public:
    using DistanceIndex = std::pair<size_t, size_t>;
    using DistanceIndexQueue = std::priority_queue<DistanceIndex>;

    static std::vector<String> getHints(const String & name, const std::vector<String> & prompting_strings)
    {
        DistanceIndexQueue queue;
        for (size_t i = 0; i < prompting_strings.size(); ++i)
            appendToQueue(i, name, queue, prompting_strings);
        return release(queue, prompting_strings);
    }

private:
    static size_t levenshteinDistance(const String & lhs, const String & rhs)
    {
        size_t m = lhs.size();
        size_t n = rhs.size();

        PODArrayWithStackMemory<size_t, 64> row(n + 1);

        for (size_t i = 1; i <= n; ++i)
            row[i] = i;

        for (size_t j = 1; j <= m; ++j)
        {
            row[0] = j;
            size_t prev = j - 1;
            for (size_t i = 1; i <= n; ++i)
            {
                size_t old = row[i];
                row[i] = std::min(prev + (std::tolower(lhs[j - 1]) != std::tolower(rhs[i - 1])),
                    std::min(row[i - 1], row[i]) + 1);
                prev = old;
            }
        }
        return row[n];
    }

    static void appendToQueue(size_t ind, const String & name, DistanceIndexQueue & queue, const std::vector<String> & prompting_strings)
    {
        const String & prompt = prompting_strings[ind];

        /// Clang SimpleTypoCorrector logic
        const size_t min_possible_edit_distance = std::abs(static_cast<int64_t>(name.size()) - static_cast<int64_t>(prompt.size()));
        const size_t mistake_factor = (name.size() + 2) / 3;
        if (min_possible_edit_distance > 0 && name.size() / min_possible_edit_distance < 3)
            return;

        if (prompt.size() <= name.size() + mistake_factor && prompt.size() + mistake_factor >= name.size())
        {
            size_t distance = levenshteinDistance(prompt, name);
            if (distance <= mistake_factor)
            {
                queue.emplace(distance, ind);
                if (queue.size() > MaxNumHints)
                    queue.pop();
            }
        }
    }

    static std::vector<String> release(DistanceIndexQueue & queue, const std::vector<String> & prompting_strings)
    {
        std::vector<String> ans;
        ans.reserve(queue.size());
        while (!queue.empty())
        {
            auto top = queue.top();
            queue.pop();
            ans.push_back(prompting_strings[top.second]);
        }
        std::reverse(ans.begin(), ans.end());
        return ans;
    }
};

template <size_t MaxNumHints, class Self>
class IHints
{
public:

    virtual std::vector<String> getAllRegisteredNames() const = 0;

    std::vector<String> getHints(const String & name) const
    {
        static const auto registered_names = getAllRegisteredNames();
        return prompter.getHints(name, registered_names);
    }

    virtual ~IHints() = default;

private:
    NamePrompter<MaxNumHints> prompter;
};

}
