#pragma once

#include <Core/Types.h>

#include <algorithm>
#include <cctype>
#include <queue>
#include <utility>

namespace DB
{
template <size_t MistakeFactor, size_t MaxNumHints>
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
        size_t n = lhs.size();
        size_t m = rhs.size();
        std::vector<std::vector<size_t>> dp(n + 1, std::vector<size_t>(m + 1));

        for (size_t i = 1; i <= n; ++i)
            dp[i][0] = i;

        for (size_t i = 1; i <= m; ++i)
            dp[0][i] = i;

        for (size_t j = 1; j <= m; ++j)
        {
            for (size_t i = 1; i <= n; ++i)
            {
                if (std::tolower(lhs[i - 1]) == std::tolower(rhs[j - 1]))
                    dp[i][j] = dp[i - 1][j - 1];
                else
                    dp[i][j] = std::min(dp[i - 1][j] + 1, std::min(dp[i][j - 1] + 1, dp[i - 1][j - 1] + 1));
            }
        }

        return dp[n][m];
    }

    static void appendToQueue(size_t ind, const String & name, DistanceIndexQueue & queue, const std::vector<String> & prompting_strings)
    {
        if (prompting_strings[ind].size() <= name.size() + MistakeFactor && prompting_strings[ind].size() + MistakeFactor >= name.size())
        {
            size_t distance = levenshteinDistance(prompting_strings[ind], name);
            if (distance <= MistakeFactor)
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

}
