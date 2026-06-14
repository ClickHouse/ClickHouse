#include <jieba.h>

#include <absl/container/flat_hash_set.h>

namespace Jieba
{

struct MPSegment
{
    static void cut(const DartsDict & dict, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges);
};

struct HMMSegment
{
    static void cut(const DartsDict & dict, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges);
};

struct MixSegment
{
    static void cut(const DartsDict & dict, const Runes & runes, size_t begin, size_t end, RuneRanges & ranges)
    {
        /// Perform initial segmentation using the dictionary (MPSegment)
        RuneRanges words;
        words.reserve(end - begin);
        MPSegment::cut(dict, runes, begin, end, words);

        /// Iterate over the preliminary word segments
        for (size_t i = 0; i < words.size();)
        {
            const auto & w = words[i];

            if (w.begin != w.end)
            {
                /// Multi-character word: directly add to the final ranges
                ranges.push_back(w);
                ++i;
                continue;
            }

            /// Handle consecutive single-character segments
            size_t j = i;
            while (j < words.size() && words[j].begin == words[j].end)
                ++j;

            /// Use HMM segmentation on the single-character sequence
            HMMSegment::cut(dict, runes, words[i].begin, words[j - 1].end + 1, ranges);

            /// Move to the next segment after the single-character block
            i = j;
        }
    }
};

struct QuerySegment
{
    static void cut(const DartsDict & dict, const Runes & runes_data, size_t begin, size_t end, RuneRanges & ranges)
    {
        RuneRanges mix_res;
        MixSegment::cut(dict, runes_data, begin, end, mix_res);
        const auto & runes = runes_data.getRunes();
        for (const auto & range : mix_res)
        {
            if (range.size() > 2)
            {
                for (size_t i = 0; i + 1 < range.size(); ++i)
                {
                    RuneRange r(range.begin + i, range.begin + i + 1);
                    std::span<const Rune> span(&runes[r.begin], r.size());
                    if (dict.find(span) != 0)
                        ranges.push_back(r);
                }
            }

            if (range.size() > 3)
            {
                for (size_t i = 0; i + 2 < range.size(); ++i)
                {
                    RuneRange r(range.begin + i, range.begin + i + 2);
                    std::span<const Rune> span(&runes[r.begin], r.size());
                    if (dict.find(span) != 0)
                        ranges.push_back(r);
                }
            }

            ranges.push_back(range);
        }
    }
};

struct FullSegment
{
    static void cut(const DartsDict & dict, const Runes & runes_data, size_t begin, size_t end, RuneRanges & ranges)
    {
        if (begin >= end || runes_data.empty())
            return;

        const auto & runes = runes_data.getRunes();
        std::span<const Rune> span(&runes[begin], end - begin);
        auto dag = dict.buildDAG(span);
        size_t max_word_end_pos = 0;
        for (size_t i = 0; i < dag.size(); i++)
        {
            for (const auto & kv : dag[i].nexts)
            {
                size_t len = kv.first - i;
                bool is_single_char_fallback = dag[i].nexts.size() == 1 && max_word_end_pos <= i;
                bool is_valid_multi_char_word = kv.second != 0 && len >= 2;
                if (is_single_char_fallback || is_valid_multi_char_word)
                    ranges.push_back({begin + i, begin + kv.first - 1});

                max_word_end_pos = std::max(max_word_end_pos, kv.first);
            }
        }
    }
};

namespace
{

std::vector<std::string_view> convertRangesToWords(std::string_view sentence, const Runes & runes, const RuneRanges & ranges)
{
    std::vector<std::string_view> words;
    words.reserve(ranges.size());
    for (const auto & [start, end] : ranges)
    {
        size_t byte_start = runes.infoAt(start).offset;
        size_t byte_end = runes.infoAt(end).offset + runes.infoAt(end).len;
        words.push_back(sentence.substr(byte_start, byte_end - byte_start));
    }
    return words;
}

/// The runes here are raw Unicode codepoints (see `decodeUTF8Rune` in `jieba_common.h`).
/// The list intentionally includes only "structural" separators that should never
/// appear inside a token: ASCII whitespace, ASCII control characters (NUL, etc., which
/// `FixedString` uses to pad short values), and a few common full-width Chinese
/// punctuation marks. ASCII punctuation (commas, semicolons, parentheses, ...) is
/// handled separately by the HMM segmenter, which drops it instead of emitting it
/// as a standalone token.
const absl::flat_hash_set<Rune> separators = {
    /// ASCII whitespace and the most common control characters that appear in real input.
    0x00, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20,
    /// Common full-width Chinese punctuation marks.
    0x3001 /* 、 */, 0x3002 /* 。 */, 0xFF0C /* ， */, 0xFF1F /* ？ */, 0xFF01 /* ！ */,
    0xFF1A /* ： */, 0xFF1B /* ； */};

class PreFilter
{
public:
    explicit PreFilter(const Runes & runes_)
        : runes(runes_)
    {
    }

    bool hasNext() const { return cursor < runes.size(); }

    /// Returns the next continuous non-separator range [begin, end)
    RuneRange next()
    {
        RuneRange range;
        /// Skip leading separators
        while (cursor < runes.size() && separators.contains(runes.runeAt(cursor)))
            ++cursor;

        if (cursor >= runes.size())
        {
            range.begin = range.end = cursor;
            return range;
        }

        range.begin = cursor;

        /// Accumulate until the next separator
        while (cursor < runes.size() && !separators.contains(runes.runeAt(cursor)))
            ++cursor;

        range.end = cursor;
        return range;
    }

private:
    const Runes & runes;
    size_t cursor = 0;
};

}

template <typename Segment>
std::vector<std::string_view> Jieba::cutImpl(std::string_view sentence)
{
    auto runes = decodeUTF8String(sentence);
    if (runes.empty())
        return {};

    PreFilter filter(runes);
    RuneRanges all_ranges;

    while (filter.hasNext())
    {
        RuneRange range = filter.next();
        if (range.begin >= range.end)
            continue;

        Segment::cut(dict, runes, range.begin, range.end, all_ranges);
    }

    return convertRangesToWords(sentence, runes, all_ranges);
}

std::vector<std::string_view> Jieba::cut(std::string_view sentence)
{
    return cutImpl<MixSegment>(sentence);
}

std::vector<std::string_view> Jieba::cutForSearch(std::string_view sentence)
{
    return cutImpl<QuerySegment>(sentence);
}

std::vector<std::string_view> Jieba::cutAll(std::string_view sentence)
{
    return cutImpl<FullSegment>(sentence);
}

}
