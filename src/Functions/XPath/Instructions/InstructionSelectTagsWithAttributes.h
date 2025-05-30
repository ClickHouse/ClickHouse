#pragma once

#include <map>
#include <unordered_map>

#include <Functions/XPath/Matchers.h>
#include <Functions/XPath/Types.h>
#include <Functions/XPath/parseAttributes.h>

namespace DB
{

class InstructionSelectTagsWithAttributes : public IInstruction
{
    struct MatchingProgress
    {
        size_t start_depth = 0;
        size_t next_matcher_index = 0;
        bool is_matched = false;

        bool needTryMatch(size_t current_depth) const { return !is_matched && (start_depth + next_matcher_index == current_depth); }

        bool matchedOnDepth(size_t current_depth) const { return is_matched || (start_depth + next_matcher_index == current_depth + 1); }
    };

    static constexpr auto name = "InstructionSelectTagsWithAttributes";

    std::vector<TagMatcher> tag_matchers;

    void handleOpeningTag(
        const TagPreview & tag,
        const char * tag_begin,
        const char * after_tag_name,
        const char * tag_end,
        size_t current_depth,
        std::map<size_t, MatchingProgress> & matching_progress_by_depth,
        std::unordered_map<size_t, const char *> & matched_doc_begin_by_depth) const
    {
        bool found_match_result_on_current_depth = false;

        std::vector<Attribute> attributes;
        parseAttributes(after_tag_name, tag_end, attributes);

        for (auto & [depth, match_result] : matching_progress_by_depth)
        {
            if (depth > current_depth)
            {
                break;
            }

            found_match_result_on_current_depth |= (depth == current_depth);

            if (match_result.needTryMatch(current_depth))
            {
                if (!tag_matchers[match_result.next_matcher_index].matchTag(
                        tag.name, current_depth - depth, match_result.next_matcher_index))
                {
                    continue;
                }

                if (!tag_matchers[match_result.next_matcher_index].matchAttributes(attributes))
                {
                    continue;
                }

                ++match_result.next_matcher_index;

                // Successful matching
                if (match_result.next_matcher_index == tag_matchers.size())
                {
                    matched_doc_begin_by_depth[current_depth] = tag_begin;
                    match_result.is_matched = true;

                    continue;
                }
            }
        }

        // Processed this case in for loop
        if (found_match_result_on_current_depth)
        {
            return;
        }

        // Try to match first tag
        if (tag_matchers.front().matchTag(tag.name, current_depth, 1))
        {
            if (!tag_matchers.front().matchAttributes(attributes))
            {
                return;
            }
            MatchingProgress match_result{
                .start_depth = current_depth,
                .next_matcher_index = 1,
            };

            if (match_result.next_matcher_index == tag_matchers.size())
            {
                matched_doc_begin_by_depth[current_depth] = tag_begin;
                match_result.is_matched = true;
            }

            matching_progress_by_depth[current_depth] = match_result;
        }
    }

    void handleClosingTag(
        const char * tag_end,
        size_t current_depth,
        std::map<size_t, MatchingProgress> & matching_progress_by_depth,
        std::unordered_map<size_t, const char *> & matched_doc_begin_by_depth,
        std::map<size_t, std::vector<std::vector<Document>>> & result_by_depth) const
    {
        if (result_by_depth.contains(current_depth + 1) && !result_by_depth[current_depth + 1].empty())
        {
            result_by_depth[current_depth + 1].push_back({});
        }

        for (auto & [depth, match_result] : matching_progress_by_depth)
        {
            if (depth > current_depth)
            {
                continue;
            }

            if (match_result.matchedOnDepth(current_depth))
            {
                match_result.is_matched = false;
                --match_result.next_matcher_index;
            }
        }

        if (!matched_doc_begin_by_depth.contains(current_depth))
        {
            return;
        }

        const char * tag_begin = matched_doc_begin_by_depth[current_depth];
        if (tag_begin == nullptr)
        {
            return;
        }

        if (!result_by_depth.contains(current_depth))
        {
            result_by_depth[current_depth] = {{}};
        }

        result_by_depth[current_depth].back().emplace_back(tag_begin, tag_end);
        matched_doc_begin_by_depth[current_depth] = nullptr;
    }

    std::vector<std::vector<Document>> processDoc(const Document & doc, TagScannerPtr tag_scanner) const
    {
        // Strange case, but sanity check
        if (tag_matchers.empty())
        {
            return {{doc}};
        }

        std::vector<std::vector<Document>> result;
        std::map<size_t, std::vector<std::vector<Document>>> result_by_depth;
        std::map<size_t, MatchingProgress> matching_progress_by_depth;
        std::unordered_map<size_t, const char *> matched_doc_begin_by_depth;

        const char * begin = doc.begin;
        const char * end = doc.end;
        TagPreview current_tag;
        size_t current_depth = 0;

        while (begin != end)
        {
            begin = tag_scanner->scan(begin, end, current_tag);
            if (begin == end)
            {
                break;
            }

            if (current_tag.is_closing)
            {
                handleClosingTag(begin + 1, current_depth, matching_progress_by_depth, matched_doc_begin_by_depth, result_by_depth);
                --current_depth;
            }
            else
            {
                ++current_depth;
                const char * tag_end = find_first_symbols<'>'>(begin, end) + 1;
                if (tag_end >= end)
                {
                    // TODO: unable to find tag end, throw exc?
                    continue;
                }
                handleOpeningTag(
                    current_tag,
                    tag_scanner->getLastTagStart(),
                    begin,
                    tag_end,
                    current_depth,
                    matching_progress_by_depth,
                    matched_doc_begin_by_depth);
                begin = tag_end;
            }
        }

        for (auto & [depth, res] : result_by_depth)
        {
            for (auto & dep_res : res)
            {
                if (!dep_res.empty())
                {
                    result.push_back(std::move(dep_res));
                }
            }
        }

        return result;
    }

public:
    String getName() override { return name; }

    void apply(const std::vector<Document> & docs, std::vector<std::vector<Document>> & res, TagScannerPtr tag_scanner) override
    {
        for (const auto & doc : docs)
        {
            std::vector<std::vector<Document>> docs_to_emplace = processDoc(doc, tag_scanner);
            for (auto & res_docs : docs_to_emplace)
            {
                res.push_back(std::move(res_docs));
            }
        }
    }

    explicit InstructionSelectTagsWithAttributes(std::vector<TagMatcher> && tag_matchers_)
        : tag_matchers(std::move(tag_matchers_))
    {
    }
};

}
