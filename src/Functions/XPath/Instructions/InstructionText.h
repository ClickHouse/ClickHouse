#pragma once

#include <Functions/XPath/Matchers.h>
#include <Functions/XPath/Types.h>

namespace DB
{

class InstructionText : public IInstruction
{
    static constexpr auto name = "InstructionText";

public:
    String getName() override { return name; }

    void apply(const std::vector<Document> & docs, std::vector<std::vector<Document>> & res, TagScannerPtr tag_scanner) override
    {
        std::vector<Document> result;

        for (const auto & doc : docs)
        {
            const char * begin = doc.begin;
            const char * end = doc.end;

            TagPreview open_tag;
            TagPreview close_tag;

            begin = tag_scanner->scan(begin, end, open_tag);

            // Invalid doc, do not start with <tag>
            if (tag_scanner->getLastTagStart() != doc.begin)
            {
                continue;
            }

            begin = tag_scanner->scan(begin, end, close_tag);
            if (begin == end)
            {
                continue;
            }

            // Check that doc one pair of open and close tags with same name
            if (open_tag.name != close_tag.name || !close_tag.is_closing)
            {
                continue;
            }

            begin = tag_scanner->scan(begin, end, open_tag);
            // Some other tag in document
            if (begin != end)
            {
                continue;
            }

            //In this case we have <tag ...> ... </tag>
            //So we can find first '>' and first  '<' after the beginning and
            const auto * text_begin = find_first_symbols<'>'>(doc.begin, doc.end);
            text_begin++;
            const auto * text_end = find_first_symbols<'<'>(doc.begin + 1, doc.end);

            if (text_begin > text_end)
            {
                continue;
            }

            result.emplace_back(text_begin, text_end);
        }

        res.push_back(std::move(result));
    }
};

}
