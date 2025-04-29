#pragma once

#include <Functions/XPath/Types.h>

namespace DB
{
inline std::vector<Document>
applyInstructions(const char * begin, const char * end, std::vector<InstructionPtr> instructions, TagScannerPtr tag_scanner)
{
    //TODO: strong refactoring!
    std::vector<Document> result;
    std::vector<std::vector<Document>> buff = {{Document{.begin = begin, .end = end}}};
    std::vector<std::vector<Document>> buff_2;

    for (const auto & instruction : instructions)
    {
        for (const auto & docs : buff)
        {
            instruction->apply(docs, buff_2, tag_scanner);
        }
        buff = buff_2;
        buff_2.clear();
    }

    for (auto & docs : buff)
    {
        for (auto & res : docs)
        {
            result.push_back(std::move(res));
        }
    }

    return result;
}
}
