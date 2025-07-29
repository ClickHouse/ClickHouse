#pragma once

#include <Functions/XPath/Types.h>

namespace DB
{

class InstructionSelectNth : public IInstruction
{
    static constexpr auto name = "InstructionSelectNth";

    UInt64 index;

public:
    String getName() override { return name; }

    void apply(const std::vector<Document> & docs, std::vector<std::vector<Document>> & res, TagScannerPtr) override
    {
        if (docs.size() < index)
        {
            return;
        }

        std::vector<Document> docs_to_emplace;
        docs_to_emplace.emplace_back(docs[index - 1]);
        res.push_back(std::move(docs_to_emplace));
    }

    explicit InstructionSelectNth(UInt64 index_)
        : index(index_)
    {
    }
};

}
