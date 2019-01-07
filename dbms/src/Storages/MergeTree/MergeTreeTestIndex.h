#pragma once

#include <Storages/MergeTree/MergeTreeIndexes.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB {

class MergeTreeTestIndex;

struct MergeTreeTestGranule : public MergeTreeIndexGranule {
    ~MergeTreeTestGranule() override {};

    void serializeBinary(WriteBuffer &ostr) const override {
        writeIntBinary(emp, ostr);
    }

    void deserializeBinary(ReadBuffer &istr) override {
        readIntBinary(emp, istr);
    }

    bool empty() const override {
        return static_cast<bool>(emp);
    }

    void update(const Block &block, size_t *pos, size_t limit) override {
        *pos += std::min(limit, block.rows() - *pos);
        emp = false;
    };

    Int32 emp = true;
};

class IndexTestCondition : public IndexCondition{
public:
    IndexTestCondition() = default;
    ~IndexTestCondition() override {};

    /// Checks if this index is useful for query.
    bool alwaysUnknownOrTrue() const override { return false; };

    bool mayBeTrueOnGranule(const MergeTreeIndexGranule &) const override {
        return true;
    }

};


class MergeTreeTestIndex : public MergeTreeIndex
{
public:
    MergeTreeTestIndex(String name, ExpressionActionsPtr expr, size_t granularity, Block key)
            : MergeTreeIndex(name, expr, granularity, key) {}

    ~MergeTreeTestIndex() override {}

    String indexType() const override { return "TEST"; }

    /// gets filename without extension

    MergeTreeIndexGranulePtr createIndexGranule() const override {
        return std::make_shared<MergeTreeTestGranule>();
    }

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & , const Context & ) const override {
        return std::make_shared<IndexTestCondition>();
    };

    void writeText(WriteBuffer & ostr) const override {
        DB::writeText(10, ostr);
    };
};

std::unique_ptr<MergeTreeIndex> MTItestCreator(std::shared_ptr<ASTIndexDeclaration> node) {
    return std::make_unique<MergeTreeTestIndex>(
            node->name, nullptr, node->granularity.get<size_t>(), Block{});
}

}