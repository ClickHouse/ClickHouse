#pragma once

#include <Storages/MergeTree/MergeTreeIndexes.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <iostream>
#include <random>

namespace DB {

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}


class MergeTreeTestIndex;

struct MergeTreeTestGranule : public MergeTreeIndexGranule {
    ~MergeTreeTestGranule() override = default;;

    void serializeBinary(WriteBuffer &ostr) const override {
        //std::cerr << "TESTINDEX: written " << emp << "\n";
        writeIntBinary(emp, ostr);
    }

    void deserializeBinary(ReadBuffer &istr) override {
        readIntBinary(emp, istr);
        if (emp != 10) {
            throw Exception("kek bad read", ErrorCodes::FILE_DOESNT_EXIST);
        }
        //std::cerr << "TESTINDEX: read " << emp << "\n";
    }

    bool empty() const override {
        return emp == 0;
    }

    void update(const Block &block, size_t *pos, size_t limit) override {
        *pos += std::min(limit, block.rows() - *pos);
        emp = 10;
    };

    Int32 emp = 0;
};

class IndexTestCondition : public IndexCondition{
public:
    IndexTestCondition(int) {};
    ~IndexTestCondition() override = default;

    /// Checks if this index is useful for query.
    bool alwaysUnknownOrTrue() const override { return false; };

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr) const override {
        return true;
    }

};


class MergeTreeTestIndex : public MergeTreeIndex
{
public:
    MergeTreeTestIndex(String name, ExpressionActionsPtr expr, size_t granularity)
            : MergeTreeIndex(name, expr, granularity) {}

    ~MergeTreeTestIndex() override = default;

    /// gets filename without extension

    MergeTreeIndexGranulePtr createIndexGranule() const override {
        return std::make_shared<MergeTreeTestGranule>();
    }

    IndexConditionPtr createIndexCondition(
            const SelectQueryInfo & , const Context & ) const override {
        return std::make_shared<IndexTestCondition>(4);
    };

};

std::unique_ptr<MergeTreeIndex> MTItestCreator(
        const MergeTreeData & data, std::shared_ptr<ASTIndexDeclaration> node, const Context & ) {
    return std::make_unique<MergeTreeTestIndex>(
            node->name, data.primary_key_expr, node->granularity.get<size_t>());
}

}