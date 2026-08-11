#pragma once

#include <Interpreters/ITokenizer.h>
#include <Formats/FormatSettings.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>

namespace DB
{

class RPNBuilderTreeNode;

class MergeTreeIndexAggregatorJSONBloomFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        String column_name_,
        DataTypePtr column_type_,
        TokenizerPtr tokenizer_);

    bool empty() const override { return total_rows == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    size_t bits_per_row;
    size_t hash_functions;
    String column_name;
    DataTypePtr column_type;
    TokenizerPtr tokenizer;
    HashSet<UInt64> hashes;
    size_t total_rows = 0;
};

class MergeTreeIndexConditionJSONBloomFilter final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionJSONBloomFilter(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & header_,
        size_t hash_functions_,
        TokenizerPtr tokenizer_);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(
        MergeTreeIndexGranulePtr granule,
        const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const override;
    std::string getDescription() const override { return {}; }

private:
    struct RPNElement
    {
        enum Function
        {
            FUNCTION_UNKNOWN,
            FUNCTION_ANY,
            FUNCTION_ALL,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {}

        Function function;
        std::vector<UInt64> hashes;
        std::vector<std::vector<UInt64>> alternatives;
    };

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    const Block & header;
    size_t hash_functions;
    TokenizerPtr tokenizer;
    const FormatSettings comparison_format_settings;
    std::vector<RPNElement> rpn;
};

class MergeTreeIndexJSONBloomFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexJSONBloomFilter(
        StorageMetadataPtr metadata_snapshot_,
        const IndexDescription & index_,
        size_t bits_per_row_,
        size_t hash_functions_,
        std::unique_ptr<ITokenizer> tokenizer_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

private:
    size_t bits_per_row;
    size_t hash_functions;
    std::unique_ptr<ITokenizer> tokenizer;
};

}
