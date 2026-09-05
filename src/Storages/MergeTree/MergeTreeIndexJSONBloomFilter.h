#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>

namespace DB
{

class RPNBuilderTreeNode;
class JSONBloomPathMatcher;

struct MergeTreeIndexJSONBloomFilterPartMetadata final : IMergeTreeIndexPartMetadata
{
    MergeTreeIndexJSONBloomFilterPartMetadata(
        size_t bits_per_row_,
        size_t hash_functions_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_)
        : bits_per_row(bits_per_row_)
        , hash_functions(hash_functions_)
        , path_matcher(std::move(path_matcher_))
    {
    }

    size_t bits_per_row;
    size_t hash_functions;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
};

class MergeTreeIndexGranuleJSONBloomFilter final : public MergeTreeIndexGranuleBloomFilter
{
public:
    MergeTreeIndexGranuleJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);
    MergeTreeIndexGranuleJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        const std::vector<HashSet<UInt64>> & column_hashes_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);

    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    size_t getHashFunctions() const { return hash_functions; }
    const JSONBloomPathMatcher & getPathMatcher() const { return *path_matcher; }

private:
    size_t hash_functions;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
};

class MergeTreeIndexAggregatorJSONBloomFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        String column_name_,
        DataTypePtr column_type_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);

    bool empty() const override { return total_rows == 0; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    size_t bits_per_row;
    size_t hash_functions;
    String column_name;
    DataTypePtr column_type;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
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
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);

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
        String path;
        std::vector<UInt64> hashes;
        std::vector<std::vector<UInt64>> alternatives;
    };

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    const Block & header;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
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
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexGranulePtr createIndexGranule(const MergeTreeIndexPartMetadataPtr & part_metadata) const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat getPhysicalFormat(
        const MergeTreeDataPartChecksums & checksums,
        const IDataPartStorage & storage,
        const std::string & relative_path_prefix) const override;
    MergeTreeIndexSubstreams getAllSubstreamsInPart(
        const MergeTreeDataPartChecksums & checksums,
        const std::string & relative_path_prefix,
        const IDataPartStorage * storage) const override;
    void serializePartMetadata(MergeTreeIndexOutputStreams & streams) const override;
    MergeTreeIndexPartMetadataPtr deserializePartMetadata(MergeTreeIndexInputStreams & streams) const override;

private:
    size_t bits_per_row;
    size_t hash_functions;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
};

}
