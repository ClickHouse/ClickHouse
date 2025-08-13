#pragma once

#include <Common/HashTable/HashSet.h>
#include <Interpreters/GinFilter.h>
#include <Interpreters/ITokenExtractor.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

struct MergeTreeIndexGranuleGin final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleGin(const String & index_name_);

    ~MergeTreeIndexGranuleGin() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_elems; }
    size_t memoryUsageBytes() const override;

    const String index_name;
    GinFilter gin_filter;
    bool has_elems;
};

using MergeTreeIndexGranuleGinPtr = std::shared_ptr<MergeTreeIndexGranuleGin>;

struct MergeTreeIndexAggregatorGin final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorGin(
        GinIndexStorePtr store_,
        const Names & index_columns_,
        const String & index_name_,
        TokenExtractorPtr token_extractor_);

    ~MergeTreeIndexAggregatorGin() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;
    void addToGinFilter(UInt32 rowID, const char * data, size_t length, GinFilter & gin_filter);

    GinIndexStorePtr store;
    Names index_columns;
    const String index_name;
    TokenExtractorPtr token_extractor;
    MergeTreeIndexGranuleGinPtr granule;
};


class MergeTreeIndexConditionGin final : public IMergeTreeIndexCondition, WithContext
{
public:
    MergeTreeIndexConditionGin(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & index_sample_block,
        const GinFilterParameters & gin_filter_params_,
        TokenExtractorPtr token_extactor_);

    ~MergeTreeIndexConditionGin() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;
    bool mayBeTrueOnGranuleInPart(MergeTreeIndexGranulePtr idx_granule, PostingsCacheForStore & cache_store) const;

private:
    struct KeyTuplePositionMapping
    {
        KeyTuplePositionMapping(size_t tuple_index_, size_t key_index_) : tuple_index(tuple_index_), key_index(key_index_) {}

        size_t tuple_index;
        size_t key_index;
    };

    /// Uses RPN like KeyCondition
    struct RPNElement
    {
        enum Function
        {
            /// Atoms
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_MATCH,
            FUNCTION_SEARCH_ANY,
            FUNCTION_SEARCH_ALL,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement( /// NOLINT
                Function function_ = FUNCTION_UNKNOWN, std::unique_ptr<GinFilter> && const_gin_filter_ = nullptr)
                : function(function_), gin_filter(std::move(const_gin_filter_)) {}

        Function function = FUNCTION_UNKNOWN;

        /// For FUNCTION_EQUALS, FUNCTION_NOT_EQUALS
        std::unique_ptr<GinFilter> gin_filter;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<GinFilters> set_gin_filters;

        /// For FUNCTION_IN and FUNCTION_NOT_IN
        std::vector<size_t> set_key_position;
    };

    using RPN = std::vector<RPNElement>;

    bool traverseAtomAST(const RPNBuilderTreeNode & node, RPNElement & out);
    bool traverseASTEquals(
        const RPNBuilderFunctionTreeNode & function_node,
        const RPNBuilderTreeNode & index_column_ast,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out);

    bool tryPrepareSetGinFilter(const RPNBuilderTreeNode & lhs, const RPNBuilderTreeNode & rhs, RPNElement & out);

    const Block & header;
    GinFilterParameters gin_filter_params;
    TokenExtractorPtr token_extractor;
    RPN rpn;
    PreparedSetsPtr prepared_sets;
};

class MergeTreeIndexGin final : public IMergeTreeIndex
{
public:
    MergeTreeIndexGin(
        const IndexDescription & index_,
        const GinFilterParameters & gin_filter_params_,
        std::unique_ptr<ITokenExtractor> && token_extractor_);

    ~MergeTreeIndexGin() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const GinIndexStorePtr & store, const MergeTreeWriterSettings & settings) const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    GinFilterParameters gin_filter_params;
    std::unique_ptr<ITokenExtractor> token_extractor;
};

}
