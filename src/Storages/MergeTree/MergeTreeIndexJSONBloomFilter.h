#pragma once

#include <Formats/FormatSettings.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <set>

namespace DB
{

class RPNBuilderTreeNode;
class JSONBloomPathMatcher;
struct JSONBloomFilterDynamicProbe;

struct JSONBloomFilterProbe
{
    UInt64 hash;
    bool is_presence = false;
    std::optional<UInt64> required_presence = std::nullopt;
    std::shared_ptr<const JSONBloomFilterDynamicProbe> dynamic = nullptr;
    auto operator<=>(const JSONBloomFilterProbe &) const = default;
};

struct JSONBloomFilterTokens
{
    HashSet<UInt64, TrivialHash> values;
    HashSet<UInt64, TrivialHash> presence;
    std::map<UInt64, std::set<String>> dynamic_types;
};

using JSONBloomFilterPaths = std::unordered_map<String, JSONBloomFilterTokens>;

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

class MergeTreeIndexGranuleJSONBloomFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);
    MergeTreeIndexGranuleJSONBloomFilter(
        size_t bits_per_row_,
        size_t hash_functions_,
        const JSONBloomFilterPaths & paths_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;
    void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;
    bool empty() const override { return !has_rows; }
    size_t memoryUsageBytes() const override;
    bool hasPath(const String & path) const { return paths.contains(path); }
    bool matches(const String & path, const JSONBloomFilterProbe & probe, bool pending_matches) const;
    void prepareDynamicProbe(const String & path, const JSONBloomFilterProbe & probe, const FormatSettings & format_settings);
    const JSONBloomPathMatcher & getPathMatcher() const { return *path_matcher; }

private:
    struct PathFilter
    {
        bool matches(const JSONBloomFilterProbe & probe, bool pending_matches) const;
        std::vector<UInt64> presence;
        std::vector<std::pair<UInt64, std::vector<String>>> dynamic_types;
        bool dynamic_types_changed = true;
        std::unordered_map<std::shared_ptr<const JSONBloomFilterDynamicProbe>, std::vector<JSONBloomFilterProbe>> dynamic_probes;
        BloomFilterPtr values;
        bool present = false;
        bool pending = false;
    };
    std::unordered_map<String, PathFilter> paths;
    std::unordered_map<std::shared_ptr<const JSONBloomFilterDynamicProbe>, std::unordered_map<String, std::vector<JSONBloomFilterProbe>>> compiled_dynamic_probes;
    size_t bits_per_row;
    size_t hash_functions;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
    bool has_rows = false;
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
    JSONBloomFilterPaths paths;
    size_t total_rows = 0;
};

class MergeTreeIndexConditionJSONBloomFilter final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionJSONBloomFilter(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & header_,
        std::shared_ptr<const JSONBloomPathMatcher> path_matcher_,
        NameSet columns_shadowing_map_subcolumns_);

    bool alwaysUnknownOrTrue() const override;
    bool usesPath(const String & path) const;
    bool needsDynamicTypes() const { return has_dynamic_probes; }
    void prepareDynamicProbes(MergeTreeIndexGranuleJSONBloomFilter & granule) const;
    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleJSONBloomFilter & granule, bool pending_matches) const;
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
            FUNCTION_EXISTS,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {}

        Function function;
        String path;
        std::vector<JSONBloomFilterProbe> hashes;
        std::vector<std::vector<JSONBloomFilterProbe>> alternatives;
    };

    bool evaluateGranule(
        const MergeTreeIndexGranuleJSONBloomFilter & granule,
        const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn,
        bool pending_matches) const;
    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    const Block & header;
    std::shared_ptr<const JSONBloomPathMatcher> path_matcher;
    const FormatSettings comparison_format_settings;
    const NameSet columns_shadowing_map_subcolumns;
    std::vector<RPNElement> rpn;
    bool has_dynamic_probes = false;
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
