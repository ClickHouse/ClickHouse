#pragma once
#include "config.h"

#include <Storages/IndicesDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace Internal
{

enum class RPNEvaluationIndexUsefulnessState : uint8_t
{
    // the following states indicate if the index might be useful
    TRUE,
    FALSE,
    // the following states indicate RPN always evaluates to TRUE or FALSE, they are used for short-circuit.
    ALWAYS_TRUE,
    ALWAYS_FALSE
};

[[nodiscard]] inline RPNEvaluationIndexUsefulnessState
evalAndRpnIndexStates(RPNEvaluationIndexUsefulnessState lhs, RPNEvaluationIndexUsefulnessState rhs)
{
    if (lhs == RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE || rhs == RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE)
    {
        // short circuit
        return RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE;
    }
    else if (lhs == RPNEvaluationIndexUsefulnessState::TRUE || rhs == RPNEvaluationIndexUsefulnessState::TRUE)
    {
        return RPNEvaluationIndexUsefulnessState::TRUE;
    }
    else if (lhs == RPNEvaluationIndexUsefulnessState::FALSE || rhs == RPNEvaluationIndexUsefulnessState::FALSE)
    {
        return RPNEvaluationIndexUsefulnessState::FALSE;
    }
    chassert(lhs == RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE && rhs == RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE);
    return RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE;
}

[[nodiscard]] inline RPNEvaluationIndexUsefulnessState
evalOrRpnIndexStates(RPNEvaluationIndexUsefulnessState lhs, RPNEvaluationIndexUsefulnessState rhs)
{
    if (lhs == RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE || rhs == RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE)
    {
        // short circuit
        return RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE;
    }
    else if (lhs == RPNEvaluationIndexUsefulnessState::TRUE || rhs == RPNEvaluationIndexUsefulnessState::TRUE)
    {
        return RPNEvaluationIndexUsefulnessState::TRUE;
    }
    else if (lhs == RPNEvaluationIndexUsefulnessState::FALSE || rhs == RPNEvaluationIndexUsefulnessState::FALSE)
    {
        return RPNEvaluationIndexUsefulnessState::FALSE;
    }
    chassert(lhs == RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE && rhs == RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE);
    return RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE;
}
}

class ActionsDAG;
class Block;
struct MergeTreeWriterSettings;
struct SelectQueryInfo;
struct MergeTreeDataPartChecksums;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// Stores some info about a single block of data.
struct IMergeTreeIndexGranule
{
    virtual ~IMergeTreeIndexGranule() = default;

    /// Serialize always last version.
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    /// Serialize with multiple streams.
    /// By analogy with ISerialization::serializeBinaryBulkWithMultipleStreams.
    virtual void serializeBinaryWithMultipleStreams(MergeTreeIndexOutputStreams & streams) const;

    /// Version of the index to deserialize:
    ///
    /// - 2 -- minmax index for proper Nullable support,
    /// - 1 -- everything else.
    ///
    /// Implementation is responsible for version check,
    /// and throws LOGICAL_ERROR in case of unsupported version.
    ///
    /// See also:
    /// - IMergeTreeIndex::getSubstreams()
    /// - IMergeTreeIndex::getDeserializedFormat()
    /// - MergeTreeDataMergerMutator::collectFilesToSkip()
    /// - MergeTreeDataMergerMutator::collectFilesForRenames()
    virtual void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) = 0;

    /// Deserialize with multiple streams.
    /// By analogy with ISerialization::deserializeBinaryBulkWithMultipleStreams.
    virtual void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state);

    virtual bool empty() const = 0;

    /// The in-memory size of the granule. Not expected to be 100% accurate.
    virtual size_t memoryUsageBytes() const = 0;
};

using MergeTreeIndexGranulePtr = std::shared_ptr<IMergeTreeIndexGranule>;
using MergeTreeIndexGranules = std::vector<MergeTreeIndexGranulePtr>;


/// Stores many granules at once in a more optimal form, allowing bulk filtering.
struct IMergeTreeIndexBulkGranules
{
    virtual ~IMergeTreeIndexBulkGranules() = default;
    virtual void deserializeBinary(size_t granule_num, ReadBuffer & istr, MergeTreeIndexVersion version) = 0;
};

using MergeTreeIndexBulkGranulesPtr = std::shared_ptr<IMergeTreeIndexBulkGranules>;


/// Aggregates info about a single block of data.
struct IMergeTreeIndexAggregator
{
    virtual ~IMergeTreeIndexAggregator() = default;

    virtual bool empty() const = 0;
    virtual MergeTreeIndexGranulePtr getGranuleAndReset() = 0;

    /// Updates the stored info using rows of the specified block.
    /// Reads no more than `limit` rows.
    /// After finishing updating `pos` will store the position of the first row which was not read.
    virtual void update(const Block & block, size_t * pos, size_t limit) = 0;
};

using MergeTreeIndexAggregatorPtr = std::shared_ptr<IMergeTreeIndexAggregator>;
using MergeTreeIndexAggregators = std::vector<MergeTreeIndexAggregatorPtr>;


/// Condition on the index.
class IMergeTreeIndexCondition
{
public:
    virtual ~IMergeTreeIndexCondition() = default;
    /// Checks if this index is useful for query.
    virtual bool alwaysUnknownOrTrue() const = 0;

    using UpdatePartialDisjunctionResultFn = KeyCondition::UpdatePartialDisjunctionResultFn;
    virtual bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const = 0;

    using FilteredGranules = std::vector<size_t>;
    virtual FilteredGranules getPossibleGranules(const MergeTreeIndexBulkGranulesPtr &) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Index does not support filtering in bulk");
    }

    /// Special method for vector similarity indexes:
    /// Returns the N nearest neighbors of a reference vector in the index granule.
    /// The nearest neighbors are returned as row positions.
    /// If VectorSearchParameters::return_distances = true, then the distances are returned as well.
    virtual NearestNeighbours calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr /*granule*/) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "calculateApproximateNearestNeighbors is not implemented for non-vector-similarity indexes");
    }

    template <typename RPNElement>
    bool rpnEvaluatesAlwaysUnknownOrTrue(
        const std::vector<RPNElement> & rpn, const std::unordered_set<typename RPNElement::Function> & matchingFunctions) const
    {
        std::vector<Internal::RPNEvaluationIndexUsefulnessState> rpn_stack;
        rpn_stack.reserve(rpn.size() - 1);

        for (const auto & element : rpn)
        {
            if (element.function == RPNElement::ALWAYS_TRUE)
            {
                rpn_stack.emplace_back(Internal::RPNEvaluationIndexUsefulnessState::ALWAYS_TRUE);
            }
            else if (element.function == RPNElement::ALWAYS_FALSE)
            {
                rpn_stack.emplace_back(Internal::RPNEvaluationIndexUsefulnessState::ALWAYS_FALSE);
            }
            else if (element.function == RPNElement::FUNCTION_UNKNOWN)
            {
                rpn_stack.emplace_back(Internal::RPNEvaluationIndexUsefulnessState::FALSE);
            }
            else if (matchingFunctions.contains(element.function))
            {
                rpn_stack.push_back(Internal::RPNEvaluationIndexUsefulnessState::TRUE);
            }
            else if (element.function == RPNElement::FUNCTION_NOT)
            {
                // do nothing
            }
            else if (element.function == RPNElement::FUNCTION_AND)
            {
                auto lhs = rpn_stack.back();
                rpn_stack.pop_back();
                auto rhs = rpn_stack.back();
                rpn_stack.back() = evalAndRpnIndexStates(lhs, rhs);
            }
            else if (element.function == RPNElement::FUNCTION_OR)
            {
                auto lhs = rpn_stack.back();
                rpn_stack.pop_back();
                auto rhs = rpn_stack.back();
                rpn_stack.back() = evalOrRpnIndexStates(lhs, rhs);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected function type in RPNElement");
        }

        chassert(rpn_stack.size() == 1);
        /*
         * In case the result is `ALWAYS_TRUE`, it means we don't need any indices at all, it might be a constant result.
         * Thus, we only check against the `TRUE` to determine the usefulness of the index condition.
         */
        return rpn_stack.front() != Internal::RPNEvaluationIndexUsefulnessState::TRUE;
    }

    virtual std::string getDescription() const = 0;
};

using MergeTreeIndexConditionPtr = std::shared_ptr<IMergeTreeIndexCondition>;

struct IMergeTreeIndex;
using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;


/// IndexCondition that checks several indexes at the same time.
class IMergeTreeIndexMergedCondition
{
public:
    explicit IMergeTreeIndexMergedCondition(size_t granularity_)
        : granularity(granularity_)
    {
    }

    virtual ~IMergeTreeIndexMergedCondition() = default;

    virtual void addIndex(const MergeTreeIndexPtr & index) = 0;
    virtual bool alwaysUnknownOrTrue() const = 0;
    virtual bool mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const = 0;

protected:
    const size_t granularity;
};

using MergeTreeIndexMergedConditionPtr = std::shared_ptr<IMergeTreeIndexMergedCondition>;


struct IMergeTreeIndex
{
    explicit IMergeTreeIndex(const IndexDescription & index_)
        : index(index_)
    {
    }

    virtual ~IMergeTreeIndex() = default;

    /// Returns the filename without extension. If escape_filenames is set (default since 26.1), the name is escaped.
    String getFileName() const;
    size_t getGranularity() const { return index.granularity; }

    virtual bool isMergeable() const { return false; }

    /// Returns substreams for serialization.
    /// Reimplement if you want new index format.
    ///
    /// NOTE: In case getSubstreams() is reimplemented,
    /// getDeserializedFormat() should be reimplemented too,
    /// and check all previous extensions for substreams too
    /// (to avoid breaking backward compatibility).
    virtual MergeTreeIndexSubstreams getSubstreams() const { return {{MergeTreeIndexSubstream::Type::Regular, "", ".idx"}}; }

    /// Returns substreams and version for deserialization.
    virtual MergeTreeIndexFormat getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & relative_path_prefix) const;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    /// A more optimal filtering method
    virtual bool supportsBulkFiltering() const { return false; }

    virtual MergeTreeIndexBulkGranulesPtr createIndexBulkGranules() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Index does not support filtering in bulk");
    }

    virtual MergeTreeIndexAggregatorPtr createIndexAggregator() const = 0;

    virtual MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * predicate, ContextPtr context) const = 0;

    /// The vector similarity index overrides this method
    virtual MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/,
        const std::optional<VectorSearchParameters> & /*parameters*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "createIndexCondition with vector search parameters is not implemented for index of type {}", index.type);
    }

    virtual bool isVectorSimilarityIndex() const { return false; }
    virtual bool isTextIndex() const { return false; }

    virtual MergeTreeIndexMergedConditionPtr createIndexMergedCondition(
        const SelectQueryInfo & /*query_info*/, StorageMetadataPtr /*storage_metadata*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "MergedCondition is not implemented for index of type {}", index.type);
    }

    Names getColumnsRequiredForIndexCalc() const;

    const IndexDescription & index;
};

using MergeTreeIndexPtr = std::shared_ptr<const IMergeTreeIndex>;
using MergeTreeIndices = std::vector<MergeTreeIndexPtr>;

struct MergeTreeIndexWithCondition
{
    MergeTreeIndexPtr index;
    MergeTreeIndexConditionPtr condition;

    MergeTreeIndexWithCondition(MergeTreeIndexPtr index_, MergeTreeIndexConditionPtr condition_)
        : index(std::move(index_)), condition(std::move(condition_))
    {
    }

    MergeTreeIndexWithCondition() = default;
};

class MergeTreeIndexFactory : private boost::noncopyable
{
public:
    static MergeTreeIndexFactory & instance();

    using Creator = std::function<MergeTreeIndexPtr(const IndexDescription & index)>;

    using Validator = std::function<void(const IndexDescription & index, bool attach)>;

    void validate(const IndexDescription & index, bool attach) const;

    MergeTreeIndexPtr get(const IndexDescription & index) const;

    MergeTreeIndices getMany(const std::vector<IndexDescription> & indices) const;

    void registerCreator(const std::string & index_type, Creator creator);
    void registerValidator(const std::string & index_type, Validator validator);

protected:
    MergeTreeIndexFactory();

private:
    using Creators = std::unordered_map<std::string, Creator>;
    using Validators = std::unordered_map<std::string, Validator>;
    Creators creators;
    Validators validators;
};

MergeTreeIndexPtr minmaxIndexCreator(const IndexDescription & index);
void minmaxIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr setIndexCreator(const IndexDescription & index);
void setIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexTextCreator(const IndexDescription & index);
void bloomFilterIndexTextValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr bloomFilterIndexCreator(const IndexDescription & index);
void bloomFilterIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr hypothesisIndexCreator(const IndexDescription & index);
void hypothesisIndexValidator(const IndexDescription & index, bool attach);

#if USE_USEARCH
MergeTreeIndexPtr vectorSimilarityIndexCreator(const IndexDescription & index);
void vectorSimilarityIndexValidator(const IndexDescription & index, bool attach);
#endif

MergeTreeIndexPtr ginIndexCreator(const IndexDescription & index);
void ginIndexValidator(const IndexDescription & index, bool attach);

MergeTreeIndexPtr textIndexCreator(const IndexDescription & index);
void textIndexValidator(const IndexDescription & index, bool attach);

String getIndexFileName(const String & index_name, bool escape_filename);

/// Check if index file exists in checksums, checking both original and hashed filenames.
/// This supports long index names that were hashed due to replace_long_file_name_to_hash setting.
bool indexFileExistsInChecksums(
    const MergeTreeDataPartChecksums & checksums,
    const std::string & path_prefix,
    const std::string & extension);
}
