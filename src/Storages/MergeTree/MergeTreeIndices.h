#pragma once
#include "config.h"

#include <Storages/IndicesDescription.h>
#include <Interpreters/ActionsDAG.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

constexpr auto INDEX_FILE_PREFIX = "skp_idx_";

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
class IDataPartStorage;
struct MergeTreeWriterSettings;
struct SelectQueryInfo;

class GinIndexStore;
using GinIndexStorePtr = std::shared_ptr<GinIndexStore>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using MergeTreeIndexVersion = uint8_t;
struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    const char* extension;

    explicit operator bool() const { return version != 0; }
};

/// A vehicle which transports elements of the SELECT query to the vector similarity index.
struct VectorSearchParameters
{
    String column;
    String distance_function;
    size_t limit;
    std::vector<Float64> reference_vector;
};

/// Stores some info about a single block of data.
struct IMergeTreeIndexGranule
{
    virtual ~IMergeTreeIndexGranule() = default;

    /// Serialize always last version.
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    /// Version of the index to deserialize:
    ///
    /// - 2 -- minmax index for proper Nullable support,
    /// - 1 -- everything else.
    ///
    /// Implementation is responsible for version check,
    /// and throws LOGICAL_ERROR in case of unsupported version.
    ///
    /// See also:
    /// - IMergeTreeIndex::getSerializedFileExtension()
    /// - IMergeTreeIndex::getDeserializedFormat()
    /// - MergeTreeDataMergerMutator::collectFilesToSkip()
    /// - MergeTreeDataMergerMutator::collectFilesForRenames()
    virtual void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) = 0;

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

    virtual bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const = 0;

    using FilteredGranules = std::vector<size_t>;
    virtual FilteredGranules getPossibleGranules(const MergeTreeIndexBulkGranulesPtr &) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Index does not support filtering in bulk");
    }

    /// Special method for vector similarity indexes:
    /// Returns the row positions of the N nearest neighbors in the index granule
    /// The returned row numbers are guaranteed to be sorted and unique.
    virtual std::vector<UInt64> calculateApproximateNearestNeighbors(MergeTreeIndexGranulePtr /*granule*/) const
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

    /// Returns filename without extension.
    String getFileName() const { return INDEX_FILE_PREFIX + index.name; }
    size_t getGranularity() const { return index.granularity; }

    virtual bool isMergeable() const { return false; }

    /// Returns extension for serialization.
    /// Reimplement if you want new index format.
    ///
    /// NOTE: In case getSerializedFileExtension() is reimplemented,
    /// getDeserializedFormat() should be reimplemented too,
    /// and check all previous extensions too
    /// (to avoid breaking backward compatibility).
    virtual const char* getSerializedFileExtension() const { return ".idx"; }

    /// Returns extension for deserialization.
    ///
    /// Return pair<extension, version>.
    virtual MergeTreeIndexFormat
    getDeserializedFormat(const IDataPartStorage & data_part_storage, const std::string & relative_path_prefix) const;

    virtual MergeTreeIndexGranulePtr createIndexGranule() const = 0;

    /// A more optimal filtering method
    virtual bool supportsBulkFiltering() const
    {
        return false;
    }

    virtual MergeTreeIndexBulkGranulesPtr createIndexBulkGranules() const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Index does not support filtering in bulk");
    }

    virtual MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const = 0;

    virtual MergeTreeIndexAggregatorPtr createIndexAggregatorForPart(const GinIndexStorePtr & /*store*/, const MergeTreeWriterSettings & settings) const
    {
        return createIndexAggregator(settings);
    }

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

}
