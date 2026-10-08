#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/KeyDescription.h>

#include <map>

namespace DB
{

class ASTCreateQuery;
class ColumnsDescription;

/// IVF index: INDEX name (vec, cid) TYPE vector_similarity('ivf', distance, dimensions, samples = ..., clustering = f(k, ...)).
/// The table is sorted by cid, so clusters are primary key ranges; each granule stores the row count per cid.
/// The skip index is used more for maintaining statistics and advanced vector math related.

struct IvfIndexArguments
{
    String distance_function;
    UInt64 dimensions = 0;
    String samples_table;
    /// Aggregate function which trains the centroids from the samples, and its parameters. By contract, the first parameter
    /// is the number of clusters k, and the function returns either the k centroids as Array(Array(Float32)) or a tree of
    /// centroids as Array(Tuple(vec Array(Float32), level UInt8, cid UInt32, parent UInt32[, radius Float32])) with k leaves.
    String clustering_function;
    Array clustering_parameters;
    /// Option to enable advanced vector math
    bool cluster_statistics = false;
    /// If not 0, clusters with more than twice this number of rows are split in the background into clusters of about this size.
    UInt64 target_cluster_size = 0;
};

/// Parses and checks the arguments of an IVF index (not the specific parameters of the clustering function).
IvfIndexArguments parseVectorSimilarityIvfIndexArguments(const ASTPtr & arguments);

using IvfClusterId = UInt32;
using IvfClusterCounts = std::map<IvfClusterId, UInt64>;

struct MergeTreeIndexGranuleVectorSimilarityIvf final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleVectorSimilarityIvf() = default;
    explicit MergeTreeIndexGranuleVectorSimilarityIvf(IvfClusterCounts && cluster_counts_);

    ~MergeTreeIndexGranuleVectorSimilarityIvf() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return cluster_counts.empty(); }

    size_t memoryUsageBytes() const override { return cluster_counts.size() * (sizeof(IvfClusterId) + sizeof(UInt64)); }

    IvfClusterCounts cluster_counts;

private:
    static constexpr UInt64 FILE_FORMAT_VERSION = 1;
};


struct MergeTreeIndexAggregatorVectorSimilarityIvf final : IMergeTreeIndexAggregator
{
    MergeTreeIndexAggregatorVectorSimilarityIvf(const String & vector_column_, const String & cluster_id_column_, UInt64 dimensions_);

    ~MergeTreeIndexAggregatorVectorSimilarityIvf() override = default;

    bool empty() const override { return cluster_counts.empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

    const String vector_column;
    const String cluster_id_column;
    const UInt64 dimensions;
    IvfClusterCounts cluster_counts;
};


/// The index is not used for filtering yet.
class MergeTreeIndexConditionVectorSimilarityIvf final : public IMergeTreeIndexCondition
{
public:
    bool alwaysUnknownOrTrue() const override { return true; }
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr, const UpdatePartialDisjunctionResultFn &) const override { return true; }
    std::string getDescription() const override { return ""; }
};


bool isVectorSimilarityIvfIndex(const IndexDescription & index);

class MergeTreeIndexVectorSimilarityIvf : public IMergeTreeIndex
{
public:
    MergeTreeIndexVectorSimilarityIvf(StorageMetadataPtr metadata_snapshot_, const IndexDescription & index_, IvfIndexArguments arguments_);

    ~MergeTreeIndexVectorSimilarityIvf() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    const IvfIndexArguments arguments;
};

/// Whether the query replays the DDL of another replica in the Shared Catalog (ClickHouse Cloud).
bool isSharedCatalogReplay(const ContextPtr & context);

/// Checks the index against its table: cid must be in the primary key; with check_samples_table, the samples table must exist,
/// have the vector column and at least k rows, and the clustering function must accept the parameters and return centroids.
void checkVectorSimilarityIvfIndexForTable(
    const IndexDescription & index,
    const KeyDescription & primary_key,
    bool check_samples_table,
    const String & current_database,
    ContextPtr context);

/// Internal objects of an IVF table : the centroids table (level, cid, parent, vec, active, radius, generation, generation_size,
/// split_from) and a dictionary over it. Level 0 holds the leaves; each write (training, split) is a generation with its expected
/// size. A leaf is retired if it is not active or another leaf has it as split_from.
StorageID getVectorSimilarityIvfCentroidsTableID(const StorageID & table_id, const String & index_name);
StorageID getVectorSimilarityIvfCentroidsDictionaryID(const StorageID & table_id, const String & index_name);
/// Starts (once per server) the background task which splits overgrown clusters of IVF indexes.
void startVectorSimilarityIvfMaintenance();

/// If the name refers to a table with an IVF index or to the centroids dictionary of an IVF index, returns the qualified name
/// of the dictionary and the table. The dictionary is found through the table UUID in its name, so it does not depend on the
/// table name. Access to the hidden objects is granted through access to the table.
struct IvfCentroidsDictionary
{
    String dictionary_name;
    StorageID table_id;
};
std::optional<IvfCentroidsDictionary> tryGetVectorSimilarityIvfCentroidsDictionary(const String & name, const ContextPtr & context);

/// In a new table, replaces references to the table itself in assignCentroid(..., 'table') (e.g. in the DEFAULT expression of the
/// cluster id column) by the name of its centroids dictionary, which stays valid after RENAME and EXCHANGE. Returns whether
/// anything was replaced.
bool rewriteVectorSimilarityIvfSelfReferences(const StorageID & table_id, const Strings & index_names, ColumnsDescription & columns);

/// Creates the hidden objects of a newly created "IVF" table (not on ATTACH).
void createVectorSimilarityIvfInnerObjects(
    const StorageID & table_id, const String & table_engine, const StorageInMemoryMetadata & metadata, ContextPtr context);

/// Trains the centroids of a newly created "IVF" table in the background: runs the clustering function over
/// the samples table, fills the centroids table and reloads the dictionary. Failures are logged.
void scheduleVectorSimilarityIvfTraining(const StorageID & table_id, const StorageInMemoryMetadata & metadata, ContextPtr context);

/// Names of the IVF indexes of a table, from its metadata or from its CREATE query (which does not require loading the table).
Strings getVectorSimilarityIvfIndexNames(const StorageInMemoryMetadata & metadata);
Strings getVectorSimilarityIvfIndexNames(const ASTCreateQuery & create);

/// Drops the internal objects of the given IVF indexes of a table.
void dropVectorSimilarityIvfInnerObjects(const StorageID & table_id, const Strings & index_names, ContextPtr context);

MergeTreeIndexPtr vectorSimilarityIvfIndexCreator(StorageMetadataPtr metadata_snapshot, const IndexDescription & index, const MergeTreeSettings & settings);
void vectorSimilarityIvfIndexValidator(const IndexDescription & index, bool attach, const MergeTreeSettings & settings);

}
