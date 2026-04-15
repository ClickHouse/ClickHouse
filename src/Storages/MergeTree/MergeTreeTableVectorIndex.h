#pragma once

#include <base/types.h>
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <Common/logger_useful.h>

#include <vector>
#include <optional>
#include <map>
#include <memory>
#include <shared_mutex>

namespace DB
{

/// Metadata for a single part's granule within a table-level vector index
struct GranuleVectorMetadata
{
    /// Granule identification
    String part_name;           // e.g., "all_0_100_2"
    UInt32 granule_id;          // Granule number within part
    
    /// Vector metadata
    std::vector<Float32> centroid;  // Representative vector for granule
    std::vector<Float32> bounds_min;// Min value for each dimension
    std::vector<Float32> bounds_max;// Max value for each dimension
    
    /// Statistics for pruning
    Float32 max_distance_to_centroid;  // Max L2 distance of any vector to centroid
    UInt64 num_vectors;                // Number of vectors in this granule
    
    /// Index in table-level HNSW graph
    UInt32 hnsw_node_id;
    
    /// Serialization helper
    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};

/// Metadata for all granules of a single part
struct PartVectorIndexMetadata
{
    /// Part identification
    String part_name;
    MergeTreeDataPartPtr part;  // Reference to the actual part
    
    /// Granule metadata
    std::vector<GranuleVectorMetadata> granules;
    
    /// Index parameters (for validation)
    String index_name;
    String column_name;
    String distance_function;  // "L2Distance" or "cosineDistance"
    UInt32 dimension;          // Vector dimension
    
    /// Build information
    UInt64 index_version;
    time_t last_built_time;
    
    /// Serialization helper
    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
};

/// Index configuration (constant across all metadata)
struct TableVectorIndexConfig
{
    String index_name;
    String column_name;
    String distance_function;
    UInt32 dimension;
    
    /// HNSW parameters
    String quantization;          // "bf16", "f32", etc.
    UInt32 hnsw_m;               // Connections per node
    UInt32 hnsw_ef_construction;  // Construction expansion
    UInt32 hnsw_ef_search;        // Search expansion
    
    /// Index granularity
    UInt64 index_granularity;
};

/// Main table-level vector index manager
/// Responsible for:
/// - Storing and managing per-granule metadata
/// - Selecting candidate granules for a query vector
/// - Coordinating with per-granule indexes
class MergeTreeTableVectorIndex
{
public:
    using Ptr = std::shared_ptr<MergeTreeTableVectorIndex>;
    
    explicit MergeTreeTableVectorIndex(const TableVectorIndexConfig & config);
    ~MergeTreeTableVectorIndex() = default;
    
    /// Configuration access
    const TableVectorIndexConfig & getConfig() const { return config_; }
    
    /// Part management
    /// Add metadata for a part's granules
    void addPartMetadata(const PartVectorIndexMetadata & metadata);
    
    /// Remove metadata for a part (called when part is deleted)
    void removePartMetadata(const String & part_name);
    
    /// Update metadata for a part's granules (after merge)
    void updatePartMetadata(const String & part_name, const PartVectorIndexMetadata & metadata);
    
    /// Query interface - Phase 1A: Metadata-based selection
    /// Select candidate granules that might contain nearest neighbors
    /// Returns: vector of {part_name, granule_id} pairs, sorted by score descending
    /// max_candidates: maximum number of candidates to return
    std::vector<std::pair<String, UInt32>> selectCandidateGranules(
        const std::vector<Float64> & query_vector,
        const String & distance_function,
        UInt32 max_candidates = 100,
        Float32 score_threshold = 0.5f) const;
    
    /// Statistics and monitoring
    UInt32 getTotalGranules() const;
    UInt32 getTotalParts() const;
    UInt64 getTotalVectors() const;
    size_t getMemoryUsage() const;
    time_t getLastBuildTime() const;
    
    /// Thread-safe access to metadata
    const std::vector<GranuleVectorMetadata> & getGranuleMetadataForPart(
        const String & part_name) const;
    
    /// Persistence
    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);
    
    /// Validation
    bool validateConfiguration(
        const String & column_name,
        const String & distance_function,
        UInt32 dimension) const;

private:
    /// Helper to compute similarity score between query and centroid
    Float32 computeSimilarityScore(
        const std::vector<Float64> & query_vector,
        const std::vector<Float32> & centroid,
        const String & distance_function) const;
    
    /// Helper to convert Float64 query to Float32 centroid
    std::vector<Float32> convertQueryVector(
        const std::vector<Float64> & query_vector) const;
    
    /// Configuration
    TableVectorIndexConfig config_;
    
    /// Per-part metadata storage
    std::map<String, PartVectorIndexMetadata> part_metadata_;
    
    /// Thread safety
    mutable std::shared_mutex metadata_mutex_;
    
    /// Logging
    LoggerPtr logger;
};

using MergeTreeTableVectorIndexPtr = std::shared_ptr<MergeTreeTableVectorIndex>;

} // namespace DB
