#include <Storages/MergeTree/MergeTreeTableVectorIndex.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/logger_useful.h>
#include <cmath>
#include <algorithm>

namespace DB
{

namespace
{
    constexpr UInt64 SERIALIZATION_VERSION = 1;
    constexpr const char * COMPONENT_NAME = "MergeTreeTableVectorIndex";
}

/// GranuleVectorMetadata serialization
void GranuleVectorMetadata::serialize(WriteBuffer & buf) const
{
    writeStringBinary(part_name, buf);
    writeIntBinary(granule_id, buf);
    
    // Serialize centroid
    writeIntBinary(static_cast<UInt32>(centroid.size()), buf);
    for (Float32 val : centroid)
        writeFloatBinary(val, buf);
    
    // Serialize bounds
    writeIntBinary(static_cast<UInt32>(bounds_min.size()), buf);
    for (Float32 val : bounds_min)
        writeFloatBinary(val, buf);
    
    writeIntBinary(static_cast<UInt32>(bounds_max.size()), buf);
    for (Float32 val : bounds_max)
        writeFloatBinary(val, buf);
    
    writeFloatBinary(max_distance_to_centroid, buf);
    writeIntBinary(num_vectors, buf);
    writeIntBinary(hnsw_node_id, buf);
}

void GranuleVectorMetadata::deserialize(ReadBuffer & buf)
{
    readStringBinary(part_name, buf);
    readIntBinary(granule_id, buf);
    
    // Deserialize centroid
    UInt32 centroid_size;
    readIntBinary(centroid_size, buf);
    centroid.resize(centroid_size);
    for (auto & val : centroid)
        readFloatBinary(val, buf);
    
    // Deserialize bounds
    UInt32 bounds_size;
    readIntBinary(bounds_size, buf);
    bounds_min.resize(bounds_size);
    for (auto & val : bounds_min)
        readFloatBinary(val, buf);
    
    readIntBinary(bounds_size, buf);
    bounds_max.resize(bounds_size);
    for (auto & val : bounds_max)
        readFloatBinary(val, buf);
    
    readFloatBinary(max_distance_to_centroid, buf);
    readIntBinary(num_vectors, buf);
    readIntBinary(hnsw_node_id, buf);
}

/// PartVectorIndexMetadata serialization
void PartVectorIndexMetadata::serialize(WriteBuffer & buf) const
{
    writeStringBinary(part_name, buf);
    writeStringBinary(index_name, buf);
    writeStringBinary(column_name, buf);
    writeStringBinary(distance_function, buf);
    writeIntBinary(dimension, buf);
    writeIntBinary(index_version, buf);
    writeIntBinary(static_cast<UInt64>(last_built_time), buf);
    
    // Serialize granules
    writeIntBinary(static_cast<UInt32>(granules.size()), buf);
    for (const auto & granule : granules)
        granule.serialize(buf);
}

void PartVectorIndexMetadata::deserialize(ReadBuffer & buf)
{
    readStringBinary(part_name, buf);
    readStringBinary(index_name, buf);
    readStringBinary(column_name, buf);
    readStringBinary(distance_function, buf);
    readIntBinary(dimension, buf);
    readIntBinary(index_version, buf);
    
    UInt64 time_val;
    readIntBinary(time_val, buf);
    last_built_time = static_cast<time_t>(time_val);
    
    // Deserialize granules
    UInt32 granule_count;
    readIntBinary(granule_count, buf);
    granules.resize(granule_count);
    for (auto & granule : granules)
        granule.deserialize(buf);
}

/// MergeTreeTableVectorIndex implementation
MergeTreeTableVectorIndex::MergeTreeTableVectorIndex(const TableVectorIndexConfig & config)
    : config_(config)
    , logger(getLogger(COMPONENT_NAME))
{
    LOG_TRACE(logger, "Created table-level vector index for column {} with dimension {}",
              config_.column_name, config_.dimension);
}

void MergeTreeTableVectorIndex::addPartMetadata(const PartVectorIndexMetadata & metadata)
{
    std::unique_lock lock(metadata_mutex_);
    
    if (part_metadata_.count(metadata.part_name) > 0)
    {
        LOG_WARNING(logger, "Part {} already exists in index, updating", metadata.part_name);
    }
    
    part_metadata_[metadata.part_name] = metadata;
    
    LOG_DEBUG(logger, "Added metadata for part {} with {} granules",
              metadata.part_name, metadata.granules.size());
}

void MergeTreeTableVectorIndex::removePartMetadata(const String & part_name)
{
    std::unique_lock lock(metadata_mutex_);
    
    auto it = part_metadata_.find(part_name);
    if (it != part_metadata_.end())
    {
        part_metadata_.erase(it);
        LOG_DEBUG(logger, "Removed metadata for part {}", part_name);
    }
    else
    {
        LOG_WARNING(logger, "Part {} not found in index", part_name);
    }
}

void MergeTreeTableVectorIndex::updatePartMetadata(
    const String & part_name,
    const PartVectorIndexMetadata & metadata)
{
    std::unique_lock lock(metadata_mutex_);
    
    if (part_metadata_.count(part_name) == 0)
    {
        LOG_WARNING(logger, "Part {} not found for update, adding new entry", part_name);
    }
    
    part_metadata_[part_name] = metadata;
    
    LOG_DEBUG(logger, "Updated metadata for part {} with {} granules",
              part_name, metadata.granules.size());
}

std::vector<Float32> MergeTreeTableVectorIndex::convertQueryVector(
    const std::vector<Float64> & query_vector) const
{
    std::vector<Float32> result;
    result.reserve(query_vector.size());
    for (Float64 val : query_vector)
        result.push_back(static_cast<Float32>(val));
    return result;
}

Float32 MergeTreeTableVectorIndex::computeSimilarityScore(
    const std::vector<Float64> & query_vector,
    const std::vector<Float32> & centroid,
    const String & distance_function) const
{
    if (query_vector.size() != centroid.size())
    {
        LOG_WARNING(logger, "Query vector size {} doesn't match centroid size {}",
                    query_vector.size(), centroid.size());
        return 0.0f;
    }
    
    if (distance_function == "L2Distance")
    {
        // Compute L2 distance: sqrt(sum((q_i - c_i)^2))
        Float32 sum_sq = 0.0f;
        for (size_t i = 0; i < query_vector.size(); ++i)
        {
            Float32 diff = static_cast<Float32>(query_vector[i]) - centroid[i];
            sum_sq += diff * diff;
        }
        Float32 l2_distance = std::sqrt(sum_sq);
        
        // Convert distance to similarity score (lower distance = higher score)
        // Use inverse: score = 1 / (1 + distance)
        return 1.0f / (1.0f + l2_distance);
    }
    else if (distance_function == "cosineDistance")
    {
        // Compute cosine distance: 1 - (dot product / (norm_q * norm_c))
        Float32 dot_product = 0.0f;
        Float32 norm_q = 0.0f;
        Float32 norm_c = 0.0f;
        
        for (size_t i = 0; i < query_vector.size(); ++i)
        {
            Float32 q_val = static_cast<Float32>(query_vector[i]);
            Float32 c_val = centroid[i];
            
            dot_product += q_val * c_val;
            norm_q += q_val * q_val;
            norm_c += c_val * c_val;
        }
        
        norm_q = std::sqrt(norm_q);
        norm_c = std::sqrt(norm_c);
        
        if (norm_q < 1e-6f || norm_c < 1e-6f)
            return 0.0f;  // One of vectors is zero
        
        Float32 cosine_similarity = dot_product / (norm_q * norm_c);
        
        // Clamp to [-1, 1] to avoid numerical errors
        cosine_similarity = std::max(-1.0f, std::min(1.0f, cosine_similarity));
        
        // Convert to score: cosine_distance = 1 - cosine_similarity
        // Then score = 1 - cosine_distance = cosine_similarity
        // Normalized to [0, 1]: (cosine_similarity + 1) / 2
        return (cosine_similarity + 1.0f) / 2.0f;
    }
    else
    {
        LOG_WARNING(logger, "Unknown distance function: {}", distance_function);
        return 0.0f;
    }
}

std::vector<std::pair<String, UInt32>> MergeTreeTableVectorIndex::selectCandidateGranules(
    const std::vector<Float64> & query_vector,
    const String & distance_function,
    UInt32 max_candidates,
    Float32 score_threshold) const
{
    std::shared_lock lock(metadata_mutex_);
    
    if (query_vector.size() != config_.dimension)
    {
        LOG_WARNING(logger, "Query vector size {} doesn't match index dimension {}",
                    query_vector.size(), config_.dimension);
        return {};
    }
    
    // Collect all granules with their scores
    std::vector<std::tuple<Float32, String, UInt32>> scored_granules;
    
    for (const auto & [part_name, part_meta] : part_metadata_)
    {
        for (const auto & granule : part_meta.granules)
        {
            Float32 score = computeSimilarityScore(query_vector, granule.centroid, distance_function);
            
            if (score >= score_threshold)
            {
                scored_granules.emplace_back(score, granule.part_name, granule.granule_id);
            }
        }
    }
    
    // Sort by score descending
    std::sort(scored_granules.rbegin(), scored_granules.rend());
    
    // Keep top max_candidates
    if (scored_granules.size() > max_candidates)
        scored_granules.resize(max_candidates);
    
    // Convert to result format
    std::vector<std::pair<String, UInt32>> result;
    for (const auto & [score, part_name, granule_id] : scored_granules)
    {
        result.emplace_back(part_name, granule_id);
    }
    
    LOG_DEBUG(logger, "Selected {} candidate granules from {} total granules",
              result.size(), getTotalGranules());
    
    return result;
}

UInt32 MergeTreeTableVectorIndex::getTotalGranules() const
{
    std::shared_lock lock(metadata_mutex_);
    
    UInt32 total = 0;
    for (const auto & [part_name, part_meta] : part_metadata_)
    {
        total += static_cast<UInt32>(part_meta.granules.size());
    }
    return total;
}

UInt32 MergeTreeTableVectorIndex::getTotalParts() const
{
    std::shared_lock lock(metadata_mutex_);
    return static_cast<UInt32>(part_metadata_.size());
}

UInt64 MergeTreeTableVectorIndex::getTotalVectors() const
{
    std::shared_lock lock(metadata_mutex_);
    
    UInt64 total = 0;
    for (const auto & [part_name, part_meta] : part_metadata_)
    {
        for (const auto & granule : part_meta.granules)
        {
            total += granule.num_vectors;
        }
    }
    return total;
}

size_t MergeTreeTableVectorIndex::getMemoryUsage() const
{
    std::shared_lock lock(metadata_mutex_);
    
    size_t total = 0;
    
    // Rough estimate:
    // - Config: ~200 bytes
    // - Per granule: dimension * 4 (centroid) + dimension * 4 * 2 (bounds) + ~50 (overhead)
    //   = dimension * 12 + 50 bytes
    
    for (const auto & [part_name, part_meta] : part_metadata_)
    {
        total += part_name.size();
        for (const auto & granule : part_meta.granules)
        {
            total += (config_.dimension * 12) + 50;
        }
    }
    
    return total;
}

time_t MergeTreeTableVectorIndex::getLastBuildTime() const
{
    std::shared_lock lock(metadata_mutex_);
    
    time_t latest = 0;
    for (const auto & [part_name, part_meta] : part_metadata_)
    {
        if (part_meta.last_built_time > latest)
            latest = part_meta.last_built_time;
    }
    return latest;
}

const std::vector<GranuleVectorMetadata> & MergeTreeTableVectorIndex::getGranuleMetadataForPart(
    const String & part_name) const
{
    std::shared_lock lock(metadata_mutex_);
    
    auto it = part_metadata_.find(part_name);
    if (it != part_metadata_.end())
    {
        return it->second.granules;
    }
    
    // Return empty vector if part not found
    static const std::vector<GranuleVectorMetadata> empty_vector;
    return empty_vector;
}

void MergeTreeTableVectorIndex::serialize(WriteBuffer & buf) const
{
    std::shared_lock lock(metadata_mutex_);
    
    // Write version
    writeIntBinary(SERIALIZATION_VERSION, buf);
    
    // Write config
    writeStringBinary(config_.index_name, buf);
    writeStringBinary(config_.column_name, buf);
    writeStringBinary(config_.distance_function, buf);
    writeIntBinary(config_.dimension, buf);
    writeStringBinary(config_.quantization, buf);
    writeIntBinary(config_.hnsw_m, buf);
    writeIntBinary(config_.hnsw_ef_construction, buf);
    writeIntBinary(config_.hnsw_ef_search, buf);
    writeIntBinary(config_.index_granularity, buf);
    
    // Write part metadata
    writeIntBinary(static_cast<UInt32>(part_metadata_.size()), buf);
    for (const auto & [part_name, metadata] : part_metadata_)
    {
        metadata.serialize(buf);
    }
}

void MergeTreeTableVectorIndex::deserialize(ReadBuffer & buf)
{
    std::unique_lock lock(metadata_mutex_);
    
    // Read version
    UInt64 version;
    readIntBinary(version, buf);
    if (version != SERIALIZATION_VERSION)
    {
        throw Exception(ErrorCodes::UNSUPPORTED_FORMAT,
                        "Unsupported serialization version: {}", version);
    }
    
    // Read config
    readStringBinary(config_.index_name, buf);
    readStringBinary(config_.column_name, buf);
    readStringBinary(config_.distance_function, buf);
    readIntBinary(config_.dimension, buf);
    readStringBinary(config_.quantization, buf);
    readIntBinary(config_.hnsw_m, buf);
    readIntBinary(config_.hnsw_ef_construction, buf);
    readIntBinary(config_.hnsw_ef_search, buf);
    readIntBinary(config_.index_granularity, buf);
    
    // Read part metadata
    UInt32 part_count;
    readIntBinary(part_count, buf);
    for (UInt32 i = 0; i < part_count; ++i)
    {
        PartVectorIndexMetadata metadata;
        metadata.deserialize(buf);
        part_metadata_[metadata.part_name] = metadata;
    }
}

bool MergeTreeTableVectorIndex::validateConfiguration(
    const String & column_name,
    const String & distance_function,
    UInt32 dimension) const
{
    if (config_.column_name != column_name)
    {
        LOG_WARNING(logger, "Column name mismatch: {} vs {}", config_.column_name, column_name);
        return false;
    }
    
    if (config_.distance_function != distance_function)
    {
        LOG_WARNING(logger, "Distance function mismatch: {} vs {}", config_.distance_function, distance_function);
        return false;
    }
    
    if (config_.dimension != dimension)
    {
        LOG_WARNING(logger, "Dimension mismatch: {} vs {}", config_.dimension, dimension);
        return false;
    }
    
    return true;
}

} // namespace DB
