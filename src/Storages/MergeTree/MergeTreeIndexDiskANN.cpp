#include <chrono>
#include <filesystem>
#include <memory>
#include <parameters.h>
#include <utils.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include "KeyCondition.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/IAST_fwd.h"

#include <Parsers/ASTFunction.h>
#include <Poco/Logger.h>

#include <Storages/MergeTree/MergeTreeIndexDiskANN.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INCORRECT_QUERY;
}

namespace detail
{

void saveDataPoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints, WriteBuffer & out)
{
    uint32_t num_of_points = static_cast<uint32_t>(datapoints.size()) / dimensions;

    out.write(reinterpret_cast<const char*>(&num_of_points), sizeof(num_of_points));
    out.write(reinterpret_cast<const char*>(&dimensions), sizeof(dimensions));

    for (float datapoint : datapoints)
    {
        out.write(reinterpret_cast<const char*>(&datapoint), sizeof(DiskANNValue));
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Saved {} points", num_of_points);
}

DiskANNIndexPtr constructIndexFromDatapoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints)
{
    if (datapoints.empty())
    {
        throw Exception("Trying to construct index with no datapoints", ErrorCodes::LOGICAL_ERROR);
    }

    if (dimensions == 0)
    {
        throw Exception("Trying to construct index with 0 dimenstions", ErrorCodes::LOGICAL_ERROR);
    }

    if (datapoints.size() % dimensions != 0)
    {
        throw Exception("Datapoints data broken", ErrorCodes::LOGICAL_ERROR);
    }

    uint32_t num_of_points = datapoints.size() / dimensions;

    try
    {
        return std::make_shared<DiskANNIndex>(
            diskann::Metric::L2,
            datapoints.data(),
            num_of_points,
            dimensions
        );
    }
    catch (diskann::ANNException& e)
    {
        throw Exception(e.message(), ErrorCodes::UNKNOWN_EXCEPTION);
    }
}

struct DiskANNSearchResult
{
    std::vector<float> distances;
    std::vector<uint64_t> indicies;
};

DiskANNSearchResult getDistancesToVector(
    std::vector<float> target_vector,
    size_t neighbours_to_search,
    std::shared_ptr<MergeTreeIndexGranuleDiskANN> granule
)
{
    size_t search_list_size = neighbours_to_search * 3;

    auto disk_ann_index = std::dynamic_pointer_cast<DiskANNIndex>(granule->base_index);

    target_vector.resize(ROUND_UP(target_vector.size(), 8));
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Searching for vector of dim {}", target_vector.size());

    // Will be populated by diskann
    std::vector<float> distances(neighbours_to_search);
    std::vector<uint64_t> indicies(neighbours_to_search);
    std::vector<unsigned> init_ids{};

    try
    {
        disk_ann_index->search(target_vector.data(), neighbours_to_search, search_list_size, init_ids,
            indicies.data(), distances.data());
    }
    catch (diskann::ANNException& e)
    {
        throw Exception(e.message(), ErrorCodes::UNKNOWN_EXCEPTION);
    }

    DiskANNSearchResult result;
    result.distances = std::move(distances);
    result.indicies = std::move(indicies);

    return result;
}

}

MergeTreeIndexGranuleDiskANN::MergeTreeIndexGranuleDiskANN(const String & index_name_,
    const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranuleDiskANN::MergeTreeIndexGranuleDiskANN(
    const String & index_name_,
    const Block & index_sample_block_,
    DiskANNIndexPtr base_index_,
    uint32_t dimensions_,
    std::vector<DiskANNValue> datapoints_
)
    : dimensions(dimensions_)
    , datapoints(std::move(datapoints_))
    , index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , base_index(base_index_)
{
}

uint64_t MergeTreeIndexGranuleDiskANN::calculateIndexSize() const
{
    uint64_t index_size = 0;

    index_size += sizeof(uint64_t) + 2 * sizeof(unsigned);

    for (unsigned i = 0; i < base_index->_nd + base_index->_num_frozen_pts; i++)
    {
      unsigned gk = static_cast<unsigned>(base_index->_final_graph[i].size());
      index_size += sizeof(unsigned) + gk * sizeof(unsigned);
    }

    return index_size;
}

void MergeTreeIndexGranuleDiskANN::serializeBinary(WriteBuffer & out) const
{
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Saving Vamana index: saving datapoints...");

    if (!dimensions.has_value())
    {
        throw Exception("Dimensions parameter was not got, despite having data", ErrorCodes::LOGICAL_ERROR);
    }
    detail::saveDataPoints(dimensions.value(), datapoints, out);
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Datapoints saved.");

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Saving Vamana index itself...");
    uint64_t total_gr_edges = 0;

    uint64_t index_size = calculateIndexSize();
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Index size: {}", index_size);
    out.write(reinterpret_cast<char*>(&index_size), sizeof(uint64_t));
    out.write(reinterpret_cast<char*>(&base_index->_width), sizeof(unsigned));
    out.write(reinterpret_cast<char*>(&base_index->_ep), sizeof(unsigned));

    for (size_t i = 0; i < base_index->_nd + base_index->_num_frozen_pts; i++)
    {
      unsigned gk = static_cast<unsigned>(base_index->_final_graph[i].size());
      out.write(reinterpret_cast<char*>(&gk), sizeof(unsigned));
      out.write(reinterpret_cast<char*>(base_index->_final_graph[i].data()), gk * sizeof(unsigned));
      total_gr_edges += gk;
    }

    LOG_DEBUG(
        &Poco::Logger::get("DiskANN"),
        "Saving Vamana index done! Avg degree: {}",
        (static_cast<float>(total_gr_edges)) / (static_cast<float>(base_index->_nd + base_index->_num_frozen_pts))
    );
}

void MergeTreeIndexGranuleDiskANN::deserializeBinary(ReadBuffer & in, MergeTreeIndexVersion /*version*/)
{
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Loading datapoints in deserialize...");

    uint32_t num_of_points = 0;
    uint32_t dims = 0;
    in.read(reinterpret_cast<char*>(&num_of_points), sizeof(num_of_points));
    in.read(reinterpret_cast<char*>(&dims), sizeof(dims));

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "num_of_points={}, dims={}", num_of_points, dims);

    datapoints.resize(num_of_points * dims);
    in.read(reinterpret_cast<char*>(datapoints.data()), sizeof(DiskANNValue) * num_of_points * dims);

    if (num_of_points * dims != datapoints.size())
    {
        LOG_ERROR(
            &Poco::Logger::get("DiskANN"),
            "num_of_points * dims != datapoints.size(); {} * {} != {}.",
            num_of_points, dims, datapoints.size());
        throw Exception("Bad datapoints read", ErrorCodes::LOGICAL_ERROR);
    }

    base_index = detail::constructIndexFromDatapoints(dims, datapoints);

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Got datapoints: {}. Constructed the index object", datapoints.size());
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Loading Vamana index...");

    uint64_t expected_file_size;
    in.read(reinterpret_cast<char*>(&expected_file_size), sizeof(uint64_t));
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Expected index size: {}", expected_file_size);

    in.read(reinterpret_cast<char*>(&base_index->_width), sizeof(unsigned));
    in.read(reinterpret_cast<char*>(&base_index->_ep), sizeof(unsigned));

    assert(base_index->_final_graph.empty());

    size_t cc = 0;
    unsigned nodes = 0;
    while (!in.eof())
    {
        unsigned k;
        in.read(reinterpret_cast<char*>(&k), sizeof(unsigned));
        if (in.eof())
            break;
        cc += k;
        ++nodes;
        std::vector<unsigned> tmp(k);
        in.read(reinterpret_cast<char*>(tmp.data()), k * sizeof(unsigned));
        base_index->_final_graph.emplace_back(tmp);
        if (nodes >= datapoints.size() / dims)
        {
            break;
        }
    }

    assert(nodes == base_index->_final_graph.size());

    if (base_index->_final_graph.size() != base_index->_nd)
    {
        LOG_ERROR(
            &Poco::Logger::get("DiskANN"), "Mismatch in "
            "number of points. Graph has {} points and loaded dataset has {} points.",
            base_index->_final_graph.size(), base_index->_nd
        );
        throw Exception("Number of points mismatch", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "..done. Index has {} nodes and {} out-edges", nodes, cc);
}

MergeTreeIndexAggregatorDiskANN::MergeTreeIndexAggregatorDiskANN(const String & index_name_,
    const Block & index_sample_block_, uint16_t num_threads_, float alpha_, uint16_t R_, uint16_t L_, uint16_t C_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , num_threads(num_threads_)
    , alpha(alpha_)
    , R(R_)
    , L(L_)
    , C(C_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorDiskANN::getGranuleAndReset()
{
    if (accumulated_data.empty())
    {
        return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block);
    }

    if (!dimensions.has_value())
    {
        throw Exception("Dimensions parameter was not got, despite having data", ErrorCodes::LOGICAL_ERROR);
    }

    auto base_index = detail::constructIndexFromDatapoints(dimensions.value(), accumulated_data);

    diskann::Parameters paras;

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Setting params: R={} L={} C={} alpha={} num_threads={}", R, L, C, alpha, num_threads);

    paras.Set<unsigned>("R", R);
    paras.Set<unsigned>("L", L);
    paras.Set<unsigned>("C", C);
    paras.Set<float>("alpha", alpha);
    paras.Set<bool>("saturate_graph", true);
    paras.Set<unsigned>("num_threads", num_threads);

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Index parameters set");

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Starting to build DiskANN index");

    try
    {
        base_index->build(paras);
    }
    catch (diskann::ANNException& e)
    {
        throw Exception(e.message(), ErrorCodes::UNKNOWN_EXCEPTION);
    }
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "DiskANN index has been successfully built!");

    return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block, base_index,
                                                          dimensions.value(), std::move(accumulated_data));
}

void MergeTreeIndexAggregatorDiskANN::flattenAccumulatedData(std::vector<std::vector<DiskANNValue>> data)
{
    if (data.empty())
    {
        throw Exception("Dimensionality must be possitive!", ErrorCodes::LOGICAL_ERROR);
    }

    dimensions = data.size();
    accumulated_data.clear();

    for (size_t current_element = 0; current_element < data[0].size(); ++current_element)
    {
        for (size_t dim = 0; dim < dimensions; ++dim)
        {
            accumulated_data.push_back(data[dim][current_element]);
        }
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Flattened the data, size: {};", accumulated_data.size());
}

void MergeTreeIndexAggregatorDiskANN::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    if (index_sample_block.columns() > 1)
    {
        throw Exception("Only one column is supported", ErrorCodes::LOGICAL_ERROR);
    }

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    std::vector<std::vector<Float32>> coords_vector;
    const auto * vectors = typeid_cast<const ColumnTuple *>(column.get());
    for (const auto & inner_column : vectors->getColumns())
    {
        const auto * coords = typeid_cast<const ColumnFloat32 *>(inner_column.get());
        auto v = std::vector<Float32>(coords->getData().begin(), coords->getData().end());
        coords_vector.push_back(std::move(v));
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Got data, dimensions: {};", coords_vector.size());
    flattenAccumulatedData(std::move(coords_vector));

    *pos += rows_read;
}

MergeTreeIndexConditionDiskANN::MergeTreeIndexConditionDiskANN(
    const IndexDescription & /*index*/,
    const SelectQueryInfo & query,
    ContextPtr context)
    : common_condition(query, context)
{
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Built DiskANN Condition");
}

bool MergeTreeIndexConditionDiskANN::alwaysUnknownOrTrue() const
{
    return common_condition.alwaysUnknownOrTrue("L2Distance");
}

bool MergeTreeIndexConditionDiskANN::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::vector<float> target_vec = common_condition.getTargetVector();
    float min_distance = common_condition.getComparisonDistance();

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleDiskANN>(idx_granule);
    auto search_result = detail::getDistancesToVector(target_vec, 1, granule);

    float distance = *std::min_element(search_result.distances.begin(), search_result.distances.end());
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Maybe true on granule distances: {} <? {}", distance, min_distance);

    return distance < min_distance * min_distance;
}

std::vector<size_t> MergeTreeIndexConditionDiskANN::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    uint64_t limit = 1;
    auto limit_length_maybe = common_condition.getLimitCount();
    if (limit_length_maybe.has_value())
    {
        limit = limit_length_maybe.value();
    }

    std::optional<float> comp_dist_maybe = std::nullopt;
    if (common_condition.queryHasWhereClause())
    {
        comp_dist_maybe = common_condition.getComparisonDistance();
    }

    std::vector<float> target_vec = common_condition.getTargetVector();

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleDiskANN>(idx_granule);
    if (!granule)
    {
        throw Exception(
            "DiskANN index condition got a granule with the wrong type.",
            ErrorCodes::LOGICAL_ERROR
        );
    }

    auto search_result = detail::getDistancesToVector(target_vec, limit, granule);

    // Temporary hard-coded constant
    const size_t granule_size = 8192;

    std::unordered_set<size_t> useful_granules;
    for (size_t i = 0; i < limit; ++i)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Distance: {}", search_result.distances[i]);

        if (comp_dist_maybe.has_value() &&
            search_result.distances[i] >= comp_dist_maybe.value() * comp_dist_maybe.value())
        {
            break;
        }

        useful_granules.insert(search_result.indicies[i] / granule_size);
    }

    std::vector<size_t> useful_granules_vec;
    useful_granules_vec.reserve(useful_granules.size());
    for (auto idx : useful_granules)
    {
        useful_granules_vec.push_back(idx);
    }

    return useful_granules_vec;
}

MergeTreeIndexGranulePtr MergeTreeIndexDiskANN::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleDiskANN>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexDiskANN::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorDiskANN>(index.name, index.sample_block, num_threads, alpha, R, L, C);
}

MergeTreeIndexConditionPtr MergeTreeIndexDiskANN::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionDiskANN>(index, query, context);
};

MergeTreeIndexFormat MergeTreeIndexDiskANN::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr diskANNIndexCreator(
    const IndexDescription & index)
{
    std::vector<std::string> param_values;

    uint16_t num_threads = 1;
    float alpha = 1.2f;
    uint16_t R = 90;
    uint16_t L = 150;
    uint16_t C = 1500;

    for (size_t argument_id = 0; argument_id < index.arguments.size(); ++argument_id)
    {
        switch(argument_id)
        {
            case DiskANNArguments::NUM_THREADS:
                num_threads = index.arguments[argument_id].get<uint16_t>();
                break;
            case DiskANNArguments::ALPHA:
                alpha = index.arguments[argument_id].get<float>();
                break;
            case DiskANNArguments::R:
                R = index.arguments[argument_id].get<uint16_t>();
                break;
            case DiskANNArguments::L:
                L = index.arguments[argument_id].get<uint16_t>();
                break;
            case DiskANNArguments::C:
                C = index.arguments[argument_id].get<uint16_t>();
                break;
        }
    }

    return std::make_shared<MergeTreeIndexDiskANN>(index, num_threads, alpha, R, L, C);
}

void diskANNIndexValidator(const IndexDescription & index, bool /* attach */)
{
    if (index.arguments.size() > 5)
    {
        throw Exception("DiskANN index cannot have more than 5 arguments", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.arguments.size() > DiskANNArguments::NUM_THREADS &&
        index.arguments[DiskANNArguments::NUM_THREADS].getType() != Field::Types::UInt64)
    {
        throw Exception("DiskANN threads argument must be a positive integer", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.arguments.size() > DiskANNArguments::L &&
        index.arguments[DiskANNArguments::L].getType() != Field::Types::UInt64)
    {
        throw Exception("DiskANN L argument must be a positive integer", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.arguments.size() > DiskANNArguments::R &&
        index.arguments[DiskANNArguments::R].getType() != Field::Types::UInt64)
    {
        throw Exception("DiskANN R argument must be a positive integer", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.arguments.size() > DiskANNArguments::C &&
        index.arguments[DiskANNArguments::C].getType() != Field::Types::UInt64)
    {
        throw Exception("DiskANN C argument must be a positive integer", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.arguments.size() > DiskANNArguments::ALPHA &&
        index.arguments[DiskANNArguments::ALPHA].getType() != Field::Types::Float64)
    {
        throw Exception("DiskANN alpha argument must be a positive float", ErrorCodes::INCORRECT_QUERY);
    }
}

}
