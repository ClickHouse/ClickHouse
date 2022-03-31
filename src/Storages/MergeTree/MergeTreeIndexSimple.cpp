#include <filesystem>
#include <memory>
#include <parameters.h>
#include <utils.h>

#include <Storages/MergeTree/MergeTreeIndexSimple.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>

#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleDiskANN::MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_) 
{}

MergeTreeIndexGranuleDiskANN::MergeTreeIndexGranuleDiskANN(
    const String & index_name_, 
    const Block & index_sample_block_, 
    DiskANNIndexPtr base_index_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , base_index(base_index_)
{
}

MergeTreeIndexAggregatorDiskANN::MergeTreeIndexAggregatorDiskANN(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

void MergeTreeIndexAggregatorDiskANN::dumpDataToFile(std::string_view filename)
{
    if (!dimensions.has_value()) {
        throw Exception("Dimensions parameter was not got, despite having data", ErrorCodes::LOGICAL_ERROR);
    }

    if (static_cast<uint32_t>(accumulated_data.size()) % dimensions.value() != 0) {
        throw Exception("Corrupted data, datasize % dim != 0", ErrorCodes::LOGICAL_ERROR);
    }

    uint32_t num_of_points = static_cast<uint32_t>(accumulated_data.size()) / dimensions.value();
    
    std::ofstream out(filename, std::ios::binary | std::ios::out);
    if (!out.is_open()) {
        throw Exception("Couldn't create a file to build DiskANN index", ErrorCodes::LOGICAL_ERROR);
    }

    out.write(reinterpret_cast<const char*>(&num_of_points), sizeof(num_of_points));
    out.write(reinterpret_cast<const char*>(&dimensions.value()), sizeof(dimensions.value()));

    for (float data_point : accumulated_data) {
        out.write(reinterpret_cast<const char*>(&data_point), sizeof(Float32));
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Data saved");
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorDiskANN::getGranuleAndReset()
{
    if (accumulated_data.empty()) {
        return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block);
    }

    String datapoints_filename = "diskann_datapoints.bin";
    
    dumpDataToFile(datapoints_filename);

    diskann::Parameters paras;
    paras.Set<unsigned>("R", 60);
    paras.Set<unsigned>("L", 75);
    paras.Set<unsigned>("C", 750);
    paras.Set<float>("alpha", 1.2);
    paras.Set<bool>("saturate_graph", false);
    paras.Set<unsigned>("num_threads", 1);

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Index parameters set");

    auto index_base = std::make_shared<MergeTreeIndexGranuleDiskANN::DiskANNIndex>(
        diskann::Metric::L2,
        datapoints_filename.c_str()
    );
    
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Starting to build DiskANN index");
    index_base->build(paras);
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "DiskANN index has been successfully built!");

    return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block, index_base);
}

void MergeTreeIndexAggregatorDiskANN::flattenAccumulatedData(std::vector<std::vector<Value>> data) {
    if (data.empty()) {
        throw Exception("Dimensionality must be possitive!", ErrorCodes::LOGICAL_ERROR);
    }
    
    dimensions = data.size();
    accumulated_data.clear();

    for (size_t current_element = 0; current_element < data[0].size(); ++current_element) {
        for (size_t dim = 0; dim < dimensions; ++dim) {
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

    if (index_sample_block.columns() > 1) {
        throw Exception("Only one column is supported", ErrorCodes::LOGICAL_ERROR);
    }

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);
    
    std::vector<std::vector<Float32>> coords_vector;
    const auto * vectors = typeid_cast<const ColumnTuple *>(column.get());
    for (const auto & inner_column : vectors->getColumns()) {
        const auto * coords = typeid_cast<const ColumnFloat32 *>(inner_column.get());
        auto v = std::vector<Float32>(coords->getData().begin(), coords->getData().end());
        coords_vector.push_back(std::move(v));
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Got data, dimensions: {};", coords_vector.size());
    flattenAccumulatedData(std::move(coords_vector));

    *pos += rows_read;
}

bool MergeTreeIndexConditionDiskANN::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleDiskANN> granule
        = std::dynamic_pointer_cast<MergeTreeIndexGranuleDiskANN>(idx_granule);
    if (!granule)
        throw Exception(
            "DiskANN index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexDiskANN::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleDiskANN>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexDiskANN::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorDiskANN>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexDiskANN::createIndexCondition(
    const SelectQueryInfo & /*query*/, ContextPtr /*context*/) const
{
    return std::make_shared<MergeTreeIndexConditionDiskANN>();
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
    return std::make_shared<MergeTreeIndexDiskANN>(index);
}

void diskANNIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
