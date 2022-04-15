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

#include <base/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace detail {

void saveDataPoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints, WriteBuffer & out) {
    uint32_t num_of_points = static_cast<uint32_t>(datapoints.size()) / dimensions;

    out.write(reinterpret_cast<const char*>(&num_of_points), sizeof(num_of_points));
    out.write(reinterpret_cast<const char*>(&dimensions), sizeof(dimensions));

    for (float data_point : datapoints) {
        out.write(reinterpret_cast<const char*>(&data_point), sizeof(Float32));
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Saved {} points", num_of_points);
}

DiskANNIndexPtr constructIndexFromDatapoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints) {
    if (datapoints.empty()) {
        throw Exception("Trying to construct index with no datapoints", ErrorCodes::LOGICAL_ERROR);
    }

    String datapoints_filename = "diskann_datapoints.bin";
    WriteBufferFromFile write_buffer(datapoints_filename);
    detail::saveDataPoints(dimensions, datapoints, write_buffer);
    write_buffer.close();

    return std::make_shared<DiskANNIndex>(
        diskann::Metric::L2,
        datapoints_filename.c_str()
    );
}

}

MergeTreeIndexGranuleDiskANN::MergeTreeIndexGranuleDiskANN(const String & index_name_, const Block & index_sample_block_)
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

uint64_t MergeTreeIndexGranuleDiskANN::calculateIndexSize() const {
    uint64_t index_size = 0;

    index_size += sizeof(uint64_t) + 2 * sizeof(unsigned);
    
    for (unsigned i = 0; i < base_index->_nd + base_index->_num_frozen_pts; i++) {
      unsigned gk = static_cast<unsigned>(base_index->_final_graph[i].size());
      index_size += sizeof(unsigned) + gk * sizeof(unsigned);
    }

    return index_size;
}

void MergeTreeIndexGranuleDiskANN::serializeBinary(WriteBuffer & out) const {
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Saving Vamana index: saving datapoints...");

    if (!dimensions.has_value()) {
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

    for (size_t i = 0; i < base_index->_nd + base_index->_num_frozen_pts; i++) {
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

void MergeTreeIndexGranuleDiskANN::deserializeBinary(ReadBuffer & in, MergeTreeIndexVersion /*version*/) {
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Loading datapoints in deserialize...");

    uint32_t num_of_points = 0;
    uint32_t dims = 0;
    in.read(reinterpret_cast<char*>(&num_of_points), sizeof(num_of_points));
    in.read(reinterpret_cast<char*>(&dims), sizeof(dims));

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "num_of_points={}, dims={}", num_of_points, dims);

    datapoints.resize(num_of_points * dims);
    in.read(reinterpret_cast<char*>(datapoints.data()), sizeof(DiskANNValue) * num_of_points * dims);

    if (num_of_points * dims != datapoints.size()) {
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
    while (!in.eof()) {
        unsigned k;
        in.read(reinterpret_cast<char*>(&k), sizeof(unsigned));
        if (in.eof())
            break;
        cc += k;
        ++nodes;
        std::vector<unsigned> tmp(k);
        in.read(reinterpret_cast<char*>(tmp.data()), k * sizeof(unsigned));
        base_index->_final_graph.emplace_back(tmp);
        if (nodes >= datapoints.size() / dims) {
            break;
        }
    }
    
    assert(nodes == base_index->_final_graph.size());

    if (base_index->_final_graph.size() != base_index->_nd) {
        LOG_ERROR(
            &Poco::Logger::get("DiskANN"), "Mismatch in "
            "number of points. Graph has {} points and loaded dataset has {} points.", 
            base_index->_final_graph.size(), base_index->_nd
        );
        throw Exception("Number of points mismatch", ErrorCodes::LOGICAL_ERROR);
    }

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "..done. Index has {} nodes and {} out-edges", nodes, cc);
}

MergeTreeIndexAggregatorDiskANN::MergeTreeIndexAggregatorDiskANN(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorDiskANN::getGranuleAndReset()
{
    if (accumulated_data.empty()) {
        return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block);
    }

    if (!dimensions.has_value()) {
        throw Exception("Dimensions parameter was not got, despite having data", ErrorCodes::LOGICAL_ERROR);
    }

    auto base_index = detail::constructIndexFromDatapoints(dimensions.value(), accumulated_data);

    diskann::Parameters paras;
    paras.Set<unsigned>("R", 100);
    paras.Set<unsigned>("L", 150);
    paras.Set<unsigned>("C", 750);
    paras.Set<float>("alpha", 1.2);
    paras.Set<bool>("saturate_graph", true);
    paras.Set<unsigned>("num_threads", 1);

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Index parameters set");
    
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Starting to build DiskANN index");
    base_index->build(paras);
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "DiskANN index has been successfully built!");

    return std::make_shared<MergeTreeIndexGranuleDiskANN>(index_name, index_sample_block, base_index, dimensions.value(), std::move(accumulated_data));
}

void MergeTreeIndexAggregatorDiskANN::flattenAccumulatedData(std::vector<std::vector<DiskANNValue>> data) {
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

MergeTreeIndexConditionDiskANN::MergeTreeIndexConditionDiskANN(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
{
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Building RPN");
    // Build Reverse Polish notation from the query
    RPN rpn = buildRPN(query, context); 

    // Match RPN with the pattern of the query for this type of index
    // and extract expression data for the future usage of the index
    matchRPN(rpn);
}

bool MergeTreeIndexConditionDiskANN::alwaysUnknownOrTrue() const
{
    // In matchRPN() function we populate expession field in case of the success
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Is always unknown or true? {}", !expression.has_value());

    return !expression.has_value();
}

bool MergeTreeIndexConditionDiskANN::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    // TODO: Change assert to the exception
    assert(expression.has_value());

    [[maybe_unused]]  std::vector<float> target_vec = expression.value().target;
    [[maybe_unused]] float min_distance = expression.value().distance;

    // Number of target vectors
    [[maybe_unused]]  size_t n = 5;

    // Number of NN to search
    [[maybe_unused]]  size_t k = n;

    // Will be populated by diskann
    [[maybe_unused]] std::vector<float> distances(n);
    [[maybe_unused]] std::vector<uint64_t> indicies(n); 
    [[maybe_unused]] std::vector<unsigned> init_ids{};

    [[maybe_unused]] auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleDiskANN>(idx_granule);
    [[maybe_unused]] auto disk_ann_index = std::dynamic_pointer_cast<DiskANNIndex>(granule->base_index);

    target_vec.resize(ROUND_UP(target_vec.size(), 8));
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Searching for vector of dim {}", target_vec.size());

    if (target_vec.empty()) {
        return true;
    }

    disk_ann_index->search(target_vec.data(), k, n, init_ids, indicies.data(), distances.data());

    float distance = *std::min_element(distances.begin(), distances.end());
    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Maybe true on granule distances: {} <? {}", distance, min_distance);

    /*
    When using L2, DiskANN returns not the exact distance, but distance squared, that's why
    we have to rise given distance to the power of 2. 
    
    Also, I don't know why, but DiskANN is not able to give precise answer to ANN task,
    maybe it depends on hyperparameters and fine-tuning is needed. Nevertheless, temporary
    ERROR_COEF is added to minimise the likelihood of false negative result
    */
    const static float ERROR_COEF = 10.f;
    return distance < min_distance * min_distance * ERROR_COEF;
}

MergeTreeIndexConditionDiskANN::RPN MergeTreeIndexConditionDiskANN::buildRPN(const SelectQueryInfo & query, ContextPtr context)
{
    RPN rpn;

    // Get block_with_constants for the future usage from query
    block_with_constants = KeyCondition::getBlockWithConstants(query.query, query.syntax_analyzer_result, context);

    const auto & select = query.query->as<ASTSelectQuery &>();

    // Sometimes our ANN expression in where can be placed in prewhere section
    // In this case we populate RPN from both source, but it can be dangerous in case
    // of some additional expressions in our query
    // We can either check prewhere or where, either match independently where and
    // prewhere
    // TODO: Need to think
    if (select.where()) 
    {
        traverseAST(select.where(), rpn);
    }
    if (select.prewhere())
    {
        traverseAST(select.prewhere(), rpn);
    }

    // Return prefix rpn, so reverse the result
    std::reverse(rpn.begin(), rpn.end());
    return rpn;
}

void MergeTreeIndexConditionDiskANN::traverseAST(const ASTPtr & node, RPN & rpn) 
{
    RPNElement element;

    // We need to go deeper only if we have ASTFunction in this node
    if (const auto * func = node->as<ASTFunction>()) 
    {
        const ASTs & args = func->arguments->children;

        // Traverse children
        for (const auto & arg : args) 
        {
            traverseAST(arg, rpn);
        }
    } 

    // Extract information about current node and populate it in the element
    if (!traverseAtomAST(node, element)) {
        // If we cannot identify our node type
        element.function = RPNElement::FUNCTION_UNKNOWN;
    }

    rpn.emplace_back(std::move(element)); 
}

bool MergeTreeIndexConditionDiskANN::traverseAtomAST(const ASTPtr & node, RPNElement & out) {
    // Firstly check if we have contants behind the node
    {
        Field const_value;
        DataTypePtr const_type;


        if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
        {
            /// Check constant type (use Float64 because all Fields implementation contains Float64 (for Float32 too))
            if (const_value.getType() == Field::Types::Float64)
            {
                out.function = RPNElement::FUNCTION_FLOAT_LITERAL;
                out.literal.emplace(const_value.get<Float32>());

                return true;
            }
        }
    }

    // Match function naming with a type
    if (const auto * function = node->as<ASTFunction>())
    {
        // TODO: Add support for other metrics 
        if (function->name == "L2Distance") 
        {
            out.function = RPNElement::FUNCTION_DISTANCE;
        } 
        else if (function->name == "tuple") 
        {
            out.function = RPNElement::FUNCTION_TUPLE;
        } 
        else if (function->name == "less") 
        {
            out.function = RPNElement::FUNCTION_LESS;
        } 
        else 
        {
            return false;
        }

        return true;
    }
    // Match identifier 
    else if (const auto * identifier = node->as<ASTIdentifier>()) 
    {
        out.function = RPNElement::FUNCTION_IDENTIFIER;
        out.identifier.emplace(identifier->name());

        return true;
    } 

    return false;
}

bool MergeTreeIndexConditionDiskANN::matchRPN(const RPN & rpn) 
{
    
    // Can we place it outside the function? 
    // Use for match the rpn
    // Take care of matching tuples (because it can contains arbitary number of fields)
    RPN prefix_template_rpn{
        RPNElement{RPNElement::FUNCTION_LESS}, 
        RPNElement{RPNElement::FUNCTION_FLOAT_LITERAL}, 
        RPNElement{RPNElement::FUNCTION_DISTANCE}, 
        RPNElement{RPNElement::FUNCTION_TUPLE}, 
        RPNElement{RPNElement::FUNCTION_IDENTIFIER}, 
    };

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Matching RPN");

    // Placeholders for the extracted data
    Target target_vec;
    float distance = 0;

    size_t rpn_idx = 0;
    size_t template_idx = 0;

    // TODO: Should we check what we have the same size of RPNs?
    // If we wand to support complex expressions, we will not check it
    while (rpn_idx < rpn.size() && template_idx < prefix_template_rpn.size()) 
    {
        const auto & element = rpn[rpn_idx];
        const auto & template_element = prefix_template_rpn[template_idx];

        if (element.function != template_element.function) 
        {
            LOG_DEBUG(&Poco::Logger::get("DiskANN"), "Bad RPN :(");
            
            return false;
        }

        if (element.function == RPNElement::FUNCTION_FLOAT_LITERAL) 
        {
            assert(element.literal.has_value());
            auto value = element.literal.value();

            distance = value; 
        }

        if (element.function == RPNElement::FUNCTION_TUPLE) 
        {
            // TODO: Better tuple extraction
            // Extract target vec
            ++rpn_idx;
            while (rpn_idx < rpn.size()) {
                if (rpn[rpn_idx].function == RPNElement::FUNCTION_FLOAT_LITERAL) 
                {
                    // Extract tuple element
                    assert(rpn[rpn_idx].literal.has_value());
                    auto value = rpn[rpn_idx].literal.value();
                    target_vec.push_back(value);
                    ++rpn_idx;
                } else {
                    ++template_idx;
                    break;
                } 
            }
            continue;
        }

        if (element.function == RPNElement::FUNCTION_IDENTIFIER) 
        {
            // TODO: Check that we have the same columns
        }

        ++rpn_idx;
        ++template_idx;
    }

    expression.emplace(ANNExpression{
        .target = std::move(target_vec),
        .distance = distance,
    });

    LOG_DEBUG(&Poco::Logger::get("DiskANN"), "RPN satisfied");

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
    return std::make_shared<MergeTreeIndexDiskANN>(index);
}

void diskANNIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
