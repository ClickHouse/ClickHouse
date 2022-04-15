#include <Storages/MergeTree/MergeTreeIndexAnnoy.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>

#include <Poco/Logger.h>
#include <base/logger_useful.h>

#include "Core/Field.h"
#include "Interpreters/Context_fwd.h"
#include "MergeTreeIndices.h"
#include "KeyCondition.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/IAST_fwd.h"
#include "Storages/SelectQueryInfo.h"
#include "base/types.h"


namespace DB
{

namespace Annoy
{

const int NUM_OF_TREES = 20;
const int DIMENSION = 512;

const int32_t LIMIT = 10;

template<typename Dist>
void AnnoyIndexSerialize<Dist>::serialize(WriteBuffer& ostr) const
{
    if (!Base::_built) {
        throw Exception("Annoy Index should be built before serialization", ErrorCodes::LOGICAL_ERROR);
    }
    writeIntBinary(Base::_s, ostr);
    writeIntBinary(Base::_n_items, ostr);
    writeIntBinary(Base::_n_nodes, ostr);
    writeIntBinary(Base::_nodes_size, ostr);
    writeIntBinary(Base::_K, ostr);
    writeIntBinary(Base::_seed, ostr);
    writeVectorBinary(Base::_roots, ostr);
    ostr.write(reinterpret_cast<const char*>(Base::_nodes), Base::_s * Base::_n_nodes);
}

template<typename Dist>
void AnnoyIndexSerialize<Dist>::deserialize(ReadBuffer& istr)
{
    readIntBinary(Base::_s, istr);
    readIntBinary(Base::_n_items, istr);
    readIntBinary(Base::_n_nodes, istr);
    readIntBinary(Base::_nodes_size, istr);
    readIntBinary(Base::_K, istr);
    readIntBinary(Base::_seed, istr);
    readVectorBinary(Base::_roots, istr);
    Base::_nodes = realloc(Base::_nodes, Base::_s * Base::_n_nodes);
    istr.read(reinterpret_cast<char*>(Base::_nodes), Base::_s * Base::_n_nodes);

    Base::_fd = 0;
    // set flags
    Base::_loaded = false;
    Base::_verbose = false;
    Base::_on_disk = false;
    Base::_built = true;
}

}


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleAnnoy::MergeTreeIndexGranuleAnnoy(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(nullptr)
{}

MergeTreeIndexGranuleAnnoy::MergeTreeIndexGranuleAnnoy(
    const String & index_name_, 
    const Block & index_sample_block_,
    AnnoyIndexPtr index_base_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::move(index_base_))
{}

bool MergeTreeIndexGranuleAnnoy::empty() const {
    return !static_cast<bool>(index_base);
}

void MergeTreeIndexGranuleAnnoy::serializeBinary(WriteBuffer & ostr) const
{
    writeIntBinary(index_base->get_f(), ostr); // write dimension
    index_base->serialize(ostr);
}

void MergeTreeIndexGranuleAnnoy::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    int dimension;
    readIntBinary(dimension, istr);
    index_base = std::make_shared<AnnoyIndex>(dimension);
    index_base->deserialize(istr);
}


MergeTreeIndexAggregatorAnnoy::MergeTreeIndexAggregatorAnnoy(const String & index_name_,
                                                                const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::make_shared<AnnoyIndex>(Annoy::DIMENSION))
{}

bool MergeTreeIndexAggregatorAnnoy::empty() const
{
    return index_base->get_n_items() == 0;
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorAnnoy::getGranuleAndReset()
{
    if (empty()) {
        return std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block);
    }

    index_base->build(Annoy::NUM_OF_TREES);
    auto granule = std::make_shared<MergeTreeIndexGranuleAnnoy>(index_name, index_sample_block, index_base);
    index_base = std::make_shared<AnnoyIndex>(Annoy::DIMENSION);
    return granule;
}

void MergeTreeIndexAggregatorAnnoy::update(const Block & block, size_t * pos, size_t limit)
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
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto & column_tuple = typeid_cast<const ColumnTuple*>(column_cut.get());
    const auto & columns = column_tuple->getColumns();

    std::vector<std::vector<Float32>> data{column_tuple->size(), std::vector<Float32>()};
    for (size_t j = 0; j < columns.size(); ++j) {
        const auto& pod_array = typeid_cast<const ColumnFloat32*>(columns[j].get())->getData();
        for (size_t i = 0; i < pod_array.size(); ++i) {
            data[i].push_back(pod_array[i]);
        }
    }
    for (const auto& item : data) {
        index_base->add_item(index_base->get_n_items(), &item[0]);
    }

    *pos += rows_read;
}


MergeTreeIndexConditionAnnoy::MergeTreeIndexConditionAnnoy(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
{
    RPN rpn = buildRPN(query, context);
    matchRPN(rpn);
}


bool MergeTreeIndexConditionAnnoy::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    // TODO: Change assert to the exception
    assert(expression.has_value());

    std::vector<float> target_vec = expression.value().target;
    float min_distance = expression.value().distance;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy>(idx_granule);
    auto annoy = std::dynamic_pointer_cast<Annoy::AnnoyIndexSerialize<>>(granule->index_base);

    std::vector<int32_t> items;
    std::vector<float> dist;
    items.reserve(1);
    dist.reserve(1);

    // 1 - num of nearest neighbour (NN)
    // next number - upper limit on the size of the internal queue; -1 means, that it is equal to num of trees * num of NN
    annoy->get_nns_by_vector(&target_vec[0], 1, 200, &items, &dist);
    return dist[0] < min_distance;
}

std::vector<int32_t> MergeTreeIndexConditionAnnoy::returnIdRecords(MergeTreeIndexGranulePtr idx_granule) const {
    // TODO: Change assert to the exception
    assert(expression.has_value());

    std::vector<int32_t> items;
    items.reserve(LIMIT);

    std::vector<float> target_vec = expression.value().target;
    float min_distance = expression.value().distance;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleAnnoy>(idx_granule);
    auto annoy = std::dynamic_pointer_cast<Annoy::AnnoyIndexSerialize<>>(granule->index_base);


    // 1 - num of nearest neighbour (NN)
    // next number - upper limit on the size of the internal queue; -1 means, that it is equal to num of trees * num of NN
    annoy->get_nns_by_vector(&target_vec[0], LIMIT, 200, &items, NULL);
    return items;
}


bool MergeTreeIndexConditionAnnoy::alwaysUnknownOrTrue() const
{
    return !expression.has_value();
}

MergeTreeIndexConditionAnnoy::RPN MergeTreeIndexConditionAnnoy::buildRPN(const SelectQueryInfo & query, ContextPtr context)
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

void MergeTreeIndexConditionAnnoy::traverseAST(const ASTPtr & node, RPN & rpn) 
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

bool MergeTreeIndexConditionAnnoy::traverseAtomAST(const ASTPtr & node, RPNElement & out) {
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

bool MergeTreeIndexConditionAnnoy::matchRPN(const RPN & rpn) 
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

    return true;
}


MergeTreeIndexGranulePtr MergeTreeIndexAnnoy::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleAnnoy>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexAnnoy::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorAnnoy>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexAnnoy::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionAnnoy>(index, query, context);
};

MergeTreeIndexFormat MergeTreeIndexAnnoy::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr AnnoyIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexAnnoy>(index);
}

void AnnoyIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
