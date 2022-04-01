#include <cassert>
#include <cmath>
#include <cstddef>
#include <string>
#include <Storages/MergeTree/MergeTreeIndexIVFFlat.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

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
#include <Common/FieldVisitorsAccurateComparison.h>

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/MetricType.h>
#include <faiss/impl/io.h>
#include <faiss/index_io.h>

// TODO: Refactor includes, they are ugly

namespace DB
{

namespace detail {
    // Wrapper class for the connection between faiss::IOWriter interface and internal WriteBuffer class
    class WriteBufferFaissWrapper : public faiss::IOWriter {
    public:
        explicit WriteBufferFaissWrapper(WriteBuffer & ostr_)
        : ostr(ostr_) 
        {}

        size_t operator()(const void* ptr, size_t size, size_t nitems) override 
        {
            ostr.write(reinterpret_cast<const char*>(ptr), size * nitems);

            // WriteBuffer guarantees to write all items so return the number of all elements 
            return nitems;
        }

    private:
        WriteBuffer & ostr;
    };

    // Wrapper class for the connection between faiss::IOReader interface and internal ReadBuffer class
    class ReadBufferFaissWrapper : public faiss::IOReader {
    public:        
        explicit ReadBufferFaissWrapper(ReadBuffer & istr_)
        : istr(istr_) 
        {}

        size_t operator()(void* ptr, size_t size, size_t nitems) override 
        {
            // Divide by size because ReadBuffer returns num of written bytes, but
            // faiss::IOReader must return num of written items
            return istr.read(reinterpret_cast<char*>(ptr), size * nitems) / size;
        }

    private:
        ReadBuffer & istr;
    };
}

MergeTreeIndexGranuleIVFFlat::MergeTreeIndexGranuleIVFFlat(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(nullptr)
{}

MergeTreeIndexGranuleIVFFlat::MergeTreeIndexGranuleIVFFlat(
    const String & index_name_, 
    const Block & index_sample_block_,
    FaissBaseIndexPtr index_base_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::move(index_base_))
{}

void MergeTreeIndexGranuleIVFFlat::serializeBinary(WriteBuffer & ostr) const
{
    // TODO: Change assert to an exception
    assert(index_base.get() != nullptr);
    detail::WriteBufferFaissWrapper ostr_wrapped(ostr);
    faiss::write_index(index_base.get(), &ostr_wrapped);
}

void MergeTreeIndexGranuleIVFFlat::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    detail::ReadBufferFaissWrapper istr_wrapped(istr);
    FaissBaseIndex* index = faiss::read_index(&istr_wrapped);
    index_base.reset(index);
}

bool MergeTreeIndexGranuleIVFFlat::empty() const
{
    return index_base->ntotal == 0;
}


MergeTreeIndexAggregatorIVFFlat::MergeTreeIndexAggregatorIVFFlat(const String & index_name_,
                                                                const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

bool MergeTreeIndexAggregatorIVFFlat::empty() const
{
    return values.empty();
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorIVFFlat::getGranuleAndReset()
{
    // TODO: Remove hardcoded params, we can get it from CREATE or from creation of the granule
    // Hyperparams for the index
    // Num of dimensions
    size_t dim = 3; 
    // Num of clusters for training
    size_t nlist = 100;

    // IndexIFVFlat will take care of memory managment
    MergeTreeIndexGranuleIVFFlat::FaissBaseIndex* coarse_quantizer = new faiss::IndexFlat(dim, faiss::METRIC_L2);
    auto index = std::make_shared<faiss::IndexIVFFlat>(coarse_quantizer, dim, nlist);
    index->own_fields = true;

    auto num_elements = values.size() / dim;
    index->train(num_elements, values.data());
    index->add(num_elements, values.data());

    values.clear();

    return std::make_shared<MergeTreeIndexGranuleIVFFlat>(index_name, index_sample_block, index);
}

void MergeTreeIndexAggregatorIVFFlat::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    // TODO: Change assert to an exception
    // This index can be used only for one column
    assert(index_sample_block.columns() == 1);

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    // TODO: Fix dummy way of the access to the Field
    for (size_t i = 0; i < rows_read; ++i) 
    {
        Field field;
        column->get(i, field);
        
        auto field_array = field.safeGet<Tuple>();

        // Store vectors in the flatten arrays
        for (const auto& value : field_array) 
        {
            auto num = value.safeGet<Float32>();
            values.push_back(num);
        }
    }

    *pos += rows_read;
}


MergeTreeIndexConditionIVFFlat::MergeTreeIndexConditionIVFFlat(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
{
    // Build Reverse Polish notation from the query
    RPN rpn = buildRPN(query, context); 

    // Match RPN with the pattern of the query for this type of index
    // and extract expression data for the future usage of the index
    matchRPN(rpn);
}

bool MergeTreeIndexConditionIVFFlat::alwaysUnknownOrTrue() const
{
    // In matchRPN() function we populate expession field in case of the success
    return !expression.has_value();
}

bool MergeTreeIndexConditionIVFFlat::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    // TODO: Change assert to the exception
    assert(expression.has_value());

    std::vector<float> target_vec = expression.value().target;
    float min_distance = expression.value().distance;

    // Number of target vectors
    size_t n = 1;

    // Number of NN to search
    size_t k = 1;

    // Will be populated by faiss
    float distance;
    int64_t label;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleIVFFlat>(idx_granule);
    auto ifv_index = std::dynamic_pointer_cast<faiss::IndexIVFFlat>(granule->index_base);

    ifv_index->search(n, target_vec.data(), k, &distance, &label);

    return distance < min_distance;
}

MergeTreeIndexConditionIVFFlat::RPN MergeTreeIndexConditionIVFFlat::buildRPN(const SelectQueryInfo & query, ContextPtr context)
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

void MergeTreeIndexConditionIVFFlat::traverseAST(const ASTPtr & node, RPN & rpn) 
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

bool MergeTreeIndexConditionIVFFlat::traverseAtomAST(const ASTPtr & node, RPNElement & out) {
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

bool MergeTreeIndexConditionIVFFlat::matchRPN(const RPN & rpn) 
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


MergeTreeIndexIVFFlat::MergeTreeIndexIVFFlat(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
{}


MergeTreeIndexGranulePtr MergeTreeIndexIVFFlat::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleIVFFlat>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexIVFFlat::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorIVFFlat>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexIVFFlat::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionIVFFlat>(index, query, context);
}

bool MergeTreeIndexIVFFlat::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    return true;
}

MergeTreeIndexFormat MergeTreeIndexIVFFlat::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr IVFFlatIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexIVFFlat>(index);
}

void IVFFlatIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
