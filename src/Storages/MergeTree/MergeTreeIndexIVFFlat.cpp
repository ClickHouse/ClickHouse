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
#include "MergeTreeIndices.h"
#include "base/types.h"
#include <Common/FieldVisitorsAccurateComparison.h>

#include <faiss/IndexFlat.h>
#include <faiss/IndexIVF.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/MetricType.h>
#include <faiss/impl/io.h>
#include <faiss/index_io.h>

namespace DB
{

namespace detail {
    class WriteBufferFaissWrapper : public faiss::IOWriter {
    public:
        explicit WriteBufferFaissWrapper(WriteBuffer & ostr_)
        : ostr(ostr_) 
        {}

        size_t operator()(const void* ptr, size_t size, size_t nitems) override 
        {
            ostr.write(reinterpret_cast<const char*>(ptr), size * nitems);

            // WriteBuffer guarantees to write all items
            return nitems;
        }

    private:
        WriteBuffer & ostr;
    };

    class ReadBufferFaissWrapper : public faiss::IOReader {
    public:        
        explicit ReadBufferFaissWrapper(ReadBuffer & istr_)
        : istr(istr_) 
        {}

        size_t operator()(void* ptr, size_t size, size_t nitems) override 
        {
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
    // return false;
    return index_base->ntotal == 0;
}


MergeTreeIndexAggregatorIVFFlat::MergeTreeIndexAggregatorIVFFlat(const String & index_name_,
                                                                const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

bool MergeTreeIndexAggregatorIVFFlat::empty() const
{
    // return true;
    return values.empty();
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorIVFFlat::getGranuleAndReset()
{
    // Build index
    size_t dim = 3;
    size_t nlist = 5;

    // IndexIFVFlat will take care of memory managment
    MergeTreeIndexGranuleIVFFlat::FaissBaseIndex* coarse_quantizer = new faiss::IndexFlat(dim, faiss::METRIC_L2);
    // faiss::IndexFlatL2 coarse_quantizer(dim);
    auto index = std::make_shared<faiss::IndexIVFFlat>(coarse_quantizer, dim, nlist);
    index->own_fields = true;
    index->verbose = true;

    // Take care about Float32 == float

    for (size_t i = 0; i < values.size(); i += 200) {
        LOG_DEBUG(&Poco::Logger::get("IVFFlat"), "i = {}, val = {}", i, values[i]);
    }

    auto num_elements = values.size() / dim;
    LOG_DEBUG(&Poco::Logger::get("IVFFlat"), "Val size: {}", num_elements);
    index->train(num_elements, values.data());
    LOG_DEBUG(&Poco::Logger::get("IVFFlat"), "Train completed");
    index->add(num_elements, values.data());
    LOG_DEBUG(&Poco::Logger::get("IVFFlat"), "Add completed");

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

    // This index can be used only for one column
    assert(index_sample_block.columns() == 1);

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    for (size_t i = 0; i < rows_read; ++i) {
        Field field;
        column->get(i, field);
        
        auto field_array = field.safeGet<Tuple>();

        // Store vectors in the flatten arrays
        for (const auto& value : field_array) {
            auto num = value.safeGet<Float32>();
            values.push_back(num);
        }
    }

    *pos += rows_read;
}


MergeTreeIndexConditionIVFFlat::MergeTreeIndexConditionIVFFlat(
    const IndexDescription & index,
    const SelectQueryInfo & /*query*/,
    ContextPtr /*context*/)
    : index_data_types(index.data_types)
{}

bool MergeTreeIndexConditionIVFFlat::alwaysUnknownOrTrue() const
{
    return false;
}

bool MergeTreeIndexConditionIVFFlat::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    // faiss::IndexFlatL2 index(64);           // call constructor
    // static std::size_t num = 1;
    // return num++ % 2 == 0;
    // void search(
    //         idx_t n,
    //         const float* x,
    //         idx_t k,
    //         float* distances,
    //         idx_t* labels) const override;

    std::vector<float> vec_0{0.0, 0.0, 0.0};
    size_t n = 1;
    size_t k = 1;
    float distance;
    int64_t label;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleIVFFlat>(idx_granule);
    auto ifv_index = std::dynamic_pointer_cast<faiss::IndexIVFFlat>(granule->index_base);

    ifv_index->search(n, vec_0.data(), k, &distance, &label);

    float min_distance = 1e-2;
    return distance < min_distance;
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
