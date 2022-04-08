#include <cstddef>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <knnquery.h>
#include <methodfactory.h>
#include <object.h>
#include <params.h>
#include <space.h>
#include <spacefactory.h>
#include <Storages/MergeTree/MergeTreeIndexSimpleHnsw.h>
#include <space/space_lp.h>
#include <space/space_scalar.h>
#include <Poco/Logger.h>
#include "Storages/MergeTree/KeyCondition.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace detail{

    using namespace similarity;

    void freeAndClearObjectVector(ObjectVector &data) {
    for (auto& datum: data) {
        delete datum;
    }
    data.clear();
}

template<typename Dist>
struct IndexWrap {
    IndexWrap(const std::string &method_,
              const std::string &space_type_,
              const AnyParams &space_params = AnyParams()) :
            method(method_), space_type(space_type_), space(SpaceFactoryRegistry<Dist>::Instance().CreateSpace(
            space_type_, space_params)) {
    }

    void createIndex(const AnyParams &index_params, bool print_progress = false) {
        index.reset(MethodFactoryRegistry<Dist>::Instance().CreateMethod(print_progress,
                                                                           method, space_type, *space, data));
        index->CreateIndex(index_params);
    }

    void loadIndex(std::istream &input, bool load_data = true) {
        index.reset(MethodFactoryRegistry<Dist>::Instance().CreateMethod(false,
                                                                           method, space_type, *space, data));
        if (load_data) {
            std::vector<std::string> dummy;
            freeAndClearObjectVector(data);
            size_t qty;
            size_t obj_size;
            readBinaryPOD(input, qty);
            for (size_t i = 0; i < qty; ++i) {
                readBinaryPOD(input, obj_size);
                unique_ptr<char[]> buf(new char[obj_size]);
                input.read(&buf[0], obj_size);
                data.push_back(new Object(buf.release(), true));
            }
        }
        index->LoadIndex(input);
        index->ResetQueryTimeParams();
    }

    void saveIndex(std::ostream &output, bool save_data = true) {
        if (save_data) {
            writeBinaryPOD(output, data.size());
            for (auto &i: data) {
                writeBinaryPOD(output, i->bufferlength());
                output.write(i->buffer(), i->bufferlength());
            }
        }
        index->SaveIndex(output);
    }

    KNNQueue<Dist> knnQuery(const Object &obj, size_t k) {
        KNNQuery<Dist> knn(*space, &obj, k);
        index->Search(knn, -1);
        return knn.Result()->Clone();
    }

    void addPoint(const Object &point) {
        data.push_back(new Object(data.size(), -1, point.datalength(), point.data()));
    }

    void addBatch(const std::vector<Object> &new_data) {
        for (const auto &elem: new_data) {
            addPoint(elem);
        }
    }


private:
    std::string method;
    std::string space_type;
    std::unique_ptr<similarity::Space<Dist>> space;
    std::unique_ptr<Index<Dist>> index;
    ObjectVector data;

};
}

MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_, 
    const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}


MergeTreeIndexGranuleSimpleHnsw::MergeTreeIndexGranuleSimpleHnsw(const String & index_name_,
    const Block & index_sample_block_, std::unique_ptr<similarity::Space<float>> space_, similarity::ObjectVector && data_, 
    std::unique_ptr<similarity::Hnsw<float>> index_impl_)
    : index_name(index_name_),
     index_sample_block(index_sample_block_), space(std::move(space_)),
     data(std::move(data_)), index_impl(std::move(index_impl_))
{}


void MergeTreeIndexGranuleSimpleHnsw::serializeBinary(WriteBuffer & ostr) const {
     std::ostringstream ost;
     index_impl->SaveIndex(ost);
     ostr.write(ost.str().data(), ost.str().size());
}

void MergeTreeIndexGranuleSimpleHnsw::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version){
    if (version != 1){
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown index version {}.", version);
    }
    size_t index_size = istr.available();
    std::string str(index_size, '\0');
    istr.read(str.data(), index_size);
    std::istringstream ist(str);
    index_impl->LoadIndex(ist);
}

MergeTreeIndexGranuleSimpleHnsw::~MergeTreeIndexGranuleSimpleHnsw() {
    for (auto & obj : data){
        delete obj;
    }
 }

MergeTreeIndexAggregatorSimpleHnsw::MergeTreeIndexAggregatorSimpleHnsw(const String & index_name_, 
const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
{}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorSimpleHnsw::getGranuleAndReset()
{
    auto space = std::unique_ptr<similarity::Space<float>>(new similarity::SpaceLp<float>(2));
    similarity::AnyParams params;
    auto index_impl = std::make_unique<similarity::Hnsw<float>>(false, *space, data);
    index_impl->CreateIndex(params);
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index_name, index_sample_block, std::move(space),
    std::move(data) ,std::move(index_impl));
}

void MergeTreeIndexAggregatorSimpleHnsw::update(const Block & block, size_t * pos, size_t limit){
     if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

   auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);

    for (size_t i = 0; i < rows_read; ++i) {
        Field field;
        column->get(i, field);

        auto field_array = field.safeGet<Tuple>();

        auto *obj = new similarity::Object(-1, i, field_array.size() * sizeof(float), nullptr); 

        float *raw_vector = reinterpret_cast<float*>(obj->data());

        // Store vectors in the flatten arrays
        for (size_t j = 0; j < field_array.size();++j) {
            auto num = field_array.at(j).safeGet<Float32>();
            raw_vector[j] = num;
        }
         // true here means that the element in vector is responsible for deleting
        data.push_back(obj); 
    }

    *pos += rows_read;
}

MergeTreeIndexConditionSimpleHnsw::MergeTreeIndexConditionSimpleHnsw(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context)
    : index_data_types(index.data_types)
    , condition(query, context, index.column_names, index.expression)
{
}

bool MergeTreeIndexConditionSimpleHnsw::alwaysUnknownOrTrue() const
{
    return false;
}

bool MergeTreeIndexConditionSimpleHnsw::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    LOG_DEBUG(&Poco::Logger::get("SimpleHnsw"), "Here we are");
    auto center_obj = std::make_unique<similarity::Object>(-1, -1, 3 * sizeof(float), nullptr);
    auto *raw_center_obj = reinterpret_cast<float*>(center_obj->data());
    raw_center_obj[0] = 0;
    raw_center_obj[1] = 0;
    raw_center_obj[2] = 0;
    LOG_DEBUG(&Poco::Logger::get("SimpleHnsw"), "Create query");
    std::shared_ptr<MergeTreeIndexGranuleSimpleHnsw> granule
         = std::dynamic_pointer_cast<MergeTreeIndexGranuleSimpleHnsw>(idx_granule);
    if (!granule)
         throw Exception(
             "SimpleHnsw index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);
    auto query = similarity::KNNQuery<float>(*granule->space, center_obj.get(), 1);
    granule->index_impl->Search(&query, -1);
    return query.Radius() < 10;
}

MergeTreeIndexGranulePtr MergeTreeIndexSimpleHnsw::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleSimpleHnsw>(index.name, index.sample_block);
}


MergeTreeIndexAggregatorPtr MergeTreeIndexSimpleHnsw::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorSimpleHnsw>(index.name, index.sample_block);
}

MergeTreeIndexConditionPtr MergeTreeIndexSimpleHnsw::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionSimpleHnsw>(index, query, context);
};

bool MergeTreeIndexSimpleHnsw::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    // const String column_name = node->getColumnName();

    // for (const auto & cname : index.column_names)
    //     if (column_name == cname)
    //         return true;

    // if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    //     if (func->arguments->children.size() == 1)
    //         return mayBenefitFromIndexForIn(func->arguments->children.front());

    return false;
}

MergeTreeIndexFormat MergeTreeIndexSimpleHnsw::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}


MergeTreeIndexPtr simpleHnswIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexSimpleHnsw>(index);
}

void simpleHnswIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{
}

}
