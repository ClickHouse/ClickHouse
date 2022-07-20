#ifdef ENABLE_NMSLIB

#include <Storages/MergeTree/MergeTreeIndexHnsw.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>

#include <sstream>
#include <knnquery.h>
#include <methodfactory.h>
#include <object.h>
#include <params.h>
#include <space.h>
#include <spacefactory.h>
#include <space/space_lp.h>
#include <space/space_scalar.h>
#include <base/types.h>

namespace HnswWrapper
{

using namespace similarity;

template <typename Dist>
IndexWrap<Dist>::IndexWrap(const std::string & space_type_, const AnyParams & space_params)
    : space_type(space_type_), space(SpaceFactoryRegistry<Dist>::Instance().CreateSpace(space_type_, space_params))
{
}

template <typename Dist>
void IndexWrap<Dist>::createIndex(const AnyParams & params)
{
    index = std::make_unique<Hnsw<Dist>>(false, *space, data);
    index->CreateIndex(params);
}


template <typename Dist>
void IndexWrap<Dist>::loadIndex(DB::ReadBuffer & istr, bool load_data)
{
    if (load_data)
    {
        std::vector<std::string> dummy;
        freeAndClearObjectVector();
        size_t qty = 0;
        size_t obj_size = 0;
        istr.read(reinterpret_cast<char *>(&qty), sizeof(qty));
        for (size_t i = 0; i < qty; ++i)
        {
            istr.read(reinterpret_cast<char *>(&obj_size), sizeof(size_t));
            unique_ptr<char[]> buf(new char[obj_size]);
            istr.read(&buf[0], obj_size);
            data.push_back(new Object(buf.release(), true));
        }
    }
    index = std::make_unique<Hnsw<Dist>>(false, *space, data);

    istr.read(reinterpret_cast<char *>(&index->totalElementsStored_), sizeof(index->totalElementsStored_));
    istr.read(reinterpret_cast<char *>(&index->memoryPerObject_), sizeof(index->memoryPerObject_));
    istr.read(reinterpret_cast<char *>(&index->offsetLevel0_), sizeof(index->offsetLevel0_));
    istr.read(reinterpret_cast<char *>(&index->offsetData_), sizeof(index->offsetData_));
    istr.read(reinterpret_cast<char *>(&index->maxlevel_), sizeof(index->maxlevel_));
    istr.read(reinterpret_cast<char *>(&index->enterpointId_), sizeof(index->enterpointId_));
    istr.read(reinterpret_cast<char *>(&index->maxM_), sizeof(index->maxM_));
    istr.read(reinterpret_cast<char *>(&index->maxM0_), sizeof(index->maxM0_));
    istr.read(reinterpret_cast<char *>(&index->dist_func_type_), sizeof(index->dist_func_type_));
    istr.read(reinterpret_cast<char *>(&index->searchMethod_), sizeof(index->searchMethod_));

    index->fstdistfunc_ = getDistFunc(index->dist_func_type_);
    index->iscosine_ = (index->dist_func_type_ == kNormCosine);
    size_t data_plus_links0_size = index->memoryPerObject_ * index->totalElementsStored_;
    index->data_level0_memory_ = static_cast<char *>(malloc(data_plus_links0_size + 64));
    istr.read(index->data_level0_memory_, data_plus_links0_size);
    index->linkLists_ = static_cast<char **>(malloc((sizeof(void *) * index->totalElementsStored_) + 64));
    index->data_rearranged_.resize(index->totalElementsStored_);

    for (size_t i = 0; i < index->totalElementsStored_; i++)
    {
        size_t link_list_size = 0;
        istr.read(reinterpret_cast<char *>(&link_list_size), sizeof(link_list_size));

        if (link_list_size == 0)
        {
            index->linkLists_[i] = nullptr;
        }
        else
        {
            index->linkLists_[i] = static_cast<char *>(malloc(link_list_size));
            istr.read(index->linkLists_[i], link_list_size);
        }
        index->data_rearranged_[i] = new Object(index->data_level0_memory_ + (i)*index->memoryPerObject_ + index->offsetData_);
    }
    index->visitedlistpool = new VisitedListPool(1, index->totalElementsStored_);
    index->ResetQueryTimeParams();
}


template <typename Dist>
void IndexWrap<Dist>::saveIndex(DB::WriteBuffer & ostr, bool save_data)
{
    if (save_data)
    {
        size_t size = data.size();
        ostr.write(reinterpret_cast<char *>(&size), sizeof(size_t));
        for (auto & obj : data)
        {
            size_t obj_size = obj->bufferlength();
            ostr.write(reinterpret_cast<char *>(&obj_size), sizeof(size_t));
            ostr.write(obj->buffer(), obj_size);
        }
    }

    index->totalElementsStored_ = index->ElList_.size();
    ostr.write(reinterpret_cast<char *>(&index->totalElementsStored_), sizeof(index->totalElementsStored_));
    ostr.write(reinterpret_cast<char *>(&index->memoryPerObject_), sizeof(index->memoryPerObject_));
    ostr.write(reinterpret_cast<char *>(&index->offsetLevel0_), sizeof(index->offsetLevel0_));
    ostr.write(reinterpret_cast<char *>(&index->offsetData_), sizeof(index->offsetData_));
    ostr.write(reinterpret_cast<char *>(&index->maxlevel_), sizeof(index->maxlevel_));
    ostr.write(reinterpret_cast<char *>(&index->enterpointId_), sizeof(index->enterpointId_));
    ostr.write(reinterpret_cast<char *>(&index->maxM_), sizeof(index->maxM_));
    ostr.write(reinterpret_cast<char *>(&index->maxM0_), sizeof(index->maxM0_));
    ostr.write(reinterpret_cast<char *>(&index->dist_func_type_), sizeof(index->dist_func_type_));
    ostr.write(reinterpret_cast<char *>(&index->searchMethod_), sizeof(index->searchMethod_));

    size_t data_plus_links0_size = index->memoryPerObject_ * index->totalElementsStored_;

    ostr.write(index->data_level0_memory_, data_plus_links0_size);
    for (size_t i = 0; i < index->totalElementsStored_; ++i)
    {
        size_t sizemass = ((index->ElList_[i]->level) * (index->maxM_ + 1)) * sizeof(int);
        ostr.write(reinterpret_cast<char *>(&sizemass), sizeof(sizemass));
        if (sizemass)
        {
            ostr.write(index->linkLists_[i], sizemass);
        }
    }
}

template <typename Dist>
KNNQueue<Dist> * IndexWrap<Dist>::knnQuery(const Object & obj, size_t k)
{
    KNNQuery<Dist> knn(*space, &obj, k);
    index->Search(&knn, -1);
    return knn.Result()->Clone();
}

template <typename Dist>
void IndexWrap<Dist>::addBatchUnsafe(ObjectVector && new_data)
{
    data = std::move(new_data);
}

template <typename Dist>
void IndexWrap<Dist>::freeAndClearObjectVector()
{
    for (auto & i : data)
    {
        delete i;
    }
    data.clear();
}

template <typename Dist>
size_t IndexWrap<Dist>::dataSize() const
{
    return data.size();
}

template <typename Dist>
IndexWrap<Dist>::~IndexWrap()
{
    freeAndClearObjectVector();
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}


MergeTreeIndexGranuleHnsw::MergeTreeIndexGranuleHnsw(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_), index_sample_block(index_sample_block_) {}


MergeTreeIndexGranuleHnsw::MergeTreeIndexGranuleHnsw(
    const String & index_name_, const Block & index_sample_block_, std::unique_ptr<HnswWrapper::IndexWrap<float>> index_impl_)
    : index_name(index_name_), index_sample_block(index_sample_block_), index_impl(std::move(index_impl_)) {}


void MergeTreeIndexGranuleHnsw::serializeBinary(WriteBuffer & ostr) const
{
    index_impl->saveIndex(ostr, true);
}

void MergeTreeIndexGranuleHnsw::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    if (!index_impl)
    {
        index_impl = std::make_unique<HnswWrapper::IndexWrap<float>>("l2");
    }
    index_impl->loadIndex(istr, true); // true for loading data
}

MergeTreeIndexAggregatorHnsw::MergeTreeIndexAggregatorHnsw(
    const String & index_name_, const Block & index_sample_block_, const similarity::AnyParams & index_params_)
    : index_name(index_name_), index_sample_block(index_sample_block_), index_params(index_params_)
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorHnsw::getGranuleAndReset()
{
    auto impl = std::make_unique<HnswWrapper::IndexWrap<float>>("l2");
    impl->addBatchUnsafe(std::move(data));
    impl->createIndex(index_params);
    return std::make_shared<MergeTreeIndexGranuleHnsw>(index_name, index_sample_block, std::move(impl));
}

void MergeTreeIndexAggregatorHnsw::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
            "The provided position is not less than the number of block rows. Position: " + toString(*pos)
                + ", Block rows: " + toString(block.rows()) + ".",
            ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column_cut = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto & column_array = typeid_cast<const ColumnArray*>(column_cut.get());
    if (column_array)
    {
        const auto & column_data = column_array->getData();
        const auto & array = typeid_cast<const ColumnFloat32&>(column_data).getData();
        const auto & offsets = column_array->getOffsets();
        size_t num_rows = column_array->size();

        /// All sizes are the same
        size_t size = offsets[1] - offsets[0];
        for (size_t i = 0; i < num_rows - 1; ++ i)
        {
            if (offsets[i + 1] - offsets[i] != size)
            {
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrays should have same length");
            }
        }

        for (size_t current_row = 0; current_row < num_rows; ++current_row)
        {
            data.push_back(new similarity::Object(data.size(), -1, size * sizeof(float), &array[offsets[current_row]]));
        }
    }
    else
    {
        /// Other possible type of column is Tuple
        const auto & column_tuple = typeid_cast<const ColumnTuple*>(column_cut.get());

        if (!column_tuple)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Wrong type was given to index.");

        const auto & columns = column_tuple->getColumns();

        std::vector<std::vector<Float32>> columns_data{column_tuple->size(), std::vector<Float32>()};
        for (const auto& column : columns)
        {
            const auto& pod_array = typeid_cast<const ColumnFloat32*>(column.get())->getData();
            for (size_t i = 0; i < pod_array.size(); ++i)
            {
                columns_data[i].push_back(pod_array[i]);
            }
        }
        assert(!columns_data.empty());
    
        for (const auto& item : columns_data)
        {
            data.push_back(new similarity::Object(data.size(), -1, item.size() * sizeof(float), item.data()));
        }
    }

    *pos += rows_read;
}

MergeTreeIndexConditionHnsw::MergeTreeIndexConditionHnsw(
    const IndexDescription &, const SelectQueryInfo & query, ContextPtr context)
    : condition(query, context)
{
}

bool MergeTreeIndexConditionHnsw::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue("L2Distance");
}

std::vector<size_t> MergeTreeIndexConditionHnsw::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    UInt64 limit = condition.getLimit();
    UInt64 index_granularity = condition.getIndexGranularity();
    std::optional<float> comp_dist = condition.getQueryType() == ApproximateNearestNeighbour::ANNQueryInformation::Type::Where ?
      std::optional<float>(condition.getComparisonDistanceForWhereQuery()) : std::nullopt;

    if (comp_dist && comp_dist.value() < 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to optimize query with where without distance");

    std::vector<float> target_vec = condition.getTargetVector();
    similarity::Object target(-1, -1, target_vec.size() * sizeof(float), target_vec.data());

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleHnsw>(idx_granule);
    if (!granule)
        throw Exception("SimpleHnsw index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    auto search_result = std::unique_ptr<similarity::KNNQueue<float>>(granule->index_impl->knnQuery(target, limit));
    std::unordered_set<size_t> granule_numbers;
    while (!search_result->Empty())
    {
        auto cur_dist = search_result->TopDistance();
        const auto * obj = search_result->Pop();
        if (comp_dist && cur_dist > comp_dist)
        {
            continue;
        }
        granule_numbers.insert(obj->id() / index_granularity);
    }

    std::vector<size_t> result_vector;
    result_vector.reserve(granule_numbers.size());
    for (auto granule_number : granule_numbers)
    {
        result_vector.push_back(granule_number);
    }

    return result_vector;
}


bool MergeTreeIndexConditionHnsw::mayBeTrueOnGranule(MergeTreeIndexGranulePtr /*idx_granule*/) const
{
    throw Exception("mayBeTrueOnGranule is not supported for ANN skip indexes", ErrorCodes::LOGICAL_ERROR);
}

MergeTreeIndexGranulePtr MergeTreeIndexHnsw::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleHnsw>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexHnsw::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorHnsw>(index.name, index.sample_block, index_params);
}

MergeTreeIndexConditionPtr MergeTreeIndexHnsw::createIndexCondition(const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionHnsw>(index, query, context);
};

MergeTreeIndexPtr hnswIndexCreator(const IndexDescription & index)
{
    static std::vector<std::string> param_places_to_names({"M", "efConstruction", "maxM", "maxM0"});

    std::vector<string> param_names;
    std::vector<string> param_values;
    for (size_t i = 0; i < index.arguments.size(); ++i)
    {
        param_names.push_back(param_places_to_names[i]);
        size_t param_val = index.arguments[i].get<size_t>();
        param_values.push_back(std::to_string(param_val));
    }
    similarity::AnyParams params(param_names, param_values);
    return std::make_shared<MergeTreeIndexHnsw>(index, params);
}

void hnswIndexValidator(const IndexDescription & index, bool /* attach */)
{
    if (index.arguments.size() > 4)
    {
        throw Exception("HNSW accepts no more than 4 arguments", ErrorCodes::INCORRECT_QUERY);
    }

    for (const auto & arg : index.arguments)
    {
        if (arg.getType() != Field::Types::UInt64)
        {
            throw Exception("Arguments must be UInt64", ErrorCodes::INCORRECT_QUERY);
        }
    }
}

}

#endif
