#include <Storages/MergeTree/MergeTreeIndexFaiss.h>

#include <Core/Field.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <Storages/MergeTree/MergeTreeIndices.h>

#include <faiss/AutoTune.h>
#include <faiss/Index.h>
#include <faiss/MetricType.h>
#include <faiss/impl/io.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>

#include <map>
#include <vector>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
}

namespace
{
    // Wrapper class for the connection between faiss::IOWriter interface and internal WriteBuffer class
    class WriteBufferFaissWrapper : public faiss::IOWriter
    {
    public:
        explicit WriteBufferFaissWrapper(WriteBuffer & ostr_)
        : ostr(ostr_)
        {}

        size_t operator()(const void* ptr, size_t size, size_t nitems) override
        {
            ostr.write(reinterpret_cast<const char*>(ptr), size * nitems);

            // WriteBuffer guarantees to write all items, so return the number of all elements 
            return nitems;
        }

    private:
        WriteBuffer & ostr;
    };

    // Wrapper class for the connection between faiss::IOReader interface and internal ReadBuffer class
    class ReadBufferFaissWrapper : public faiss::IOReader
    {
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

    // Parser for the input arguments of the index in the CREATE query
    class ArgumentParser
    {
    public:
        struct Argument
        {
            size_t position;
            Field default_value;
        };

        explicit ArgumentParser(FieldVector arguments_)
        : arguments(std::move(arguments_))
        {
            initialize();
        }

        Field getArgumentByName(String name)
        {
            if (argument_mapping.contains(name))
            {
                auto argument = argument_mapping[name];
                if (argument.position < arguments.size())
                    return arguments[argument.position];
                else
                    return argument.default_value;
            }
            else
                return {};
        }

    private:
        FieldVector arguments;
        std::map<String, Argument> argument_mapping;

        void initialize()
        {
            registerArgument("index_key", 0, {});
            registerArgument("metric_type", 1, "L2Distance");
        }

        void registerArgument(String name, size_t position, Field default_value)
        {
            argument_mapping[name] = {position, std::move(default_value)};
        }
    };

    inline faiss::MetricType StringToMetric(const String & str)
    {
        if (str == "L2Distance")
            return faiss::METRIC_L2;
        else
            throw Exception("Unsupported metric type. Faiss indexes right now support only L2 metric.", ErrorCodes::INCORRECT_QUERY);
    }
}

MergeTreeIndexGranuleFaiss::MergeTreeIndexGranuleFaiss(const String & index_name_, const Block & index_sample_block_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(nullptr)
    , is_incomplete(false)
{}

MergeTreeIndexGranuleFaiss::MergeTreeIndexGranuleFaiss(
    const String & index_name_,
    const Block & index_sample_block_,
    FaissBaseIndexPtr index_base_,
    bool is_incomplete_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_base(std::move(index_base_))
    , is_incomplete(is_incomplete_)
{}

void MergeTreeIndexGranuleFaiss::serializeBinary(WriteBuffer & ostr) const
{
    WriteBufferFaissWrapper ostr_wrapped(ostr);
    faiss::write_index(index_base.get(), &ostr_wrapped);
}

void MergeTreeIndexGranuleFaiss::deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion /*version*/)
{
    ReadBufferFaissWrapper istr_wrapped(istr);
    FaissBaseIndex* index = faiss::read_index(&istr_wrapped);
    index_base.reset(index);
}

bool MergeTreeIndexGranuleFaiss::empty() const
{
    // Granule is empty if we didn't try to train index and failed (flag is_incomplete),
    // and we didn't set base index, or base index doesn't contain elements
    return !is_incomplete && (index_base == nullptr || index_base->ntotal == 0);
}


MergeTreeIndexAggregatorFaiss::MergeTreeIndexAggregatorFaiss(const String & index_name_,
                                                                const Block & index_sample_block_,
                                                                const String & index_key_,
                                                                const String & metric_type_)
    : index_name(index_name_)
    , index_sample_block(index_sample_block_)
    , index_key(index_key_)
    , metric_type(metric_type_)
{}

bool MergeTreeIndexAggregatorFaiss::empty() const
{
    return values.empty();
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorFaiss::getGranuleAndReset()
{
    std::unique_ptr<faiss::Index> index(faiss::index_factory(dimension, index_key.c_str(), StringToMetric(metric_type)));
    auto num_elements = values.size() / dimension;

    try
    {
        index->train(num_elements, values.data());
        index->add(num_elements, values.data());
    }
    catch (const faiss::FaissException&)
    {
        index.reset();
    }
    values.clear();

    bool is_incomplete = (index == nullptr);
    return std::make_shared<MergeTreeIndexGranuleFaiss>(index_name, index_sample_block, std::move(index), is_incomplete);
}

void MergeTreeIndexAggregatorFaiss::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    if (index_sample_block.columns() != 1)
        throw Exception("Faiss indexes support construction only on the one column.", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    auto index_column_name = index_sample_block.getByPosition(0).name;
    const auto & column = block.getByName(index_column_name).column->cut(*pos, rows_read);
    const auto * vectors = typeid_cast<const ColumnTuple *>(column.get());

    dimension = vectors->getColumns().size();

    size_t offset = values.size();
    values.resize(offset + dimension * rows_read);

    for (size_t col_idx = 0; col_idx < dimension; ++col_idx)
    {
        const auto & inner_column = vectors->getColumns()[col_idx];
        const auto * coordinate_column = typeid_cast<const ColumnFloat32 *>(inner_column.get());

        for (size_t row_idx = 0; row_idx < coordinate_column->size(); ++row_idx)
            values[offset + dimension * row_idx + col_idx] = coordinate_column->getElement(row_idx);
    }

    *pos += rows_read;
}


MergeTreeIndexConditionFaiss::MergeTreeIndexConditionFaiss(
    const IndexDescription & index,
    const SelectQueryInfo & query,
    ContextPtr context,
    const String & metric_type_)
    : index_data_types(index.data_types)
    , condition(query, context)
    , metric_type(metric_type_)
{}

bool MergeTreeIndexConditionFaiss::alwaysUnknownOrTrue() const
{
    return condition.alwaysUnknownOrTrue(metric_type);
}

bool MergeTreeIndexConditionFaiss::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::vector<float> target_vec = condition.getTargetVector();
    float min_distance = condition.getComparisonDistance();

    // Number of target vectors
    size_t n = 1;

    // Number of NN to search
    size_t k = 1;

    // Will be populated by faiss
    float distance;
    int64_t label;

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleFaiss>(idx_granule);
    String index_setting = condition.getSettingsStr();

    if (!index_setting.empty())
        faiss::ParameterSpace().set_index_parameters(granule->index_base.get(), condition.getSettingsStr().c_str());

    granule->index_base->search(n, target_vec.data(), k, &distance, &label);

    return distance < min_distance;
}

std::vector<size_t> MergeTreeIndexConditionFaiss::getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const
{
    UInt64 limit = condition.getLimitCount() ? condition.getLimitCount().value() : 1;
    std::vector<float> target_vec = condition.getTargetVector();
    std::optional<float> distance = condition.queryHasWhereClause() ? std::optional<float>(condition.getComparisonDistance()) : std::nullopt;

    // Number of target vectors
    size_t n = 1;

    // Number of NN to search
    size_t k = limit;

    // Will be populated by faiss
    std::vector<float> distances(k);
    std::vector<int64_t> labels(k);

    auto granule = std::dynamic_pointer_cast<MergeTreeIndexGranuleFaiss>(idx_granule);

    // Skip searching in the incomplete granule
    if (granule->is_incomplete)
        return {0};

    String index_setting = condition.getSettingsStr();
    if (!index_setting.empty())
        faiss::ParameterSpace().set_index_parameters(granule->index_base.get(), condition.getSettingsStr().c_str());

    granule->index_base->search(n, target_vec.data(), k, distances.data(), labels.data());

    // Temporary hard-coded constant
    const size_t granule_size = 8192;

    std::unordered_set<size_t> useful_granules;
    for (size_t i = 0; i < k; ++i)
    {
        // In the case of queries like WHERE ... < distance, 
        // we have to stop iteration if we meet a greater distance than the distance variable
        if (distance.has_value() && distances[i] > distance.value())
            break;

        useful_granules.insert(labels[i] / granule_size);
    }

    std::vector<size_t> useful_granules_vec;
    useful_granules_vec.reserve(useful_granules.size());
    for (auto idx : useful_granules)
    {
        useful_granules_vec.push_back(idx);
    }

    return useful_granules_vec;
}

MergeTreeIndexFaiss::MergeTreeIndexFaiss(const IndexDescription & index_)
    : IMergeTreeIndex(index_)
{
    if (index.arguments.empty())
        throw Exception("Faiss indexes require at least one argument: key string for the index factory.", ErrorCodes::INCORRECT_QUERY);

    ArgumentParser parser(index.arguments);
    index_key = parser.getArgumentByName("index_key").safeGet<String>();
    metric_type = parser.getArgumentByName("metric_type").safeGet<String>();
}


MergeTreeIndexGranulePtr MergeTreeIndexFaiss::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleFaiss>(index.name, index.sample_block);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexFaiss::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorFaiss>(index.name, index.sample_block, index_key, metric_type);
}

MergeTreeIndexConditionPtr MergeTreeIndexFaiss::createIndexCondition(
    const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionFaiss>(index, query, context, metric_type);
}

bool MergeTreeIndexFaiss::mayBenefitFromIndexForIn(const ASTPtr & /*node*/) const
{
    return false;
}

MergeTreeIndexFormat MergeTreeIndexFaiss::getDeserializedFormat(const DiskPtr disk, const std::string & relative_path_prefix) const
{
    if (disk->exists(relative_path_prefix + ".idx2"))
        return {2, ".idx2"};
    else if (disk->exists(relative_path_prefix + ".idx"))
        return {1, ".idx"};
    return {0 /* unknown */, ""};
}

MergeTreeIndexPtr FaissIndexCreator(
    const IndexDescription & index)
{
    return std::make_shared<MergeTreeIndexFaiss>(index);
}

void FaissIndexValidator(const IndexDescription & /* index */, bool /* attach */)
{}

}
