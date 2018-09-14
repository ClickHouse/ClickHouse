#include <Dictionaries/CatBoostModel.h>
#include <Common/FieldVisitors.h>
#include <mutex>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/PODArray.h>
#include <Common/SharedLibrary.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_LOAD_CATBOOST_MODEL;
extern const int CANNOT_APPLY_CATBOOST_MODEL;
}


/// CatBoost wrapper interface functions.
struct CatBoostWrapperAPI
{
    typedef void ModelCalcerHandle;

    ModelCalcerHandle * (* ModelCalcerCreate)();

    void (* ModelCalcerDelete)(ModelCalcerHandle * calcer);

    const char * (* GetErrorString)();

    bool (* LoadFullModelFromFile)(ModelCalcerHandle * calcer, const char * filename);

    bool (* CalcModelPredictionFlat)(ModelCalcerHandle * calcer, size_t docCount,
                                     const float ** floatFeatures, size_t floatFeaturesSize,
                                     double * result, size_t resultSize);

    bool (* CalcModelPrediction)(ModelCalcerHandle * calcer, size_t docCount,
                                 const float ** floatFeatures, size_t floatFeaturesSize,
                                 const char *** catFeatures, size_t catFeaturesSize,
                                 double * result, size_t resultSize);

    bool (* CalcModelPredictionWithHashedCatFeatures)(ModelCalcerHandle * calcer, size_t docCount,
                                                      const float ** floatFeatures, size_t floatFeaturesSize,
                                                      const int ** catFeatures, size_t catFeaturesSize,
                                                      double * result, size_t resultSize);

    int (* GetStringCatFeatureHash)(const char * data, size_t size);

    int (* GetIntegerCatFeatureHash)(long long val);

    size_t (* GetFloatFeaturesCount)(ModelCalcerHandle* calcer);

    size_t (* GetCatFeaturesCount)(ModelCalcerHandle* calcer);
};


namespace
{

class CatBoostModelHolder
{
private:
    CatBoostWrapperAPI::ModelCalcerHandle * handle;
    const CatBoostWrapperAPI * api;
public:
    explicit CatBoostModelHolder(const CatBoostWrapperAPI * api) : api(api) { handle = api->ModelCalcerCreate(); }
    ~CatBoostModelHolder() { api->ModelCalcerDelete(handle); }

    CatBoostWrapperAPI::ModelCalcerHandle * get() { return handle; }
};


class CatBoostModelImpl : public ICatBoostModel
{
public:
    CatBoostModelImpl(const CatBoostWrapperAPI * api, const std::string & model_path) : api(api)
    {
        auto handle_ = std::make_unique<CatBoostModelHolder>(api);
        if (!handle_)
        {
            std::string msg = "Cannot create CatBoost model: ";
            throw Exception(msg + api->GetErrorString(), ErrorCodes::CANNOT_LOAD_CATBOOST_MODEL);
        }
        if (!api->LoadFullModelFromFile(handle_->get(), model_path.c_str()))
        {
            std::string msg = "Cannot load CatBoost model: ";
            throw Exception(msg + api->GetErrorString(), ErrorCodes::CANNOT_LOAD_CATBOOST_MODEL);
        }

        float_features_count = api->GetFloatFeaturesCount(handle_->get());
        cat_features_count = api->GetCatFeaturesCount(handle_->get());

        handle = std::move(handle_);
    }

    ColumnPtr evaluate(const ColumnRawPtrs & columns) const override
    {
        if (columns.empty())
            throw Exception("Got empty columns list for CatBoost model.", ErrorCodes::BAD_ARGUMENTS);

        if (columns.size() != float_features_count + cat_features_count)
        {
            std::string msg;
            {
                WriteBufferFromString buffer(msg);
                buffer << "Number of columns is different with number of features: ";
                buffer << columns.size() << " vs " << float_features_count << " + " << cat_features_count;
            }
            throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
        }

        for (size_t i = 0; i < float_features_count; ++i)
        {
            if (!columns[i]->isNumeric())
            {
                std::string msg;
                {
                    WriteBufferFromString buffer(msg);
                    buffer << "Column " << i << "should be numeric to make float feature.";
                }
                throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
            }
        }

        bool cat_features_are_strings = true;
        for (size_t i = float_features_count; i < float_features_count + cat_features_count; ++i)
        {
            auto column = columns[i];
            if (column->isNumeric())
                cat_features_are_strings = false;
            else if (!(typeid_cast<const ColumnString *>(column)
                       || typeid_cast<const ColumnFixedString *>(column)))
            {
                std::string msg;
                {
                    WriteBufferFromString buffer(msg);
                    buffer << "Column " << i << "should be numeric or string.";
                }
                throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return evalImpl(columns, float_features_count, cat_features_count, cat_features_are_strings);
    }

    size_t getFloatFeaturesCount() const override { return float_features_count; }
    size_t getCatFeaturesCount() const override { return cat_features_count; }

private:
    std::unique_ptr<CatBoostModelHolder> handle;
    const CatBoostWrapperAPI * api;
    size_t float_features_count;
    size_t cat_features_count;

    /// Buffer should be allocated with features_count * column->size() elements.
    /// Place column elements in positions buffer[0], buffer[features_count], ... , buffer[size * features_count]
    template <typename T>
    void placeColumnAsNumber(const IColumn * column, T * buffer, size_t features_count) const
    {
        size_t size = column->size();
        FieldVisitorConvertToNumber<T> visitor;
        for (size_t i = 0; i < size; ++i)
        {
            /// TODO: Replace with column visitor.
            Field field;
            column->get(i, field);
            *buffer = applyVisitor(visitor, field);
            buffer += features_count;
        }
    }

    /// Buffer should be allocated with features_count * column->size() elements.
    /// Place string pointers in positions buffer[0], buffer[features_count], ... , buffer[size * features_count]
    void placeStringColumn(const ColumnString & column, const char ** buffer, size_t features_count) const
    {
        size_t size = column.size();
        for (size_t i = 0; i < size; ++i)
        {
            *buffer = const_cast<char *>(column.getDataAtWithTerminatingZero(i).data);
            buffer += features_count;
        }
    }

    /// Buffer should be allocated with features_count * column->size() elements.
    /// Place string pointers in positions buffer[0], buffer[features_count], ... , buffer[size * features_count]
    /// Returns PODArray which holds data (because ColumnFixedString doesn't store terminating zero).
    PODArray<char> placeFixedStringColumn(
            const ColumnFixedString & column, const char ** buffer, size_t features_count) const
    {
        size_t size = column.size();
        size_t str_size = column.getN();
        PODArray<char> data(size * (str_size + 1));
        char * data_ptr = data.data();

        for (size_t i = 0; i < size; ++i)
        {
            auto ref = column.getDataAt(i);
            memcpy(data_ptr, ref.data, ref.size);
            data_ptr[ref.size] = 0;
            *buffer = data_ptr;
            data_ptr += ref.size + 1;
            buffer += features_count;
        }

        return data;
    }

    /// Place columns into buffer, returns column which holds placed data. Buffer should contains column->size() values.
    template <typename T>
    ColumnPtr placeNumericColumns(const ColumnRawPtrs & columns,
                                  size_t offset, size_t size, const T** buffer) const
    {
        if (size == 0)
            return nullptr;
        size_t column_size = columns[offset]->size();
        auto data_column = ColumnVector<T>::create(size * column_size);
        T * data = data_column->getData().data();
        for (size_t i = 0; i < size; ++i)
        {
            auto column = columns[offset + i];
            if (column->isNumeric())
                placeColumnAsNumber(column, data + i, size);
        }

        for (size_t i = 0; i < column_size; ++i)
        {
            *buffer = data;
            ++buffer;
            data += size;
        }

        return data_column;
    }

    /// Place columns into buffer, returns data which was used for fixed string columns.
    /// Buffer should contains column->size() values, each value contains size strings.
    std::vector<PODArray<char>> placeStringColumns(
            const ColumnRawPtrs & columns, size_t offset, size_t size, const char ** buffer) const
    {
        if (size == 0)
            return {};

        std::vector<PODArray<char>> data;
        for (size_t i = 0; i < size; ++i)
        {
            auto column = columns[offset + i];
            if (auto column_string = typeid_cast<const ColumnString *>(column))
                placeStringColumn(*column_string, buffer + i, size);
            else if (auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column))
                data.push_back(placeFixedStringColumn(*column_fixed_string, buffer + i, size));
            else
                throw Exception("Cannot place string column.", ErrorCodes::LOGICAL_ERROR);
        }

        return data;
    }

    /// Calc hash for string cat feature at ps positions.
    template <typename Column>
    void calcStringHashes(const Column * column, size_t ps, const int ** buffer) const
    {
        size_t column_size = column->size();
        for (size_t j = 0; j < column_size; ++j)
        {
            auto ref = column->getDataAt(j);
            const_cast<int *>(*buffer)[ps] = api->GetStringCatFeatureHash(ref.data, ref.size);
            ++buffer;
        }
    }

    /// Calc hash for int cat feature at ps position. Buffer at positions ps should contains unhashed values.
    void calcIntHashes(size_t column_size, size_t ps, const int ** buffer) const
    {
        for (size_t j = 0; j < column_size; ++j)
        {
            const_cast<int *>(*buffer)[ps] = api->GetIntegerCatFeatureHash((*buffer)[ps]);
            ++buffer;
        }
    }

    /// buffer contains column->size() rows and size columns.
    /// For int cat features calc hash inplace.
    /// For string cat features calc hash from column rows.
    void calcHashes(const ColumnRawPtrs & columns, size_t offset, size_t size, const int ** buffer) const
    {
        if (size == 0)
            return;
        size_t column_size = columns[offset]->size();

        std::vector<PODArray<char>> data;
        for (size_t i = 0; i < size; ++i)
        {
            auto column = columns[offset + i];
            if (auto column_string = typeid_cast<const ColumnString *>(column))
                calcStringHashes(column_string, i, buffer);
            else if (auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column))
                calcStringHashes(column_fixed_string, i, buffer);
            else
                calcIntHashes(column_size, i, buffer);
        }
    }

    /// buffer[column_size * cat_features_count] -> char * => cat_features[column_size][cat_features_count] -> char *
    void fillCatFeaturesBuffer(const char *** cat_features, const char ** buffer,
                               size_t column_size, size_t cat_features_count_current) const
    {
        for (size_t i = 0; i < column_size; ++i)
        {
            *cat_features = buffer;
            ++cat_features;
            buffer += cat_features_count_current;
        }
    }

    /// Convert values to row-oriented format and call evaluation function from CatBoost wrapper api.
    ///  * CalcModelPredictionFlat if no cat features
    ///  * CalcModelPrediction if all cat features are strings
    ///  * CalcModelPredictionWithHashedCatFeatures if has int cat features.
    ColumnPtr evalImpl(const ColumnRawPtrs & columns, size_t float_features_count_current, size_t cat_features_count_current,
                       bool cat_features_are_strings) const
    {
        std::string error_msg = "Error occurred while applying CatBoost model: ";
        size_t column_size = columns.front()->size();

        auto result = ColumnFloat64::create(column_size);
        auto result_buf = result->getData().data();

        /// Prepare float features.
        PODArray<const float *> float_features(column_size);
        auto float_features_buf = float_features.data();
        /// Store all float data into single column. float_features is a list of pointers to it.
        auto float_features_col = placeNumericColumns<float>(columns, 0, float_features_count_current, float_features_buf);

        if (cat_features_count_current == 0)
        {
            if (!api->CalcModelPredictionFlat(handle->get(), column_size,
                                              float_features_buf, float_features_count_current,
                                              result_buf, column_size))
            {

                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
            return result;
        }

        /// Prepare cat features.
        if (cat_features_are_strings)
        {
            /// cat_features_holder stores pointers to ColumnString data or fixed_strings_data.
            PODArray<const char *> cat_features_holder(cat_features_count_current * column_size);
            PODArray<const char **> cat_features(column_size);
            auto cat_features_buf = cat_features.data();

            fillCatFeaturesBuffer(cat_features_buf, cat_features_holder.data(), column_size, cat_features_count_current);
            /// Fixed strings are stored without termination zero, so have to copy data into fixed_strings_data.
            auto fixed_strings_data = placeStringColumns(columns, float_features_count_current,
                                                         cat_features_count_current, cat_features_holder.data());

            if (!api->CalcModelPrediction(handle->get(), column_size,
                                          float_features_buf, float_features_count_current,
                                          cat_features_buf, cat_features_count_current,
                                          result_buf, column_size))
            {
                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
        }
        else
        {
            PODArray<const int *> cat_features(column_size);
            auto cat_features_buf = cat_features.data();
            auto cat_features_col = placeNumericColumns<int>(columns, float_features_count_current,
                                                             cat_features_count_current, cat_features_buf);
            calcHashes(columns, float_features_count_current, cat_features_count_current, cat_features_buf);
            if (!api->CalcModelPredictionWithHashedCatFeatures(
                    handle->get(), column_size,
                    float_features_buf, float_features_count_current,
                    cat_features_buf, cat_features_count_current,
                    result_buf, column_size))
            {
                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
        }

        return result;
    }
};


/// Holds CatBoost wrapper library and provides wrapper interface.
class CatBoostLibHolder: public CatBoostWrapperAPIProvider
{
public:
    explicit CatBoostLibHolder(std::string lib_path_) : lib_path(std::move(lib_path_)), lib(lib_path) { initAPI(); }

    const CatBoostWrapperAPI & getAPI() const override { return api; }
    const std::string & getCurrentPath() const { return lib_path; }

private:
    CatBoostWrapperAPI api;
    std::string lib_path;
    SharedLibrary lib;

    void initAPI();

    template <typename T>
    void load(T& func, const std::string & name) { func = lib.get<T>(name); }
};

void CatBoostLibHolder::initAPI()
{
    load(api.ModelCalcerCreate, "ModelCalcerCreate");
    load(api.ModelCalcerDelete, "ModelCalcerDelete");
    load(api.GetErrorString, "GetErrorString");
    load(api.LoadFullModelFromFile, "LoadFullModelFromFile");
    load(api.CalcModelPredictionFlat, "CalcModelPredictionFlat");
    load(api.CalcModelPrediction, "CalcModelPrediction");
    load(api.CalcModelPredictionWithHashedCatFeatures, "CalcModelPredictionWithHashedCatFeatures");
    load(api.GetStringCatFeatureHash, "GetStringCatFeatureHash");
    load(api.GetIntegerCatFeatureHash, "GetIntegerCatFeatureHash");
    load(api.GetFloatFeaturesCount, "GetFloatFeaturesCount");
    load(api.GetCatFeaturesCount, "GetCatFeaturesCount");
}

std::shared_ptr<CatBoostLibHolder> getCatBoostWrapperHolder(const std::string & lib_path)
{
    static std::weak_ptr<CatBoostLibHolder> ptr;
    static std::mutex mutex;

    std::lock_guard<std::mutex> lock(mutex);
    auto result = ptr.lock();

    if (!result || result->getCurrentPath() != lib_path)
    {
        result = std::make_shared<CatBoostLibHolder>(lib_path);
        /// This assignment is not atomic, which prevents from creating lock only inside 'if'.
        ptr = result;
    }

    return result;
}

}


CatBoostModel::CatBoostModel(std::string name_, std::string model_path_, std::string lib_path_,
                             const ExternalLoadableLifetime & lifetime)
    : name(std::move(name_)), model_path(std::move(model_path_)), lib_path(std::move(lib_path_)), lifetime(lifetime)
{
    try
    {
        init();
    }
    catch (...)
    {
        creation_exception = std::current_exception();
    }

    creation_time = std::chrono::system_clock::now();
}

void CatBoostModel::init()
{
    api_provider = getCatBoostWrapperHolder(lib_path);
    api = &api_provider->getAPI();
    model = std::make_unique<CatBoostModelImpl>(api, model_path);
    float_features_count = model->getFloatFeaturesCount();
    cat_features_count = model->getCatFeaturesCount();
}

const ExternalLoadableLifetime & CatBoostModel::getLifetime() const
{
    return lifetime;
}

bool CatBoostModel::isModified() const
{
    return true;
}

std::unique_ptr<IExternalLoadable> CatBoostModel::clone() const
{
    return std::make_unique<CatBoostModel>(name, model_path, lib_path, lifetime);
}

size_t CatBoostModel::getFloatFeaturesCount() const
{
    return float_features_count;
}

size_t CatBoostModel::getCatFeaturesCount() const
{
    return cat_features_count;
}

ColumnPtr CatBoostModel::evaluate(const ColumnRawPtrs & columns) const
{
    if (!model)
        throw Exception("CatBoost model was not loaded.", ErrorCodes::LOGICAL_ERROR);
    return model->evaluate(columns);
}

}
