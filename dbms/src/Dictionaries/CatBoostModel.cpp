#include <Dictionaries/CatBoostModel.h>
#include <Core/FieldVisitors.h>
#include <boost/dll/import.hpp>
#include <mutex>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/PODArray.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_LOAD_CATBOOST_MODEL;
extern const int CANNOT_APPLY_CATBOOST_MODEL;
}


struct CatBoostWrapperApi
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
};


namespace
{

class CatBoostModelHolder
{
private:
    CatBoostWrapperApi::ModelCalcerHandle * handle;
    const CatBoostWrapperApi * api;
public:
    explicit CatBoostModelHolder(const CatBoostWrapperApi * api) : api(api) { handle = api->ModelCalcerCreate(); }
    ~CatBoostModelHolder() { api->ModelCalcerDelete(handle); }

    CatBoostWrapperApi::ModelCalcerHandle * get() { return handle; }
    explicit operator CatBoostWrapperApi::ModelCalcerHandle * () { return handle; }
};


class CatBoostModelImpl : public ICatBoostModel
{
public:
    CatBoostModelImpl(const CatBoostWrapperApi * api, const std::string & model_path) : api(api)
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
        handle = std::move(handle_);
    }

    ColumnPtr calc(const Columns & columns, size_t float_features_count, size_t cat_features_count) const override
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
            const auto & column = columns[i];
            if (column->isNumeric())
                cat_features_are_strings = false;
            else if (!(typeid_cast<const ColumnString *>(column.get())
                       || typeid_cast<const ColumnFixedString *>(column.get())))
            {
                std::string msg;
                {
                    WriteBufferFromString buffer(msg);
                    buffer << "Column " << i << "should be numeric or string.";
                }
                throw Exception(msg, ErrorCodes::BAD_ARGUMENTS);
            }
        }

        return calcImpl(columns, float_features_count, cat_features_count, cat_features_are_strings);
    }

private:
    std::unique_ptr<CatBoostModelHolder> handle;
    const CatBoostWrapperApi * api;

    /// Buffer should be allocated with features_count * column->size() elements.
    /// Place column elements in positions buffer[0], buffer[features_count], ... , buffer[size * features_count]
    template <typename T>
    void placeColumnAsNumber(const ColumnPtr & column, T * buffer, size_t features_count) const
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
    ColumnPtr placeNumericColumns(const Columns & columns, size_t offset, size_t size, const T** buffer) const
    {
        if (size == 0)
            return nullptr;
        size_t column_size = columns[offset]->size();
        auto data_column = std::make_shared<ColumnVector<T>>(size * column_size);
        T* data = data_column->getData().data();
        for (size_t i = 0; i < size; ++i)
        {
            const auto & column = columns[offset + i];
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
            const Columns & columns, size_t offset, size_t size, const char ** buffer) const
    {
        if (size == 0)
            return {};

        std::vector<PODArray<char>> data;
        for (size_t i = 0; i < size; ++i)
        {
            const auto & column = columns[offset + i];
            if (auto column_string = typeid_cast<const ColumnString *>(column.get()))
                placeStringColumn(*column_string, buffer + i, size);
            else if (auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column.get()))
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
    void calcHashes(const Columns & columns, size_t offset, size_t size, const int ** buffer) const
    {
        if (size == 0)
            return;
        size_t column_size = columns[offset]->size();

        std::vector<PODArray<char>> data;
        for (size_t i = 0; i < size; ++i)
        {
            const auto & column = columns[offset + i];
            if (auto column_string = typeid_cast<const ColumnString *>(column.get()))
                calcStringHashes(column_string, i, buffer);
            else if (auto column_fixed_string = typeid_cast<const ColumnFixedString *>(column.get()))
                calcStringHashes(column_fixed_string, i, buffer);
            else
                calcIntHashes(column_size, i, buffer);
        }
    }

    /// buffer[column_size * cat_features_count] -> char * => cat_features[column_size][cat_features_count] -> char *
    void fillCatFeaturesBuffer(const char *** cat_features, const char ** buffer,
                               size_t column_size, size_t cat_features_count) const
    {
        for (size_t i = 0; i < column_size; ++i)
        {
            *cat_features = buffer;
            ++cat_features;
            buffer += cat_features_count;
        }
    }

    ColumnPtr calcImpl(const Columns & columns, size_t float_features_count, size_t cat_features_count,
                       bool cat_features_are_strings) const
    {
        // size_t size = columns.size();
        size_t column_size = columns.front()->size();

        PODArray<const float *> float_features(column_size);
        auto float_features_buf = float_features.data();
        auto float_features_col = placeNumericColumns<float>(columns, 0, float_features_count, float_features_buf);

        auto result= std::make_shared<ColumnFloat64>(column_size);
        auto result_buf = result->getData().data();

        std::string error_msg = "Error occurred while applying CatBoost model: ";

        if (cat_features_count == 0)
        {
            if (!api->CalcModelPredictionFlat(handle->get(), column_size,
                                              float_features_buf, float_features_count,
                                              result_buf, column_size))
            {

                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
            return result;
        }


        if (cat_features_are_strings)
        {
            PODArray<const char *> cat_features_holder(cat_features_count * column_size);
            PODArray<const char **> cat_features(column_size);
            auto cat_features_buf = cat_features.data();

            fillCatFeaturesBuffer(cat_features_buf, cat_features_holder.data(), column_size, cat_features_count);
            auto fixed_strings_data = placeStringColumns(columns, float_features_count,
                                                         cat_features_count, cat_features_holder.data());

            if (!api->CalcModelPrediction(handle->get(), column_size,
                                          float_features_buf, float_features_count,
                                          cat_features_buf, cat_features_count,
                                          result_buf, column_size))
            {
                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
        }
        else
        {
            PODArray<const int *> cat_features(column_size);
            auto cat_features_buf = cat_features.data();
            auto cat_features_col = placeNumericColumns<int>(columns, float_features_count,
                                                             cat_features_count, cat_features_buf);
            calcHashes(columns, float_features_count, cat_features_count, cat_features_buf);
            if (!api->CalcModelPredictionWithHashedCatFeatures(
                    handle->get(), column_size,
                    float_features_buf, float_features_count,
                    cat_features_buf, cat_features_count,
                    result_buf, column_size))
            {
                throw Exception(error_msg + api->GetErrorString(), ErrorCodes::CANNOT_APPLY_CATBOOST_MODEL);
            }
        }

        return result;
    }
};


class CatBoostLibHolder: public CatBoostWrapperApiProvider
{
public:
    explicit CatBoostLibHolder(const std::string & lib_path) : lib_path(lib_path), lib(lib_path) { initApi(); }

    const CatBoostWrapperApi & getApi() const override { return api; }
    const std::string & getCurrentPath() const { return lib_path; }

private:
    CatBoostWrapperApi api;
    std::string lib_path;
    boost::dll::shared_library lib;

    void initApi();

    template <typename T>
    void load(T& func, const std::string & name)
    {
        using Type = typename std::remove_pointer<T>::type;
        func = lib.get<Type>(name);
    }
};

void CatBoostLibHolder::initApi()
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


CatBoostModel::CatBoostModel(const std::string & name, const std::string & model_path, const std::string & lib_path,
                             const ExternalLoadableLifetime & lifetime,
                             size_t float_features_count, size_t cat_features_count)
    : name(name), model_path(model_path), lib_path(lib_path), lifetime(lifetime),
      float_features_count(float_features_count), cat_features_count(cat_features_count)
{
    try
    {
        init(lib_path);
    }
    catch (...)
    {
        creation_exception = std::current_exception();
    }
}

void CatBoostModel::init(const std::string & lib_path)
{
    api_provider = getCatBoostWrapperHolder(lib_path);
    api = &api_provider->getApi();
    model = std::make_unique<CatBoostModelImpl>(api, model_path);
}

const ExternalLoadableLifetime & CatBoostModel::getLifetime() const
{
    return lifetime;
}

bool CatBoostModel::isModified() const
{
    return true;
}

std::unique_ptr<IExternalLoadable> CatBoostModel::cloneObject() const
{
    return std::make_unique<CatBoostModel>(name, model_path, lib_path, lifetime, float_features_count, cat_features_count);
}

size_t CatBoostModel::getFloatFeaturesCount() const
{
    return float_features_count;
}

size_t CatBoostModel::getCatFeaturesCount() const
{
    return cat_features_count;
}

ColumnPtr CatBoostModel::evaluate(const Columns & columns) const
{
    if (!model)
        throw Exception("CatBoost model was not loaded.", ErrorCodes::LOGICAL_ERROR);
    return model->calc(columns, float_features_count, cat_features_count);
}

}
