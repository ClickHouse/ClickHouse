#include <Dictionaries/CatBoostModel.h>
#include <boost/dll/import.hpp>
#include <mutex>

namespace DB
{

namespace
{

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

class CatBoostWrapperHolder : public CatBoostWrapperApiProvider
{
public:
    CatBoostWrapperHolder(const std::string & lib_path) : lib(lib_path), lib_path(lib_path) { initApi(); }

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
        using Type = std::remove_pointer<T>::type;
        func = lib.get<Type>(name);
    }
};

void CatBoostWrapperHolder::initApi()
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

std::shared_ptr<CatBoostWrapperHolder> getCatBoostWrapperHolder(const std::string & lib_path)
{
    static std::weak_ptr<CatBoostWrapperHolder> ptr;
    static std::mutex mutex;

    std::lock_guard<std::mutex> lock(mutex);
    auto result = ptr.lock();

    if (!result || result->getCurrentPath() != lib_path)
    {
        result = std::make_shared<CatBoostWrapperHolder>(lib_path);
        /// This assignment is not atomic, which prevents from creating lock only inside 'if'.
        ptr = result;
    }

    return result;
}

}

CatBoostModel::CatBoostModel(const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix, const std::string & lib_path)
    : lifetime(config, config_prefix)
{

}

CatBoostModel::CatBoostModel(const std::string & name, const std::string & model_path, const std::string & lib_path,
                             const ExternalLoadableLifetime & lifetime)
    : name(name), model_path(model_path), lifetime(lifetime)
{
    try
    {
        api_provider = getCatBoostWrapperHolder(lib_path);
        api = &api_provider->getApi();
    }
    catch (...)
    {
        creation_exception = std::current_exception();
    }
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
    return nullptr;
}

size_t CatBoostModel::getFloatFeaturesCount() const
{
    return 0;
}

size_t CatBoostModel::getCatFeaturesCount() const
{
    return 0;
}

void CatBoostModel::apply(const Columns & floatColumns, const Columns & catColumns, ColumnFloat64 & result)
{

}

}
