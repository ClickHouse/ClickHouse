#pragma once
#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

struct CatBoostWrapperApi;
class CatBoostWrapperApiProvider
{
public:
    virtual ~CatBoostWrapperApiProvider() = default;
    virtual const CatBoostWrapperApi & getApi() const = 0;
};


class CatBoostModel : public IExternalLoadable
{
public:
    CatBoostModel(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
                  const std::string & lib_path);

    const ExternalLoadableLifetime & getLifetime() const override;

    std::string getName() const override { return name; }

    bool supportUpdates() const override { return true; }

    bool isModified() const override;

    std::unique_ptr<IExternalLoadable> cloneObject() const override;

    std::exception_ptr getCreationException() const override { return creation_exception; }

    size_t getFloatFeaturesCount() const;
    size_t getCatFeaturesCount() const;

    void apply(const Columns & floatColumns, const Columns & catColumns, ColumnFloat64 & result);

private:
    std::string name;
    std::string model_path;
    ExternalLoadableLifetime lifetime;
    std::exception_ptr creation_exception;
    std::shared_ptr<CatBoostWrapperApiProvider> api_provider;
    const CatBoostWrapperApi * api;

    CatBoostModel(const std::string & name, const std::string & model_path,
                  const std::string & lib_path, const ExternalLoadableLifetime & lifetime);
};

}
