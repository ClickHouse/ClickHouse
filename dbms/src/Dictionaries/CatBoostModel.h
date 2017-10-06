#pragma once
#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

class CatBoostModel : public IExternalLoadable
{
public:
    CatBoostModel(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

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
    ExternalLoadableLifetime lifetime;
    std::string name;
    std::exception_ptr creation_exception;

};

}
