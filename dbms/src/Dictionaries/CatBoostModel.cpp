#include <Dictionaries/CatBoostModel.h>

namespace DB
{

CatBoostModel::CatBoostModel(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : lifetime(config, config_prefix)
{

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
