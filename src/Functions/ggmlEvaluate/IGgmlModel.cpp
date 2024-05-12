#include "IGgmlModel.h"


namespace DB
{

void IGgmlModel::load(ConfigPtr config)
{
    std::lock_guard lock{load_mutex};
    if (!loaded)
    {
        loadImpl(config);
        loaded = true;
    }
}

std::string IGgmlModel::eval(const std::string & input, const GgmlModelParams & user_params)
{
    return evalImpl(input, user_params);
}

}
