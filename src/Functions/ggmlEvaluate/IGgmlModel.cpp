#include "IGgmlModel.h"


namespace DB {

void IGgmlModel::load(ConfigPtr config)
{
    std::lock_guard lock{load_mutex};
    if (!loaded) {
        loadImpl(config);
        loaded = true;
    }
}

std::string IGgmlModel::eval(GgmlModelParams params, const std::string & input)
{
    return evalImpl(params, input);
}

}
