#include "IGgmlModel.h"

namespace DB {

void IGgmlModel::load(ConfigPtr config)
{
    std::lock_guard lock{load_mutex};
    if (!loaded) {
        LoadImpl(config);
        loaded = true;
    }
}

}
