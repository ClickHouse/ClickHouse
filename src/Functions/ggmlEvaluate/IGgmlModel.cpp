#include "IGgmlModel.h"

namespace DB {

void IGgmlModel::load(const std::string & fname)
{
    std::lock_guard lock{load_mutex};
    if (!loaded) {
        LoadImpl(fname);
        loaded = true;
    }
}

}
