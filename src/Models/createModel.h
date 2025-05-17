
#pragma once

#include <Models/IModel.h>

namespace DB
{

ModelPtr createModel(const String& algorithm, const HyperParameters& hyperparamers);

}
