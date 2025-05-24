#include <base/types.h>
#include <base/strong_typedef.h>
#include <Core/Types_fwd.h>

namespace DB
{

using HyperParameters = std::unordered_map<String, String>;

// NOLINTBEGIN (Force strict types to distinct featurs and targets)
typedef Float64 Feature;
typedef std::vector<Feature> Features;
typedef std::vector<Features> FeatureMatrix;

typedef Float64 Target;
typedef std::vector<Target> Targets;
//NOLINTEND

static constexpr inline std::size_t kRecommendedBatchSize = 128;

class IModel;
using ModelPtr = std::shared_ptr<IModel>;

}
