#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

String toString(MergeAlgorithm merge_algorithm)
{
    switch (merge_algorithm)
    {
        case MergeAlgorithm::Undecided:
            return "Undecided";
        case MergeAlgorithm::Horizontal:
            return "Horizontal";
        case MergeAlgorithm::Vertical:
            return "Vertical";
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown MergeAlgorithm {}", static_cast<UInt64>(merge_algorithm));
}

}
