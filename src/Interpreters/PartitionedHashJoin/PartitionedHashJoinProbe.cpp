#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/PartitionedHashJoin/PartitionedHashJoin.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/** Each combination's maps shape mirrors the `MapGetter` table, and only the combinations a real
  * query plan can reach are listed - anything else is a logic error, as in `HashJoin::joinBlock`.
  * `prefer_use_maps_all` is set when the build barrier promoted ALL to RightAny on a unique-key
  * build, in which case the ALL-built `RowRefList` maps are probed with RightAny semantics and the
  * probe skips the replication machinery, as the other hash joins do.
  *
  * The bodies live in `PartitionedHashJoinProbeImpl.h`, explicitly instantiated per kind so no one
  * translation unit has to compile them all.
  */
JoinResultPtr PartitionedHashJoin::probeDispatch(Block block, size_t lane)
{
    const JoinKind kind = leaf_join->getKind();
    const JoinStrictness strictness = leaf_join->getStrictness();
    const bool prefer_use_maps_all = leaf_join->preferUseMapsAll();

    using enum JoinKind;
    using enum JoinStrictness;

    if (prefer_use_maps_all)
    {
        if (kind == Inner && strictness == RightAny)
            return probeImpl<Inner, RightAny, HashJoin::MapsAll>(std::move(block), lane);
        if (kind == Left && strictness == RightAny)
            return probeImpl<Left, RightAny, HashJoin::MapsAll>(std::move(block), lane);
    }
    else
    {
        if (kind == Inner)
        {
            switch (strictness)
            {
                case All: return probeImpl<Inner, All, HashJoin::MapsAll>(std::move(block), lane);
                case RightAny: return probeImpl<Inner, RightAny, HashJoin::MapsOne>(std::move(block), lane);
                case Any: return probeImpl<Inner, Any, HashJoin::MapsOne>(std::move(block), lane);
                case Asof: return probeImpl<Inner, Asof, HashJoin::MapsAsof>(std::move(block), lane);
                default: break;
            }
        }
        else if (kind == Left)
        {
            switch (strictness)
            {
                case All: return probeImpl<Left, All, HashJoin::MapsAll>(std::move(block), lane);
                case RightAny: return probeImpl<Left, RightAny, HashJoin::MapsOne>(std::move(block), lane);
                case Any: return probeImpl<Left, Any, HashJoin::MapsOne>(std::move(block), lane);
                case Semi: return probeImpl<Left, Semi, HashJoin::MapsOne>(std::move(block), lane);
                case Anti: return probeImpl<Left, Anti, HashJoin::MapsOne>(std::move(block), lane);
                case Asof: return probeImpl<Left, Asof, HashJoin::MapsAsof>(std::move(block), lane);
                default: break;
            }
        }
        else if (kind == Right)
        {
            switch (strictness)
            {
                case All: return probeImpl<Right, All, HashJoin::MapsAll>(std::move(block), lane);
                case RightAny: return probeImpl<Right, RightAny, HashJoin::MapsAll>(std::move(block), lane);
                case Any: return probeImpl<Right, Any, HashJoin::MapsAll>(std::move(block), lane);
                case Semi: return probeImpl<Right, Semi, HashJoin::MapsAll>(std::move(block), lane);
                case Anti: return probeImpl<Right, Anti, HashJoin::MapsAll>(std::move(block), lane);
                default: break;
            }
        }
        else if (kind == Full)
        {
            switch (strictness)
            {
                case All: return probeImpl<Full, All, HashJoin::MapsAll>(std::move(block), lane);
                case RightAny: return probeImpl<Full, RightAny, HashJoin::MapsAll>(std::move(block), lane);
                default: break;
            }
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination for PartitionedHashJoin: {} {}", strictness, kind);
}

}
