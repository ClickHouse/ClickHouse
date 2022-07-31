#include <Core/Joins.h>

namespace DB
{

const char * toString(JoinKind kind)
{
    switch (kind)
    {
        case JoinKind::Inner: return "INNER";
        case JoinKind::Left: return "LEFT";
        case JoinKind::Right: return "RIGHT";
        case JoinKind::Full: return "FULL";
        case JoinKind::Cross: return "CROSS";
        case JoinKind::Comma: return "COMMA";
    }
};

const char * toString(JoinStrictness strictness)
{
    switch (strictness)
    {
        case JoinStrictness::Unspecified: return "UNSPECIFIED";
        case JoinStrictness::RightAny: return "RIGHT_ANY";
        case JoinStrictness::Any: return "ANY";
        case JoinStrictness::All: return "ALL";
        case JoinStrictness::Asof: return "ASOF";
        case JoinStrictness::Semi: return "SEMI";
        case JoinStrictness::Anti: return "ANTI";
    }
}

const char * toString(JoinLocality locality)
{
    switch (locality)
    {
        case JoinLocality::Unspecified: return "UNSPECIFIED";
        case JoinLocality::Local: return "LOCAL";
        case JoinLocality::Global: return "GLOBAL";
    }
}

const char * toString(ASOFJoinInequality asof_join_inequality)
{
    switch (asof_join_inequality)
    {
        case ASOFJoinInequality::None: return "NONE";
        case ASOFJoinInequality::Less: return "LESS";
        case ASOFJoinInequality::Greater: return "GREATER";
        case ASOFJoinInequality::LessOrEquals: return "LESS_OR_EQUALS";
        case ASOFJoinInequality::GreaterOrEquals: return "GREATER_OR_EQUALS";
    }
}

const char * toString(JoinAlgorithm join_algorithm)
{
    switch (join_algorithm)
    {
        case JoinAlgorithm::DEFAULT: return "DEFAULT";
        case JoinAlgorithm::AUTO: return "AUTO";
        case JoinAlgorithm::HASH: return "HASH";
        case JoinAlgorithm::PARTIAL_MERGE: return "PARTIAL_MERGE";
        case JoinAlgorithm::PREFER_PARTIAL_MERGE: return "PREFER_PARTIAL_MERGE";
        case JoinAlgorithm::PARALLEL_HASH: return "PARALLEL_HASH";
        case JoinAlgorithm::DIRECT: return "DIRECT";
        case JoinAlgorithm::FULL_SORTING_MERGE: return "FULL_SORTING_MERGE";
    }
}

const char * toString(JoinTableSide join_table_side)
{
    switch (join_table_side)
    {
        case JoinTableSide::Left: return "LEFT";
        case JoinTableSide::Right: return "RIGHT";
    }
}

}
