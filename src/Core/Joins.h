#pragma once

namespace DB
{

/// Join method.
enum class JoinKind
{
    Inner, /// Leave only rows that was JOINed.
    Left, /// If in "right" table there is no corresponding rows, use default values instead.
    Right,
    Full,
    Cross, /// Direct product. Strictness and condition doesn't matter.
    Comma /// Same as direct product. Intended to be converted to INNER JOIN with conditions from WHERE.
};

const char * toString(JoinKind kind);

inline constexpr bool isLeft(JoinKind kind)         { return kind == JoinKind::Left; }
inline constexpr bool isRight(JoinKind kind)        { return kind == JoinKind::Right; }
inline constexpr bool isInner(JoinKind kind)        { return kind == JoinKind::Inner; }
inline constexpr bool isFull(JoinKind kind)         { return kind == JoinKind::Full; }
inline constexpr bool isCrossOrComma(JoinKind kind) { return kind == JoinKind::Comma || kind == JoinKind::Cross; }
inline constexpr bool isRightOrFull(JoinKind kind)  { return kind == JoinKind::Right || kind == JoinKind::Full; }
inline constexpr bool isLeftOrFull(JoinKind kind)   { return kind == JoinKind::Left  || kind == JoinKind::Full; }
inline constexpr bool isInnerOrRight(JoinKind kind) { return kind == JoinKind::Inner || kind == JoinKind::Right; }
inline constexpr bool isInnerOrLeft(JoinKind kind)  { return kind == JoinKind::Inner || kind == JoinKind::Left; }

/// Allows more optimal JOIN for typical cases.
enum class JoinStrictness
{
    Unspecified,
    RightAny, /// Old ANY JOIN. If there are many suitable rows in right table, use any from them to join.
    Any, /// Semi Join with any value from filtering table. For LEFT JOIN with Any and RightAny are the same.
    All, /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
    Asof, /// For the last JOIN column, pick the latest value
    Semi, /// LEFT or RIGHT. SEMI LEFT JOIN filters left table by values exists in right table. SEMI RIGHT - otherwise.
    Anti, /// LEFT or RIGHT. Same as SEMI JOIN but filter values that are NOT exists in other table.
};

const char * toString(JoinStrictness strictness);

/// Algorithm for distributed query processing.
enum class JoinLocality
{
    Unspecified,
    Local, /// Perform JOIN, using only data available on same servers (co-located data).
    Global /// Collect and merge data from remote servers, and broadcast it to each server.
};

const char * toString(JoinLocality locality);

/// ASOF JOIN inequality type
enum class ASOFJoinInequality
{
    None,
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
};

const char * toString(ASOFJoinInequality asof_join_inequality);

inline constexpr ASOFJoinInequality getASOFJoinInequality(std::string_view func_name)
{
    ASOFJoinInequality inequality = ASOFJoinInequality::None;

    if (func_name == "less")
        inequality = ASOFJoinInequality::Less;
    else if (func_name == "greater")
        inequality = ASOFJoinInequality::Greater;
    else if (func_name == "lessOrEquals")
        inequality = ASOFJoinInequality::LessOrEquals;
    else if (func_name == "greaterOrEquals")
        inequality = ASOFJoinInequality::GreaterOrEquals;

    return inequality;
}

inline constexpr ASOFJoinInequality reverseASOFJoinInequality(ASOFJoinInequality inequality)
{
    if (inequality == ASOFJoinInequality::Less)
        return ASOFJoinInequality::Greater;
    else if (inequality == ASOFJoinInequality::Greater)
        return ASOFJoinInequality::Less;
    else if (inequality == ASOFJoinInequality::LessOrEquals)
        return ASOFJoinInequality::GreaterOrEquals;
    else if (inequality == ASOFJoinInequality::GreaterOrEquals)
        return ASOFJoinInequality::LessOrEquals;

    return ASOFJoinInequality::None;
}

enum class JoinAlgorithm
{
    DEFAULT = 0,
    AUTO,
    HASH,
    PARTIAL_MERGE,
    PREFER_PARTIAL_MERGE,
    PARALLEL_HASH,
    DIRECT,
    FULL_SORTING_MERGE,
};

const char * toString(JoinAlgorithm join_algorithm);

enum class JoinTableSide
{
    Left,
    Right
};

const char * toString(JoinTableSide join_table_side);

}
