#include <Interpreters/Streaming/WindowCommon.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/FunctionHelpers.h>
// #include <Interpreters/Streaming/TableFunctionDescription.h>
// #include <Interpreters/Streaming/TimeTransformHelper.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
// #include <Parsers/Streaming/ASTSessionRangeComparision.h>
// #include <Common/ProtonCommon.h>
#include <Common/intExp.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CONVERT_TYPE;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int BAD_ARGUMENTS;
extern const int MISSING_SESSION_KEY;
}

namespace Streaming
{
namespace
{
std::optional<IntervalKind> mapIntervalKind(const String & func_name)
{
    if (func_name == "to_interval_nanosecond")
        return IntervalKind::Nanosecond;
    else if (func_name == "to_interval_microsecond")
        return IntervalKind::Microsecond;
    else if (func_name == "to_interval_millisecond")
        return IntervalKind::Millisecond;
    else if (func_name == "to_interval_second")
        return IntervalKind::Second;
    else if (func_name == "to_interval_minute")
        return IntervalKind::Minute;
    else if (func_name == "to_interval_hour")
        return IntervalKind::Hour;
    else if (func_name == "to_interval_day")
        return IntervalKind::Day;
    else if (func_name == "to_interval_week")
        return IntervalKind::Week;
    else if (func_name == "to_interval_month")
        return IntervalKind::Month;
    else if (func_name == "to_interval_quarter")
        return IntervalKind::Quarter;
    else if (func_name == "to_interval_year")
        return IntervalKind::Year;
    else
        return {};
}

// ALWAYS_INLINE bool isTimeExprAST(const ASTPtr ast)
// {
//     /// Assume it is a time or time_expr, we will check it later again
//     if (ast->as<ASTIdentifier>())
//         return true;
//     else if (auto * func = ast->as<ASTFunction>())
//         return !mapIntervalKind(func->name);
//     return false;
// }

// ALWAYS_INLINE bool isIntervalAST(const ASTPtr ast)
// {
//     auto func_node = ast->as<ASTFunction>();
//     return (func_node && mapIntervalKind(func_node->name));
// }

// ALWAYS_INLINE bool isTimeZoneAST(const ASTPtr ast)
// {
//     return (ast->as<ASTLiteral>());
// }

// /// Calculate window start / end for time column in num_units
// /// @return (window_start, window_end) tuple
// template <IntervalKind::Kind unit, typename TimeColumnType>
// Columns getWindowStartAndEndFor(const TimeColumnType & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
// {
//     constexpr bool time_col_is_datetime64 = std::is_same_v<TimeColumnType, ColumnDateTime64>;

//     const auto & time_data = time_column.getData();
//     size_t size = time_column.size();
//     typename TimeColumnType::MutablePtr start, end;
//     if constexpr (time_col_is_datetime64)
//     {
//         start = TimeColumnType::create(size, time_column.getScale());
//         end = TimeColumnType::create(size, time_column.getScale());
//     }
//     else
//     {
//         start = TimeColumnType::create(size);
//         end = TimeColumnType::create(size);
//     }

//     auto & start_data = start->getData();
//     auto & end_data = end->getData();

//     for (size_t i = 0; i != size; ++i)
//     {
//         if constexpr (time_col_is_datetime64)
//         {
//             start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone, time_column.getScale());
//             end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone, time_column.getScale());
//         }
//         else
//         {
//             start_data[i] = ToStartOfTransform<unit>::execute(time_data[i], num_units, time_zone);
//             end_data[i] = AddTime<unit>::execute(start_data[i], num_units, time_zone);
//         }
//     }

//     Columns result;
//     result.emplace_back(std::move(start));
//     result.emplace_back(std::move(end));
//     return result;
// }
// }

// WindowType toWindowType(const String & func_name)
// {
//     WindowType type = WindowType::NONE;
//     if (func_name == ProtonConsts::HOP_FUNC_NAME)
//         type = WindowType::HOP;
//     else if (func_name == ProtonConsts::TUMBLE_FUNC_NAME)
//         type = WindowType::TUMBLE;
//     else if (func_name == ProtonConsts::SESSION_FUNC_NAME)
//         type = WindowType::SESSION;

//     return type;
// }

// ALWAYS_INLINE bool isTableFunctionTumble(const ASTFunction * ast)
// {
//     assert(ast);
//     return !strcasecmp("tumble", ast->name.c_str());
// }

// ALWAYS_INLINE bool isTableFunctionHop(const ASTFunction * ast)
// {
//     assert(ast);
//     return !strcasecmp("hop", ast->name.c_str());
// }

// ALWAYS_INLINE bool isTableFunctionSession(const ASTFunction * ast)
// {
//     assert(ast);
//     return !strcasecmp("session", ast->name.c_str());
// }

// ALWAYS_INLINE bool isTableFunctionTable(const ASTFunction * ast)
// {
//     assert(ast);
//     return !strcasecmp("table", ast->name.c_str());
// }

// ALWAYS_INLINE bool isTableFunctionChangelog(const ASTFunction * ast)
// {
//     assert(ast);
//     return !strcasecmp("changelog", ast->name.c_str());
// }

// ASTs checkAndExtractTumbleArguments(const ASTFunction * func_ast)
// {
//     assert(isTableFunctionTumble(func_ast));

//     /// tumble(table, [timestamp_expr], win_interval, [timezone])
//     if (func_ast->children.size() != 1)
//         throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

//     const auto & args = func_ast->arguments->children;
//     if (args.size() < 2)
//         throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

//     if (args.size() > 4)
//         throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

//     ASTPtr table;
//     ASTPtr time_expr;
//     ASTPtr win_interval;
//     ASTPtr timezone;

//     do
//     {
//         table = args[0];

//         if (args.size() == 2)
//         {
//             /// Case: tumble(table, INTERVAL 5 SECOND)
//             if (isIntervalAST(args[1]))
//                 win_interval = args[1];
//             else
//                 break; /// throw error
//         }
//         else if (args.size() == 3)
//         {
//             if (isIntervalAST(args[1]) && isTimeZoneAST(args[2]))
//             {
//                 /// Case: tumble(table, INTERVAL 5 SECOND, timezone)
//                 win_interval = args[1];
//                 timezone = args[2];
//             }
//             else if (isTimeExprAST(args[1]) && isIntervalAST(args[2]))
//             {
//                 /// Case: tumble(table, time_column, INTERVAL 5 SECOND)
//                 time_expr = args[1];
//                 win_interval = args[2];
//             }
//             else
//                 break; /// throw error
//         }
//         else
//         {
//             assert(args.size() == 4);
//             if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isTimeZoneAST(args[3]))
//             {
//                 /// Case: tumble(table, time_expr, INTERVAL 5 SECOND, timezone)
//                 time_expr = args[1];
//                 win_interval = args[2];
//                 timezone = args[3];
//             }
//             else
//                 break; /// throw error
//         }

//         return {table, time_expr, win_interval, timezone};
//     } while (false);

//     throw Exception(TUMBLE_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
// }

// ASTs checkAndExtractHopArguments(const ASTFunction * func_ast)
// {
//     assert(isTableFunctionHop(func_ast));

//     /// hop(table, [timestamp_expr], hop_interval, win_interval, [timezone])
//     if (func_ast->children.size() != 1)
//         throw Exception(HOP_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

//     const auto & args = func_ast->arguments->children;
//     if (args.size() < 3)
//         throw Exception(HOP_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

//     if (args.size() > 5)
//         throw Exception(HOP_HELP_MESSAGE, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

//     ASTPtr table;
//     ASTPtr time_expr;
//     ASTPtr hop_interval;
//     ASTPtr win_interval;
//     ASTPtr timezone;

//     do
//     {
//         table = args[0];

//         if (args.size() == 3)
//         {
//             /// Case: hop(table, INTERVAL 5 SECOND, INTERVAL 1 MINITUE)
//             if (isIntervalAST(args[1]) && isIntervalAST(args[2]))
//             {
//                 hop_interval = args[1];
//                 win_interval = args[2];
//             }
//             else
//                 break; /// throw error
//         }
//         else if (args.size() == 4)
//         {
//             if (isIntervalAST(args[1]) && isIntervalAST(args[2]) && isTimeZoneAST(args[3]))
//             {
//                 /// Case: hop(table, INTERVAL 5 SECOND, INTERVAL 1 MINITUE, timezone)
//                 hop_interval = args[1];
//                 win_interval = args[2];
//                 timezone = args[3];
//             }
//             else if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isIntervalAST(args[3]))
//             {
//                 /// Case: hop(table, time_expr, INTERVAL 5 SECOND, INTERVAL 1 MINITUE)
//                 time_expr = args[1];
//                 hop_interval = args[2];
//                 win_interval = args[3];
//             }
//             else
//                 break; /// throw error
//         }
//         else
//         {
//             assert(args.size() == 5);
//             if (isTimeExprAST(args[1]) && isIntervalAST(args[2]) && isIntervalAST(args[3]) && isTimeZoneAST(args[4]))
//             {
//                 /// Case: hop(table, time_column, INTERVAL 5 SECOND, INTERVAL 1 MINITUE, timezone)
//                 time_expr = args[1];
//                 hop_interval = args[2];
//                 win_interval = args[3];
//                 timezone = args[4];
//             }
//             else
//                 break; /// throw error
//         }

//         return {table, time_expr, hop_interval, win_interval, timezone};
//     } while (false);

//     throw Exception(HOP_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
}

// ASTs checkAndExtractSessionArguments(const ASTFunction * func_ast)
// {
//     assert(isTableFunctionSession(func_ast));

//     /// session(stream, [timestamp_expr], timeout_interval, [max_emit_interval], [range_comparision])
//     if (func_ast->children.size() != 1)
//         throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

//     const auto & args = func_ast->arguments->children;
//     if (args.size() < 2)
//         throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

//     if (args.size() > 5)
//         throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);

//     ASTs asts;
//     ASTPtr table;
//     ASTPtr time_expr;
//     ASTPtr timeout_interval;
//     ASTPtr max_session_size;
//     ASTPtr start_condition;
//     ASTPtr start_with_inclusion;
//     ASTPtr end_condition;
//     ASTPtr end_with_inclusion;

//     do
//     {
//         table = args[0];
//         size_t i = 1;

//         /// Handle optional timestamp argument
//         if (isTimeExprAST(args[i]) && !isIntervalAST(args[i]))
//         {
//             /// Case: session(stream, timestamp, INTERVAL 5 SECOND, ...)
//             time_expr = args[i++];
//         }
//         else
//         {
//             /// Case: session(stream, INTERVAL 5 SECOND, ...)
//             time_expr = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME);
//         }

//         if (isIntervalAST(args[i]))
//         {
//             /// Case: session(stream, INTERVAL 5 SECOND...)
//             timeout_interval = args[i++];
//         }
//         else
//         {
//             /// Must contains `timeout_interval`
//             throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);
//         }

//         /// Handle optional max_session_size
//         if (i < args.size() && isIntervalAST(args[i]))
//         {
//             /// Case: session(stream, INTERVAL 5 SECOND, INTERVAL 4 HOUR)
//             /// When the timestamp of the latest event is larger than session window_start + max_session_size,
//             /// session will emit.
//             max_session_size = args[i++];
//         }
//         else
//         {
//             /// Set default max session size.
//             auto [unit_nums, unit] = extractInterval(timeout_interval->as<ASTFunction>());
//             max_session_size = makeASTInterval(unit_nums * ProtonConsts::SESSION_SIZE_MULTIPLIER, unit);
//         }

//         /// Handle optional start_condition/end_condition
//         if (i < args.size())
//         {
//             /// OPT-1: Handle range comparision
//             if (auto * range_comparision = args[i]->as<ASTSessionRangeComparision>())
//             {
//                 assert(range_comparision->children.size() == 2);
//                 start_condition = range_comparision->children[0];
//                 end_condition = range_comparision->children[1];
//                 start_with_inclusion = std::make_shared<ASTLiteral>(range_comparision->start_with_inclusion);
//                 end_with_inclusion = std::make_shared<ASTLiteral>(range_comparision->end_with_inclusion);
//                 ++i;
//             }
//             /// OPT-2: handle start/end prediction
//             else if (args[i]->as<ASTFunction>())
//             {
//                 start_condition = args[i++]; /// start_predication
//                 if (i < args.size() && args[i]->as<ASTFunction>())
//                     end_condition = args[i++]; /// end_predication
//                 else
//                     throw Exception(
//                         "session window requires both start and end predictions or none, but only start or end prediction is specified",
//                         ErrorCodes::MISSING_SESSION_KEY);

//                 start_with_inclusion = std::make_shared<ASTLiteral>(true);
//                 end_with_inclusion = std::make_shared<ASTLiteral>(true);
//             }
//         }
//         else
//         {
//             /// OPT-3: If range predication is not assigned, any incoming event should be able to start a session window.
//             start_condition = std::make_shared<ASTLiteral>(true);
//             start_with_inclusion = std::make_shared<ASTLiteral>(true);
//             end_condition = std::make_shared<ASTLiteral>(false);
//             end_with_inclusion = std::make_shared<ASTLiteral>(true);
//         }

//         if (i != args.size())
//             break;

//         /// They may be used in aggregation transform, so set an internal alias for them
//         start_condition->setAlias(ProtonConsts::STREAMING_SESSION_START);
//         end_condition->setAlias(ProtonConsts::STREAMING_SESSION_END);

//         asts.emplace_back(std::move(table));
//         asts.emplace_back(std::move(time_expr));
//         asts.emplace_back(std::move(timeout_interval));
//         asts.emplace_back(std::move(max_session_size));
//         asts.emplace_back(std::move(start_condition));
//         asts.emplace_back(std::move(start_with_inclusion));
//         asts.emplace_back(std::move(end_condition));
//         asts.emplace_back(std::move(end_with_inclusion));
//         return asts;

//     } while (false);

//     throw Exception(SESSION_HELP_MESSAGE, ErrorCodes::BAD_ARGUMENTS);
// }

void checkIntervalAST(const ASTPtr & ast, const String & msg)
{
    assert(ast);
    auto func_node = ast->as<ASTFunction>();
    if (func_node)
    {
        auto kind = mapIntervalKind(func_node->name);
        if (kind)
        {
            if (*kind <= IntervalKind::Day)
                return;
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}: the max interval kind supported is DAY.", msg);
        }
    }
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{}", msg);
}

void extractInterval(const ASTFunction * ast, Int64 & interval, IntervalKind::Kind & kind)
{
    assert(ast);

    if (auto opt_kind = mapIntervalKind(ast->name); opt_kind)
        kind = opt_kind.value();
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval function");

    const auto * val = ast->arguments ? ast->arguments->children.front()->as<ASTLiteral>() : nullptr;
    if (!val)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval argument");

    if (val->value.getType() == Field::Types::UInt64)
    {
        interval = val->value.safeGet<UInt64>();
    }
    else if (val->value.getType() == Field::Types::Int64)
    {
        interval = val->value.safeGet<Int64>();
    }
    else if (val->value.getType() == Field::Types::String)
    {
        interval = std::stoi(val->value.safeGet<String>());
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid interval argument");
}

WindowInterval extractInterval(const ASTFunction * ast)
{
    WindowInterval window_interval;
    extractInterval(ast, window_interval.interval, window_interval.unit);
    return window_interval;
}

WindowInterval extractInterval(const ColumnWithTypeAndName & interval_column)
{
    const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
    assert(interval_type);
    const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
    assert(interval_column_const_int64);
    return {interval_column_const_int64->getValue<Int64>(), interval_type->getKind()};
}

// UInt32 toStartTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
// {
//     switch (kind)
//     {
// #define CASE_WINDOW_KIND(KIND) \
//     case IntervalKind::KIND: { \
//         return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
//     }
//         CASE_WINDOW_KIND(Nanosecond)
//         CASE_WINDOW_KIND(Microsecond)
//         CASE_WINDOW_KIND(Millisecond)
//         CASE_WINDOW_KIND(Second)
//         CASE_WINDOW_KIND(Minute)
//         CASE_WINDOW_KIND(Hour)
//         CASE_WINDOW_KIND(Day)
//         CASE_WINDOW_KIND(Week)
//         CASE_WINDOW_KIND(Month)
//         CASE_WINDOW_KIND(Quarter)
//         CASE_WINDOW_KIND(Year)
// #undef CASE_WINDOW_KIND
//     }
//     __builtin_unreachable();
// }

// Int64 toStartTime(Int64 dt, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone, UInt32 time_scale)
// {
//     if (time_scale == 0)
//         return toStartTime(static_cast<UInt32>(dt), kind, num_units, time_zone);

//     switch (kind)
//     {
// #define CASE_WINDOW_KIND(KIND) \
//     case IntervalKind::KIND: { \
//         return ToStartOfTransform<IntervalKind::KIND>::execute(dt, num_units, time_zone, time_scale); \
//     }
//         CASE_WINDOW_KIND(Nanosecond)
//         CASE_WINDOW_KIND(Microsecond)
//         CASE_WINDOW_KIND(Millisecond)
//         CASE_WINDOW_KIND(Second)
//         CASE_WINDOW_KIND(Minute)
//         CASE_WINDOW_KIND(Hour)
//         CASE_WINDOW_KIND(Day)
//         CASE_WINDOW_KIND(Week)
//         CASE_WINDOW_KIND(Month)
//         CASE_WINDOW_KIND(Quarter)
//         CASE_WINDOW_KIND(Year)
// #undef CASE_WINDOW_KIND
//     }
//     __builtin_unreachable();
// }

// ALWAYS_INLINE UInt32 addTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
// {
//     switch (kind)
//     {
// #define CASE_WINDOW_KIND(KIND) \
//     case IntervalKind::KIND: { \
//         return AddTime<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
//     }
//         CASE_WINDOW_KIND(Nanosecond)
//         CASE_WINDOW_KIND(Microsecond)
//         CASE_WINDOW_KIND(Millisecond)
//         CASE_WINDOW_KIND(Second)
//         CASE_WINDOW_KIND(Minute)
//         CASE_WINDOW_KIND(Hour)
//         CASE_WINDOW_KIND(Day)
//         CASE_WINDOW_KIND(Week)
//         CASE_WINDOW_KIND(Month)
//         CASE_WINDOW_KIND(Quarter)
//         CASE_WINDOW_KIND(Year)
// #undef CASE_WINDOW_KIND
//     }
//     __builtin_unreachable();
// }

// ALWAYS_INLINE Int64 addTime(Int64 dt, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone, UInt32 time_scale)
// {
//     if (time_scale == 0)
//         return addTime(static_cast<UInt32>(dt), kind, num_units, time_zone);

//     switch (kind)
//     {
// #define CASE_WINDOW_KIND(KIND) \
//     case IntervalKind::KIND: { \
//         return AddTime<IntervalKind::KIND>::execute(dt, num_units, time_zone, time_scale); \
//     }
//         CASE_WINDOW_KIND(Nanosecond)
//         CASE_WINDOW_KIND(Microsecond)
//         CASE_WINDOW_KIND(Millisecond)
//         CASE_WINDOW_KIND(Second)
//         CASE_WINDOW_KIND(Minute)
//         CASE_WINDOW_KIND(Hour)
//         CASE_WINDOW_KIND(Day)
//         CASE_WINDOW_KIND(Week)
//         CASE_WINDOW_KIND(Month)
//         CASE_WINDOW_KIND(Quarter)
//         CASE_WINDOW_KIND(Year)
// #undef CASE_WINDOW_KIND
//     }
//     __builtin_unreachable();
// }

// ASTPtr makeASTInterval(Int64 num_units, IntervalKind kind)
// {
//     return makeASTFunction(
//         kind.toNameOfFunctionToIntervalDataType(), std::make_shared<ASTLiteral>(num_units < 0 ? Int64(num_units) : UInt64(num_units)));
// }

// ASTPtr makeASTInterval(const WindowInterval & interval)
// {
//     return makeASTInterval(interval.interval, interval.unit);
// }

// void convertToSameKindIntervalAST(const BaseScaleInterval & bs1, const BaseScaleInterval & bs2, ASTPtr & ast1, ASTPtr & ast2)
// {
//     if (bs1.src_kind < bs2.src_kind)
//         ast2 = makeASTInterval(bs2.toIntervalKind(bs1.src_kind), bs1.src_kind);
//     else if (bs1.src_kind > bs2.src_kind)
//         ast1 = makeASTInterval(bs1.toIntervalKind(bs2.src_kind), bs2.src_kind);
// }

Int64 BaseScaleInterval::toIntervalKind(IntervalKind::Kind to_kind) const
{
    if (scale == to_kind)
        return num_units;

    const auto & bs = toBaseScale(1, to_kind);
    if (scale != bs.scale)
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE,
            "Scale conversion is not possible between '{}' and '{}'",
            IntervalKind(src_kind).toString(),
            IntervalKind(to_kind).toString());

    if (num_units < bs.num_units)
        return 1;

    return num_units / bs.num_units;
}

String BaseScaleInterval::toString() const
{
    return fmt::format("{}{}", num_units, (scale == SCALE_NANOSECOND ? "ns" : "M"));
}

// UInt32 getAutoScaleByInterval(Int64 num_units, IntervalKind kind)
// {
//     if (kind >= IntervalKind::Second)
//         return 0;

//     UInt32 scale = 9;
//     if (kind == IntervalKind::Millisecond)
//         scale = 3;
//     else if (kind == IntervalKind::Microsecond)
//         scale = 6;

//     /// To reduce scale, for examples: 1000ms <=> 1s, actual scale is 0
//     int to_reduce = 1;
//     while (num_units % common::exp10_i64(to_reduce++) == 0)
//     {
//         scale -= 1;
//         if (scale == 0)
//             return 0;
//     }

//     return scale;
// }

// WindowParams::WindowParams(TableFunctionDescriptionPtr window_desc) : desc(std::move(window_desc))
// {
//     assert(desc->type != WindowType::NONE);
//     type = desc->type;
//     time_col_name = desc->argument_names[0];
//     time_col_is_datetime64 = isDateTime64(desc->argument_types[0]);
//     if (time_col_is_datetime64)
//     {
//         const auto & type = assert_cast<const DataTypeDateTime64 &>(*desc->argument_types[0].get());
//         time_scale = type.getScale();
//         time_zone = &type.getTimeZone();
//     }
//     else if (isDateTime(desc->argument_types[0]))
//     {
//         const auto & type = assert_cast<const DataTypeDateTime &>(*desc->argument_types[0].get());
//         time_scale = 0;
//         time_zone = &type.getTimeZone();
//     }
//     else
//     {
//         throw Exception(
//             ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
//             "Illegal column {} argument of function {}. Must be datetime or datetime64",
//             time_col_name,
//             magic_enum::enum_name(type));
//     }
// }

// TumbleWindowParams::TumbleWindowParams(TableFunctionDescriptionPtr window_desc) : WindowParams(std::move(window_desc))
// {
//     assert(desc->type == WindowType::TUMBLE);

//     /// __tumble(time_expr, win_interval, [timezone])
//     auto & args = desc->func_ast->as<ASTFunction &>().arguments->children;
//     assert(args.size() >= 2);
//     extractInterval(args[1]->as<ASTFunction>(), window_interval, interval_kind);

//     /// Use specified timezone
//     if (args.size() == 3)
//     {
//         if (auto * literal = args[2]->as<ASTLiteral>())
//             time_zone = &DateLUT::instance(literal->value.safeGet<String>());
//         else
//             throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only support literal timezone argument for tumble");
//     }

//     /// Validate window
//     auto window_scale = getAutoScaleByInterval(window_interval, interval_kind);
//     if (window_scale > time_scale)
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS,
//             "Invalid window interval, the window scale '{}' cannot exceed the event time scale '{}' in tumble function",
//             window_scale,
//             time_scale);

//     if ((interval_kind == IntervalKind::Millisecond && (3600 * common::exp10_i64(3)) % window_interval != 0)
//         || (interval_kind == IntervalKind::Microsecond && (3600 * common::exp10_i64(6)) % window_interval != 0)
//         || (interval_kind == IntervalKind::Nanosecond && (3600 * common::exp10_i64(9)) % window_interval != 0))
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS, "Invalid window interval, one hour must have an integer number of windows in tumble function");
// }

// HopWindowParams::HopWindowParams(TableFunctionDescriptionPtr window_desc) : WindowParams(std::move(window_desc))
// {
//     assert(desc->type == WindowType::HOP);

//     /// __hop(time_expr, hop_interval, win_interval, [timezone])
//     auto & args = desc->func_ast->as<ASTFunction &>().arguments->children;
//     assert(args.size() >= 3);

//     IntervalKind::Kind slide_interval_kind, window_interval_kind;
//     extractInterval(args[1]->as<ASTFunction>(), slide_interval, slide_interval_kind);
//     extractInterval(args[2]->as<ASTFunction>(), window_interval, window_interval_kind);

//     /// Use specified timezone
//     if (args.size() == 4)
//     {
//         if (auto * literal = args[3]->as<ASTLiteral>())
//             time_zone = &DateLUT::instance(literal->value.safeGet<String>());
//         else
//             throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only support literal timezone argument for hop");
//     }

//     /// Validate window
//     if (slide_interval_kind != window_interval_kind)
//         throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal type of window and hop column of function hop must be same");

//     interval_kind = slide_interval_kind;

//     if (slide_interval > window_interval)
//         throw Exception(ErrorCodes::BAD_ARGUMENTS, "Slide size shall be less than or equal to window size in hop function");

//     auto hop_window_scale = getAutoScaleByInterval(slide_interval, interval_kind);
//     if (hop_window_scale > time_scale)
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS,
//             "Invalid slide interval, the slide scale '{}' cannot exceed the event time scale '{}' in hop function",
//             hop_window_scale,
//             time_scale);

//     if ((interval_kind == IntervalKind::Millisecond && (3600 * common::exp10_i64(3)) % slide_interval != 0)
//         || (interval_kind == IntervalKind::Microsecond && (3600 * common::exp10_i64(6)) % slide_interval != 0)
//         || (interval_kind == IntervalKind::Nanosecond && (3600 * common::exp10_i64(9)) % slide_interval != 0))
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS, "Invalid slide interval, one hour must have an integer number of slides in hop function");

//     gcd_interval = std::gcd(slide_interval, window_interval);
// }

// SessionWindowParams::SessionWindowParams(TableFunctionDescriptionPtr window_desc) : WindowParams(std::move(window_desc))
// {
//     assert(desc->type == WindowType::SESSION);

//     /// __session(timestamp_expr, timeout_interval, max_emit_interval, start_cond, start_with_inclusion, end_cond, end_with_inclusion)
//     auto & args = desc->func_ast->as<ASTFunction &>().arguments->children;
//     IntervalKind::Kind session_timeout_kind, session_size_kind;
//     extractInterval(args[1]->as<ASTFunction>(), session_timeout, session_timeout_kind);
//     extractInterval(args[2]->as<ASTFunction>(), max_session_size, session_size_kind);
//     start_with_inclusion = args[4]->as<ASTLiteral &>().value.get<bool>();
//     end_with_inclusion = args[6]->as<ASTLiteral &>().value.get<bool>();

//     /// Validate window
//     if (session_timeout_kind != session_size_kind)
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS,
//             "Illegal type of timeout interval kind and session size interval kind of function session, must be same");

//     interval_kind = session_timeout_kind;

//     if (session_timeout > max_session_size)
//         throw Exception(
//             ErrorCodes::BAD_ARGUMENTS, "Session timeout size shall be less than or equal to max session size in session function");
// }

// WindowParamsPtr WindowParams::create(const TableFunctionDescriptionPtr & desc)
// {
//     assert(desc);
//     switch (desc->type)
//     {
//         case WindowType::TUMBLE:
//             return std::make_shared<TumbleWindowParams>(desc);
//         case WindowType::HOP:
//             return std::make_shared<HopWindowParams>(desc);
//         case WindowType::SESSION:
//             return std::make_shared<SessionWindowParams>(desc);
//         default:
//             throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No support window type: {}", magic_enum::enum_name(desc->type));
//     }
//     __builtin_unreachable();
// }

// void assignWindow(
//     Columns & columns, const WindowInterval & interval, size_t time_col_pos, bool time_col_is_datetime64, const DateLUTImpl & time_zone)
// {
//     assert(columns.size() > time_col_pos);
//     const auto & time_column = columns[time_col_pos];
//     Columns window_cols;
//     if (time_col_is_datetime64)
//     {
// #define M(INTERVAL_KIND) \
//     const auto & time_column_vec = assert_cast<const ColumnDateTime64 &>(*time_column); \
//     window_cols = getWindowStartAndEndFor<INTERVAL_KIND, ColumnDateTime64>(time_column_vec, interval.interval, time_zone);

//         DISPATCH_FOR_WINDOW_INTERVAL(interval.unit, M)
// #undef M
//     }
//     else
//     {
// #define M(INTERVAL_KIND) \
//     const auto & time_column_vec = assert_cast<const ColumnDateTime &>(*time_column); \
//     window_cols = getWindowStartAndEndFor<INTERVAL_KIND, ColumnDateTime>(time_column_vec, interval.interval, time_zone);

//         DISPATCH_FOR_WINDOW_INTERVAL(interval.unit, M)
// #undef M
//     }

//     /// Append window start/end columns
//     assert(window_cols.size() == 2);
//     columns.emplace_back(std::move(window_cols[0]));
//     columns.emplace_back(std::move(window_cols[1]));
// }

// void reassignWindow(Block & block, const Window & window)
// {
//     auto fill_time = [](ColumnWithTypeAndName & column_with_type, Int64 ts) {
//         auto column = IColumn::mutate(std::move(column_with_type.column));
//         if (isDateTime64(column_with_type.type))
//             std::ranges::fill(assert_cast<ColumnDateTime64 &>(*column).getData(), ts);
//         else
//             std::ranges::fill(assert_cast<ColumnDateTime &>(*column).getData(), static_cast<UInt32>(ts));
//         column_with_type.column = std::move(column);
//     };

//     if (auto * column_with_type = block.findByName(ProtonConsts::STREAMING_WINDOW_START))
//         fill_time(*column_with_type, window.start);

//     if (auto * column_with_type = block.findByName(ProtonConsts::STREAMING_WINDOW_END))
//         fill_time(*column_with_type, window.end);
// }

}
}
