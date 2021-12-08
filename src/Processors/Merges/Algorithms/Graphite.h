#pragma once
#include <Common/OptimizedRegularExpression.h>

namespace DB
{

class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<const IAggregateFunction>;

}

/** Intended for implementation of "rollup" - aggregation (rounding) of older data
  *  for a table with Graphite data (Graphite is the system for time series monitoring).
  *
  * Table with graphite data has at least the following columns (accurate to the name):
  * Path, Time, Value, Version
  *
  * Path - name of metric (sensor);
  * Time - time of measurement;
  * Value - value of measurement;
  * Version - a number, that for equal pairs of Path and Time, need to leave only record with maximum version.
  *
  * Each row in a table correspond to one value of one sensor.
  *
  * Pattern should contain function, retention scheme, or both of them. The order of patterns does mean as well:
  *   * Aggregation OR retention patterns should be first
  *   * Then aggregation AND retention full patterns have to be placed
  *   * default pattern without regexp must be the last
  *
  * Rollup rules are specified in the following way:
  *
  * pattern
  *     regexp
  *     function
  * pattern
  *     regexp
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     regexp
  *     function
  *     age -> precision
  *     age -> precision
  *     ...
  * pattern
  *     ...
  * default
  *     function
  *        age -> precision
  *     ...
  *
  * regexp - pattern for sensor name
  * default - if no pattern has matched
  *
  * age - minimal data age (in seconds), to start rounding with specified precision.
  * precision - rounding precision (in seconds)
  *
  * function - name of aggregate function to be applied for values, that time was rounded to same.
  *
  * Example:
  *
  * <graphite_rollup>
  *     <pattern>
  *         <regexp>\.max$</regexp>
  *         <function>max</function>
  *     </pattern>
  *     <pattern>
  *         <regexp>click_cost</regexp>
  *         <function>any</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>5</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>60</precision>
  *         </retention>
  *     </pattern>
  *     <default>
  *         <function>max</function>
  *         <retention>
  *             <age>0</age>
  *             <precision>60</precision>
  *         </retention>
  *         <retention>
  *             <age>3600</age>
  *             <precision>300</precision>
  *         </retention>
  *         <retention>
  *             <age>86400</age>
  *             <precision>3600</precision>
  *         </retention>
  *     </default>
  * </graphite_rollup>
  */
namespace DB::Graphite
{

struct Retention
{
    UInt32 age;
    UInt32 precision;
};

using Retentions = std::vector<Retention>;

struct Pattern
{
    std::shared_ptr<OptimizedRegularExpression> regexp;
    std::string regexp_str;
    AggregateFunctionPtr function;
    Retentions retentions;    /// Must be ordered by 'age' descending.
    enum { TypeUndef, TypeRetention, TypeAggregation, TypeAll } type = TypeAll; /// The type of defined pattern, filled automatically
};

using Patterns = std::vector<Pattern>;
using RetentionPattern = Pattern;
using AggregationPattern = Pattern;

struct Params
{
    String config_name;
    String path_column_name;
    String time_column_name;
    String value_column_name;
    String version_column_name;
    Graphite::Patterns patterns;
};

using RollupRule = std::pair<const RetentionPattern *, const AggregationPattern *>;

}
